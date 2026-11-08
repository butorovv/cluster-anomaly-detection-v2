package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type payloadSQLSender struct {
	raw []byte
	err error
}

func (s *payloadSQLSender) Send(_ context.Context, raw []byte, _ string) error {
	if !bytes.Equal(s.raw, raw) {
		return domain.PermanentDelivery(errors.New("pending bytes changed"))
	}
	return s.err
}
func (*payloadSQLSender) Close() error { return nil }

// Real PostgreSQL Page / dispatcher-restart / ACK regression. The row is an
// administrative synthetic transport fixture, not a valid scientific result;
// this does not claim compute->Save atomicity, a real Worker restart or Kafka.
func TestPayloadSQLPendingDispatcherRestart(t *testing.T) {
	for _, size := range []int{1 << 20, (1 << 20) + 1, domain.MaxMessageBytes, domain.MaxMessageBytes + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			r := newTimestampRig(t, 4, 0, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			// The shared timestamp rig deliberately defaults to REPEATABLE READ
			// for Master transaction tests. V10 Page requires standalone READ
			// COMMITTED; preserve the runtime role and all existing timeouts.
			cfg := r.worker.Config()
			setup := cfg.AfterConnect
			cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
				if setup != nil {
					if err := setup(ctx, c); err != nil {
						return err
					}
				}
				_, err := c.Exec(ctx, "SET default_transaction_isolation='read committed'")
				return err
			}
			worker, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(worker.Close)
			e := r.fragments[0][0]
			e.SchemaVersion = 1
			e.Kind = "group_result"
			e.Terminal = true
			e.Payload = json.RawMessage(`{"padding":""}`)
			raw, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			var canonicalSize int
			if err := r.admin.QueryRow(ctx, `SELECT octet_length($1::jsonb::text)`, raw).Scan(&canonicalSize); err != nil {
				t.Fatal(err)
			}
			e.Payload = json.RawMessage(`{"padding":"` + string(bytes.Repeat([]byte{'x'}, size-canonicalSize)) + `"}`)
			raw, err = json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			var saved []byte
			err = r.admin.QueryRow(ctx, `INSERT INTO cad_v2.outbox_messages(execution_id,group_id,message_id,checkpoint_revision,ordinal,kind,revision,event_time,terminal,envelope)
   VALUES($1::uuid,'a01',$2,1,1,'group_result',1,$3,true,$4::jsonb) RETURNING envelope`, e.ExecutionID, e.MessageID, e.EventTime, raw).Scan(&saved)
			if err != nil || len(saved) != size {
				t.Fatal("synthetic row size", len(saved), size, err)
			}
			owner := domain.Owner{ExecutionID: e.ExecutionID, GroupID: "a01", OwnerID: "123e4567-e89b-12d3-a456-426614174001"}
			if err := worker.QueryRow(ctx, `SELECT cad_v2.acquire_partition($1::uuid,$2,$3::uuid,'30 seconds')`, owner.ExecutionID, owner.GroupID, owner.OwnerID).Scan(&owner.Epoch); err != nil {
				t.Fatal(err)
			}
			store := repo.OutboxStore{DB: worker}
			items, err := store.Page(ctx, owner, domain.Cursor{}, 64, true)
			if size > domain.MaxMessageBytes {
				if !domain.IsPermanentDelivery(err) {
					t.Fatal("oversize Page must stop", err)
				}
			} else if err != nil || len(items) != 1 || !bytes.Equal(items[0].Envelope, saved) {
				t.Fatal("page", err)
			}
			sender := &payloadSQLSender{raw: saved, err: domain.PermanentDelivery(errors.New("synthetic MESSAGE_TOO_LARGE"))}
			first := &usecase.Dispatcher{ExecutionID: e.ExecutionID, Mode: "kafka", Store: store, Sender: sender}
			if err := first.Track(ctx, owner); err != nil {
				t.Fatal(err)
			}
			if err := first.Run(ctx); !domain.IsPermanentDelivery(err) {
				t.Fatal("permanent result", err)
			}
			var pending bool
			if err := r.admin.QueryRow(ctx, `SELECT delivered_at IS NULL FROM cad_v2.outbox_messages WHERE execution_id=$1::uuid AND message_id=$2`, e.ExecutionID, e.MessageID).Scan(&pending); err != nil || !pending {
				t.Fatal("false SQL ACK", pending, err)
			}
			if size > domain.MaxMessageBytes {
				return
			} // Remains pending; cannot send outside contract.
			second := &usecase.Dispatcher{ExecutionID: e.ExecutionID, Mode: "kafka", Store: repo.OutboxStore{DB: worker}, Sender: &payloadSQLSender{raw: saved}}
			if err := second.Track(ctx, owner); err != nil {
				t.Fatal(err)
			}
			runCtx, stop := context.WithCancel(ctx)
			defer stop()
			done := make(chan error, 1)
			go func() { done <- second.Run(runCtx) }()
			defer func() {
				stop()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			for pending && ctx.Err() == nil {
				if err := r.admin.QueryRow(ctx, `SELECT delivered_at IS NULL FROM cad_v2.outbox_messages WHERE execution_id=$1::uuid AND message_id=$2`, e.ExecutionID, e.MessageID).Scan(&pending); err != nil {
					t.Fatal(err)
				}
				if pending {
					time.Sleep(10 * time.Millisecond)
				}
			}
			if pending {
				t.Fatal("pending replay not ACKed", ctx.Err())
			}
			var after []byte
			if err := r.admin.QueryRow(ctx, `SELECT envelope FROM cad_v2.outbox_messages WHERE execution_id=$1::uuid AND message_id=$2`, e.ExecutionID, e.MessageID).Scan(&after); err != nil || !bytes.Equal(saved, after) {
				t.Fatal("SQL bytes changed", err)
			}
		})
	}
}
