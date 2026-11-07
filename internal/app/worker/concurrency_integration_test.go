package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type saveBarrierKey struct{}
type saveBarrier struct {
	once           sync.Once
	ready, release chan struct{}
}

func (b *saveBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, saveBarrierKey{}, strings.HasPrefix(d.SQL, "SELECT cad_v2.commit_checkpoint(") && len(d.Args) > 1 && d.Args[1] == "a01")
}
func (b *saveBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	if match, _ := ctx.Value(saveBarrierKey{}).(bool); match && d.Err == nil {
		b.once.Do(func() {
			close(b.ready)
			select {
			case <-b.release:
			case <-ctx.Done():
			}
		})
	}
}

func TestWorkerIndependentRenewalAndRetirementDuringSave(t *testing.T) {
	for _, retirement := range []string{"release", "revoke", "close"} {
		t.Run(retirement, func(t *testing.T) {
			admin := postgrestest.New(t)
			fixture := workerfixture.New(t, 6, 30)
			processor, err := group.NewGroupProcessor(fixture.Bundle, fixture.Options)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := processor.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			registration := repo.Registration{Identity: snapshot.Identity, Binding: fixture.Options.Binding, ClusterID: "p15-worker", TopicID: "AAAAAAAAAAAAAAAAAAAAAA==", ConsumerGroup: "p15-worker", OutputTransport: "polling", Diagnostics: true, CheckpointIntervalTicks: 1, StartOffsets: map[string]int64{}}
			for p := 0; p < 16; p++ {
				registration.StartOffsets[fmt.Sprint(p)] = 0
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := repo.Register(ctx, admin, registration); err != nil {
				t.Fatal(err)
			}
			barrier := &saveBarrier{ready: make(chan struct{}), release: make(chan struct{})}
			cfg := admin.Config()
			cfg.MaxConns = 8
			cfg.ConnConfig.Tracer = barrier
			runtime, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close()
			var release sync.Once
			defer release.Do(func() { close(barrier.release) })
			store := &repo.Store{DB: runtime, Registration: registration}
			runner, err := New(Config{Bundle: fixture.Bundle, Options: fixture.Options, Broker: kafkainput.Broker{Seeds: []string{"unused:9092"}}, ConsumerGroup: registration.ConsumerGroup, LeaseTTL: 15 * time.Second, Logger: discardLog()}, store)
			if err != nil {
				t.Fatal(err)
			}
			defer runner.close()
			ids := make([]kafkainput.Identity, 4)
			for p := range ids {
				ids[p] = kafkainput.Identity{Topic: kafkainput.Topic, TopicID: registration.TopicID, Partition: int32(p), GroupID: fmt.Sprintf("a%02d", p+1), Binding: fixture.Options.Binding, Window: domain.InputWindow{Start: fixture.Options.Window.Start, End: fixture.Options.Window.End}}
			}
			if err := runner.assignedHook(ctx, ids); err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				if _, err := runner.load(ctx, id); err != nil {
					t.Fatal(err)
				}
			}
			var prior time.Time
			if err := admin.QueryRow(ctx, `SELECT max(lease_until) FROM cad_v2.partition_ownership WHERE group_id<>'a01'`).Scan(&prior); err != nil {
				t.Fatal(err)
			}
			saved := make(chan error, 1)
			go func() {
				rig := workerRig{fixture: fixture}
				for offset, m := range rig.messages("a01", 0) {
					if err := runner.handle(ctx, &kgo.Record{Topic: kafkainput.Topic, Partition: 0, Offset: int64(offset)}, m); err != nil {
						saved <- err
						return
					}
				}
				saved <- nil
			}()
			select {
			case <-barrier.ready:
			case err := <-saved:
				t.Fatal("Save barrier not reached", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			renewed := make(chan error, 1)
			go func() { renewed <- runner.renew(ctx) }()
			// All three independent ownership rows must renew while a01's SQL TX
			// holds its own ownership row and its processor mutex.
			waitWorker(t, 3*time.Second, func() bool {
				var n int
				err := admin.QueryRow(ctx, `SELECT count(*) FROM cad_v2.partition_ownership WHERE group_id<>'a01' AND lease_until>$1`, prior).Scan(&n)
				return err == nil && n == 3
			})
			progress := make(chan struct{})
			go func() { runner.fetchProgress(1, 100); close(progress) }()
			select {
			case <-progress:
			case <-ctx.Done():
				t.Fatal("registry blocked by Save")
			}
			if retirement == "release" {
				release.Do(func() { close(barrier.release) })
				if err := <-saved; err != nil {
					t.Fatal(err)
				}
				if err := <-renewed; err != nil {
					t.Fatal(err)
				}
				var cp int
				if err := admin.QueryRow(ctx, `SELECT count(*) FROM cad_v2.group_checkpoints`).Scan(&cp); err != nil || cp != 1 {
					t.Fatal(cp, err)
				}
				return
			}
			runner.mu.Lock()
			old := runner.parts[0]
			lease := old.lease
			runner.mu.Unlock()
			retired := make(chan error, 1)
			go func() {
				if retirement == "close" {
					retired <- runner.close()
				} else {
					retired <- runner.revokeHook(ctx, ids[:1])
				}
			}()
			select {
			case err := <-saved:
				if err == nil {
					t.Fatal("retired Save reported RAM ACK")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case err := <-retired:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("retirement did not join Save")
			}
			if err := <-renewed; err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			var cp int
			if err := admin.QueryRow(ctx, `SELECT count(*) FROM cad_v2.group_checkpoints`).Scan(&cp); err != nil || cp != 0 {
				t.Fatal("retired TX leaked checkpoint", cp, err)
			}
			if err := store.Renew(ctx, lease, 15*time.Second); !stale(err) {
				t.Fatal("retired owner still authorized", err)
			}
			// Failed processor state cannot acknowledge an unconfirmed checkpoint.
			if err := old.core.Ack(1); err == nil {
				t.Fatal("retired core accepted stale ACK")
			}
		})
	}
}
