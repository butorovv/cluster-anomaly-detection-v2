package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"testing"
	"time"
)

func diagnosticCandidate(t *testing.T) (*pgxpool.Pool, *postgres.Store, postgres.Lease, *core.Partition, *core.Transition, workerfixture.Fixture) {
	t.Helper()
	pool := postgrestest.New(t)
	fixture := workerfixture.New(t, 1, 0)
	partition, err := core.NewPartition(fixture.Bundle, fixture.Options, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	var candidate *core.Transition
	for h := 1; h <= 18; h++ {
		record := workerfixture.Record("a01", h, fixture.Options.Window.Start, 2)
		message := domain.InputMessage{Version: domain.InputVersion, Type: domain.RecordMessage, Binding: fixture.Options.Binding, GroupID: "a01", Timestamp: record.Timestamp, Record: &record}
		if _, err := partition.Accept(int64(h-1), message); err != nil {
			t.Fatal(err)
		}
	}
	completion := domain.TimestampComplete{GroupID: "a01", Timestamp: fixture.Options.Window.Start, Records: 18, ValidRecords: 18}
	candidate, err = partition.Accept(18, domain.InputMessage{Version: domain.InputVersion, Type: domain.CompleteMessage, Binding: fixture.Options.Binding, GroupID: "a01", Timestamp: completion.Timestamp, Completion: &completion})
	if err != nil {
		t.Fatal(err)
	}
	var snapshot group.GroupProcessorSnapshot
	if err := json.Unmarshal(candidate.Checkpoint.Snapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	r := postgres.Registration{Identity: snapshot.Identity, Binding: fixture.Options.Binding, ClusterID: "diagnostic", TopicID: "diagnostic-topic", ConsumerGroup: "diagnostic", OutputTransport: "http_push", Diagnostics: true, StartOffsets: map[string]int64{}}
	for p := 0; p < 16; p++ {
		r.StartOffsets[fmt.Sprint(p)] = 0
	}
	if err := postgres.Register(context.Background(), pool, r); err != nil {
		t.Fatal(err)
	}
	store := &postgres.Store{DB: pool, Registration: r}
	lease, _, err := store.Acquire(context.Background(), "a01", "00000000-0000-4000-8000-000000000099", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return pool, store, lease, partition, candidate, fixture
}
func TestDiagnosticCheckpointAtomicBarrier(t *testing.T) {
	pool, store, lease, partition, candidate, _ := diagnosticCandidate(t)
	ctx := context.Background()
	role := fmt.Sprintf("p10_atomic_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{role}.Sanitize()
	// commit_checkpoint is permitted, but the later fragment write is denied. The
	// SQL error must roll back the already-written checkpoint and terminal event.
	if _, err := pool.Exec(ctx, `CREATE ROLE `+quoted+` NOLOGIN;GRANT USAGE ON SCHEMA cad_v2 TO `+quoted+`;GRANT SELECT ON ALL TABLES IN SCHEMA cad_v2 TO `+quoted+`;GRANT EXECUTE ON FUNCTION cad_v2.commit_checkpoint(uuid,text,uuid,bigint,bigint,bigint,timestamptz,integer,text,bytea,cad_v2.sha256_hex,boolean,jsonb) TO `+quoted); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error { _, err := c.Exec(ctx, "SET ROLE "+quoted); return err }
	restricted, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		restricted.Close()
		if _, err := pool.Exec(context.Background(), "DROP OWNED BY "+quoted+";DROP ROLE "+quoted); err != nil {
			t.Error(err)
		}
	})
	if _, err := (&postgres.Store{DB: restricted, Registration: store.Registration}).Save(ctx, lease, *candidate); err == nil {
		t.Fatal("partial diagnostic transaction committed")
	}
	var checkpoints, outbox, fragments int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM cad_v2.group_checkpoints),(SELECT count(*) FROM cad_v2.outbox_messages),(SELECT count(*) FROM cad_v2.worker_tick_fragments)`).Scan(&checkpoints, &outbox, &fragments); err != nil || checkpoints != 0 || outbox != 0 || fragments != 0 {
		t.Fatal("partial durable state", checkpoints, outbox, fragments, err)
	}
	partition.Fail(fmt.Errorf("unconfirmed diagnostic barrier"))
	if err := partition.Ack(1); err == nil {
		t.Fatal("unconfirmed RAM resumed")
	}
}
func TestDiagnosticCommitOutcomeReconcilesAllOutputs(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			pool, store, lease, partition, candidate, _ := diagnosticCandidate(t)
			fault := &commitFaultDB{DB: pool, after: after}
			revision, err := (&postgres.Store{DB: fault, Registration: store.Registration}).Save(context.Background(), lease, *candidate)
			var fragments int
			if readErr := pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.worker_tick_fragments`).Scan(&fragments); readErr != nil {
				t.Fatal(readErr)
			}
			pending, readErr := store.Pending(context.Background(), "a01")
			if readErr != nil {
				t.Fatal(readErr)
			}
			if after {
				if err != nil || revision != 1 || fragments != 1 || len(pending) != 1 {
					t.Fatal("committed tuple not reconciled", revision, fragments, len(pending), err)
				}
				if err := partition.Ack(revision); err != nil {
					t.Fatal(err)
				}
			} else {
				if err == nil || fragments != 0 || len(pending) != 0 {
					t.Fatal("RAM fragment/event escaped", fragments, len(pending), err)
				}
				partition.Fail(err)
				if err := partition.Ack(1); err == nil {
					t.Fatal("failed state ACK")
				}
			}
			if fault.attempts != 1 {
				t.Fatal("blind COMMIT retry", fault.attempts)
			}
		})
	}
}

func TestDiagnosticMissingCheckpointFailsClosed(t *testing.T) {
	pool, store, lease, _, candidate, _ := diagnosticCandidate(t)
	ctx := context.Background()
	if _, err := store.Save(ctx, lease, *candidate); err != nil {
		t.Fatal(err)
	}
	// Model an administrative loss of the checkpoint while durable diagnostic rows
	// remain. No fresh Journal is permitted even when no terminal parent exists.
	if _, err := pool.Exec(ctx, `DELETE FROM cad_v2.outbox_messages;DELETE FROM cad_v2.group_checkpoints`); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if _, saved, err := store.Acquire(ctx, "a01", "00000000-0000-4000-8000-000000000098", 15*time.Second); err == nil {
		t.Fatal("missing checkpoint silently initialized", saved)
	}
}
