package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kadm"
)

// These wrappers fault only the acknowledgement of a real SQL transaction.
// Acquire/renew and every query still execute against the isolated PostgreSQL.
type workerCommitFault struct {
	repo.DB
	armed atomic.Bool
	count atomic.Int32
	after bool
}

type workerFaultTx struct {
	pgx.Tx
	fault      *workerCommitFault
	checkpoint bool
}

func (d *workerCommitFault) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, err := d.DB.BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	return &workerFaultTx{Tx: tx, fault: d}, nil
}

func (tx *workerFaultTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	tx.checkpoint = strings.HasPrefix(sql, "SELECT cad_v2.commit_checkpoint(")
	return tx.Tx.QueryRow(ctx, sql, args...)
}

func (tx *workerFaultTx) Commit(ctx context.Context) error {
	if !tx.checkpoint || !tx.fault.armed.CompareAndSwap(true, false) {
		return tx.Tx.Commit(ctx)
	}
	tx.fault.count.Add(1)
	if tx.fault.after {
		if err := tx.Tx.Commit(ctx); err != nil {
			return err
		}
	} else {
		if err := tx.Tx.Rollback(ctx); err != nil {
			return err
		}
	}
	return errors.New("injected Worker COMMIT acknowledgement loss")
}

func failedWorker(t *testing.T, cancel context.CancelFunc, done <-chan error) error {
	t.Helper()
	defer cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected fail-closed error, got %v", err)
		}
		return err
	case <-time.After(25 * time.Second):
		t.Fatal("Worker failed to stop after fault")
		return nil
	}
}

func TestWorkerKafkaCheckpointFaultRecovery(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			rig := workerIntegration(t, 0)
			fault := &workerCommitFault{DB: rig.pool, after: after}
			// Install before starting goroutines; never mutate a live store.
			rig.store = &repo.Store{DB: fault, Registration: rig.store.Registration}
			_, cancel, done := rig.start(t)
			active := true
			defer func() {
				if active {
					stopWorker(t, cancel, done)
				}
			}()
			rig.sendTick(t, 0)
			rig.waitRevision(t, 1)
			fault.armed.Store(true)
			for _, m := range rig.messages("a01", 1) {
				rig.send(t, m)
			}
			if !after {
				if err := failedWorker(t, cancel, done); !strings.Contains(err.Error(), "checkpoint not confirmed") {
					t.Fatal("unexpected checkpoint failure", err)
				}
				active = false
				durable, err := rig.store.Load(context.Background(), "a01")
				if err != nil || durable == nil || durable.Revision != 1 {
					t.Fatal("failed commit advanced durable state", durable, err)
				}
				outputs, err := rig.store.Pending(context.Background(), "a01")
				if err != nil || len(outputs) != 0 {
					t.Fatal("RAM-only terminal event escaped", outputs, err)
				}
				rig.store = &repo.Store{DB: rig.pool, Registration: rig.store.Registration}
				_, restartCancel, restartDone := rig.start(t)
				defer stopWorker(t, restartCancel, restartDone)
			}
			for g := 2; g <= 16; g++ {
				for _, m := range rig.messages(fmt.Sprintf("a%02d", g), 1) {
					rig.send(t, m)
				}
			}
			rig.waitRevision(t, 2)
			for tick := 2; tick < 6; tick++ {
				rig.sendTick(t, tick)
				rig.waitRevision(t, int64(tick+1))
			}
			if fault.count.Load() != 1 {
				t.Fatal("unexpected COMMIT retries", fault.count.Load())
			}
			rig.assertEvents(t)
		})
	}
}

func TestWorkerKafkaIntegrityFailClosed(t *testing.T) {
	for _, kind := range []string{"sha", "version", "frozen_identity"} {
		t.Run(kind, func(t *testing.T) {
			rig := workerIntegration(t, 30)
			_, cancel, done := rig.start(t)
			rig.sendTick(t, 0)
			rig.waitRevision(t, 1)
			stopWorker(t, cancel, done)
			ctx := context.Background()
			original, err := rig.store.Load(ctx, "a01")
			if err != nil {
				t.Fatal(err)
			}
			var snap group.GroupProcessorSnapshot
			if err := json.Unmarshal(original.Snapshot, &snap); err != nil {
				t.Fatal(err)
			}
			version := group.MathSnapshotSchemaVersion
			switch kind {
			case "sha":
				snap.SHA256 = strings.Repeat("0", 64)
			case "version":
				version++
			case "frozen_identity":
				snap.Identity.ProfileSHA256 = strings.Repeat("0", 64)
				snap.SHA256 = ""
				unsigned, err := json.Marshal(snap)
				if err != nil {
					t.Fatal(err)
				}
				snap.SHA256 = core.SnapshotHash(unsigned)
			}
			bad, err := json.Marshal(snap)
			if err != nil {
				t.Fatal(err)
			}
			// Preserve the SQL exact-byte checksum so recovery must validate the
			// mathematical DTO as well. No production constraints are disabled.
			_, err = rig.pool.Exec(ctx, `UPDATE cad_v2.group_checkpoints SET snapshot=$1,snapshot_sha256=$2,snapshot_schema_version=$3 WHERE group_id='a01'`, bad, core.SnapshotHash(bad), version)
			if err != nil {
				t.Fatal(err)
			}
			for tick := 1; tick < 6; tick++ {
				rig.sendTick(t, tick)
			}
			_, cancel, done = rig.start(t)
			failure := failedWorker(t, cancel, done)
			if !strings.Contains(failure.Error(), map[string]string{"sha": "SHA256", "version": "schema/codec", "frozen_identity": "identity"}[kind]) {
				t.Fatal("unexpected recovery failure", failure)
			}
			var revision int64
			if err := rig.pool.QueryRow(ctx, `SELECT checkpoint_revision FROM cad_v2.group_checkpoints WHERE group_id='a01'`).Scan(&revision); err != nil || revision != 1 {
				t.Fatal("corrupt group advanced", revision, err)
			}
			_, err = rig.pool.Exec(ctx, `UPDATE cad_v2.group_checkpoints SET snapshot=$1,snapshot_sha256=$2,snapshot_schema_version=$3 WHERE group_id='a01'`, original.Snapshot, original.SHA256, group.MathSnapshotSchemaVersion)
			if err != nil {
				t.Fatal(err)
			}
			_, cancel, done = rig.start(t)
			defer stopWorker(t, cancel, done)
			rig.waitRevision(t, 6)
			rig.assertEvents(t)
		})
	}
}

func TestWorkerKafkaLeaseLossStopsRuntime(t *testing.T) {
	rig := workerIntegration(t, 30)
	a, cancel, done := rig.start(t)
	rig.sendTick(t, 0)
	rig.waitRevision(t, 1)
	a.mu.Lock()
	old := a.parts[0].lease
	a.mu.Unlock()
	// Inject an expired SQL lease; acquire uses the existing epoch mechanism.
	ctx := context.Background()
	if _, err := rig.pool.Exec(ctx, `UPDATE cad_v2.partition_ownership SET lease_until=clock_timestamp()-interval '1 second' WHERE group_id='a01'`); err != nil {
		t.Fatal(err)
	}
	owner, err := NewOwnerID()
	if err != nil {
		t.Fatal(err)
	}
	newLease, _, err := rig.store.Acquire(ctx, "a01", owner, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newLease.Epoch <= old.Epoch {
		t.Fatal("epoch did not advance")
	}
	var fenced *pgconn.PgError
	if err := failedWorker(t, cancel, done); !errors.As(err, &fenced) || fenced.Code != "55000" {
		t.Fatal("runtime did not report SQL fencing failure", err)
	}
	if err := rig.store.Renew(ctx, old, 3*time.Second); err == nil {
		t.Fatal("old runtime renewed after takeover")
	}
	if err := rig.store.Release(ctx, newLease); err != nil {
		t.Fatal(err)
	}
	_, cancel, done = rig.start(t)
	defer stopWorker(t, cancel, done)
	for tick := 1; tick < 6; tick++ {
		rig.sendTick(t, tick)
		rig.waitRevision(t, int64(tick+1))
	}
	rig.assertEvents(t)
}

func TestWorkerKafkaSourceMismatchAndTailLoss(t *testing.T) {
	for _, kind := range []string{"topic_identity", "retention"} {
		t.Run(kind, func(t *testing.T) {
			rig := workerIntegration(t, 30)
			_, cancel, done := rig.start(t)
			rig.sendTick(t, 0)
			rig.waitRevision(t, 1)
			stopWorker(t, cancel, done)
			originalTopic := rig.store.Registration.TopicID
			if kind == "topic_identity" {
				rig.store.Registration.TopicID = "different-incarnation"
			} else {
				// This test requires a dedicated broker explicitly acknowledged by
				// P07_TEST_KAFKA_ISOLATED. Delete only a01's test-owned prefix.
				for _, m := range rig.messages("a01", 1) {
					rig.send(t, m)
				}
				end := rig.store.Registration.StartOffsets["0"] + 38
				ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
				defer done()
				result, err := kadm.NewClient(rig.producer).DeleteRecords(ctx, kadm.Offsets{"telemetry.raw": {0: {Topic: "telemetry.raw", Partition: 0, At: end, LeaderEpoch: -1}}})
				if err != nil {
					t.Fatal(err)
				}
				if err := result.Error(); err != nil {
					t.Fatal(err)
				}
			}
			_, cancel, done = rig.start(t)
			err := failedWorker(t, cancel, done)
			if !strings.Contains(err.Error(), map[string]string{"topic_identity": "incarnation", "retention": "outside"}[kind]) {
				t.Fatal(err)
			}
			if kind == "topic_identity" {
				rig.store.Registration.TopicID = originalTopic
				_, cancel, done = rig.start(t)
				defer stopWorker(t, cancel, done)
				for tick := 1; tick < 6; tick++ {
					rig.sendTick(t, tick)
					rig.waitRevision(t, int64(tick+1))
				}
				rig.assertEvents(t)
			}
		})
	}
}
