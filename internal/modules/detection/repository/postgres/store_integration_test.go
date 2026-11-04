package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testDB(t *testing.T) (*pgxpool.Pool, *postgres.Store, workerfixture.Fixture) {
	t.Helper()
	base := os.Getenv("P07_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("P07_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	var suffix [8]byte
	if _, err = rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	name := "p07_store_" + hex.EncodeToString(suffix[:])
	if _, err = admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close(ctx)
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = admin.Close(context.Background())
		}
		c, e := pgx.Connect(context.Background(), base)
		if e != nil {
			t.Errorf("connect to drop owned test DB: %v", e)
			return
		}
		_, e = c.Exec(context.Background(), `DROP DATABASE `+pgx.Identifier{name}.Sanitize()+` WITH (FORCE)`)
		if e != nil {
			t.Errorf("drop owned test DB %s: %v", name, e)
		}
		_ = c.Close(context.Background())
	})
	if err = admin.Close(ctx); err != nil {
		t.Fatal(err)
	}
	closed = true
	if !strings.Contains(u.Path, "/") {
		t.Fatal("base URL must identify a maintenance database")
	}
	dbURL := *u
	dbURL.Path = "/" + name
	conn, err := pgx.Connect(ctx, dbURL.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Apply(ctx, conn, db.Files); err != nil {
		conn.Close(ctx)
		t.Fatal(err)
	}
	_ = conn.Close(ctx)
	pool, err := pgxpool.New(ctx, dbURL.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f := workerfixture.New(t, 18, 0)
	p, err := group.NewGroupProcessor(f.Bundle, f.Options)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r := postgres.Registration{Identity: snapshot.Identity, Binding: f.Options.Binding, ClusterID: "test-cluster", TopicID: "topic-incarnation-1", ConsumerGroup: "p07-test", StartOffsets: map[string]int64{}, OutputTransport: "polling"}
	for i := 0; i < 16; i++ {
		r.StartOffsets[fmt.Sprint(i)] = 0
	}
	if err = postgres.Register(ctx, pool, r); err != nil {
		t.Fatal(err)
	}
	return pool, &postgres.Store{DB: pool, Registration: r}, f
}

func completeMessages(f workerfixture.Fixture, tick int) []domain.InputMessage {
	at := f.Options.Window.Start.Add(time.Duration(tick) * domain.GridStep)
	out := make([]domain.InputMessage, 0, 19)
	for h := 1; h <= 18; h++ {
		r := workerfixture.Record("a01", h, at, float64(tick))
		out = append(out, domain.InputMessage{Version: domain.InputVersion, Type: domain.RecordMessage, Binding: f.Options.Binding, GroupID: "a01", Timestamp: at, Record: &r})
	}
	c := domain.TimestampComplete{GroupID: "a01", Timestamp: at, Records: 18, ValidRecords: 18}
	return append(out, domain.InputMessage{Version: domain.InputVersion, Type: domain.CompleteMessage, Binding: f.Options.Binding, GroupID: "a01", Timestamp: at, Completion: &c})
}

func TestStoreRegistrationIdentityAndTopicIncarnation(t *testing.T) {
	pool, s, _ := testDB(t)
	ctx := context.Background()
	if err := postgres.Register(ctx, pool, s.Registration); err != nil {
		t.Fatal(err)
	}
	bad := s.Registration
	bad.TopicID = "topic-incarnation-2"
	if err := postgres.ValidateRegistration(ctx, pool, bad); err == nil {
		t.Fatal("accepted different topic incarnation")
	}
	if err := postgres.ValidateRegistration(ctx, pool, s.Registration); err != nil {
		t.Fatal(err)
	}
}

func TestStoreCheckpointOutboxRecoveryAndLeaseFencing(t *testing.T) {
	_, s, f := testDB(t)
	ctx := context.Background()
	owner := "00000000-0000-4000-8000-000000000072"
	lease, saved, err := s.Acquire(ctx, "a01", owner, 10*time.Second)
	if err != nil || saved != nil {
		t.Fatalf("acquire: %#v %v", saved, err)
	}
	p, err := core.NewPartition(f.Bundle, f.Options, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	var tr *core.Transition
	off := int64(0)
	for tick := 0; tick < 18; tick++ {
		for _, m := range completeMessages(f, tick) {
			tr, err = p.Accept(off, m)
			off++
			if err != nil {
				t.Fatal(err)
			}
			if tr != nil {
				rev, e := s.Save(ctx, lease, *tr)
				if e != nil {
					t.Fatal(e)
				}
				if rev != tr.Checkpoint.Revision {
					t.Fatalf("revision %d", rev)
				}
				if e = p.Ack(rev); e != nil {
					t.Fatal(e)
				}
			}
		}
	}
	if tr == nil || !tr.Checkpoint.Finalized {
		t.Fatal("final tick did not finalize")
	}
	restored, err := s.Load(ctx, "a01")
	if err != nil || restored == nil || restored.NextOffset != off || restored.Revision != 18 || !restored.Finalized {
		t.Fatalf("durable checkpoint %#v: %v", restored, err)
	}
	pending, err := s.Pending(ctx, "a01")
	if err != nil || len(pending) == 0 {
		t.Fatalf("pending %d: %v", len(pending), err)
	}
	for _, e := range pending {
		payload, _ := json.Marshal(e)
		if e.MessageID == "" || strings.Contains(string(payload), "group_results") {
			t.Fatalf("bad terminal event: %#v", e)
		}
	}
	if err := s.Renew(ctx, lease, time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	newLease, _, err := s.Acquire(ctx, "a01", "00000000-0000-4000-8000-000000000073", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Renew(ctx, lease, time.Second); err == nil {
		t.Fatal("stale lease renewed")
	}
	if _, err = s.Save(ctx, lease, *tr); err == nil {
		t.Fatal("stale lease committed")
	}
	if err = s.Release(ctx, newLease); err != nil {
		t.Fatal(err)
	}
}

type commitFaultDB struct {
	postgres.DB
	after    bool
	attempts int
}
type commitFaultTx struct {
	pgx.Tx
	owner *commitFaultDB
}

func (d *commitFaultDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, err := d.DB.BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	d.attempts++
	return &commitFaultTx{Tx: tx, owner: d}, nil
}
func (t *commitFaultTx) Commit(ctx context.Context) error {
	if t.owner.after {
		if err := t.Tx.Commit(ctx); err != nil {
			return err
		}
	} else {
		_ = t.Tx.Rollback(ctx)
	}
	return fmt.Errorf("injected COMMIT connection acknowledgement failure")
}

func prepareTerminal(t *testing.T, s *postgres.Store, f workerfixture.Fixture) (postgres.Lease, *core.Partition, *core.Transition) {
	t.Helper()
	ctx := context.Background()
	lease, _, err := s.Acquire(ctx, "a01", "00000000-0000-4000-8000-000000000074", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	p, err := core.NewPartition(f.Bundle, f.Options, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	var transition *core.Transition
	offset := int64(0)
	for tick := 0; tick < 18; tick++ {
		for _, m := range completeMessages(f, tick) {
			transition, err = p.Accept(offset, m)
			offset++
			if err != nil {
				t.Fatal(err)
			}
			if transition != nil && tick < 17 {
				rev, err := s.Save(ctx, lease, *transition)
				if err != nil {
					t.Fatal(err)
				}
				if err := p.Ack(rev); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if transition == nil || len(transition.Result.Events) == 0 || !transition.Checkpoint.Finalized {
		t.Fatal("terminal fixture missing")
	}
	return lease, p, transition
}

func TestStoreCommitOutcomeReconciliation(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			_, s, f := testDB(t)
			lease, p, candidate := prepareTerminal(t, s, f)
			fault := &commitFaultDB{DB: s.DB, after: after}
			faulted := &postgres.Store{DB: fault, Registration: s.Registration}
			revision, err := faulted.Save(context.Background(), lease, *candidate)
			if fault.attempts != 1 {
				t.Fatal("blind write retry", fault.attempts)
			}
			durable, readErr := s.Load(context.Background(), "a01")
			if readErr != nil {
				t.Fatal(readErr)
			}
			outputs, readErr := s.Pending(context.Background(), "a01")
			if readErr != nil {
				t.Fatal(readErr)
			}
			if after {
				if err != nil || revision != 18 || durable.Revision != 18 || len(outputs) != len(candidate.Result.Events) {
					t.Fatal("committed outcome not reconciled", revision, durable, len(outputs), err)
				}
				if err := p.Ack(revision); err != nil {
					t.Fatal(err)
				}
			} else {
				if err == nil || durable.Revision != 17 || len(outputs) != 0 {
					t.Fatal("uncommitted state acknowledged", durable, len(outputs), err)
				}
				p.Fail(err)
				if err := p.Ack(18); err == nil {
					t.Fatal("unconfirmed RAM resumed")
				}
				// A fresh instance resumes from the exact last durable complete tick.
				restored, err := core.NewPartition(f.Bundle, f.Options, 0, durable)
				if err != nil {
					t.Fatal(err)
				}
				offset := durable.NextOffset
				for _, m := range completeMessages(f, 17) {
					tr, err := restored.Accept(offset, m)
					offset++
					if err != nil {
						t.Fatal(err)
					}
					if tr != nil {
						if tr.Checkpoint.SHA256 != candidate.Checkpoint.SHA256 {
							t.Fatal("replay changed mathematical state")
						}
						rev, err := s.Save(context.Background(), lease, *tr)
						if err != nil {
							t.Fatal(err)
						}
						if err := restored.Ack(rev); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
		})
	}
}

func TestStoreAtomicOutboxRollbackAndSnapshotIntegrity(t *testing.T) {
	pool, s, f := testDB(t)
	lease, _, candidate := prepareTerminal(t, s, f)
	bad := *candidate
	bad.Result = candidate.Result
	bad.Result.Events = append([]group.RBI2Event{}, candidate.Result.Events...)
	invalid := bad.Result.Events[0]
	invalid.KernelConfigID = strings.Repeat("0", 64)
	bad.Result.Events = append(bad.Result.Events, invalid)
	if _, err := s.Save(context.Background(), lease, bad); err == nil {
		t.Fatal("invalid late output accepted")
	}
	durable, err := s.Load(context.Background(), "a01")
	if err != nil || durable.Revision != 17 {
		t.Fatal("failed outbox batch advanced checkpoint", durable, err)
	}
	outputs, err := s.Pending(context.Background(), "a01")
	if err != nil || len(outputs) != 0 {
		t.Fatal("failed outbox batch left partial output", outputs, err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE cad_v2.group_checkpoints SET snapshot_sha256=repeat('0',64) WHERE group_id='a01'`); err == nil {
		t.Fatal("SQL exact-byte checksum invariant missing")
	}
	if _, err := pool.Exec(context.Background(), `UPDATE cad_v2.group_checkpoints SET snapshot_schema_version=99 WHERE group_id='a01'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(context.Background(), "a01"); err == nil {
		t.Fatal("unsupported snapshot version loaded")
	}
	if _, err := pool.Exec(context.Background(), `UPDATE cad_v2.group_checkpoints SET snapshot_schema_version=1 WHERE group_id='a01'`); err != nil {
		t.Fatal(err)
	}
	other := workerfixture.New(t, 18, 0)
	if _, err := core.NewPartition(other.Bundle, other.Options, 0, durable); err == nil {
		t.Fatal("foreign frozen identity restored")
	}
}

func TestStoreStaleEpochRace(t *testing.T) {
	_, s, f := testDB(t)
	ctx := context.Background()
	old, _, err := s.Acquire(ctx, "a01", "00000000-0000-4000-8000-000000000075", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	p, err := core.NewPartition(f.Bundle, f.Options, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	var tr *core.Transition
	offset := int64(0)
	for _, m := range completeMessages(f, 0) {
		tr, err = p.Accept(offset, m)
		offset++
		if err != nil {
			t.Fatal(err)
		}
	}
	rev, err := s.Save(ctx, old, *tr)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Ack(rev); err != nil {
		t.Fatal(err)
	}
	for _, m := range completeMessages(f, 1) {
		tr, err = p.Accept(offset, m)
		offset++
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(1100 * time.Millisecond)
	current, _, err := s.Acquire(ctx, "a01", "00000000-0000-4000-8000-000000000076", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if current.Epoch <= old.Epoch {
		t.Fatal("epoch did not advance")
	}
	start := make(chan struct{})
	oldResult := make(chan error, 1)
	newResult := make(chan error, 1)
	go func() { <-start; _, e := s.Save(ctx, old, *tr); oldResult <- e }()
	go func() { <-start; _, e := s.Save(ctx, current, *tr); newResult <- e }()
	close(start)
	var fenced *pgconn.PgError
	if e := <-oldResult; !errors.As(e, &fenced) || fenced.Code != "55000" {
		t.Fatal("stale epoch not fenced", e)
	}
	if e := <-newResult; e != nil {
		t.Fatal("new owner failed", e)
	}
	durable, err := s.Load(ctx, "a01")
	if err != nil || durable.Revision != 2 {
		t.Fatal(durable, err)
	}
	if err := s.Release(ctx, old); err == nil {
		t.Fatal("stale release removed new owner")
	}
}

func TestStoreRestrictedRuntimeAndMasterCompatibility(t *testing.T) {
	pool, s, f := testDB(t)
	ctx := context.Background()
	role := "p07_role_" + strings.ReplaceAll(f.Options.Binding.ExecutionID, "-", "") + fmt.Sprint(time.Now().UnixNano())
	quoted := pgx.Identifier{role}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE ROLE "+quoted+" NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, e := pool.Exec(context.Background(), "DROP OWNED BY "+quoted+"; DROP ROLE "+quoted)
		if e != nil {
			t.Error(e)
		}
	})
	grant := `GRANT USAGE ON SCHEMA cad_v2 TO ` + quoted + `; GRANT SELECT ON ALL TABLES IN SCHEMA cad_v2 TO ` + quoted + `; GRANT EXECUTE ON FUNCTION cad_v2.acquire_partition(uuid,text,uuid,interval),cad_v2.renew_partition(uuid,text,uuid,bigint,interval),cad_v2.release_partition(uuid,text,uuid,bigint),cad_v2.commit_checkpoint(uuid,text,uuid,bigint,bigint,bigint,timestamptz,integer,text,bytea,cad_v2.sha256_hex,boolean,jsonb) TO ` + quoted
	if _, err := pool.Exec(ctx, grant); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error { _, err := c.Exec(ctx, "SET ROLE "+quoted); return err }
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	restricted := &postgres.Store{DB: runtime, Registration: s.Registration}
	if err := postgres.ValidateRegistration(ctx, runtime, s.Registration); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Exec(ctx, `DELETE FROM cad_v2.outbox_messages`); err == nil {
		t.Fatal("runtime unexpectedly has DML privileges")
	}
	lease, _, transition := prepareTerminal(t, restricted, f)
	if _, err := restricted.Save(ctx, lease, *transition); err != nil {
		t.Fatal(err)
	}
	outputs, err := restricted.Pending(ctx, "a01")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range outputs {
		raw, _ := json.Marshal(e)
		if _, err := pool.Exec(ctx, `SELECT cad_v2.accept_result($1::uuid,$2::jsonb)`, s.Registration.Binding.ExecutionID, raw); err != nil {
			t.Fatal("V5 rejected terminal envelope", err)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM cad_v2.master_events").Scan(&count); err != nil || count != len(outputs) {
		t.Fatal("V5 compatibility mismatch", count, err)
	}
}
