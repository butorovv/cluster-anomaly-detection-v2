package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func timeoutPool(t *testing.T, pool *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	cfg := pool.Config()
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `SET statement_timeout='35ms'`)
		return err
	}
	out, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(out.Close)
	return out
}

func installFragmentDelay(t *testing.T, pool *pgxpool.Pool, sleep string) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `CREATE SEQUENCE retry_fragment_calls;
CREATE FUNCTION retry_fragment_delay() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF nextval('public.retry_fragment_calls') <= 1 THEN PERFORM pg_sleep(`+sleep+`); END IF; RETURN NEW; END $$;
CREATE TRIGGER retry_fragment_delay BEFORE INSERT ON cad_v2.worker_tick_fragments FOR EACH ROW EXECUTE FUNCTION retry_fragment_delay()`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStoreRetriesRealStatementTimeoutBeforeFragmentStage(t *testing.T) {
	pool, store, lease, partition, candidate, _ := diagnosticCandidate(t)
	installFragmentDelay(t, pool, "0.2")
	retrying := &postgres.Store{DB: timeoutPool(t, pool), Registration: store.Registration}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	revision, err := retrying.Save(ctx, lease, *candidate)
	if err != nil || revision != 1 {
		t.Fatalf("retry Save revision=%d err=%v", revision, err)
	}
	var attempts int64
	if err := pool.QueryRow(ctx, `SELECT last_value FROM retry_fragment_calls`).Scan(&attempts); err != nil || attempts < 2 {
		t.Fatalf("expected a failed SQL attempt and retry, attempts=%d err=%v", attempts, err)
	}
	durable, err := store.Load(ctx, "a01")
	if err != nil || durable == nil || durable.Revision != 1 || durable.NextOffset != candidate.Checkpoint.NextOffset || string(durable.Snapshot) != string(candidate.Checkpoint.Snapshot) {
		t.Fatalf("checkpoint mismatch: %#v err=%v", durable, err)
	}
	if err := partition.Ack(revision); err != nil {
		t.Fatal(err)
	}
	var fragments int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.worker_tick_fragments`).Scan(&fragments); err != nil || fragments != 1 {
		t.Fatalf("fragment count=%d err=%v", fragments, err)
	}
	pending, err := store.Pending(ctx, "a01")
	if err != nil || len(pending) != len(candidate.Result.Events) {
		t.Fatalf("events=%d want %d err=%v", len(pending), len(candidate.Result.Events), err)
	}
}

func TestStoreStatementTimeoutExhaustionRollsBack(t *testing.T) {
	pool, store, lease, _, candidate, _ := diagnosticCandidate(t)
	installFragmentDelay(t, pool, "0.2")
	// Every invocation sleeps, so each SQL statement is cancelled by PostgreSQL.
	_, err := pool.Exec(context.Background(), `CREATE OR REPLACE FUNCTION retry_fragment_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('public.retry_fragment_calls'); PERFORM pg_sleep(0.2); RETURN NEW; END $$`)
	if err != nil {
		t.Fatal(err)
	}
	retrying := &postgres.Store{DB: timeoutPool(t, pool), Registration: store.Registration}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = retrying.Save(ctx, lease, *candidate)
	if err == nil {
		t.Fatal("expected exhausted SQL timeout")
	}
	var attempts int64
	if queryErr := pool.QueryRow(ctx, `SELECT last_value FROM retry_fragment_calls`).Scan(&attempts); queryErr != nil || attempts != 3 {
		t.Fatalf("expected exactly three failed SQL attempts, got %d (%v), Save error %v", attempts, queryErr, err)
	}
	durable, readErr := store.Load(context.Background(), "a01")
	var fragments int
	countErr := pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.worker_tick_fragments`).Scan(&fragments)
	if readErr != nil || durable != nil || countErr != nil || fragments != 0 {
		t.Fatalf("partial state checkpoint=%#v fragments=%d errors=%v/%v", durable, fragments, readErr, countErr)
	}
}

func TestStoreRetryWaitHonorsCallerDeadline(t *testing.T) {
	pool, store, lease, _, candidate, _ := diagnosticCandidate(t)
	installFragmentDelay(t, pool, "0.2")
	// The first statement times out, while the caller deadline expires during the retry pause.
	retrying := &postgres.Store{DB: timeoutPool(t, pool), Registration: store.Registration}
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()
	_, err := retrying.Save(ctx, lease, *candidate)
	if err == nil || ctx.Err() == nil {
		t.Fatalf("deadline not preserved: %v ctx=%v", err, ctx.Err())
	}
	durable, readErr := store.Load(context.Background(), "a01")
	if readErr != nil || durable != nil {
		t.Fatalf("deadline attempt persisted checkpoint %#v err=%v", durable, readErr)
	}
}
