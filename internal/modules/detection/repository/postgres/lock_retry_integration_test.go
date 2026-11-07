package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type stageObservedDB struct {
	postgres.DB
	begins  int
	onError func(error)
}
type stageObservedTx struct {
	pgx.Tx
	db *stageObservedDB
}

func (d *stageObservedDB) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	d.begins++
	tx, err := d.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &stageObservedTx{Tx: tx, db: d}, nil
}
func (tx *stageObservedTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tag, err := tx.Tx.Exec(ctx, sql, args...)
	if err != nil && strings.Contains(sql, "stage_tick_fragment") && tx.db.onError != nil {
		tx.db.onError(err)
	}
	return tag, err
}

// Reproduce the historical failure at the actual SQL stage, after checkpoint
// and terminal outbox writes, with an independently held PostgreSQL row lock.
func TestStoreRetriesRealFragmentLockTimeout(t *testing.T) {
	pool, store, lease, partition, candidate, _ := diagnosticCandidate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := pool.Exec(ctx, `CREATE TABLE public.retry_block(id int PRIMARY KEY, n int); INSERT INTO public.retry_block VALUES(1,0);
 CREATE FUNCTION public.retry_block_fragment() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE public.retry_block SET n=n+1 WHERE id=1; RETURN NEW; END $$;
 CREATE TRIGGER retry_block_fragment BEFORE INSERT ON cad_v2.worker_tick_fragments FOR EACH ROW EXECUTE FUNCTION public.retry_block_fragment()`)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(ctx, `UPDATE public.retry_block SET n=n+1 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, e := c.Exec(ctx, `SET lock_timeout='35ms'; SET statement_timeout='5000ms'`)
		return e
	}
	limited, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	observed := &stageObservedDB{DB: limited}
	failures := 0
	observed.onError = func(err error) {
		failures++
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "55P03" {
			t.Errorf("expected actual lock_timeout 55P03, got %v", err)
		}
		durable, e := store.Load(ctx, "a01")
		if e != nil || durable != nil {
			t.Errorf("uncommitted checkpoint visible: %#v %v", durable, e)
		}
		var outbox, fragments int
		e = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM cad_v2.outbox_messages),(SELECT count(*) FROM cad_v2.worker_tick_fragments)`).Scan(&outbox, &fragments)
		if e != nil || outbox != 0 || fragments != 0 {
			t.Errorf("uncommitted outputs visible: %d/%d %v", outbox, fragments, e)
		}
		if e = blocker.Rollback(ctx); e != nil {
			t.Error(e)
		}
	}
	retrying := &postgres.Store{DB: observed, Registration: store.Registration}
	revision, err := retrying.Save(ctx, lease, *candidate)
	if err != nil || revision != 1 || observed.begins != 2 || failures != 1 {
		t.Fatalf("recovery revision=%d attempts=%d failures=%d err=%v", revision, observed.begins, failures, err)
	}
	if err = partition.Ack(revision); err != nil {
		t.Fatal(err)
	}
	durable, err := store.Load(ctx, "a01")
	if err != nil || durable == nil || durable.SHA256 != candidate.Checkpoint.SHA256 || durable.NextOffset != candidate.Checkpoint.NextOffset {
		t.Fatalf("checkpoint changed: %#v %v", durable, err)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.worker_tick_fragments`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("fragments=%d err=%v", count, err)
	}
	outputs, err := store.Pending(ctx, "a01")
	if err != nil || len(outputs) != len(candidate.Result.Events) {
		t.Fatalf("outbox=%d err=%v", len(outputs), err)
	}
	t.Log("SQLSTATE=55P03 stage_tick_fragment rollback/retry confirmed; exact checkpoint and one durable output set")
}

func TestStoreFencingFailureDoesNotRetry(t *testing.T) {
	pool, store, lease, _, candidate, _ := diagnosticCandidate(t)
	ctx := context.Background()
	if err := store.Release(ctx, lease); err != nil {
		t.Fatal(err)
	}
	observed := &stageObservedDB{DB: pool}
	_, err := (&postgres.Store{DB: observed, Registration: store.Registration}).Save(ctx, lease, *candidate)
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "55000" || observed.begins != 1 {
		t.Fatalf("fencing attempts=%d err=%v", observed.begins, err)
	}
	durable, err := store.Load(ctx, "a01")
	if err != nil || durable != nil {
		t.Fatalf("stale owner persisted checkpoint: %#v %v", durable, err)
	}
}
