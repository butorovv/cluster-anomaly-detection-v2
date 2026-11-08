package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const ownershipExecution = "00000000-0000-0000-0000-000000000001"
const ownershipOwner = "00000000-0000-0000-0000-000000000011"

func ownershipSeries(t *testing.T, version int) fstest.MapFS {
	t.Helper()
	series, err := readSeries(Files)
	if err != nil {
		t.Fatal(err)
	}
	files := fstest.MapFS{}
	for _, m := range series {
		if m.version <= version {
			files["migrations/"+m.name] = &fstest.MapFile{Data: []byte(m.sql)}
		}
	}
	return files
}

func ownershipFixture(t *testing.T, c *pgx.Conn, nonempty bool) {
	t.Helper()
	fixture, err := os.ReadFile("tests/fixtures.sql")
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, c, string(fixture))
	execSQL(t, c, `SELECT cad_v2.acquire_partition('`+ownershipExecution+`','a01','`+ownershipOwner+`',interval '5 minutes')`)
	if nonempty {
		execSQL(t, c, `SELECT p01_test.commit(0,1501,'2022-01-18 08:00Z',jsonb_build_array(p01_test.message()))`)
	}
}

func ownershipRenew(ctx context.Context, c *pgx.Conn) error {
	_, err := c.Exec(ctx, `SELECT cad_v2.renew_partition($1::uuid,'a01',$2::uuid,1,interval '5 minutes')`, ownershipExecution, ownershipOwner)
	return err
}

func ownershipPage(ctx context.Context, tx pgx.Tx, function string) (int, error) {
	// The function is a test-controlled identifier, never an external parameter.
	var count int
	err := tx.QueryRow(ctx, `SELECT count(*) FROM cad_v2.`+function+`($1::uuid,'a01',$2::uuid,1,0,0,64,true)`, ownershipExecution, ownershipOwner).Scan(&count)
	return count, err
}

func ownershipLockTrace(t *testing.T, ctx context.Context, c *pgx.Conn, reader, renewer uint32) {
	t.Helper()
	var trace string
	if err := c.QueryRow(ctx, `SELECT coalesce(jsonb_agg(jsonb_build_object('pid',pid,'wait_event_type',wait_event_type,'wait_event',wait_event,'query',query,'blocking_pids',pg_blocking_pids(pid)) ORDER BY pid),'[]'::jsonb)::text FROM pg_stat_activity WHERE pid IN($1,$2)`, reader, renewer).Scan(&trace); err != nil {
		t.Fatal(err)
	}
	t.Log("OWNERSHIP_ACTIVITY_TRACE", trace)
	if err := c.QueryRow(ctx, `SELECT coalesce(jsonb_agg(jsonb_build_object('pid',pid,'mode',mode,'granted',granted) ORDER BY pid,mode),'[]'::jsonb)::text FROM pg_locks WHERE relation='cad_v2.partition_ownership'::regclass AND pid IN($1,$2)`, reader, renewer).Scan(&trace); err != nil {
		t.Fatal(err)
	}
	t.Log("OWNERSHIP_RELATION_LOCK_TRACE", trace)
}

func TestV9OutboxReadBlocksRenewal(t *testing.T) {
	for _, nonempty := range []bool{false, true} {
		t.Run(fmt.Sprintf("nonempty_%t", nonempty), func(t *testing.T) {
			c := testDatabase(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if n, err := Apply(ctx, c, ownershipSeries(t, 9)); err != nil || n != 9 {
				t.Fatal(n, err)
			}
			ownershipFixture(t, c, nonempty)
			reader, renewer := openPeer(t, c), openPeer(t, c)
			execSQL(t, renewer, `SET lock_timeout='2s'`)
			tx, err := reader.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			count, err := ownershipPage(ctx, tx, "read_outbox")
			want := 0
			if nonempty {
				want = 1
			}
			if err != nil || count != want {
				t.Fatal("legacy page", count, err)
			}
			started := time.Now()
			done := make(chan error, 1)
			go func() { done <- ownershipRenew(ctx, renewer) }()
			observed := false
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				if err := c.QueryRow(ctx, `SELECT $1::integer=ANY(pg_blocking_pids($2::integer))`, reader.PgConn().PID(), renewer.PgConn().PID()).Scan(&observed); err != nil {
					t.Fatal(err)
				}
				if observed {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !observed {
				t.Fatal("no real renewal -> Page blocking edge observed")
			}
			ownershipLockTrace(t, ctx, c, reader.PgConn().PID(), renewer.PgConn().PID())
			err = <-done
			var pgerr *pgconn.PgError
			if !errors.As(err, &pgerr) || pgerr.Code != "55P03" {
				t.Fatalf("expected unchanged 2s lock_timeout 55P03, got %v", err)
			}
			t.Logf("RED_REPRODUCED version=9 page_rows=%d reader_pid=%d renew_pid=%d blocking_edge=true sqlstate=%s elapsed=%s", count, reader.PgConn().PID(), renewer.PgConn().PID(), pgerr.Code, time.Since(started))
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if err := ownershipRenew(ctx, renewer); err != nil {
				t.Fatal("renew failed after releasing Page transaction", err)
			}
		})
	}
}
