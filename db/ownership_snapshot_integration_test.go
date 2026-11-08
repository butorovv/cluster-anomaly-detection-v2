package db

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const ownershipNext = "00000000-0000-0000-0000-000000000012"
const ownershipFragment = `jsonb_build_object(
 'schema_version',2,'kind','tick_fragment','execution_id','00000000-0000-0000-0000-000000000001',
 'run_id',repeat('1',64),'config_id',repeat('2',64),'group_id','a01','message_id',repeat('a',64),
 'revision',1,'terminal',false,'event_time','2022-01-18T08:00:00Z',
 'payload',jsonb_build_object('raw',jsonb_build_object('group','a01','timestamp','2022-01-18T08:00:00Z','if',jsonb_build_object('outside_groups',NULL)),
 'temporal',jsonb_build_object('group','a01','timestamp','2022-01-18T08:00:00Z')))`
const ownershipStage = `SELECT cad_v2.stage_tick_fragment('` + ownershipExecution + `','a01','` + ownershipOwner + `',1,1,2,` + ownershipFragment + `)`

func ownershipSQLState(t *testing.T, err error, code string) {
	t.Helper()
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != code {
		t.Fatalf("expected SQLSTATE %s, got %v", code, err)
	}
}

func TestV10HeldPageDoesNotBlockRenewal(t *testing.T) {
	for _, nonempty := range []bool{false, true} {
		t.Run(fmt.Sprintf("nonempty_%t", nonempty), func(t *testing.T) {
			c := testDatabase(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := Apply(ctx, c, Files); err != nil {
				t.Fatal(err)
			}
			ownershipFixture(t, c, nonempty)
			reader, renewer := openPeer(t, c), openPeer(t, c)
			execSQL(t, renewer, `SET lock_timeout='2s'`)
			tx, err := reader.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			count, err := ownershipPage(ctx, tx, "read_outbox_page")
			want := 0
			if nonempty {
				want = 1
			}
			if err != nil || count != want {
				t.Fatal("snapshot page", count, err)
			}
			var exclusive, shared int
			if err := c.QueryRow(ctx, `SELECT count(*) FILTER(WHERE mode<>'AccessShareLock'), count(*) FILTER(WHERE mode='AccessShareLock') FROM pg_locks WHERE pid=$1 AND relation='cad_v2.partition_ownership'::regclass AND granted`, reader.PgConn().PID()).Scan(&exclusive, &shared); err != nil || exclusive != 0 || shared != 1 {
				t.Fatal("page acquired an ownership write/row lock", exclusive, shared, err)
			}
			started := time.Now()
			if err := ownershipRenew(ctx, renewer); err != nil {
				t.Fatal("renewal while Page TX is still open", err)
			}
			elapsed := time.Since(started)
			var headroom float64
			if err := c.QueryRow(ctx, `SELECT extract(epoch FROM lease_until-clock_timestamp())::double precision FROM cad_v2.partition_ownership WHERE execution_id=$1::uuid AND group_id='a01'`, ownershipExecution).Scan(&headroom); err != nil || headroom < 290 {
				t.Fatal("renewal did not extend the lease", headroom, err)
			}
			if count, err = ownershipPage(ctx, tx, "read_outbox_page"); err != nil || count != want {
				t.Fatal("Page TX was not still usable/open", count, err)
			}
			t.Logf("GREEN version=10 page_rows=%d page_tx_open=true ownership_mode=AccessShareLock renew_sqlstate=00000 elapsed=%s headroom_seconds=%.3f", count, elapsed, headroom)
			ownershipLockTrace(t, ctx, c, reader.PgConn().PID(), renewer.PgConn().PID())
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestV10PageSnapshotSurvivesConcurrentTakeover(t *testing.T) {
	c := testDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := Apply(ctx, c, Files); err != nil {
		t.Fatal(err)
	}
	ownershipFixture(t, c, true)
	reader, locker := openPeer(t, c), openPeer(t, c)
	barrier, err := locker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Rollback(context.Background())
	if _, err := barrier.Exec(ctx, `LOCK TABLE cad_v2.worker_tick_fragments IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	tx, err := reader.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	type pageResult struct {
		count int
		err   error
	}
	done := make(chan pageResult, 1)
	go func() {
		count, err := ownershipPage(ctx, tx, "read_outbox_page")
		done <- pageResult{count, err}
	}()
	observed := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.QueryRow(ctx, `SELECT $1::integer=ANY(pg_blocking_pids($2::integer))`, locker.PgConn().PID(), reader.PgConn().PID()).Scan(&observed); err != nil {
			t.Fatal(err)
		}
		if observed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !observed {
		t.Fatal("page did not reach the relation-lock barrier")
	}
	// The read is delayed after its statement snapshot. It must not block renewal,
	// release or takeover, nor see output committed after that snapshot.
	if err := ownershipRenew(ctx, c); err != nil {
		t.Fatal("delayed page blocks renewal", err)
	}
	execSQL(t, c, `SELECT cad_v2.release_partition('`+ownershipExecution+`','a01','`+ownershipOwner+`',1)`)
	var epoch int64
	if err := c.QueryRow(ctx, `SELECT cad_v2.acquire_partition($1::uuid,'a01',$2::uuid,interval '5 minutes')`, ownershipExecution, ownershipNext).Scan(&epoch); err != nil || epoch != 2 {
		t.Fatal("takeover while read is delayed", epoch, err)
	}
	execSQL(t, c, `SELECT cad_v2.commit_checkpoint('`+ownershipExecution+`','a01','`+ownershipNext+`',2,1,1502,'2022-01-18 08:00:10Z',1,'opaque-v1','\x010203',encode(sha256('\x010203'::bytea),'hex'),false,jsonb_build_array(p01_test.message('group_result',1,'2022-01-18T08:00:10Z')))`)
	if err := barrier.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	page := <-done
	if page.err != nil || page.count != 1 {
		t.Fatal("page mixed ownership/output snapshots", page.count, page.err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	err = c.QueryRow(ctx, `SELECT count(*) FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,0,0,64,true)`, ownershipExecution, ownershipOwner).Scan(&count)
	ownershipSQLState(t, err, "55000")
	if err := c.QueryRow(ctx, `SELECT count(*) FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,2,0,0,64,true)`, ownershipExecution, ownershipNext).Scan(&count); err != nil || count != 2 {
		t.Fatal("new owner lost committed output", count, err)
	}
	t.Log("snapshot barrier: delayed old page=1, new owner page=2, fresh old page=55000; renew/release/takeover completed before read resumed")
}

func TestV10PageNeverExposesUncommittedCheckpoint(t *testing.T) {
	c := testDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := Apply(ctx, c, Files); err != nil {
		t.Fatal(err)
	}
	ownershipFixture(t, c, false)
	writer := openPeer(t, c)
	for _, commit := range []bool{false, true} {
		tx, err := writer.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		if _, err := tx.Exec(ctx, `SELECT p01_test.commit(0,1501,'2022-01-18 08:00Z',jsonb_build_array(p01_test.message())); `+ownershipStage, pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := c.QueryRow(ctx, `SELECT count(*) FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,0,0,64,true)`, ownershipExecution, ownershipOwner).Scan(&count); err != nil || count != 0 {
			t.Fatal("uncommitted checkpoint/event/fragment became visible or blocked Page", count, err)
		}
		if commit {
			err = tx.Commit(ctx)
		} else {
			err = tx.Rollback(ctx)
		}
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		if commit {
			want = 2
		}
		if err := c.QueryRow(ctx, `SELECT count(*) FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,0,0,64,true)`, ownershipExecution, ownershipOwner).Scan(&count); err != nil || count != want {
			t.Fatal("atomic visibility after rollback/commit", commit, count, err)
		}
	}
	var ordinals []int32
	if err := c.QueryRow(ctx, `SELECT array_agg(ordinal ORDER BY ordinal) FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,0,0,64,true)`, ownershipExecution, ownershipOwner).Scan(&ordinals); err != nil || len(ordinals) != 2 || ordinals[0] != 1 || ordinals[1] != 2 {
		t.Fatal("event/fragment order changed", ordinals, err)
	}
}

func TestV10TakeoverKeepsMutationFencingAndLateDelivery(t *testing.T) {
	c := testDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := Apply(ctx, c, Files); err != nil {
		t.Fatal(err)
	}
	ownershipFixture(t, c, true)
	execSQL(t, c, ownershipStage)
	var cached []byte
	if err := c.QueryRow(ctx, `SELECT envelope FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,0,0,1,true)`, ownershipExecution, ownershipOwner).Scan(&cached); err != nil {
		t.Fatal(err)
	}
	execSQL(t, c, `SELECT cad_v2.release_partition('`+ownershipExecution+`','a01','`+ownershipOwner+`',1)`)
	var epoch int64
	if err := c.QueryRow(ctx, `SELECT cad_v2.acquire_partition($1::uuid,'a01',$2::uuid,interval '5 minutes')`, ownershipExecution, ownershipNext).Scan(&epoch); err != nil || epoch != 2 {
		t.Fatal(epoch, err)
	}
	for _, statement := range []string{
		`SELECT cad_v2.renew_partition($1::uuid,'a01',$2::uuid,1,interval '5 minutes')`,
		`SELECT cad_v2.release_partition($1::uuid,'a01',$2::uuid,1)`,
		`SELECT cad_v2.ack_outbox($1::uuid,'a01',$2::uuid,1,'unused')`,
		`SELECT cad_v2.ack_delivery($1::uuid,'a01',$2::uuid,1,'unused')`,
		`SELECT cad_v2.ack_poll_cursor($1::uuid,'a01',$2::uuid,1,1,2)`,
		`SELECT cad_v2.advertise_worker($1::uuid,'a01',$2::uuid,1,'http://127.0.0.1:1')`,
		`SELECT cad_v2.commit_checkpoint($1::uuid,'a01',$2::uuid,1,1,1502,'2022-01-18 08:00:10Z',1,'opaque-v1','\x010203',encode(sha256('\x010203'::bytea),'hex'),false,'[]')`,
		`SELECT cad_v2.stage_tick_fragment($1::uuid,'a01',$2::uuid,1,1,2,` + ownershipFragment + `)`,
		`SELECT * FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,0,0,64,true)`,
	} {
		_, err := c.Exec(ctx, statement, ownershipExecution, ownershipOwner)
		ownershipSQLState(t, err, "55000")
	}
	// Old in-flight output is already durable. Master still accepts it once,
	// deduplicates exact retry and rejects a conflicting complete envelope.
	for _, want := range []bool{true, false} {
		var accepted bool
		if err := c.QueryRow(ctx, `SELECT cad_v2.accept_result($1::uuid,$2::jsonb)`, ownershipExecution, cached).Scan(&accepted); err != nil || accepted != want {
			t.Fatal("late/exact delivery", accepted, err)
		}
	}
	_, err := c.Exec(ctx, `SELECT cad_v2.accept_result($1::uuid,jsonb_set($2::jsonb,'{payload,raw,R}','"Positive"'))`, ownershipExecution, cached)
	ownershipSQLState(t, err, "23505")
	var accepted bool
	if err := c.QueryRow(ctx, `SELECT cad_v2.accept_tick_fragment($1::uuid,`+ownershipFragment+`)`, ownershipExecution).Scan(&accepted); err != nil || !accepted {
		t.Fatal("late fragment", accepted, err)
	}
	execSQL(t, c, `SELECT cad_v2.advance_poll_cursor('`+ownershipExecution+`','a01',1,2)`)
	// A lost SQL ACK receipt can be retried by the new owner without deleting or
	// rewriting scientific output. Both stores remain pending until that ACK.
	var pending int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,2,0,0,64,true)`, ownershipExecution, ownershipNext).Scan(&pending); err != nil || pending != 2 {
		t.Fatal("stale mutation changed pending output", pending, err)
	}
	for i := 0; i < 2; i++ {
		if _, err := c.Exec(ctx, `SELECT cad_v2.ack_poll_cursor($1::uuid,'a01',$2::uuid,2,1,2)`, ownershipExecution, ownershipNext); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.QueryRow(ctx, `SELECT count(*) FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,2,0,0,64,true)`, ownershipExecution, ownershipNext).Scan(&pending); err != nil || pending != 0 {
		t.Fatal("new owner cursor ACK did not drain both stores", pending, err)
	}
	var unchanged bool
	if err := c.QueryRow(ctx, `SELECT envelope=$2::jsonb FROM cad_v2.outbox_messages WHERE execution_id=$1::uuid`, ownershipExecution, cached).Scan(&unchanged); err != nil || !unchanged {
		t.Fatal("delivery mutated scientific bytes", unchanged, err)
	}
	// Reacquisition by the same UUID also increments epoch.
	execSQL(t, c, `SELECT cad_v2.release_partition('`+ownershipExecution+`','a01','`+ownershipNext+`',2)`)
	if err := c.QueryRow(ctx, `SELECT cad_v2.acquire_partition($1::uuid,'a01',$2::uuid,interval '5 minutes')`, ownershipExecution, ownershipNext).Scan(&epoch); err != nil || epoch != 3 {
		t.Fatal("same UUID reused epoch", epoch, err)
	}
	_, err = c.Exec(ctx, `SELECT cad_v2.renew_partition($1::uuid,'a01',$2::uuid,2,interval '5 minutes')`, ownershipExecution, ownershipNext)
	ownershipSQLState(t, err, "55000")
}

func TestV10ExpiredPageAndRenewFailClosed(t *testing.T) {
	c := testDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := Apply(ctx, c, Files); err != nil {
		t.Fatal(err)
	}
	ownershipFixture(t, c, false)
	reader := openPeer(t, c)
	tx, err := reader.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var count int
	if count, err = ownershipPage(ctx, tx, "read_outbox_page"); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	// Administrative fixture fault, not a changed production TTL/timeout. A new
	// command in an old transaction must use statement time and the new snapshot.
	execSQL(t, c, `UPDATE cad_v2.partition_ownership SET lease_until=clock_timestamp()-interval '1 second' WHERE execution_id='`+ownershipExecution+`' AND group_id='a01'`)
	_, err = ownershipPage(ctx, tx, "read_outbox_page")
	ownershipSQLState(t, err, "55000")
	ownershipSQLState(t, ownershipRenew(ctx, c), "55000")
	var epoch int64
	if err := c.QueryRow(ctx, `SELECT cad_v2.acquire_partition($1::uuid,'a01',$2::uuid,interval '5 minutes')`, ownershipExecution, ownershipNext).Scan(&epoch); err != nil || epoch != 2 {
		t.Fatal("expiry takeover", epoch, err)
	}
}
