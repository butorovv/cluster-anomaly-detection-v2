package db

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestIntegratedFreshAndUpgrade(t *testing.T) {
	for _, upgrade := range []bool{false, true} {
		t.Run(fmt.Sprint("upgrade_", upgrade), func(t *testing.T) {
			c := testDatabase(t)
			ctx := context.Background()
			wantApplied := 5
			if upgrade {
				if n, err := Apply(ctx, c, baselineFiles(t)); err != nil || n != 1 {
					t.Fatal(n, err)
				}
				if _, err := c.Exec(ctx, `INSERT INTO project.node_group(code,name,group_type) VALUES('KEEP','preserved','cluster');
 INSERT INTO project.node(node_group_id,hostname,node_index) SELECT id,'a01n01',1 FROM project.node_group;
 INSERT INTO project.component(node_id,kind,local_index) SELECT id,'cpu',0 FROM project.node;
 INSERT INTO project.metric_source(component_id,metric_type_id,source_key) SELECT c.id,m.id,'cpu0_power' FROM project.component c, project.metric_type m WHERE m.code='CPU_POWER';
 INSERT INTO project.telemetry(metric_source_id,ts,value) SELECT id,'2022-01-18T08:00:00Z',42.5 FROM project.metric_source`); err != nil {
					t.Fatal(err)
				}
				wantApplied = 4
			}
			var before, after string
			if err := c.QueryRow(ctx, "SHOW search_path").Scan(&before); err != nil {
				t.Fatal(err)
			}
			if n, err := Apply(ctx, c, Files); err != nil || n != wantApplied {
				t.Fatal(n, err)
			}
			if err := c.QueryRow(ctx, "SHOW search_path").Scan(&after); err != nil || before != after {
				t.Fatal("search_path", before, after, err)
			}
			if n, err := Apply(ctx, c, Files); err != nil || n != 0 {
				t.Fatal(n, err)
			}
			var versions []int32
			if err := c.QueryRow(ctx, "SELECT array_agg(version ORDER BY version) FROM public.schema_migrations").Scan(&versions); err != nil || !reflect.DeepEqual(versions, []int32{1, 2, 3, 4, 5}) {
				t.Fatal(versions, err)
			}
			var tables []string
			rows, err := c.Query(ctx, "SELECT tablename FROM pg_tables WHERE schemaname='cad_v2' ORDER BY tablename")
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					t.Fatal(err)
				}
				tables = append(tables, name)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tables, []string{"execution_partitions", "executions", "group_checkpoints", "master_events", "master_inbox", "master_results", "outbox_messages", "partition_ownership", "runs"}) {
				t.Fatal(tables)
			}
			var metrics, kept int
			if err := c.QueryRow(ctx, "SELECT count(*) FROM project.metric_type").Scan(&metrics); err != nil || metrics != 5 {
				t.Fatal(metrics, err)
			}
			if upgrade {
				var value string
				if err := c.QueryRow(ctx, "SELECT value::text FROM project.telemetry").Scan(&value); err != nil || value != "42.5" {
					t.Fatal(value, err)
				}
				if err := c.QueryRow(ctx, "SELECT count(*) FROM project.node_group WHERE code='KEEP'").Scan(&kept); err != nil || kept != 1 {
					t.Fatal(kept, err)
				}
			}
		})
	}
}
func TestIntegratedSeriesAtomicRollback(t *testing.T) {
	c := testDatabase(t)
	ctx := context.Background()
	series, err := readSeries(Files)
	if err != nil {
		t.Fatal(err)
	}
	broken := fstest.MapFS{}
	for _, m := range series {
		sql := m.sql
		if m.version == 5 {
			sql += "\nSELECT 1/0;"
		}
		broken["migrations/"+m.name] = &fstest.MapFile{Data: []byte(sql)}
	}
	if _, err := Apply(ctx, c, broken); err == nil {
		t.Fatal("broken V5 committed")
	}
	var clean bool
	if err := c.QueryRow(ctx, "SELECT to_regnamespace('project') IS NULL AND to_regnamespace('cad_v2') IS NULL AND to_regclass('public.schema_migrations') IS NULL").Scan(&clean); err != nil || !clean {
		t.Fatal("partial migration committed", clean, err)
	}
	if n, err := Apply(ctx, c, Files); err != nil || n != 5 {
		t.Fatal(n, err)
	}
}
func TestRefuseUntrackedP01(t *testing.T) {
	for _, trackedCore := range []bool{false, true} {
		t.Run(fmt.Sprint(trackedCore), func(t *testing.T) {
			c := testDatabase(t)
			ctx := context.Background()
			if trackedCore {
				if _, err := Apply(ctx, c, baselineFiles(t)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := c.Exec(ctx, "CREATE SCHEMA cad_v2; CREATE TABLE cad_v2.keep_me(id int); INSERT INTO cad_v2.keep_me VALUES(42)"); err != nil {
				t.Fatal(err)
			}
			if _, err := Apply(ctx, c, Files); err == nil || !strings.Contains(err.Error(), "untracked") {
				t.Fatal("adopted standalone P01", err)
			}
			var kept int
			if err := c.QueryRow(ctx, "SELECT id FROM cad_v2.keep_me").Scan(&kept); err != nil || kept != 42 {
				t.Fatal(kept, err)
			}
		})
	}
}
func TestOriginalP01SQLSuites(t *testing.T) {
	c := testDatabase(t)
	ctx := context.Background()
	if _, err := Apply(ctx, c, Files); err != nil {
		t.Fatal(err)
	}
	fixture, err := os.ReadFile("tests/fixtures.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"01_invariants.sql", "02_transactions.sql", "03_events.sql", "04_privileges.sql", "05_execution_scope.sql"} {
		t.Run(name, func(t *testing.T) {
			suite, err := os.ReadFile("tests/" + name)
			if err != nil {
				t.Fatal(err)
			}
			// Suites end in ROLLBACK; role creation in 04 is rolled back as well.
			if _, err := c.Exec(ctx, "BEGIN;\n"+string(fixture)+"\n"+string(suite), pgx.QueryExecModeSimpleProtocol); err != nil {
				c.Exec(ctx, "ROLLBACK")
				t.Fatal(err)
			}
		})
	}
}

func openPeer(t *testing.T, c *pgx.Conn) *pgx.Conn {
	t.Helper()
	peer, err := pgx.ConnectConfig(context.Background(), c.Config().Copy())
	if err != nil {
		t.Fatal("connect test peer failed")
	}
	t.Cleanup(func() { peer.Close(context.Background()) })
	return peer
}
func execSQL(t *testing.T, c *pgx.Conn, sql string) {
	t.Helper()
	if _, err := c.Exec(context.Background(), sql, pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
}
func waitSQL(t *testing.T, c *pgx.Conn, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var ready bool
		if err := c.QueryRow(ctx, sql, args...).Scan(&ready); err != nil {
			t.Fatal(err)
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("condition timeout")
		case <-ticker.C:
		}
	}
}
func TestP01ConcurrentFencingAndCrash(t *testing.T) {
	c := testDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := Apply(ctx, c, Files); err != nil {
		t.Fatal(err)
	}
	fixture, err := os.ReadFile("tests/fixtures.sql")
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, c, string(fixture))
	old, newOwner, observer := openPeer(t, c), openPeer(t, c), openPeer(t, c)
	execSQL(t, c, `SELECT cad_v2.acquire_partition('00000000-0000-0000-0000-000000000001','a01','00000000-0000-0000-0000-000000000011',interval '1 second')`)
	tx, err := old.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `SELECT p01_test.commit(0,1501,'2022-01-18 08:00Z',jsonb_build_array(p01_test.message()))`); err != nil {
		t.Fatal(err)
	}
	execSQL(t, observer, `SELECT p01_test.assert(NOT EXISTS(SELECT 1 FROM cad_v2.group_checkpoints),'uncommitted checkpoint invisible'); SELECT p01_test.assert(NOT EXISTS(SELECT 1 FROM cad_v2.outbox_messages),'uncommitted outbox invisible')`)
	type result struct {
		epoch int64
		err   error
	}
	acquired := make(chan result, 1)
	go func() {
		var epoch int64
		err := newOwner.QueryRow(ctx, `SELECT cad_v2.acquire_partition('00000000-0000-0000-0000-000000000001','a01','00000000-0000-0000-0000-000000000012',interval '5 minutes')`).Scan(&epoch)
		acquired <- result{epoch, err}
	}()
	waitSQL(t, observer, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')", int32(newOwner.PgConn().PID()))
	waitSQL(t, observer, "SELECT lease_until <= clock_timestamp() FROM cad_v2.partition_ownership WHERE group_id='a01'")
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	r := <-acquired
	if r.err != nil || r.epoch != 2 {
		t.Fatal(r)
	}
	execSQL(t, observer, `SELECT p01_test.assert((SELECT next_offset=1501 FROM cad_v2.group_checkpoints),'old commit precedes takeover'); SELECT p01_test.expect_state($q$SELECT p01_test.commit(1,1503,'2022-01-18 08:00:10Z')$q$,'55000')`)
	newCheckpoint := func(expected, offset int, tick string) string {
		return fmt.Sprintf(`SELECT cad_v2.commit_checkpoint('00000000-0000-0000-0000-000000000001','a01','00000000-0000-0000-0000-000000000012',2,%d,%d,'%s',1,'opaque-v1','\x010203',encode(sha256('\x010203'::bytea),'hex'),false,jsonb_build_array(p01_test.message('group_result',1,'%s')))`, expected, offset, tick, tick)
	}
	execSQL(t, newOwner, newCheckpoint(1, 1503, "2022-01-18T08:00:10Z"))
	crashed := openPeer(t, c)
	pending, err := crashed.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Rollback(context.Background())
	if _, err := pending.Exec(ctx, newCheckpoint(2, 1505, "2022-01-18T08:00:20Z")); err != nil {
		t.Fatal(err)
	}
	var stopped bool
	if err := observer.QueryRow(ctx, "SELECT pg_terminate_backend($1)", int32(crashed.PgConn().PID())).Scan(&stopped); err != nil || !stopped {
		t.Fatal(stopped, err)
	}
	crashed.Close(ctx)
	waitSQL(t, observer, "SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)", int32(crashed.PgConn().PID()))
	execSQL(t, observer, `SELECT p01_test.assert((SELECT next_offset=1503 FROM cad_v2.group_checkpoints),'crash rolls back offset'); SELECT p01_test.assert((SELECT count(*)=2 FROM cad_v2.outbox_messages),'crash rolls back outbox')`)

	masterTx, err := old.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer masterTx.Rollback(context.Background())
	const accept = `SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',p01_test.message())`
	if _, err := masterTx.Exec(ctx, accept); err != nil {
		t.Fatal(err)
	}
	type accepted struct {
		fresh bool
		err   error
	}
	delivery := make(chan accepted, 1)
	go func() {
		var fresh bool
		err := newOwner.QueryRow(ctx, accept).Scan(&fresh)
		delivery <- accepted{fresh, err}
	}()
	waitSQL(t, observer, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')", int32(newOwner.PgConn().PID()))
	execSQL(t, observer, `SELECT p01_test.assert(NOT EXISTS(SELECT 1 FROM cad_v2.master_inbox),'uncommitted Master invisible')`)
	if err := masterTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	delivered := <-delivery
	if delivered.err != nil || !delivered.fresh {
		t.Fatal(delivered)
	}
	var duplicate bool
	if err := newOwner.QueryRow(ctx, accept).Scan(&duplicate); err != nil || duplicate {
		t.Fatal(duplicate, err)
	}
	execSQL(t, observer, `SELECT p01_test.assert((SELECT count(*)=1 FROM cad_v2.master_results),'one result after concurrent rollback retry')`)
}
