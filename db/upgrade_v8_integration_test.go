package db

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestV8UpgradeRollbackAndRetry(t *testing.T) {
	c := testDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	series, err := readSeries(Files)
	if err != nil {
		t.Fatal(err)
	}
	old := fstest.MapFS{}
	latestV8 := fstest.MapFS{}
	for _, m := range series {
		if m.version <= 8 {
			latestV8["migrations/"+m.name] = &fstest.MapFile{Data: []byte(m.sql)}
		}
		if m.version <= 7 {
			old["migrations/"+m.name] = &fstest.MapFile{Data: []byte(m.sql)}
		}
	}
	if n, err := Apply(ctx, c, old); err != nil || n != 7 {
		t.Fatal(n, err)
	}
	fixture, err := os.ReadFile("tests/fixtures.sql")
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, c, string(fixture))
	execSQL(t, c, `
 SELECT cad_v2.acquire_partition('00000000-0000-0000-0000-000000000001','a01','00000000-0000-0000-0000-000000000011',interval '5 minutes');
 SELECT p01_test.commit(0,1501,'2022-01-18 08:00Z',jsonb_build_array(p01_test.message()));
 CREATE FUNCTION p01_test.fragment() RETURNS jsonb LANGUAGE sql AS $$ SELECT jsonb_build_object(
 'schema_version',2,'kind','tick_fragment','execution_id','00000000-0000-0000-0000-000000000001',
 'run_id',repeat('1',64),'config_id',repeat('2',64),'group_id','a01','message_id',repeat('a',64),
 'revision',1,'terminal',false,'event_time','2022-01-18T08:00:00Z',
 'payload',jsonb_build_object('raw',jsonb_build_object('group','a01','timestamp','2022-01-18T08:00:00Z','if',jsonb_build_object('outside_groups',NULL)),
 'temporal',jsonb_build_object('group','a01','timestamp','2022-01-18T08:00:00Z'))) $$;
 SELECT cad_v2.stage_tick_fragment('00000000-0000-0000-0000-000000000001','a01','00000000-0000-0000-0000-000000000011',1,1,2,p01_test.fragment());
 SELECT cad_v2.accept_tick_fragment('00000000-0000-0000-0000-000000000001',p01_test.fragment());`)

	snapshot := func() []string {
		t.Helper()
		var values []string
		for _, table := range []string{"runs", "executions", "execution_partitions", "partition_ownership", "group_checkpoints", "outbox_messages", "worker_tick_fragments", "master_tick_fragments", "master_tick_joins", "master_inbox", "master_results", "master_events"} {
			var value string
			if err := c.QueryRow(ctx, `SELECT coalesce(jsonb_agg(v ORDER BY v::text),'[]'::jsonb)::text FROM (SELECT to_jsonb(t) v FROM cad_v2.`+table+` t) s`).Scan(&value); err != nil {
				t.Fatal(err)
			}
			values = append(values, value)
		}
		return values
	}
	definitions := func() []string {
		t.Helper()
		var result []string
		for _, signature := range []string{"cad_v2.accept_result(uuid,jsonb)", "cad_v2.accept_tick_fragment(uuid,jsonb)"} {
			var definition string
			if err := c.QueryRow(ctx, `SELECT pg_get_functiondef($1::regprocedure)`, signature).Scan(&definition); err != nil {
				t.Fatal(err)
			}
			result = append(result, definition)
		}
		return result
	}
	beforeRows, beforeFunctions := snapshot(), definitions()
	var beforeHistory string
	if err := c.QueryRow(ctx, `SELECT coalesce(jsonb_agg(to_jsonb(h) ORDER BY h.version),'[]'::jsonb)::text FROM public.schema_migrations h`).Scan(&beforeHistory); err != nil {
		t.Fatal(err)
	}
	broken := fstest.MapFS{}
	for path, file := range old {
		broken[path] = file
	}
	for _, m := range series {
		if m.version == 8 {
			broken["migrations/"+m.name] = &fstest.MapFile{Data: []byte(m.sql + "\nSELECT 1/0;\n")}
		}
	}
	if _, err := Apply(ctx, c, broken); err == nil {
		t.Fatal("broken V8 migration unexpectedly succeeded")
	}
	if got := snapshot(); !reflect.DeepEqual(beforeRows, got) {
		t.Fatal("failed V8 changed durable rows")
	}
	if got := definitions(); !reflect.DeepEqual(beforeFunctions, got) {
		t.Fatal("failed V8 did not restore both function definitions")
	}
	var afterHistory string
	if err := c.QueryRow(ctx, `SELECT coalesce(jsonb_agg(to_jsonb(h) ORDER BY h.version),'[]'::jsonb)::text FROM public.schema_migrations h`).Scan(&afterHistory); err != nil || beforeHistory != afterHistory {
		t.Fatal("failed V8 changed migration history", err)
	}
	// Upgrade while an old Master transaction still holds V7's UPDATE guard
	// and an independent Worker has read the durable outbox under ownership
	// fencing. Replacing functions cannot retroactively weaken existing locks.
	master, worker := openPeer(t, c), openPeer(t, c)
	masterTx, err := master.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer masterTx.Rollback(context.Background())
	if _, err := masterTx.Exec(ctx, `SELECT cad_v2.accept_tick_fragment('00000000-0000-0000-0000-000000000001',p01_test.fragment())`); err != nil {
		t.Fatal(err)
	}
	workerTx, err := worker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer workerTx.Rollback(context.Background())
	var pending int
	if err := workerTx.QueryRow(ctx, `SELECT count(*) FROM cad_v2.read_outbox('00000000-0000-0000-0000-000000000001','a01','00000000-0000-0000-0000-000000000011',1,0,0,64,true)`).Scan(&pending); err != nil || pending != 2 {
		t.Fatal(pending, err)
	}
	if n, err := Apply(ctx, c, latestV8); err != nil || n != 1 {
		t.Fatal("V8 retry failed", n, err)
	}
	if _, err := c.Exec(ctx, `SET lock_timeout='100ms'`); err != nil {
		t.Fatal(err)
	}
	_, err = c.Exec(ctx, `SELECT 1 FROM cad_v2.execution_partitions FOR KEY SHARE`)
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "55P03" {
		t.Fatalf("old transaction guard vanished during upgrade: %v", err)
	}
	if err := masterTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := workerTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := snapshot(); !reflect.DeepEqual(beforeRows, got) {
		t.Fatal("online V8 retry changed durable state")
	}
	if n, err := Apply(ctx, c, latestV8); err != nil || n != 0 {
		t.Fatal("repeat Apply was not a no-op", n, err)
	}
	newMasterTx, err := master.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer newMasterTx.Rollback(context.Background())
	if _, err := newMasterTx.Exec(ctx, `SELECT cad_v2.accept_tick_fragment('00000000-0000-0000-0000-000000000001',p01_test.fragment())`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, `SELECT 1 FROM cad_v2.execution_partitions FOR KEY SHARE`); err != nil {
		t.Fatal("new V8 guard still blocks FK reader", err)
	}
}
