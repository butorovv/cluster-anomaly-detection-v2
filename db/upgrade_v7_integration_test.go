package db

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
)

func TestV7UpgradePreservesPendingDiagnosticJoin(t *testing.T) {
	c := testDatabase(t)
	ctx := context.Background()
	series, err := readSeries(Files)
	if err != nil {
		t.Fatal(err)
	}
	old := fstest.MapFS{}
	for _, m := range series {
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
	// Synthetic structural SQL fixtures, not scientific golden rows/snapshots.
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
	before := snapshot()
	if n, err := Apply(ctx, c, Files); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if after := snapshot(); !reflect.DeepEqual(before, after) {
		t.Fatal("V8 changed durable checkpoint/outbox/join data")
	}
	if n, err := Apply(ctx, c, Files); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	var accepted bool
	if err := c.QueryRow(ctx, `SELECT cad_v2.accept_tick_fragment('00000000-0000-0000-0000-000000000001',p01_test.fragment())`).Scan(&accepted); err != nil || accepted {
		t.Fatal("pending fragment lost duplicate identity on upgrade", accepted, err)
	}
	var definition string
	if err := c.QueryRow(ctx, `SELECT pg_get_functiondef('cad_v2.accept_tick_fragment(uuid,jsonb)'::regprocedure)`).Scan(&definition); err != nil || !strings.Contains(definition, "FOR NO KEY UPDATE") {
		t.Fatal("new guard not installed", err)
	}
	var count int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM cad_v2.read_outbox('00000000-0000-0000-0000-000000000001','a01','00000000-0000-0000-0000-000000000011',1,0,0,64,true)`).Scan(&count); err != nil || count != 2 {
		t.Fatal("pending output unavailable after V8", count, err)
	}
}
