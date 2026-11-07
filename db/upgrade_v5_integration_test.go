package db

import (
	"context"
	"os"
	"reflect"
	"testing"
	"testing/fstest"
)

func TestV5UpgradePreservesDurableExecution(t *testing.T) {
	c := testDatabase(t)
	ctx := context.Background()
	series, err := readSeries(Files)
	if err != nil {
		t.Fatal(err)
	}
	old := fstest.MapFS{}
	for _, m := range series {
		if m.version <= 5 {
			old["migrations/"+m.name] = &fstest.MapFile{Data: []byte(m.sql)}
		}
	}
	if n, err := Apply(ctx, c, old); err != nil || n != 5 {
		t.Fatal(n, err)
	}
	fixture, err := os.ReadFile("tests/fixtures.sql")
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, c, string(fixture))
	execSQL(t, c, `SELECT cad_v2.acquire_partition('00000000-0000-0000-0000-000000000001','a01','00000000-0000-0000-0000-000000000011',interval '5 minutes'); SELECT p01_test.commit(0,1501,'2022-01-18 08:00Z',jsonb_build_array(p01_test.message())); SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',p01_test.message());`)
	// Hash every pre-existing row, including exact snapshot/progress/outbox and inbox.
	snapshot := func() []string {
		values := []string{}
		for _, table := range []string{"runs", "executions", "execution_partitions", "partition_ownership", "group_checkpoints", "outbox_messages", "master_inbox", "master_results", "master_events"} {
			var value string
			if err := c.QueryRow(ctx, `SELECT coalesce(jsonb_agg(v ORDER BY v::text),'[]'::jsonb)::text FROM (SELECT to_jsonb(t) v FROM cad_v2.`+table+` t) s`).Scan(&value); err != nil {
				t.Fatal(err)
			}
			values = append(values, value)
		}
		return values
	}
	before := snapshot()
	if n, err := Apply(ctx, c, Files); err != nil || n != 4 {
		t.Fatal(n, err)
	}
	if after := snapshot(); !reflect.DeepEqual(before, after) {
		t.Fatal("V6-V9 changed existing durable rows")
	}
	if n, err := Apply(ctx, c, Files); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	var count int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM cad_v2.read_outbox('00000000-0000-0000-0000-000000000001','a01','00000000-0000-0000-0000-000000000011',1,0,0,64,true)`).Scan(&count); err != nil || count != 1 {
		t.Fatal("upgraded outbox inaccessible", count, err)
	}
}
