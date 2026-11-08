package db

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestV10UpgradeRollbackAndRetry(t *testing.T) {
	c := testDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if n, err := Apply(ctx, c, ownershipSeries(t, 9)); err != nil || n != 9 {
		t.Fatal(n, err)
	}
	ownershipFixture(t, c, true)

	snapshot := func() []string {
		t.Helper()
		var result []string
		for _, table := range []string{"runs", "executions", "execution_partitions", "partition_ownership", "group_checkpoints", "outbox_messages", "worker_tick_fragments", "master_tick_fragments", "master_tick_joins", "master_inbox", "master_results", "master_events", "master_poll_cursors", "result_topic_bindings", "result_consumer_offsets"} {
			var value string
			if err := c.QueryRow(ctx, `SELECT coalesce(jsonb_agg(v ORDER BY v::text),'[]'::jsonb)::text FROM (SELECT to_jsonb(t) v FROM cad_v2.`+table+` t) s`).Scan(&value); err != nil {
				t.Fatal(err)
			}
			result = append(result, value)
		}
		return result
	}
	writerDefinitions := func() []string {
		t.Helper()
		var defs []string
		for _, sig := range []string{
			"cad_v2.assert_owner(uuid,text,uuid,bigint)",
			"cad_v2.acquire_partition(uuid,text,uuid,interval)",
			"cad_v2.renew_partition(uuid,text,uuid,bigint,interval)",
			"cad_v2.release_partition(uuid,text,uuid,bigint)",
			"cad_v2.commit_checkpoint(uuid,text,uuid,bigint,bigint,bigint,timestamptz,integer,text,bytea,cad_v2.sha256_hex,boolean,jsonb)",
			"cad_v2.stage_tick_fragment(uuid,text,uuid,bigint,bigint,integer,jsonb)",
			"cad_v2.ack_outbox(uuid,text,uuid,bigint,text)",
			"cad_v2.ack_delivery(uuid,text,uuid,bigint,text)",
			"cad_v2.ack_poll_cursor(uuid,text,uuid,bigint,bigint,integer)",
			"cad_v2.advertise_worker(uuid,text,uuid,bigint,text)",
			"cad_v2.accept_result(uuid,jsonb)", "cad_v2.accept_tick_fragment(uuid,jsonb)",
		} {
			var def string
			if err := c.QueryRow(ctx, `SELECT pg_get_functiondef($1::regprocedure)`, sig).Scan(&def); err != nil {
				t.Fatal(err)
			}
			defs = append(defs, def)
		}
		return defs
	}
	var historyBefore string
	if err := c.QueryRow(ctx, `SELECT coalesce(jsonb_agg(to_jsonb(h) ORDER BY h.version),'[]'::jsonb)::text FROM public.schema_migrations h`).Scan(&historyBefore); err != nil {
		t.Fatal(err)
	}
	var oldHistory string
	if err := c.QueryRow(ctx, `SELECT coalesce(jsonb_agg(to_jsonb(h) ORDER BY h.version),'[]'::jsonb)::text FROM public.schema_migrations h WHERE h.version<=9`).Scan(&oldHistory); err != nil {
		t.Fatal(err)
	}
	rowsBefore, writersBefore := snapshot(), writerDefinitions()
	all, err := readSeries(Files)
	if err != nil {
		t.Fatal(err)
	}
	broken := fstest.MapFS{}
	for _, m := range all {
		if m.version <= 9 {
			broken["migrations/"+m.name] = &fstest.MapFile{Data: []byte(m.sql)}
		} else {
			broken["migrations/"+m.name] = &fstest.MapFile{Data: []byte(m.sql + "\nSELECT 1/0;\n")}
		}
	}
	if _, err := Apply(ctx, c, broken); err == nil {
		t.Fatal("broken V10 migration unexpectedly succeeded")
	}
	if !reflect.DeepEqual(rowsBefore, snapshot()) || !reflect.DeepEqual(writersBefore, writerDefinitions()) {
		t.Fatal("failed V10 migration changed rows or writer definitions")
	}
	var historyAfter string
	if err := c.QueryRow(ctx, `SELECT coalesce(jsonb_agg(to_jsonb(h) ORDER BY h.version),'[]'::jsonb)::text FROM public.schema_migrations h`).Scan(&historyAfter); err != nil || historyBefore != historyAfter {
		t.Fatal("failed V10 migration changed history", err)
	}
	var absent bool
	if err := c.QueryRow(ctx, `SELECT to_regprocedure('cad_v2.read_outbox_page(uuid,text,uuid,bigint,bigint,integer,integer,boolean)') IS NULL`).Scan(&absent); err != nil || !absent {
		t.Fatal("failed V10 leaked new page API", err)
	}
	if n, err := Apply(ctx, c, Files); err != nil || n != 1 {
		t.Fatal("V10 retry failed", n, err)
	}
	var oldHistoryAfter string
	if err := c.QueryRow(ctx, `SELECT coalesce(jsonb_agg(to_jsonb(h) ORDER BY h.version),'[]'::jsonb)::text FROM public.schema_migrations h WHERE h.version<=9`).Scan(&oldHistoryAfter); err != nil || oldHistoryAfter != oldHistory {
		t.Fatal("V10 changed migration history for versions 1-9", err)
	}
	if !reflect.DeepEqual(rowsBefore, snapshot()) || !reflect.DeepEqual(writersBefore, writerDefinitions()) {
		t.Fatal("successful V10 migration altered durable state or writer functions")
	}
	if n, err := Apply(ctx, c, Files); err != nil || n != 0 {
		t.Fatal("repeated V10 Apply was not a no-op", n, err)
	}
	var historyVersions int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM public.schema_migrations`).Scan(&historyVersions); err != nil || historyVersions != 10 {
		t.Fatal("V10 history count", historyVersions, err)
	}
}

func TestV10FreshAndRuntimePagePrivileges(t *testing.T) {
	c := testDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if n, err := Apply(ctx, c, Files); err != nil || n != 10 {
		t.Fatal("fresh Apply", n, err)
	}
	ownershipFixture(t, c, true)
	var token [8]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	worker, master := "v10_worker_"+hex.EncodeToString(token[:]), "v10_master_"+hex.EncodeToString(token[:])
	if _, err := c.Exec(ctx, `CREATE ROLE `+pgx.Identifier{worker}.Sanitize()+` NOLOGIN; CREATE ROLE `+pgx.Identifier{master}.Sanitize()+` NOLOGIN`, pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		_, err := c.Exec(cleanup, `RESET ROLE; DROP OWNED BY `+pgx.Identifier{worker}.Sanitize()+`, `+pgx.Identifier{master}.Sanitize()+`; DROP ROLE `+pgx.Identifier{worker}.Sanitize()+`, `+pgx.Identifier{master}.Sanitize(), pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			t.Error("scoped runtime role cleanup", err)
		}
	})
	if err := GrantRuntime(ctx, c, worker, master); err != nil {
		t.Fatal("GrantRuntime", err)
	}
	var volatility string
	var definer bool
	var config []string
	if err := c.QueryRow(ctx, `SELECT provolatile::text,prosecdef,proconfig FROM pg_proc WHERE oid='cad_v2.read_outbox_page(uuid,text,uuid,bigint,bigint,integer,integer,boolean)'::regprocedure`).Scan(&volatility, &definer, &config); err != nil || volatility != "s" || !definer || !reflect.DeepEqual(config, []string{"search_path=pg_catalog, cad_v2"}) {
		t.Fatal("snapshot/security function attributes", volatility, definer, config, err)
	}
	if _, err := c.Exec(ctx, `SET ROLE `+pgx.Identifier{worker}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,0,0,64,true)`, ownershipExecution, ownershipOwner).Scan(&count); err != nil || count != 1 {
		t.Fatal("Worker role owner page", count, err)
	}
	if _, err := c.Exec(ctx, `RESET ROLE`); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`SELECT * FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,-1,0,64,true)`,
		`SELECT * FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,0,NULL,64,true)`,
		`SELECT * FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,NULL,0,64,true)`,
		`SELECT * FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,0,0,NULL,true)`,
		`SELECT * FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,0,0,129,true)`,
		`SELECT * FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,0,0,64,NULL)`,
	} {
		_, err := c.Exec(ctx, query, ownershipExecution, ownershipOwner)
		ownershipSQLState(t, err, "22023")
	}
	if _, err := c.Exec(ctx, `BEGIN ISOLATION LEVEL REPEATABLE READ`); err != nil {
		t.Fatal(err)
	}
	_, isolationErr := c.Exec(ctx, `SELECT * FROM cad_v2.read_outbox_page($1::uuid,'a01',$2::uuid,1,0,0,64,true)`, ownershipExecution, ownershipOwner)
	_, rollbackErr := c.Exec(ctx, `ROLLBACK`)
	if rollbackErr != nil {
		t.Fatal(rollbackErr)
	}
	var isolationPgErr *pgconn.PgError
	if isolationErr == nil || !errors.As(isolationErr, &isolationPgErr) || isolationPgErr.Code != "0A000" {
		t.Fatalf("expected non-READ-COMMITTED rejection 0A000, got %v", isolationErr)
	}
	for _, test := range []struct {
		name, role string
		query      string
		code       string
	}{
		{"wrong_owner_empty", worker, fmt.Sprintf(`SELECT count(*) FROM cad_v2.read_outbox_page('%s','a01','%s',1,99,0,64,true)`, ownershipExecution, ownershipNext), "55000"},
		{"null_owner_empty", worker, fmt.Sprintf(`SELECT count(*) FROM cad_v2.read_outbox_page('%s','a01',NULL,1,99,0,64,true)`, ownershipExecution), "55000"},
		{"null_epoch_empty", worker, fmt.Sprintf(`SELECT count(*) FROM cad_v2.read_outbox_page('%s','a01','%s',NULL,99,0,64,true)`, ownershipExecution, ownershipOwner), "55000"},
		{"unknown_group", worker, fmt.Sprintf(`SELECT count(*) FROM cad_v2.read_outbox_page('%s','a17','%s',1,0,0,64,true)`, ownershipExecution, ownershipOwner), "P0002"},
		{"master_cannot_page", master, fmt.Sprintf(`SELECT count(*) FROM cad_v2.read_outbox_page('%s','a01','%s',1,0,0,64,true)`, ownershipExecution, ownershipOwner), "42501"},
		{"worker_direct_dml", worker, `INSERT INTO cad_v2.outbox_messages DEFAULT VALUES`, "42501"},
		{"worker_writer_helper", worker, `SELECT cad_v2.assert_owner('00000000-0000-0000-0000-000000000001','a01','00000000-0000-0000-0000-000000000011',1)`, "42501"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := c.Exec(ctx, `SET ROLE `+pgx.Identifier{test.role}.Sanitize()); err != nil {
				t.Fatal(err)
			}
			_, err := c.Exec(ctx, test.query)
			_, resetErr := c.Exec(ctx, `RESET ROLE`)
			if resetErr != nil {
				t.Fatal(resetErr)
			}
			var pgerr *pgconn.PgError
			if err == nil || !errors.As(err, &pgerr) || pgerr.Code != test.code {
				t.Fatalf("expected SQLSTATE %s, got %v", test.code, err)
			}
		})
	}
	var publicExecute, masterExecute bool
	if err := c.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_proc p,LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE p.oid='cad_v2.read_outbox_page(uuid,text,uuid,bigint,bigint,integer,integer,boolean)'::regprocedure AND a.grantee=0 AND a.privilege_type='EXECUTE'), has_function_privilege($1,'cad_v2.read_outbox_page(uuid,text,uuid,bigint,bigint,integer,integer,boolean)','EXECUTE')`, master).Scan(&publicExecute, &masterExecute); err != nil || publicExecute || masterExecute {
		t.Fatal("page function exposed to PUBLIC or Master", publicExecute, masterExecute, err)
	}
	var legacyWorker, legacyPublic bool
	if err := c.QueryRow(ctx, `SELECT has_function_privilege($1,'cad_v2.read_outbox(uuid,text,uuid,bigint,bigint,integer,integer,boolean)','EXECUTE'), EXISTS(SELECT 1 FROM pg_proc p,LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE p.oid='cad_v2.read_outbox(uuid,text,uuid,bigint,bigint,integer,integer,boolean)'::regprocedure AND a.grantee=0 AND a.privilege_type='EXECUTE')`, worker).Scan(&legacyWorker, &legacyPublic); err != nil || legacyWorker || legacyPublic {
		t.Fatal("legacy reader is not admin-only", legacyWorker, legacyPublic, err)
	}
}
