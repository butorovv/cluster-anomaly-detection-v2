package db

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestSeriesRejectsAmbiguousVersions(t *testing.T) {
	for _, names := range [][]string{{"V1__a.sql", "V1__b.sql"}, {"V2__a.sql"}, {"000001_a.sql"}, {"V1__a.down.sql"}, {}} {
		files := fstest.MapFS{}
		for _, name := range names {
			files["migrations/"+name] = &fstest.MapFile{Data: []byte("SELECT 1;")}
		}
		if _, err := readSeries(files); err == nil {
			t.Fatalf("accepted %v", names)
		}
	}
}
func TestBaselinePin(t *testing.T) {
	b, err := Files.ReadFile("migrations/V1__core_schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	if fmt.Sprintf("%x", sum) != "4841ede64a4c4a26a58caef13505b255cdb6aa9bc5bfcfde5037fe0332cfe714" {
		t.Fatal("canonical baseline changed")
	}
}
func testDatabase(t *testing.T) *pgx.Conn {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL required; fresh PostgreSQL integration NOT_RUN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal("PostgreSQL connection failed (credentials omitted)")
	}
	var token [8]byte
	if _, err = rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	name := "p02_test_" + hex.EncodeToString(token[:])
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+quoted+" TEMPLATE template0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer admin.Close(context.Background())
		_, err := admin.Exec(context.Background(), "DROP DATABASE "+quoted+" WITH (FORCE)")
		if err != nil {
			t.Error(err)
		}
	})
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal("invalid test database config")
	}
	cfg.Database = name
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal("fresh database connection failed")
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}
func TestFreshBaselineAndHistory(t *testing.T) {
	c := testDatabase(t)
	ctx := context.Background()
	var version, pathBefore, pathAfter string
	c.QueryRow(ctx, "SHOW server_version").Scan(&version)
	t.Log("PostgreSQL", version)
	c.QueryRow(ctx, "SHOW search_path").Scan(&pathBefore)
	if n, err := Apply(ctx, c, Files); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	c.QueryRow(ctx, "SHOW search_path").Scan(&pathAfter)
	if pathBefore != pathAfter {
		t.Fatal("search_path leaked", pathAfter)
	}
	var tables []string
	rows, err := c.Query(ctx, "SELECT tablename FROM pg_tables WHERE schemaname='project' ORDER BY tablename")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var s string
		rows.Scan(&s)
		tables = append(tables, s)
	}
	rows.Close()
	want := []string{"component", "document", "engineer", "event", "maintenance", "maintenance_resource", "metric_source", "metric_type", "node", "node_group", "resource", "telemetry"}
	if !reflect.DeepEqual(tables, want) {
		t.Fatal(tables)
	}
	var dictionary []string
	rows, err = c.Query(ctx, "SELECT code||':'||component_kind||':'||quantity||':'||unit FROM project.metric_type ORDER BY code")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var s string
		rows.Scan(&s)
		dictionary = append(dictionary, s)
	}
	rows.Close()
	if !reflect.DeepEqual(dictionary, []string{"CPU_MEAN_TEMP:cpu:temperature:degC", "CPU_POWER:cpu:power:W", "GPU_CORE_TEMP:gpu:temperature:degC", "GPU_POWER:gpu:power:W", "PSU_INPUT_POWER:psu:power:W"}) {
		t.Fatal(dictionary)
	}
	var triggers, functions, fks int
	if err = c.QueryRow(ctx, "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='project' AND NOT t.tgisinternal").Scan(&triggers); err != nil || triggers != 3 {
		t.Fatal(triggers, err)
	}
	if err = c.QueryRow(ctx, "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='project'").Scan(&functions); err != nil || functions != 2 {
		t.Fatal(functions, err)
	}
	if err = c.QueryRow(ctx, "SELECT count(*) FROM pg_constraint WHERE connamespace='project'::regnamespace AND contype='f' AND confdeltype='r'").Scan(&fks); err != nil || fks != 12 {
		t.Fatal(fks, err)
	}
	fixture := `INSERT INTO project.node_group(code,name,group_type) VALUES('SYNTHETIC','test','cluster');
 INSERT INTO project.node(node_group_id,hostname,node_index) SELECT id,'a01n01',1 FROM project.node_group;
 INSERT INTO project.component(node_id,kind,local_index) SELECT id,'cpu',0 FROM project.node;
 INSERT INTO project.metric_source(component_id,metric_type_id,source_key) SELECT c.id,m.id,'cpu0_power' FROM project.component c,project.metric_type m WHERE m.code='CPU_POWER';
 INSERT INTO project.telemetry(metric_source_id,ts,value) SELECT id,'2022-01-18 08:00:00Z',123.456 FROM project.metric_source;`
	if _, err = c.Exec(ctx, fixture, pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	invalid := []struct{ sql, code string }{
		{"INSERT INTO project.node_group(code,name,group_type) VALUES('bad','bad','topological')", "23514"},
		{"INSERT INTO project.node(node_group_id,hostname,node_index) SELECT id,'bad',19 FROM project.node_group", "23514"},
		{"INSERT INTO project.node(node_group_id,hostname,node_index) VALUES(999,'bad',1)", "23503"},
		{"INSERT INTO project.metric_source(component_id,metric_type_id,source_key) SELECT c.id,m.id,'bad' FROM project.component c,project.metric_type m WHERE m.code='GPU_POWER'", "23514"},
		{"UPDATE project.component SET kind='gpu'", "23514"},
		{"UPDATE project.metric_type SET component_kind='gpu' WHERE code='CPU_POWER'", "23514"},
		{"INSERT INTO project.telemetry(metric_source_id,ts,value) SELECT metric_source_id,ts,value FROM project.telemetry", "23505"},
		{"DELETE FROM project.component", "23503"},
	}
	for _, v := range invalid {
		_, err = c.Exec(ctx, v.sql)
		pgerr, ok := err.(*pgconn.PgError)
		if !ok || pgerr.Code != v.code {
			t.Fatalf("%s: %v", v.sql, err)
		}
	}
	if n, err := Apply(ctx, c, Files); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	var count int
	var value string
	if err = c.QueryRow(ctx, "SELECT count(*),min(value)::text FROM project.telemetry").Scan(&count, &value); err != nil || count != 1 || value != "123.456" {
		t.Fatal(count, value, err)
	}
	baseline, _ := Files.ReadFile("migrations/V1__core_schema.sql")
	tx, _ := c.Begin(ctx)
	_, err = tx.Exec(ctx, string(baseline), pgx.QueryExecModeSimpleProtocol)
	tx.Rollback(ctx)
	if err == nil {
		t.Fatal("untracked repeat succeeded")
	}
	// A failed pending migration rolls back DDL and the history row together.
	upgraded := fstest.MapFS{"migrations/V1__core_schema.sql": {Data: baseline}, "migrations/V2__synthetic.sql": {Data: []byte("CREATE TABLE public.p02_rollback(id int); SELECT 1/0;")}}
	if _, err = Apply(ctx, c, upgraded); err == nil {
		t.Fatal("failed SQL accepted")
	}
	var absent bool
	c.QueryRow(ctx, "SELECT to_regclass('public.p02_rollback') IS NULL").Scan(&absent)
	if !absent {
		t.Fatal("partial DDL committed")
	}
	upgraded["migrations/V2__synthetic.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE public.p02_upgrade(id int);")}
	if n, err := Apply(ctx, c, upgraded); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if n, err := Apply(ctx, c, upgraded); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if _, err := Apply(ctx, c, Files); err == nil {
		t.Fatal("unknown applied version silently ignored")
	}
	upgraded["migrations/V1__core_schema.sql"] = &fstest.MapFile{Data: append(baseline, []byte("\n--changed\n")...)}
	if _, err := Apply(ctx, c, upgraded); err == nil {
		t.Fatal("changed applied checksum accepted")
	}
}
func TestRefuseUntrackedSchema(t *testing.T) {
	c := testDatabase(t)
	ctx := context.Background()
	if _, err := c.Exec(ctx, "CREATE SCHEMA project; CREATE TABLE project.keep_me(id int)"); err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(ctx, c, Files); err == nil {
		t.Fatal("silently adopted untracked schema")
	}
	var kept bool
	c.QueryRow(ctx, "SELECT to_regclass('project.keep_me') IS NOT NULL").Scan(&kept)
	if !kept {
		t.Fatal("existing data altered")
	}
}
