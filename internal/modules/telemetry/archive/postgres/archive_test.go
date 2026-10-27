package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/jackc/pgx/v5"
)

func archiveDB(t testing.TB) (*pgx.Conn, *pgx.Conn) {
	t.Helper()
	u := os.Getenv("TEST_DATABASE_URL")
	if u == "" {
		t.Skip("TEST_DATABASE_URL unset; PostgreSQL integration skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, u)
	if err != nil {
		t.Fatal("TEST_DATABASE_URL connection failed (credentials omitted)")
	}
	var b [8]byte
	if _, err = rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	name := "archive_test_" + hex.EncodeToString(b[:])
	q := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+q+" TEMPLATE template0"); err != nil {
		admin.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer admin.Close(context.Background())
		if _, e := admin.Exec(context.Background(), "DROP DATABASE "+q+" WITH (FORCE)"); e != nil {
			t.Error(e)
		}
	})
	cfg, err := pgx.ParseConfig(u)
	if err != nil {
		t.Fatal("invalid TEST_DATABASE_URL")
	}
	cfg.Database = name
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	if n, err := db.Apply(ctx, conn, db.Files); err != nil || n != 5 {
		t.Fatalf("apply V1-V5: %d %v", n, err)
	}
	return conn, conn
}

func recordAt(ts time.Time, host string, mask uint32, value float64) domain.TelemetryRecord {
	vals := [domain.FeatureCount]float64{}
	for i := range vals {
		vals[i] = math.NaN()
		if mask&(1<<i) != 0 {
			vals[i] = value + float64(i)
		}
	}
	group, index, _ := topology.ParseHostname(host)
	r := domain.TelemetryRecord{Timestamp: ts, Hostname: host, GroupID: group, NodeIndex: index, AvailabilityMask: mask, Valid: mask == domain.FullAvailabilityMask}
	r.SetFeatures(vals)
	return r
}

func TestWriterBatchIdempotenceConflictValidationAndCancellation(t *testing.T) {
	p, _ := archiveDB(t)
	ctx := context.Background()
	w, err := New(ctx, p, 3)
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2025, 1, 2, 3, 4, 5, 123000, time.UTC)
	r := recordAt(ts, "a01n01", domain.FullAvailabilityMask, 1)
	cadBefore := cadSnapshot(t, p)
	got, err := w.WriteBatch(ctx, []domain.TelemetryRecord{r, r})
	if err != nil {
		t.Fatal(err)
	}
	if got.Records != 2 || got.Measurements != 36 || got.Inserted != 18 || got.Duplicates != 18 {
		t.Fatalf("within-batch duplicate result: %+v", got)
	}
	got, err = w.WriteBatch(ctx, []domain.TelemetryRecord{r})
	if err != nil || got.Inserted != 0 || got.Duplicates != 18 {
		t.Fatalf("replay: %+v %v", got, err)
	}
	changed := r
	changed.CPUPower[0] += 100
	if _, err = w.WriteBatch(ctx, []domain.TelemetryRecord{r, changed}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting duplicate within input: %v", err)
	}
	if _, err = w.WriteBatch(ctx, []domain.TelemetryRecord{changed}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict error=%v", err)
	}
	if _, err = w.WriteBatch(ctx, []domain.TelemetryRecord{r, r, r, r}); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("limit error=%v", err)
	}
	for name, bad := range map[string]domain.TelemetryRecord{
		"host":  {Timestamp: ts, Hostname: "bad", GroupID: "a01", NodeIndex: 1},
		"group": recordAt(ts, "a01n01", 0, 0),
	} {
		if name == "group" {
			bad.GroupID = "wrong"
		}
		want := ErrInvalidRecord
		if name == "host" {
			want = ErrUnknownSource
		}
		if _, e := w.WriteBatch(ctx, []domain.TelemetryRecord{bad}); !errors.Is(e, want) {
			t.Errorf("%s: %v", name, e)
		}
	}
	if _, err = w.WriteBatch(ctx, nil); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
	partial := recordAt(ts.Add(time.Minute), "a01n01", 1<<7, 5)
	got, err = w.WriteBatch(ctx, []domain.TelemetryRecord{partial})
	if err != nil || got.Measurements != 1 || got.Inserted != 1 {
		t.Fatalf("partial record: %+v %v", got, err)
	}
	noMeasurements := recordAt(ts.Add(2*time.Minute), "a01n01", 0, 0)
	got, err = w.WriteBatch(ctx, []domain.TelemetryRecord{noMeasurements})
	if err != nil || got.Records != 1 || got.Measurements != 0 || got.Inserted != 0 {
		t.Fatalf("empty availability: %+v %v", got, err)
	}
	ctxCancel, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = w.WriteBatch(ctxCancel, []domain.TelemetryRecord{r}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	var n int
	if err = p.QueryRow(ctx, "SELECT count(*) FROM project.telemetry").Scan(&n); err != nil || n != 19 {
		t.Fatalf("rows after rejected calls=%d err=%v", n, err)
	}
	if !reflect.DeepEqual(cadBefore, cadSnapshot(t, p)) {
		t.Fatal("cad_v2 content changed")
	}
}

func cadSnapshot(t *testing.T, c *pgx.Conn) map[string]string {
	t.Helper()
	ctx := context.Background()
	rows, e := c.Query(ctx, `SELECT tablename FROM pg_tables WHERE schemaname='cad_v2' ORDER BY tablename`)
	if e != nil {
		t.Fatal(e)
	}
	var names []string
	for rows.Next() {
		var name string
		if e = rows.Scan(&name); e != nil {
			t.Fatal(e)
		}
		names = append(names, name)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	result := map[string]string{}
	for _, name := range names {
		var content string
		if e = c.QueryRow(ctx, `SELECT coalesce(jsonb_agg(v ORDER BY v::text),'[]'::jsonb)::text FROM (SELECT to_jsonb(t) v FROM `+pgx.Identifier{"cad_v2", name}.Sanitize()+` t) x`).Scan(&content); e != nil {
			t.Fatal(e)
		}
		result[name] = content
	}
	return result
}

func TestWriterRejectsInvalidLimitsAndTimestampPrecision(t *testing.T) {
	p, _ := archiveDB(t)
	for _, limit := range []int{-1, 0, MaxBatchRecords + 1} {
		if _, err := New(context.Background(), p, limit); !errors.Is(err, ErrInvalidRecord) {
			t.Errorf("limit %d: %v", limit, err)
		}
	}
	w, err := New(context.Background(), p, 1)
	if err != nil {
		t.Fatal(err)
	}
	r := recordAt(time.Date(2025, 1, 1, 0, 0, 0, 1, time.UTC), "a01n01", domain.FullAvailabilityMask, 1)
	if _, err = w.WriteBatch(context.Background(), []domain.TelemetryRecord{r}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("sub-microsecond timestamp: %v", err)
	}
}

func TestWriterRollsBackEarlierChunksOnConflict(t *testing.T) {
	p, _ := archiveDB(t)
	w, err := New(context.Background(), p, domain.FeatureCount*60)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]domain.TelemetryRecord, 0, 60)
	base := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 60; i++ {
		rows = append(rows, recordAt(base.Add(time.Duration(i)*time.Minute), "a01n01", domain.FullAvailabilityMask, float64(i)))
	}
	if _, err = w.WriteBatch(context.Background(), rows[59:]); err != nil {
		t.Fatal(err)
	}
	ids, err := w.SourceIDs("a01n01")
	if err != nil {
		t.Fatal(err)
	}
	lastFeature := 0
	for i, id := range ids {
		if id > ids[lastFeature] {
			lastFeature = i
		}
	}
	values := rows[59].Features()
	values[lastFeature]++
	rows[59].SetFeatures(values)
	if _, err = w.WriteBatch(context.Background(), rows); !errors.Is(err, ErrConflict) {
		t.Fatalf("late conflict: %v", err)
	}
	var n int
	if err = p.QueryRow(context.Background(), "SELECT count(*) FROM project.telemetry").Scan(&n); err != nil || n != 18 {
		t.Fatalf("rollback count=%d err=%v", n, err)
	}
}

func BenchmarkWriteBatch(b *testing.B) {
	for _, size := range []int{18, 288, 1024} {
		for _, replay := range []bool{false, true} {
			b.Run(fmt.Sprintf("%d/replay=%t", size, replay), func(b *testing.B) {
				p, _ := archiveDB(b)
				w, e := New(context.Background(), p, size)
				if e != nil {
					b.Fatal(e)
				}
				records := make([]domain.TelemetryRecord, size)
				for i := range records {
					records[i] = recordAt(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i)*time.Second), "a01n01", domain.FullAvailabilityMask, float64(i))
				}
				if replay {
					if _, e = w.WriteBatch(context.Background(), records); e != nil {
						b.Fatal(e)
					}
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if !replay {
						for j := range records {
							records[j].Timestamp = records[j].Timestamp.Add(time.Duration(size) * time.Second)
						}
					}
					result, e := w.WriteBatch(context.Background(), records)
					if e != nil || (!replay && result.Inserted != size*18) || (replay && result.Duplicates != size*18) {
						b.Fatal(result, e)
					}
				}
				b.ReportMetric(float64(b.N*size*18)/b.Elapsed().Seconds(), "measurements/s")
			})
		}
	}
}
