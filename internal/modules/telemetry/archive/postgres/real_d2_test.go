package postgres

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"math/bits"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	prepared "github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/parquet"
	"github.com/jackc/pgx/v5"
)

// Reproduce with TEST_DATABASE_URL (CREATEDB role) and ARCHIVE_REAL_PARQUET set
// to the original full-day prepared Summit F18-v1 file. Only D2 is archived.
// Memory is bounded by 1024 records; no production replayer or Kafka is involved.
func TestRealD2Archive(t *testing.T) {
	path := os.Getenv("ARCHIVE_REAL_PARQUET")
	if path == "" {
		t.Skip("REAL_D2_ARCHIVE: NOT_RUN; ARCHIVE_REAL_PARQUET required")
	}
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Fatal("REAL_D2_ARCHIVE requires TEST_DATABASE_URL")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, err = io.Copy(hash, f)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	if fmt.Sprintf("%x", hash.Sum(nil)) != "cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7" {
		t.Fatal("original prepared Summit asset checksum differs")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal("test PostgreSQL connection failed")
	}
	defer admin.Close(context.Background())
	var token [8]byte
	if _, err = rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("p04db_real_%x", token)
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" TEMPLATE template0"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, e := admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); e != nil {
			t.Error(e)
		}
	}()
	cfg := admin.Config().Copy()
	cfg.Database = name
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal("isolated PostgreSQL connection failed")
	}
	defer c.Close(context.Background())
	if n, e := db.Apply(ctx, c, db.Files); e != nil || n != 5 {
		t.Fatal(n, e)
	}
	w, err := New(ctx, c, 1024)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	end := start.Add(8 * time.Hour)
	var wantRows int64
	for pass := 0; pass < 2; pass++ {
		r, err := prepared.OpenParquet(path)
		if err != nil {
			t.Fatal(err)
		}
		batch := make([]domain.TelemetryRecord, 0, 1024)
		nodes, groups := map[string]bool{}, map[string]bool{}
		var records, available, inserted, duplicates int64
		began := time.Now()
		var writeTime time.Duration
		flush := func() {
			if len(batch) == 0 {
				return
			}
			beganWrite := time.Now()
			result, e := w.WriteBatch(ctx, batch)
			writeTime += time.Since(beganWrite)
			if e != nil {
				t.Fatal(e)
			}
			inserted += int64(result.Inserted)
			duplicates += int64(result.Duplicates)
			if pass == 0 {
				verifyArchiveReadback(t, ctx, c, w, batch)
			}
			batch = batch[:0]
		}
		for {
			record, e := r.Next(ctx)
			if e == io.EOF {
				break
			}
			if e != nil {
				r.Close()
				t.Fatal(e)
			}
			if record.Timestamp.Before(start) || !record.Timestamp.Before(end) {
				continue
			}
			records++
			available += int64(bits.OnesCount32(record.AvailabilityMask))
			nodes[record.Hostname] = true
			groups[record.GroupID] = true
			batch = append(batch, record)
			if len(batch) == cap(batch) {
				flush()
			}
		}
		flush()
		if err = r.Close(); err != nil {
			t.Fatal(err)
		}
		if records != 829440 || len(nodes) != 288 || len(groups) != 16 {
			t.Fatalf("D2 identity counts: records=%d nodes=%d groups=%d", records, len(nodes), len(groups))
		}
		if pass == 0 {
			wantRows = available
			if inserted != available || duplicates != 0 {
				t.Fatal(inserted, available, duplicates)
			}
		} else if available != wantRows || inserted != 0 || duplicates != wantRows {
			t.Fatal(available, wantRows, inserted, duplicates)
		}
		var actual int64
		if err = c.QueryRow(ctx, `SELECT count(*) FROM project.telemetry`).Scan(&actual); err != nil || actual != wantRows {
			t.Fatal(actual, wantRows, err)
		}
		t.Logf("REAL_D2_ARCHIVE pass=%d records=%d measurements=%d inserted=%d duplicates=%d elapsed=%s writer=%s writer_rows/s=%.0f", pass+1, records, available, inserted, duplicates, time.Since(began), writeTime, float64(available)/writeTime.Seconds())
	}
}

func verifyArchiveReadback(t *testing.T, ctx context.Context, c *pgx.Conn, w *Writer, batch []domain.TelemetryRecord) {
	t.Helper()
	want := make(map[measurementKey]float64, len(batch)*domain.FeatureCount)
	var ids []int32
	var times []time.Time
	for _, r := range batch {
		sources, err := w.SourceIDs(r.Hostname)
		if err != nil {
			t.Fatal(err)
		}
		for f, v := range r.Features() {
			if r.AvailabilityMask&(1<<f) != 0 {
				key := measurementKey{sources[f], r.Timestamp.UTC()}
				want[key] = v
				ids = append(ids, key.source)
				times = append(times, key.ts)
			}
		}
	}
	rows, err := c.Query(ctx, `SELECT t.metric_source_id,t.ts,t.value::text FROM project.telemetry t
 JOIN unnest($1::int[],$2::timestamptz[]) AS x(id,ts) ON t.metric_source_id=x.id AND t.ts=x.ts`, ids, times)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var key measurementKey
		var text string
		if err = rows.Scan(&key.source, &key.ts, &text); err != nil {
			t.Fatal(err)
		}
		key.ts = key.ts.UTC()
		value, e := strconv.ParseFloat(text, 64)
		if e != nil {
			t.Fatal(e)
		}
		expected, ok := want[key]
		if !ok || value != expected {
			t.Fatalf("readback differs: %v %s expected %g", key, text, expected)
		}
		delete(want, key)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(want) != 0 {
		t.Fatalf("missing %d measurements", len(want))
	}
}
