package postgres

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

func TestAvailabilityAndFloat64RoundTrip(t *testing.T) {
	c, _ := archiveDB(t)
	ctx := context.Background()
	w, e := New(ctx, c, 2)
	if e != nil {
		t.Fatal(e)
	}
	ts := time.Date(2022, 1, 18, 8, 0, 0, 123456000, time.FixedZone("source", 3*3600))
	r := recordAt(ts, "a01n01", domain.FullAvailabilityMask, 1)
	for _, mutate := range []func(*domain.TelemetryRecord){
		func(r *domain.TelemetryRecord) { r.AvailabilityMask |= 1 << 18 },
		func(r *domain.TelemetryRecord) { r.AvailabilityMask ^= 1 },
		func(r *domain.TelemetryRecord) { r.Valid = false },
		func(r *domain.TelemetryRecord) { r.PSUPower[0] = math.NaN() },
		func(r *domain.TelemetryRecord) { r.GPUTemp[5] = math.Inf(1) },
		func(r *domain.TelemetryRecord) { r.NodeIndex = 2 },
		func(r *domain.TelemetryRecord) { r.Timestamp = time.Time{} },
	} {
		bad := r
		mutate(&bad)
		if _, e = w.WriteBatch(ctx, []domain.TelemetryRecord{bad}); !errors.Is(e, ErrInvalidRecord) {
			t.Fatalf("invalid record accepted: %+v %v", bad, e)
		}
	}
	var values [domain.FeatureCount]float64
	samples := []float64{math.SmallestNonzeroFloat64, math.MaxFloat64, math.Nextafter(1, 2), 0.1, -0.1, math.Copysign(0, -1)}
	for i := range values {
		values[i] = samples[i%len(samples)]
	}
	r.SetFeatures(values)
	if result, e := w.WriteBatch(ctx, []domain.TelemetryRecord{r}); e != nil || result.Inserted != 18 {
		t.Fatal(result, e)
	}
	verifyArchiveReadback(t, ctx, c, w, []domain.TelemetryRecord{r})
	// Same instant in another zone is the same natural key. Distinct microseconds
	// remain distinct. -0 and +0 have the same V1 numeric semantics.
	r.Timestamp = ts.UTC()
	r.GPUPower[1] = 0
	if result, e := w.WriteBatch(ctx, []domain.TelemetryRecord{r}); e != nil || result.Duplicates != 18 {
		t.Fatal(result, e)
	}
	r.Timestamp = r.Timestamp.Add(time.Microsecond)
	if result, e := w.WriteBatch(ctx, []domain.TelemetryRecord{r}); e != nil || result.Inserted != 18 {
		t.Fatal(result, e)
	}
}

func TestBootstrapRejectsContradictionsAndPreservesHistory(t *testing.T) {
	for _, tt := range []struct{ name, sql string }{
		{"node group", `UPDATE project.node SET node_group_id=(SELECT id FROM project.node_group WHERE code='a02') WHERE hostname='a01n01'`},
		{"node index", `UPDATE project.node SET node_index=2 WHERE hostname='a01n01'`},
		{"group parent", `INSERT INTO project.node_group(code,name,group_type) VALUES('OTHER','OTHER','cluster'); UPDATE project.node_group SET parent_id=(SELECT id FROM project.node_group WHERE code='OTHER') WHERE code='a01'`},
		{"metric unit", `UPDATE project.metric_type SET unit='kW' WHERE code='CPU_POWER'`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := archiveDB(t)
			ctx := context.Background()
			w, e := New(ctx, c, 1)
			if e != nil {
				t.Fatal(e)
			}
			r := recordAt(time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC), "a01n01", domain.FullAvailabilityMask, 12)
			if _, e = w.WriteBatch(ctx, []domain.TelemetryRecord{r}); e != nil {
				t.Fatal(e)
			}
			if _, e = c.Exec(ctx, tt.sql); e != nil {
				t.Fatal(e)
			}
			if e = Bootstrap(ctx, c); !errors.Is(e, ErrSchema) {
				t.Fatal(e)
			}
			verifyArchiveReadback(t, ctx, c, w, []domain.TelemetryRecord{r})
		})
	}
	c, _ := archiveDB(t)
	ctx := context.Background()
	w, e := New(ctx, c, 1)
	if e != nil {
		t.Fatal(e)
	}
	r := recordAt(time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC), "a16n18", domain.FullAvailabilityMask, 15)
	if _, e = w.WriteBatch(ctx, []domain.TelemetryRecord{r}); e != nil {
		t.Fatal(e)
	}
	if e = Bootstrap(ctx, c); e != nil {
		t.Fatal(e)
	}
	verifyArchiveReadback(t, ctx, c, w, []domain.TelemetryRecord{r})
	var invented int
	if e = c.QueryRow(ctx, `SELECT count(*) FROM project.component WHERE model IS NOT NULL`).Scan(&invented); e != nil || invented != 0 {
		t.Fatal(invented, e)
	}
}

func TestDictionaryUsesDatabaseIDsAndClassifiesStaleSource(t *testing.T) {
	c, _ := archiveDB(t)
	ctx := context.Background()
	w, e := New(ctx, c, 1)
	if e != nil {
		t.Fatal(e)
	}
	ids, e := w.SourceIDs("a01n01")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.Exec(ctx, `WITH old AS (DELETE FROM project.metric_source WHERE id=$1 RETURNING component_id,metric_type_id,source_key)
 INSERT INTO project.metric_source(id,component_id,metric_type_id,source_key) OVERRIDING SYSTEM VALUE
 SELECT 0,component_id,metric_type_id,source_key FROM old`, ids[0]); e != nil {
		t.Fatal(e)
	}
	w, e = New(ctx, c, 1)
	if e != nil {
		t.Fatal(e)
	}
	ids, e = w.SourceIDs("a01n01")
	if e != nil || ids[0] != 0 {
		t.Fatal(ids, e)
	}
	r := recordAt(time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC), "a01n01", 1, 1)
	if _, e = w.WriteBatch(ctx, []domain.TelemetryRecord{r}); e != nil {
		t.Fatal(e)
	}
	verifyArchiveReadback(t, ctx, c, w, []domain.TelemetryRecord{r})
	if _, e = c.Exec(ctx, `DELETE FROM project.metric_source WHERE id=$1`, ids[1]); e != nil {
		t.Fatal(e)
	}
	r = recordAt(r.Timestamp.Add(time.Second), "a01n01", 2, 2)
	if result, e := w.WriteBatch(ctx, []domain.TelemetryRecord{r}); !errors.Is(e, ErrUnknownSource) || result != (Result{}) {
		t.Fatal(result, e)
	}
}
