package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Compare only INSERT construction; BenchmarkWriteBatch includes validation,
// equality checks and COMMIT. Reproduce: TEST_DATABASE_URL=... go test -run '^$'
// -bench 'Benchmark(InsertStrategy|WriteBatch)' -benchtime=3x -count=1 .
func BenchmarkInsertStrategy(b *testing.B) {
	for _, strategy := range []string{"unnest", "values"} {
		b.Run(strategy, func(b *testing.B) {
			c, _ := archiveDB(b)
			ctx := context.Background()
			w, e := New(ctx, c, 1)
			if e != nil {
				b.Fatal(e)
			}
			sources, e := w.SourceIDs("a01n01")
			if e != nil {
				b.Fatal(e)
			}
			const size = 1024
			ids := make([]int32, size)
			times := make([]time.Time, size)
			values := make([]string, size)
			args := make([]any, size*3)
			var sql strings.Builder
			sql.WriteString(`INSERT INTO project.telemetry(metric_source_id,ts,value) VALUES `)
			for j := 0; j < size; j++ {
				if j > 0 {
					sql.WriteByte(',')
				}
				fmt.Fprintf(&sql, "($%d,$%d,$%d::text::numeric)", 3*j+1, 3*j+2, 3*j+3)
				ids[j] = sources[0]
				values[j] = "1.2345678901234567"
			}
			sql.WriteString(` ON CONFLICT(metric_source_id,ts) DO NOTHING`)
			base := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := range times {
					times[j] = base.Add(time.Duration(i*size+j) * time.Second)
					args[3*j], args[3*j+1], args[3*j+2] = ids[j], times[j], values[j]
				}
				if strategy == "unnest" {
					_, e = c.Exec(ctx, `INSERT INTO project.telemetry(metric_source_id,ts,value) SELECT id,ts,v::numeric FROM unnest($1::int[],$2::timestamptz[],$3::text[]) x(id,ts,v) ORDER BY id,ts ON CONFLICT(metric_source_id,ts) DO NOTHING`, ids, times, values)
				} else {
					_, e = c.Exec(ctx, sql.String(), args...)
				}
				if e != nil {
					b.Fatal(e)
				}
			}
			b.ReportMetric(float64(b.N*size)/b.Elapsed().Seconds(), "measurements/s")
		})
	}
}
