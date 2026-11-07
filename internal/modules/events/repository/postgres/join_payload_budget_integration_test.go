package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type payloadReadCounter struct{ count atomic.Int64 }

func (t *payloadReadCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(d.SQL, "SELECT envelope FROM cad_v2.master_tick_fragments") {
		t.count.Add(1)
	}
	return ctx
}
func (*payloadReadCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// Partial ticks and duplicate deliveries must not cause quadratic payload reads.
// Racing duplicate last fragments materialize one durable final set and read it once.
func TestJoinPayloadReadBudgetWithConcurrentLastDuplicate(t *testing.T) {
	for _, groups := range []int{4, 8, 16} {
		t.Run(fmt.Sprint(groups), func(t *testing.T) {
			pool := postgrestest.New(t)
			ctx := context.Background()
			fixture := workerfixture.New(t, 1, 0)
			fixture.Options.Groups = groups
			first, err := group.NewGroupProcessor(fixture.Bundle, fixture.Options)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := first.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			r := checkpoints.Registration{Identity: snapshot.Identity, Binding: fixture.Options.Binding, ClusterID: "p14-budget", TopicID: "p14-budget", ConsumerGroup: "p14-budget", OutputTransport: "http_push", Diagnostics: true, StartOffsets: map[string]int64{}}
			for p := 0; p < 16; p++ {
				r.StartOffsets[fmt.Sprint(p)] = 0
			}
			if err := checkpoints.Register(ctx, pool, r); err != nil {
				t.Fatal(err)
			}
			counter := &payloadReadCounter{}
			cfg := pool.Config()
			cfg.ConnConfig.Tracer = counter
			traced, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer traced.Close()
			service := usecase.Master{Store: repo.MasterStore{DB: traced}}
			var last []byte
			for g := 1; g <= groups; g++ {
				options := fixture.Options
				options.GroupID = fmt.Sprintf("a%02d", g)
				processor, err := group.NewGroupProcessor(fixture.Bundle, options)
				if err != nil {
					t.Fatal(err)
				}
				records := map[string]telemetry.TelemetryRecord{}
				for h := 1; h <= 18; h++ {
					record := workerfixture.Record(options.GroupID, h, options.Window.Start, 2)
					records[record.Hostname] = record
				}
				result, err := processor.Step(telemetry.GroupTick{Binding: options.Binding, GroupID: options.GroupID, Timestamp: options.Window.Start, Records: records, ValidRecords: 18})
				if err != nil {
					t.Fatal(err)
				}
				fragment, err := domain.TickFragment(options.Binding.ExecutionID, r.Identity, result)
				if err != nil {
					t.Fatal(err)
				}
				last, err = json.Marshal(fragment)
				if err != nil {
					t.Fatal(err)
				}
				if g < groups {
					for duplicate := 0; duplicate < 2; duplicate++ {
						if _, err := service.Accept(ctx, last); err != nil {
							t.Fatal(err)
						}
					}
					if got := counter.count.Load(); got != 0 {
						t.Fatalf("partial timestamp read payloads %d times", got)
					}
				}
			}
			completed := make(chan error, 2)
			for i := 0; i < 2; i++ {
				go func() { _, err := service.Accept(ctx, last); completed <- err }()
			}
			for i := 0; i < 2; i++ {
				if err := <-completed; err != nil {
					t.Fatal(err)
				}
			}
			if got := counter.count.Load(); got != 1 {
				t.Fatalf("full timestamp payload reads=%d, want 1", got)
			}
			var finals int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM cad_v2.master_results").Scan(&finals); err != nil || finals != groups {
				t.Fatalf("finals=%d err=%v", finals, err)
			}
		})
	}
}
