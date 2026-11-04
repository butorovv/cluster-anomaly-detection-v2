package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"strings"
	"testing"
	"time"
)

func TestDurableJoinDuplicateReorderRestart(t *testing.T) {
	for _, groups := range []int{4, 8, 16} {
		for _, delta := range []int{0, 10, 30, 60} {
			t.Run(fmt.Sprintf("g%d-d%d", groups, delta), func(t *testing.T) {
				pool := postgrestest.New(t)
				fixture := workerfixture.New(t, 3, delta)
				fixture.Options.Groups = groups
				first, err := group.NewGroupProcessor(fixture.Bundle, fixture.Options)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := first.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				r := checkpoints.Registration{Identity: snapshot.Identity, Binding: fixture.Options.Binding, ClusterID: "join-test", TopicID: "join-topic", ConsumerGroup: "join", OutputTransport: "polling", Diagnostics: true, StartOffsets: map[string]int64{}}
				for p := 0; p < 16; p++ {
					r.StartOffsets[fmt.Sprint(p)] = 0
				}
				ctx := context.Background()
				if err := checkpoints.Register(ctx, pool, r); err != nil {
					t.Fatal(err)
				}
				fragments := []domain.Envelope{}
				for g := 1; g <= groups; g++ {
					options := fixture.Options
					options.GroupID = fmt.Sprintf("a%02d", g)
					p, err := group.NewGroupProcessor(fixture.Bundle, options)
					if err != nil {
						t.Fatal(err)
					}
					for tick, count := range []int{18, 17, 0} {
						at := options.Window.Start.Add(time.Duration(tick) * telemetry.GridStep)
						records := map[string]telemetry.TelemetryRecord{}
						missing := []string{}
						for h := 1; h <= 18; h++ {
							record := workerfixture.Record(options.GroupID, h, at, 2)
							if h <= count {
								records[record.Hostname] = record
							} else {
								missing = append(missing, record.Hostname)
							}
						}
						result, err := p.Step(telemetry.GroupTick{Binding: options.Binding, GroupID: options.GroupID, Timestamp: at, Records: records, Missing: missing, ValidRecords: count})
						if err != nil {
							t.Fatal(err)
						}
						fragment, err := domain.TickFragment(options.Binding.ExecutionID, r.Identity, result)
						if err != nil {
							t.Fatal(err)
						}
						fragments = append(fragments, fragment)
					}
				}
				store := repo.MasterStore{DB: pool}
				service := usecase.Master{Store: store}
				raw, _ := json.Marshal(fragments[0])
				if _, err := service.Accept(ctx, raw); err != nil {
					t.Fatal(err)
				}
				var finals int
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_results`).Scan(&finals); err != nil || finals != 0 {
					t.Fatal("premature final diagnostic", finals, err)
				}
				// A fresh Master object has no RAM join history. Deliver multiple timestamps
				// backwards and duplicate every fragment, including after final materialization.
				for i := len(fragments) - 1; i >= 0; i-- {
					service = usecase.Master{Store: repo.MasterStore{DB: pool}}
					b, _ := json.Marshal(fragments[i])
					if _, err := service.Accept(ctx, b); err != nil {
						t.Fatal(err)
					}
					receipt, err := service.Accept(ctx, b)
					if err != nil || receipt.Accepted {
						t.Fatal("duplicate fragment not idempotent", receipt, err)
					}
				}
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_results`).Scan(&finals); err != nil || finals != groups*3 {
					t.Fatal(finals, err)
				}
				summary, err := store.Summary(ctx, r.Binding.ExecutionID)
				if err != nil || !summary.Complete || summary.Ticks != int64(groups*3) || summary.PositiveTicks != int64(groups) {
					t.Fatal("stored aggregate mismatch", summary, err)
				}
				cursor := domain.ResultCursor{}
				seen := 0
				for {
					page, err := store.Results(ctx, r.Binding.ExecutionID, cursor, 5)
					if err != nil {
						t.Fatal(err)
					}
					if len(page.Items) == 0 {
						break
					}
					if page.Next.Tick.Before(cursor.Tick) || (page.Next.Tick.Equal(cursor.Tick) && page.Next.GroupID <= cursor.GroupID) {
						t.Fatal("result cursor regression")
					}
					seen += len(page.Items)
					cursor = page.Next
				}
				if seen != groups*3 {
					t.Fatal("result pagination lost rows", seen)
				}
				var joined int
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_tick_joins`).Scan(&joined); err != nil || joined != 3 {
					t.Fatal(joined, err)
				}
				conflict := fragments[0]
				conflict.MessageID = strings.Repeat("f", 64)
				b, _ := json.Marshal(conflict)
				if _, err := service.Accept(ctx, b); err == nil {
					t.Fatal("alternate fragment identity overwrote finalized tick")
				}
				var count int
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_results WHERE payload#>'{raw,if,outside_groups}'='null'::jsonb`).Scan(&count); err != nil || count != 0 {
					t.Fatal("uncomputed outside exposed", count, err)
				}
			})
		}
	}
}
