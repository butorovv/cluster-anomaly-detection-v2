package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	masterapp "github.com/butorovv/cluster-anomaly-detection-v2/internal/app/master"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/httpinput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"
)

func diagnosticOracle(t *testing.T, r *workerRig) map[string]domain.GroupResult {
	t.Helper()
	groups := r.fixture.Options.Groups
	journals := make([]*group.RBI2Journal, groups)
	for i := 0; i < groups; i++ {
		g := fmt.Sprintf("a%02d", i+1)
		hosts := []string{}
		for h := 1; h <= 18; h++ {
			hosts = append(hosts, fmt.Sprintf("%sn%02d", g, h))
		}
		j, err := group.NewRBI2Journal(r.store.Registration.Identity, g, hosts)
		if err != nil {
			t.Fatal(err)
		}
		journals[i] = j
	}
	expected := map[string]domain.GroupResult{}
	for tick := 0; tick < 6; tick++ {
		raw := make([]group.Raw, groups)
		for i := 0; i < groups; i++ {
			g := fmt.Sprintf("a%02d", i+1)
			messages := r.messages(g, tick)
			source := group.Tick{Timestamp: messages[0].Timestamp, Records: mapRecord(messages)}
			row, err := r.fixture.Bundle.EvaluateGroup(g, source, nil)
			if err != nil {
				t.Fatal(err)
			}
			raw[i] = row
		}
		group.AnnotateOutside(raw)
		for i, row := range raw {
			temporal, evidence, _, err := journals[i].Step(row)
			if err != nil {
				t.Fatal(err)
			}
			key := row.Group + "/" + row.Timestamp.Format(time.RFC3339Nano)
			expected[key] = domain.GroupResult{Raw: row, Temporal: temporal, Evidence: evidence}
		}
	}
	return expected
}
func TestWorkerCompleteDiagnosticJoin(t *testing.T) {
	for _, groups := range []int{4, 8, 16} {
		for _, mode := range []string{"polling", "http_push", "kafka"} {
			t.Run(fmt.Sprintf("g%d-%s", groups, mode), func(t *testing.T) {
				// Do not generate fixtures or touch external services unless opted in.
				if !workerTestsEnabled(t) {
					return
				}
				fixture := workerfixture.New(t, 6, 30)
				fixture.Options.Groups = groups
				rig := workerIntegrationFixture(t, 30, mode, &fixture, true)
				store := restrictedMaster(t, rig)
				service := usecase.Master{Store: store}
				server := httptest.NewServer(httpinput.Handler(service, discardLog()))
				defer server.Close()
				masterCtx, stopMaster := context.WithCancel(context.Background())
				defer stopMaster()
				masterDone := make(chan error, 1)
				if mode == "polling" {
					go func() {
						masterDone <- (usecase.Poller{ExecutionID: rig.fixture.Options.Binding.ExecutionID, Groups: groups, Interval: 10 * time.Millisecond, Store: store, Master: service, Logger: discardLog()}).Run(masterCtx)
					}()
				} else if mode == "kafka" {
					rig.bindResults(t, "p10-"+rig.group)
					go func() {
						masterDone <- masterapp.RunKafka(masterCtx, rig.broker.Seeds, "p10-"+rig.group, rig.fixture.Options.Binding.ExecutionID, true, store, service)
					}()
				} else {
					go func() { <-masterCtx.Done(); masterDone <- nil }()
				}
				defer func() {
					stopMaster()
					select {
					case err := <-masterDone:
						if err != nil && !errors.Is(err, context.Canceled) {
							t.Error(err)
						}
					case <-time.After(15 * time.Second):
						t.Error("Master shutdown timeout")
					}
				}()
				_, cancel, done := rig.startDelivery(t, mode, server.URL)
				defer stopWorker(t, cancel, done)
				for _, m := range rig.messages("a01", 0) {
					rig.send(t, m)
				}
				waitWorker(t, 20*time.Second, func() bool {
					var n int
					err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_tick_fragments`).Scan(&n)
					return err == nil && n == 1
				})
				var finals int
				if err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_results`).Scan(&finals); err != nil || finals != 0 {
					t.Fatal("incomplete outside published as final", finals, err)
				}
				for g := 2; g <= 16; g++ {
					for _, m := range rig.messages(fmt.Sprintf("a%02d", g), 0) {
						rig.send(t, m)
					}
				}
				rig.waitRevision(t, 1)
				for tick := 1; tick < 6; tick++ {
					rig.sendTick(t, tick)
					rig.waitRevision(t, int64(tick+1))
				}
				waitWorker(t, 35*time.Second, func() bool {
					var n int
					err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_results`).Scan(&n)
					return err == nil && n == groups*6
				})
				expected := diagnosticOracle(t, rig)
				rows, err := rig.pool.Query(context.Background(), `SELECT group_id,tick,payload FROM cad_v2.master_results ORDER BY tick,group_id`)
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				for rows.Next() {
					var g string
					var at time.Time
					var b []byte
					if err := rows.Scan(&g, &at, &b); err != nil {
						t.Fatal(err)
					}
					var got domain.GroupResult
					if err := json.Unmarshal(b, &got); err != nil {
						t.Fatal(err)
					}
					key := g + "/" + at.UTC().Format(time.RFC3339Nano)
					if !reflect.DeepEqual(got, expected[key]) {
						wantJSON, _ := json.Marshal(expected[key])
						t.Fatalf("diagnostic parity mismatch %s\ngot=%s\nwant=%s", key, b, wantJSON)
					}
					count++
				}
				rows.Close()
				if rows.Err() != nil || count != groups*6 {
					t.Fatal(count, rows.Err())
				}
				assertMaster(t, rig, store)
				var joined int
				if err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_tick_joins`).Scan(&joined); err != nil || joined != 6 {
					t.Fatal("join not exactly once per timestamp", joined, err)
				}
				t.Logf("groups=%d mode=%s complete_ticks=%d exact_raw_temporal_evidence=true", groups, mode, count)
			})
		}
	}
}
func mapRecord(messages []telemetry.InputMessage) map[string]telemetry.TelemetryRecord {
	result := map[string]telemetry.TelemetryRecord{}
	for _, m := range messages {
		if m.Record != nil {
			result[m.Record.Hostname] = *m.Record
		}
	}
	return result
}
func workerTestsEnabled(t *testing.T) bool {
	t.Helper()
	if os.Getenv("P07_WORKER_INTEGRATION") != "1" {
		t.Skip("real Worker integration requires P07_WORKER_INTEGRATION=1")
		return false
	}
	return true
}
