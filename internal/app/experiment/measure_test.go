package experiment

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMeasurementJoinClocksAndQuantiles(t *testing.T) {
	root := t.TempDir()
	at := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	run := "run"
	tick := at.Add(-time.Hour)
	write := func(negative bool) {
		f, err := os.Create(filepath.Join(root, "worker.log"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		e := json.NewEncoder(f)
		for i := 1; i <= 4; i++ {
			g := fmt.Sprintf("a%02d", i)
			id := domain.MessageID(run, g, "tick_fragment", tick.Format(time.RFC3339), 1)
			source := at
			if negative {
				source = at.Add(20 * time.Second)
			}
			for _, row := range []trace{{Message: "tick measurement", Group: g, Tick: tick, Received: at.Add(time.Second), Computed: at.Add(2 * time.Second), Source: source, Compute: .01}, {Message: "tick durable", Group: g, Tick: tick, Durability: 1, Confirmed: at.Add(3 * time.Second)}, {Message: "output attempt", MessageID: id, Sent: at.Add(4 * time.Second)}, {Message: "result measurement", Group: g, Tick: tick, MessageID: id, Kind: "tick_fragment", Received: at.Add(5 * time.Second), Committed: at.Add(time.Duration(6+i) * time.Second), Accepted: true}} {
				if err := e.Encode(row); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	write(false)
	m, err := measureLogs(root, run, 4, newSampler())
	if err != nil {
		t.Fatal(err)
	}
	if m.EndToEnd.Count != 4 || m.EndToEnd.P50 != 10 || m.Compute.P99 != .01 || m.Transport.P95 != 1 || m.Throughput != .4 {
		t.Fatal("incorrect component/join measurement", m)
	}
	if _, err := measureLogs(root, run, 4, newSampler(), 8); err == nil {
		t.Fatal("incomplete timing trace accepted")
	}
	write(true)
	if _, err := measureLogs(root, run, 4, newSampler()); err == nil {
		t.Fatal("negative clock samples accepted")
	}
	d := distribution([]float64{4, 1, 3, 2})
	if d.P50 != 2 || d.P95 != 4 || d.Mean != 2.5 {
		t.Fatal("quantile definition drift", d)
	}
}

func TestSamplerIncludesNonzeroPartitionLag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"gauges":{"kafka_lag_records_a01":17,"kafka_lag_records_a16":25,"other":999}}`))
	}))
	defer server.Close()
	s := newSampler()
	s.Add("worker", server.URL)
	s.capture(context.Background())
	if got := s.Samples()[0].Lag; got != 42 {
		t.Fatalf("lost input Kafka lag: got %v want 42", got)
	}
}

func TestReanalysisCannotUpgradeLegacyLag(t *testing.T) {
	for _, original := range []*Measurements{nil, {KafkaLagMax: 0}} {
		if got := reanalysisLagStatus(original); got != "NOT_VERIFIED_LEGACY_SAMPLER" {
			t.Fatal("legacy missing gauges silently certified", got)
		}
	}
	if got := reanalysisLagStatus(&Measurements{KafkaLagStatus: "MEASURED_PARTITION_HIGH_WATERMARK_1HZ", KafkaLagMax: 42}); got != "MEASURED_PARTITION_HIGH_WATERMARK_1HZ" {
		t.Fatal("modern quality metadata lost", got)
	}
}
