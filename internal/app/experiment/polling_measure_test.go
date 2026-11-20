package experiment

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

func TestMeasurementRejectsMissingPollingCommitConfirmation(t *testing.T) {
	root := t.TempDir()
	start := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	tick := start.Add(-time.Hour)
	for _, missing := range []bool{false, true} {
		file, err := os.Create(filepath.Join(root, "polling.log"))
		if err != nil {
			t.Fatal(err)
		}
		encoder := json.NewEncoder(file)
		for i := 1; i <= 4; i++ {
			group := fmt.Sprintf("a%02d", i)
			id := domain.MessageID("run", group, "tick_fragment", tick.Format(time.RFC3339), 1)
			rows := []trace{
				{Message: "tick measurement", Group: group, Tick: tick, Source: start, Received: start.Add(time.Second), Computed: start.Add(2 * time.Second), Compute: .01},
				{Message: "tick durable", Group: group, Tick: tick, Confirmed: start.Add(3 * time.Second), Durability: 1},
				{Message: "output attempt", MessageID: id, Sent: start.Add(4 * time.Second)},
			}
			if !missing || i != 4 {
				rows = append(rows, trace{Message: "result measurement", Group: group, Tick: tick, MessageID: id, Kind: "tick_fragment", Received: start.Add(5 * time.Second), Committed: start.Add(6 * time.Second), Accepted: true})
			}
			for _, row := range rows {
				if err := encoder.Encode(row); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		measurement, err := measureLogs(root, "run", 4, newSampler(), 4)
		if missing {
			if err == nil || measurement.Status != "FAIL" || measurement.Throughput != 0 {
				t.Fatal("worker durability or cursor progress substituted for missing original COMMIT confirmation", measurement, err)
			}
		} else if err != nil || measurement.Status != "PASS" || measurement.Throughput != 4.0/6.0 {
			t.Fatal("complete join confirmation rejected or measurement boundary changed", measurement, err)
		}
	}
}
