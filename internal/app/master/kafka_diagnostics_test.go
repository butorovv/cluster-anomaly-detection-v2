package master

import (
	"errors"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/resulttransport"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/twmb/franz-go/pkg/kadm"
)

func TestKafkaBacklogRequiresCompleteSnapshotForRate(t *testing.T) {
	metrics := observability.New()
	first := durableSample{at: time.Now(), next: map[int32]int64{}}
	second := durableSample{at: first.at.Add(2 * time.Second), next: map[int32]int64{}}
	end := map[int32]kadm.ListedOffset{}
	ends := kadm.ListedOffsets{resulttransport.ResultsTopic: end}
	for p := int32(0); p < 16; p++ {
		first.next[p], second.next[p] = 20, 26
		end[p] = kadm.ListedOffset{Offset: 30}
	}
	previous := publishKafkaBacklog(metrics, durableSample{}, first, ends, nil)
	if gauges := metrics.Snapshot().Gauges; gauges[resultsBacklogValid] != 1 || gauges[resultsRateValid] != 0 {
		t.Fatal("first sample must have backlog and no rate", gauges)
	}
	previous = publishKafkaBacklog(metrics, previous, second, ends, nil)
	if gauges := metrics.Snapshot().Gauges; gauges[resultsBacklogValid] != 1 || gauges[resultsRateValid] != 1 ||
		gauges["results_kafka_backlog_records_p15"] != 4 || gauges["results_kafka_backlog_records"] != 64 ||
		gauges["results_kafka_durable_confirmations_per_second"] != 48 {
		t.Fatal("wrong backlog or durable rate", gauges)
	}
	for _, err := range []error{errors.New("broker timed out"), nil} {
		delete(end, 15)
		previous = publishKafkaBacklog(metrics, previous, second, ends, err)
		if !previous.at.IsZero() {
			t.Fatal("incomplete sample retained a rate baseline")
		}
	}
	data := metrics.Snapshot()
	if data.Gauges[resultsBacklogValid] != 0 || data.Gauges[resultsRateValid] != 0 || data.Counters["results_kafka_backlog_sample_errors"] != 2 {
		t.Fatal("unavailable samples were not invalidated", data)
	}
	end[15] = kadm.ListedOffset{Offset: 30}
	publishKafkaBacklog(metrics, previous, second, ends, nil)
	if gauges := metrics.Snapshot().Gauges; gauges[resultsBacklogValid] != 1 || gauges[resultsRateValid] != 0 {
		t.Fatal("recovery must establish a fresh rate baseline", gauges)
	}
}
