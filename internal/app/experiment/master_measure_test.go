package experiment

import (
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
)

func TestMasterKafkaSampleOmitsInvalidBacklogAndRate(t *testing.T) {
	gauges := map[string]float64{
		"results_kafka_backlog_sample_valid":             0,
		"results_kafka_backlog_records":                  100,
		"results_kafka_backlog_records_p15":              8,
		"results_kafka_durable_rate_valid":               0,
		"results_kafka_durable_confirmations_per_second": 42,
		"results_kafka_poll_batch_records":               16,
	}
	at := time.Now()
	data := observability.Data{At: at, Gauges: gauges, Process: observability.Process{PID: 42},
		Histograms: map[string]observability.Histogram{"master_pg_commit_duration": {Count: 2, Sum: 0.01}}}
	sample := captureMasterKafkaSample(data)
	if len(sample.Gauges) != 3 || sample.Gauges["results_kafka_poll_batch_records"] != 16 {
		t.Fatal("stale data included in sample", sample)
	}
	if sample.At != at || sample.PID != 42 || sample.Timings["master_pg_commit_duration"] != (timingAggregate{Count: 2, Seconds: 0.01}) {
		t.Fatal("timing window cannot be reconstructed", sample)
	}
	gauges["results_kafka_backlog_sample_valid"] = 1
	gauges["results_kafka_durable_rate_valid"] = 1
	sample = captureMasterKafkaSample(data)
	if len(sample.Gauges) != len(gauges) || sample.Gauges["results_kafka_backlog_records_p15"] != 8 {
		t.Fatal("valid data missing from sample", sample)
	}
}
