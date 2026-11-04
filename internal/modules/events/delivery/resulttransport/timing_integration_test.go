package resulttransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// A bounded transport probe, not an anomaly benchmark. Deliberately slow intake
// proves which queue can grow AFTER a broker fetch has already returned.
func TestKafkaConsumerBatchQueueMeasurements(t *testing.T) {
	if os.Getenv("SYSTEM_KAFKA_INTEGRATION") != "1" {
		t.Skip("real isolated Kafka required")
	}
	seeds := strings.Split(os.Getenv("P07_TEST_KAFKA_BROKERS"), ",")
	if os.Getenv("SYSTEM_KAFKA_ISOLATED") != "1" || seeds[0] == "" {
		t.Fatal("isolated Kafka required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	metrics := observability.New()
	sender, err := NewKafka(ctx, seeds, true, metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	ends, err := kadm.NewClient(sender.client).ListEndOffsets(ctx, ResultsTopic)
	if err != nil {
		t.Fatal(err)
	}
	execution := "12345678-1234-1234-1234-123456789abc"
	for i := 0; i < 32; i++ {
		e := domain.Envelope{SchemaVersion: 1, ExecutionID: execution, RunID: strings.Repeat("a", 64), ConfigID: strings.Repeat("b", 64), GroupID: "a01", MessageID: fmt.Sprintf("timing-probe-%d", i), Kind: "rbi2_event", Revision: 1, EventTime: time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC), Payload: json.RawMessage(`{}`)}
		raw, _ := json.Marshal(e)
		if err := sender.Send(ctx, raw, "a01"); err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	stop := errors.New("probe complete")
	err = ConsumeKafka(ctx, seeds, fmt.Sprintf("p13-timing-%d", time.Now().UnixNano()), sender.Identity, func(_ context.Context, p int32) (int64, error) { return ends[ResultsTopic][p].Offset, nil }, func(_ context.Context, r *kgo.Record, _ []byte) error {
		count++
		time.Sleep(10 * time.Millisecond)
		if count == 32 {
			return stop
		}
		return nil
	}, metrics)
	if !errors.Is(err, stop) || count != 32 {
		t.Fatal("probe did not consume exact bounded records", count, err)
	}
	data := metrics.Snapshot()
	if data.Histograms["results_kafka_publish_ack"].Count != 32 || data.Histograms["results_kafka_poll_to_intake"].Count != 32 || data.Histograms["results_kafka_poll_to_intake"].Max < .01 || data.Histograms["results_kafka_create_to_poll"].Count != 32 {
		t.Fatal("missing decomposed Kafka measurements", data.Histograms)
	}
	encoded, _ := json.Marshal(data.Histograms)
	t.Logf("real_Kafka_bounded_timing_probe=%s", encoded)
}
