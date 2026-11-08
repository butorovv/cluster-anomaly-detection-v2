package resulttransport

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func producerIdentityBroker(t *testing.T) []string {
	t.Helper()
	if os.Getenv("SYSTEM_KAFKA_INTEGRATION") != "1" {
		t.Skip("real isolated Kafka opt-in")
	}
	seeds := strings.Split(os.Getenv("P07_TEST_KAFKA_BROKERS"), ",")
	if os.Getenv("SYSTEM_KAFKA_ISOLATED") != "1" || seeds[0] == "" {
		t.Fatal("isolated Kafka prerequisites required")
	}
	return seeds
}

func TestKafkaSenderClusterAndTopicPins(t *testing.T) {
	seeds := producerIdentityBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sender, err := NewKafka(ctx, seeds, true)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	raw, err := json.Marshal(domain.Envelope{SchemaVersion: 1, ExecutionID: "12345678-1234-1234-1234-123456789abc", RunID: strings.Repeat("a", 64), ConfigID: strings.Repeat("b", 64), GroupID: "a01", MessageID: "producer-pin-probe", Kind: "group_result", Revision: 1, EventTime: time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC), Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Send(ctx, raw, "a01"); err != nil {
		t.Fatal("matching pins", err)
	}
	pin := sender.Identity
	admin := kadm.NewClient(sender.client)
	before, err := admin.ListEndOffsets(ctx, ResultsTopic)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		pin  SourceIdentity
	}{
		{"wrong cluster", SourceIdentity{ClusterID: "different-cluster", TopicID: pin.TopicID}},
		{"wrong topic", SourceIdentity{ClusterID: pin.ClusterID, TopicID: "different-topic"}},
		{"empty cluster", SourceIdentity{TopicID: pin.TopicID}},
		{"empty topic", SourceIdentity{ClusterID: pin.ClusterID}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sender.Identity = tc.pin
			if err := sender.Send(ctx, raw, "a01"); err == nil || !strings.Contains(err.Error(), "incarnation changed") {
				t.Fatal("mismatched pin accepted", err)
			}
		})
	}
	after, err := admin.ListEndOffsets(ctx, ResultsTopic)
	if err != nil {
		t.Fatal(err)
	}
	if before[ResultsTopic][0].Err != nil || after[ResultsTopic][0].Err != nil || before[ResultsTopic][0].Offset != after[ResultsTopic][0].Offset {
		t.Fatal("pin mismatch must not publish", before, after)
	}
	sender.Identity = pin
	if err := sender.Send(ctx, raw, "a01"); err != nil {
		t.Fatal("restored matching pins", err)
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if err := sender.Send(cancelled, raw, "a01"); err == nil {
		t.Fatal("canceled metadata request accepted")
	}
}

// Only this newly-created, unique test topic is deleted/recreated. Historical
// result/input topics and their records are never deleted by this test.
func TestProducerSourceRealTopicRecreation(t *testing.T) {
	seeds := producerIdentityBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	admin := kadm.NewClient(client)
	topic := fmt.Sprintf("p142-b1-incarnation-%d", time.Now().UnixNano())
	policy := "delete"
	maxBytes := fmt.Sprint(KafkaBatchMaxBytes)
	create := func() {
		t.Helper()
		results, err := admin.CreateTopics(ctx, resultPartitions, 1, map[string]*string{"cleanup.policy": &policy, "max.message.bytes": &maxBytes}, topic)
		if err != nil {
			t.Fatal(err)
		}
		r, ok := results[topic]
		if !ok || r.Err != nil {
			t.Fatalf("create owned topic: %v", results)
		}
	}
	create()
	t.Cleanup(func() {
		c, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		rs, e := admin.DeleteTopics(c, topic)
		if e != nil {
			t.Error("delete owned test topic", e)
		} else if r, ok := rs[topic]; !ok || r.Err != nil {
			t.Error("delete owned test topic", rs)
		}
	})
	cluster, oldID, err := describeProducerSource(ctx, admin, topic)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := admin.DeleteTopics(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if r, ok := deleted[topic]; !ok || r.Err != nil {
		t.Fatal("delete before recreation", deleted)
	}
	// A successful delete response may precede the controller removing metadata.
	for {
		m, e := admin.Metadata(ctx, topic)
		if e == nil {
			if d, ok := m.Topics[topic]; !ok || d.Err != nil {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("old incarnation did not disappear", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	if _, _, err := describeProducerSource(ctx, admin, topic); err == nil {
		t.Fatal("missing topic accepted")
	}
	create()
	for {
		newCluster, newID, e := describeProducerSource(ctx, admin, topic)
		if e == nil {
			if newCluster != cluster || newID == oldID {
				t.Fatalf("recreation identity %s/%s -> %s/%s", cluster, oldID, newCluster, newID)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("new incarnation unavailable", e)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
