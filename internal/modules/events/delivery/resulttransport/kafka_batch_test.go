package resulttransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/twmb/franz-go/pkg/kgo"
)

func batchRecord(group string, offset int64) *kgo.Record {
	partition := int32(group[1]-'0')*10 + int32(group[2]-'0') - 1
	value, _ := json.Marshal(map[string]any{"schema_version": 1, "execution_id": "12345678-1234-1234-1234-123456789abc", "run_id": string(make([]byte, 0)), "config_id": "", "group_id": group, "message_id": "m", "kind": "group_result", "revision": 1, "event_time": "2020-01-01T00:00:00Z", "payload": json.RawMessage(`{"x":1}`)})
	// Header validation requires lowercase SHA-256 identities.
	var wire map[string]any
	_ = json.Unmarshal(value, &wire)
	wire["run_id"] = fmt.Sprintf("%064x", 1)
	wire["config_id"] = fmt.Sprintf("%064x", 2)
	value, _ = json.Marshal(wire)
	return &kgo.Record{Topic: ResultsTopic, Partition: partition, Offset: offset, Key: []byte(group), Value: value}
}

func batchExpected(groups ...string) map[int32]int64 {
	m := map[int32]int64{}
	for _, g := range groups {
		m[batchRecord(g, 0).Partition] = 0
	}
	return m
}

func waitBatch(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for handler")
	}
}

func TestKafkaBatchSlowPartitionDoesNotBlockOthersAndHonorsConcurrency(t *testing.T) {
	started := make(chan string, 4)
	release := make(chan struct{})
	allStarted := make(chan struct{})
	finished := make(chan struct{}, 3)
	var mu sync.Mutex
	var active, peak int
	handle := func(_ context.Context, r *kgo.Record, _ []byte) error {
		mu.Lock()
		active++
		if active > peak {
			peak = active
		}
		mu.Unlock()
		started <- string(r.Key)
		<-allStarted
		if string(r.Key) == "a01" {
			<-release
		} else {
			finished <- struct{}{}
		}
		mu.Lock()
		active--
		mu.Unlock()
		return nil
	}
	done := make(chan error, 1)
	go func() {
		_, err := processKafkaBatch(context.Background(), KafkaConsumerConfig{Concurrency: 4, MaxPollRecords: 8, BatchTimeout: 5 * time.Second}, []*kgo.Record{batchRecord("a01", 0), batchRecord("a02", 0), batchRecord("a03", 0), batchRecord("a04", 0)}, batchExpected("a01", "a02", "a03", "a04"), time.Now(), handle, observability.New())
		done <- err
	}()
	seen := map[string]bool{}
	for len(seen) < 4 {
		select {
		case g := <-started:
			seen[g] = true
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("handlers did not start")
		}
	}
	close(allStarted)
	for i := 0; i < 3; i++ {
		waitBatch(t, finished)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if peak != 4 {
		t.Fatalf("peak handlers=%d, want 4", peak)
	}
}

func TestKafkaBatchPartitionFIFOAndPrefixOnGap(t *testing.T) {
	seen := []int64{}
	records := []*kgo.Record{batchRecord("a01", 0), batchRecord("a01", 1), batchRecord("a01", 3)}
	last, err := processKafkaBatch(context.Background(), KafkaConsumerConfig{Concurrency: 3, MaxPollRecords: 4, BatchTimeout: time.Second}, records, batchExpected("a01"), time.Now(), func(_ context.Context, r *kgo.Record, _ []byte) error { seen = append(seen, r.Offset); return nil }, observability.New())
	if err == nil || !reflect.DeepEqual(seen, []int64{0, 1}) {
		t.Fatalf("seen=%v err=%v", seen, err)
	}
	if len(last) != 1 || last[0].Offset != 1 {
		t.Fatalf("successful prefix=%v", last)
	}
}

func TestKafkaBatchFailureCancelsAndJoinsHandlers(t *testing.T) {
	failure := errors.New("handler failed")
	started := make(chan struct{}, 2)
	joined := make(chan struct{}, 1)
	release := make(chan struct{})
	failNow := make(chan struct{})
	records := []*kgo.Record{batchRecord("a01", 0), batchRecord("a02", 0), batchRecord("a03", 0)}
	done := make(chan error, 1)
	canceled := make(chan struct{}, 1)
	go func() {
		_, err := processKafkaBatch(context.Background(), KafkaConsumerConfig{Concurrency: 2, MaxPollRecords: 4, BatchTimeout: time.Second}, records, batchExpected("a01", "a02", "a03"), time.Now(), func(ctx context.Context, r *kgo.Record, _ []byte) error {
			started <- struct{}{}
			if r.Partition == 0 {
				<-failNow
				return failure
			}
			<-ctx.Done()
			canceled <- struct{}{}
			<-release
			joined <- struct{}{}
			return ctx.Err()
		}, observability.New())
		done <- err
	}()
	waitBatch(t, started)
	waitBatch(t, started)
	close(failNow)
	waitBatch(t, canceled)
	close(release)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected handler error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("batch did not return")
	}
}

func TestKafkaBatchRejectsInvalidInputWithoutMutatingExpected(t *testing.T) {
	base := batchRecord("a01", 0)
	cases := map[string][]*kgo.Record{"wrong topic": {func() *kgo.Record { r := *base; r.Topic = "wrong"; return &r }()}, "wrong route": {func() *kgo.Record { r := *base; r.Partition = 1; return &r }()}, "invalid bytes": {func() *kgo.Record { r := *base; r.Value = []byte("not-json"); return &r }()}, "nil": {nil}}
	for name, records := range cases {
		t.Run(name, func(t *testing.T) {
			expected := batchExpected("a01")
			before := map[int32]int64{0: 0}
			_, err := processKafkaBatch(context.Background(), KafkaConsumerConfig{Concurrency: 1, MaxPollRecords: 4, BatchTimeout: time.Second}, records, expected, time.Now(), func(context.Context, *kgo.Record, []byte) error { return nil }, observability.New())
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !reflect.DeepEqual(expected, before) {
				t.Fatalf("expected mutated: %v", expected)
			}
		})
	}
}
