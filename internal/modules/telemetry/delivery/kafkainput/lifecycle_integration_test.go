package kafkainput

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/inputcodec"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaLifecycleRestoreBeforeFetchAndFreshBounds(t *testing.T) {
	cfg, admin := integration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var mu sync.Mutex
	assigned, restored := map[int32]bool{}, map[int32]bool{}
	released := false
	cfg.Assigned = func(_ context.Context, ids []Identity) error {
		mu.Lock()
		defer mu.Unlock()
		for _, id := range ids {
			assigned[id.Partition] = true
		}
		return nil
	}
	cfg.Revoked = func(_ context.Context, ids []Identity) error {
		mu.Lock()
		defer mu.Unlock()
		for _, id := range ids {
			delete(assigned, id.Partition)
			delete(restored, id.Partition)
		}
		released = true
		return nil
	}
	cfg.Lost = cfg.Revoked
	cfg.Load = func(loadCtx context.Context, id Identity) (Position, error) {
		mu.Lock()
		defer mu.Unlock()
		if !assigned[id.Partition] {
			return Position{}, fmt.Errorf("Load before assignment")
		}
		restored[id.Partition] = true
		if id.Partition != 0 {
			return Position{id, 0}, nil
		}
		// Simulate progress by the previous SQL owner during acquisition. A
		// next_offset read after that wait can exceed the pre-Load Kafka end.
		for tick := 0; tick < 2; tick++ {
			at := cfg.Window.Start.Add(time.Duration(tick) * domain.GridStep)
			c := domain.TimestampComplete{GroupID: "a01", Timestamp: at}
			m := domain.InputMessage{Version: domain.InputVersion, Type: domain.CompleteMessage, Binding: cfg.Binding, GroupID: "a01", Timestamp: at, Completion: &c}
			b, err := inputcodec.Encode(m, cfg.Window)
			if err != nil {
				return Position{}, err
			}
			if err := admin.client.ProduceSync(loadCtx, &kgo.Record{Topic: cfg.topic(), Partition: 0, Key: []byte("a01"), Value: b}).FirstErr(); err != nil {
				return Position{}, err
			}
		}
		return Position{id, 1}, nil
	}
	err := ConsumeGroup(ctx, cfg, func(_ context.Context, r *kgo.Record, _ domain.InputMessage) error {
		mu.Lock()
		defer mu.Unlock()
		if !assigned[r.Partition] || !restored[r.Partition] || r.Offset != 1 {
			return fmt.Errorf("Handler before restored position: %d/%d", r.Partition, r.Offset)
		}
		return errDone
	})
	if !errors.Is(err, errDone) {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !released || len(assigned) != 0 || len(restored) != 0 {
		t.Fatal("Close did not complete synchronous revoke barrier")
	}
}
