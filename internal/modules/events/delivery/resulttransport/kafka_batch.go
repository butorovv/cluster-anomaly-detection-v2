package resulttransport

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaConsumerConfig bounds one poll wave; there is no persistent record queue.
type KafkaConsumerConfig struct {
	Concurrency    int
	MaxPollRecords int
	BatchTimeout   time.Duration
}

func (c KafkaConsumerConfig) WithDefaults() KafkaConsumerConfig {
	if c.Concurrency == 0 {
		c.Concurrency = 4
	}
	if c.MaxPollRecords == 0 {
		c.MaxPollRecords = 16
	}
	if c.BatchTimeout == 0 {
		c.BatchTimeout = 8 * time.Second
	}
	return c
}
func (c KafkaConsumerConfig) Validate() error {
	if c.Concurrency < 1 || c.Concurrency > 16 || c.MaxPollRecords < 1 || c.MaxPollRecords > 128 || c.BatchTimeout < time.Millisecond || c.BatchTimeout > 15*time.Second {
		return fmt.Errorf("invalid Kafka intake limits")
	}
	return nil
}

type partitionBatch struct {
	partition int32
	next      int64
	records   []*kgo.Record
}
type partitionProgress struct {
	last *kgo.Record
	err  error
}

// processKafkaBatch owns at most Concurrency workers and one bounded poll batch.
// Each partition appears in one FIFO job. A handler must honor cancellation and
// return nil only after its SQL transaction is confirmed committed.
func processKafkaBatch(ctx context.Context, cfg KafkaConsumerConfig, records []*kgo.Record, expected map[int32]int64, polledAt time.Time, handle func(context.Context, *kgo.Record, []byte) error, metrics *observability.Registry) ([]*kgo.Record, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if len(records) > cfg.MaxPollRecords {
		return nil, fmt.Errorf("Kafka poll exceeded record bound")
	}
	parts := map[int32]*partitionBatch{}
	var batchBytes int64
	for _, r := range records {
		if r == nil {
			return nil, fmt.Errorf("nil Kafka result record")
		}
		next, ok := expected[r.Partition]
		if r.Topic != ResultsTopic || !ok || r.Partition < 0 || r.Partition >= resultPartitions {
			return nil, fmt.Errorf("unexpected result assignment at %s/%d", r.Topic, r.Partition)
		}
		if len(r.Value) == 0 || len(r.Value) > domain.MaxMessageBytes || len(r.Key) == 0 || len(r.Key) > 256 {
			return nil, fmt.Errorf("result record exceeds message/key limit")
		}
		batchBytes += int64(len(r.Value) + len(r.Key))
		part := parts[r.Partition]
		if part == nil {
			part = &partitionBatch{partition: r.Partition, next: next}
			parts[r.Partition] = part
		}
		part.records = append(part.records, r)
	}
	metrics.Set("results_kafka_batch_bytes", float64(batchBytes))
	ctx, cancel := context.WithTimeout(ctx, cfg.BatchTimeout)
	defer cancel()
	jobs := make(chan *partitionBatch)
	results := make(chan partitionProgress, len(parts))
	var workers sync.WaitGroup
	var active, peak atomic.Int64
	var firstError error
	var failOnce sync.Once
	fail := func(err error) { failOnce.Do(func() { firstError = err; cancel() }) }
	for i := 0; i < min(cfg.Concurrency, len(parts)); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for part := range jobs {
				if err := ctx.Err(); err != nil {
					results <- partitionProgress{err: err}
					continue
				}
				n := active.Add(1)
				for p := peak.Load(); n > p; p = peak.Load() {
					if peak.CompareAndSwap(p, n) {
						break
					}
				}
				metrics.Set("results_kafka_active_partitions", float64(n))
				progress := processPartition(ctx, *part, polledAt, handle, metrics)
				metrics.Set("results_kafka_active_partitions", float64(active.Add(-1)))
				if progress.err != nil {
					fail(progress.err)
				}
				results <- progress
			}
		}()
	}
	keys := make([]int, 0, len(parts))
	for p := range parts {
		keys = append(keys, int(p))
	}
	sort.Ints(keys)
	admitted := 0
admission:
	for _, p := range keys {
		if ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
			break admission
		case jobs <- parts[int32(p)]:
			admitted++
		}
	}
	close(jobs)
	workers.Wait() // callbacks/SQL have returned before the caller releases assignment
	metrics.Set("results_kafka_active_partitions", 0)
	metrics.Set("results_kafka_peak_partitions", float64(peak.Load()))
	lasts := make([]*kgo.Record, 0, admitted)
	for i := 0; i < admitted; i++ {
		r := <-results
		if r.last != nil {
			lasts = append(lasts, r.last)
		}
	}
	if firstError != nil {
		return lasts, firstError
	}
	if err := ctx.Err(); err != nil {
		return lasts, err
	}
	return lasts, nil
}

func processPartition(ctx context.Context, part partitionBatch, polledAt time.Time, handle func(context.Context, *kgo.Record, []byte) error, metrics *observability.Registry) (progress partitionProgress) {
	for _, r := range part.records {
		if err := ctx.Err(); err != nil {
			progress.err = err
			return
		}
		if r.Offset != part.next {
			progress.err = fmt.Errorf("unexpected result record position at %s/%d@%d: want %d", r.Topic, r.Partition, r.Offset, part.next)
			return
		}
		e, err := domain.Decode(r.Value)
		if err != nil {
			progress.err = fmt.Errorf("decode output %d@%d: %w", r.Partition, r.Offset, err)
			return
		}
		p, err := topology.PartitionForGroup(e.GroupID)
		if err != nil || p != int(r.Partition) || string(r.Key) != e.GroupID {
			progress.err = fmt.Errorf("physical/logical output routing mismatch at %d@%d", r.Partition, r.Offset)
			return
		}
		intakeAt := time.Now()
		metrics.Observe("results_kafka_poll_to_intake", intakeAt.Sub(polledAt))
		if !r.Timestamp.IsZero() && !polledAt.Before(r.Timestamp) {
			metrics.Observe("results_kafka_create_to_poll", polledAt.Sub(r.Timestamp))
		}
		if err := handle(ctx, r, r.Value); err != nil {
			progress.err = fmt.Errorf("handle output %d@%d: %w", r.Partition, r.Offset, err)
			return
		}
		metrics.Observe("results_kafka_intake_service", time.Since(intakeAt))
		part.next = r.Offset + 1
		progress.last = r
	}
	return
}
