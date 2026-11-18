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

const (
	KafkaIntakeWave     = "wave"
	KafkaIntakePipeline = "pipeline"
)

// KafkaConsumerConfig bounds result processing within a rebalance barrier.
type KafkaConsumerConfig struct {
	IntakeMode     string
	Concurrency    int
	MaxPollRecords int
	BatchTimeout   time.Duration
	Diagnostics    bool
}

func (c KafkaConsumerConfig) WithDefaults() KafkaConsumerConfig {
	if c.IntakeMode == "" {
		c.IntakeMode = KafkaIntakeWave
	}
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
	if c.IntakeMode != KafkaIntakeWave && c.IntakeMode != KafkaIntakePipeline {
		return fmt.Errorf("invalid Kafka intake mode %q: want wave or pipeline", c.IntakeMode)
	}
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
	work time.Duration
	ran  bool
}

// processKafkaBatch owns at most Concurrency workers and one bounded poll batch.
// Each partition appears in one FIFO job. A handler must honor cancellation and
// return nil only after its SQL transaction is confirmed committed.
func processKafkaBatch(ctx context.Context, cfg KafkaConsumerConfig, records []*kgo.Record, expected map[int32]int64, polledAt time.Time, handle func(context.Context, *kgo.Record, domain.DecodedEnvelope) error, metrics *observability.Registry) (lasts []*kgo.Record, retErr error) {
	if len(records) == 0 {
		return nil, nil
	}
	if cfg.Diagnostics {
		defer func() {
			metrics.Observe("results_kafka_poll_wave_duration", time.Since(polledAt))
			if retErr != nil {
				metrics.Add("results_kafka_poll_wave_errors", 1)
			}
			// Include a successfully committed prefix of a failed wave.
			var confirmed int64
			for _, last := range lasts {
				confirmed += last.Offset - expected[last.Partition] + 1
			}
			metrics.Add("results_kafka_durable_confirmations", confirmed)
		}()
	}
	if len(records) > cfg.MaxPollRecords {
		return nil, fmt.Errorf("Kafka poll exceeded record bound")
	}
	parts := map[int32]*partitionBatch{}
	var batchBytes int64
	for _, r := range records {
		next, err := validateKafkaRecord(r, expected)
		if err != nil {
			return nil, err
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
	if cfg.Diagnostics {
		metrics.Set("results_kafka_distinct_partitions_per_poll", float64(len(parts)))
		metrics.Add("results_kafka_polled_partitions", int64(len(parts)))
	}
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
				var started time.Time
				if cfg.Diagnostics {
					started = time.Now()
				}
				progress := processPartition(ctx, *part, polledAt, handle, metrics)
				if cfg.Diagnostics {
					progress.work = time.Since(started)
					progress.ran = true
				}
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
	lasts = make([]*kgo.Record, 0, admitted)
	var slowest, work time.Duration
	var ran bool
	for i := 0; i < admitted; i++ {
		r := <-results
		if r.ran {
			slowest = max(slowest, r.work)
			work += r.work
			ran = true
		}
		if r.last != nil {
			lasts = append(lasts, r.last)
		}
	}
	if ran {
		metrics.Observe("results_kafka_slowest_partition_job_duration", slowest)
		metrics.Observe("results_kafka_partition_handler_work", work)
	}
	if firstError != nil {
		return lasts, firstError
	}
	if err := ctx.Err(); err != nil {
		return lasts, err
	}
	return lasts, nil
}

func validateKafkaRecord(r *kgo.Record, expected map[int32]int64) (int64, error) {
	if r == nil {
		return 0, fmt.Errorf("nil Kafka result record")
	}
	next, ok := expected[r.Partition]
	if r.Topic != ResultsTopic || !ok || r.Partition < 0 || r.Partition >= resultPartitions {
		return 0, fmt.Errorf("unexpected result assignment at %s/%d", r.Topic, r.Partition)
	}
	if len(r.Value) == 0 || len(r.Value) > domain.MaxMessageBytes || len(r.Key) == 0 || len(r.Key) > 256 {
		return 0, fmt.Errorf("result record exceeds message/key limit")
	}
	return next, nil
}

func processPartition(ctx context.Context, part partitionBatch, polledAt time.Time, handle func(context.Context, *kgo.Record, domain.DecodedEnvelope) error, metrics *observability.Registry) (progress partitionProgress) {
	for _, r := range part.records {
		if err := ctx.Err(); err != nil {
			progress.err = err
			return
		}
		if r.Offset != part.next {
			progress.err = fmt.Errorf("unexpected result record position at %s/%d@%d: want %d", r.Topic, r.Partition, r.Offset, part.next)
			return
		}
		decoded, err := domain.DecodeEnvelope(r.Value)
		if err != nil {
			progress.err = fmt.Errorf("decode output %d@%d: %w", r.Partition, r.Offset, err)
			return
		}
		e := decoded.Envelope()
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
		if err := handle(ctx, r, decoded); err != nil {
			progress.err = fmt.Errorf("handle output %d@%d: %w", r.Partition, r.Offset, err)
			return
		}
		metrics.Observe("results_kafka_intake_service", time.Since(intakeAt))
		part.next = r.Offset + 1
		progress.last = r
	}
	return
}
