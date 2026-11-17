package resulttransport

import (
	"context"
	"fmt"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaPoll struct {
	records []*kgo.Record
	at      time.Time
	err     error
}

type queuedKafkaRecord struct {
	record   *kgo.Record
	polledAt time.Time
	bytes    int64
}

type kafkaPipelineResult struct {
	partition int32
	progress  partitionProgress
	work      time.Duration
}

// processKafkaPipeline holds the rebalance barrier across successive polls.
// It drains all returned records before the caller can commit and release the
// barrier. On error the caller must close the client and restore from SQL.
func processKafkaPipeline(ctx context.Context, cfg KafkaConsumerConfig, first kafkaPoll, positions func() map[int32]int64, poll func(context.Context) kafkaPoll, rebalance <-chan struct{}, handle func(context.Context, *kgo.Record, []byte) error, metrics *observability.Registry) ([]*kgo.Record, error) {
	// Stop admitting after a short interval even on continuous input, so
	// auxiliary commits and rebalances get a regular drain point. Each poll
	// still has the configured processing deadline, including queue time.
	admitUntil := first.at.Add(min(time.Second, cfg.BatchTimeout))
	admission := time.NewTimer(time.Until(admitUntil))
	defer admission.Stop()
	admissionDone := admission.C
	ctx, cancel := context.WithDeadline(ctx, admitUntil.Add(cfg.BatchTimeout))

	// Reserve space for a full poll before calling into franz-go. The byte
	// limit covers record data (key, value and headers) held by this scheduler.
	// The SDK's own fetch buffers keep their existing limits.
	pollBytes := int64(cfg.MaxPollRecords) * KafkaBatchMaxBytes
	maxBytes := pollBytes + (8 << 20)
	maxRecords := 2 * cfg.MaxPollRecords
	var queues [resultPartitions][]queuedKafkaRecord
	ready := make([]int32, 0, resultPartitions)
	var pending, active, peak int
	var pendingBytes int64
	var last [resultPartitions]*kgo.Record

	polled := make(chan kafkaPoll, 1)
	completed := make(chan kafkaPipelineResult, cfg.Concurrency)
	var cancelPoll context.CancelFunc
	var work time.Duration
	var ran bool
	observeCompletion := func(result kafkaPipelineResult) {
		active--
		if cfg.Diagnostics {
			work += result.work
			ran = true
			if result.progress.last != nil {
				metrics.Add("results_kafka_durable_confirmations", 1)
			}
		}
	}
	defer func() {
		cancel()
		for active > 0 {
			observeCompletion(<-completed)
		}
		if cancelPoll != nil {
			cancelPoll()
			<-polled
		}
		metrics.Set("results_kafka_active_partitions", 0)
		metrics.Set("results_kafka_peak_partitions", float64(peak))
		if ran {
			metrics.Observe("results_kafka_partition_handler_work", work)
		}
	}()

	expected := positions()
	admit := func(batch kafkaPoll) error {
		if len(batch.records) > cfg.MaxPollRecords {
			return fmt.Errorf("Kafka poll exceeded record bound")
		}
		var batchBytes int64
		var parts [resultPartitions]bool
		var distinct int
		for _, r := range batch.records {
			// Cooperative assignment can finish loading offsets for a new
			// partition while retained partitions are already being polled.
			if r != nil {
				if _, tracked := expected[r.Partition]; !tracked {
					if next, assigned := positions()[r.Partition]; assigned {
						expected[r.Partition] = next
					}
				}
			}
			if _, err := validateKafkaRecord(r, expected); err != nil {
				return err
			}
			size := int64(len(r.Key) + len(r.Value))
			for _, header := range r.Headers {
				size += int64(len(header.Key) + len(header.Value))
			}
			if size > KafkaBatchMaxBytes {
				return fmt.Errorf("result record exceeds Kafka intake byte limit at %d@%d", r.Partition, r.Offset)
			}
			p := r.Partition
			if len(queues[p]) == 0 {
				ready = append(ready, p)
			}
			queues[p] = append(queues[p], queuedKafkaRecord{record: r, polledAt: batch.at, bytes: size})
			pending++
			pendingBytes += size
			batchBytes += size
			if !parts[p] {
				parts[p] = true
				distinct++
			}
		}
		metrics.Set("results_kafka_batch_bytes", float64(batchBytes))
		if cfg.Diagnostics {
			metrics.Set("results_kafka_distinct_partitions_per_poll", float64(distinct))
			metrics.Add("results_kafka_polled_partitions", int64(distinct))
		}
		return nil
	}
	if err := admit(first); err != nil {
		return nil, err
	}
	first.records = nil

	draining := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		select {
		case <-rebalance:
			draining = true
		default:
		}
		if pending == 0 || !time.Now().Before(admitUntil) {
			draining = true
		}
		if draining {
			admissionDone = nil
			if cancelPoll != nil {
				cancelPoll()
			} else if pending == 0 {
				break
			}
		}

		for active < cfg.Concurrency && len(ready) > 0 {
			p := ready[0]
			ready = ready[1:]
			item := queues[p][0]
			next := expected[p]
			active++
			peak = max(peak, active)
			metrics.Set("results_kafka_active_partitions", float64(active))
			go func() {
				bounded, done := context.WithDeadline(ctx, item.polledAt.Add(cfg.BatchTimeout))
				started := time.Now()
				progress := processPartition(bounded, partitionBatch{partition: p, next: next, records: []*kgo.Record{item.record}}, item.polledAt, handle, metrics)
				done()
				completed <- kafkaPipelineResult{partition: p, progress: progress, work: time.Since(started)}
			}()
		}

		if !draining && cancelPoll == nil && pending+cfg.MaxPollRecords <= maxRecords && pendingBytes+pollBytes <= maxBytes {
			var pollCtx context.Context
			pollCtx, cancelPoll = context.WithCancel(ctx)
			go func(c context.Context) { polled <- poll(c) }(pollCtx)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-rebalance:
			draining = true
		case <-admissionDone:
			draining = true
		case batch := <-polled:
			cancelPoll()
			cancelPoll = nil
			if batch.err != nil {
				return nil, batch.err
			}
			// Cancellation can race with a successful poll. Keep any records
			// it returned so the client's advanced positions remain usable.
			if len(batch.records) == 0 {
				continue
			}
			if err := admit(batch); err != nil {
				return nil, err
			}
		case result := <-completed:
			observeCompletion(result)
			metrics.Set("results_kafka_active_partitions", float64(active))
			if result.progress.err != nil {
				return nil, result.progress.err
			}
			p := result.partition
			item := queues[p][0]
			pending--
			pendingBytes -= item.bytes
			queues[p][0] = queuedKafkaRecord{}
			queues[p] = queues[p][1:]
			if len(queues[p]) > 0 {
				ready = append(ready, p)
			}
			r := result.progress.last
			expected[p] = r.Offset + 1
			// CommitRecords needs only the position, so completed payloads
			// need not remain in the intake window.
			last[p] = &kgo.Record{Topic: r.Topic, Partition: p, Offset: r.Offset, LeaderEpoch: r.LeaderEpoch}
		}
	}
	lasts := make([]*kgo.Record, 0, resultPartitions)
	for _, r := range last {
		if r != nil {
			lasts = append(lasts, r)
		}
	}
	return lasts, nil
}
