package resulttransport

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/twmb/franz-go/pkg/kgo"
)

func pipelineFetch(records ...*kgo.Record) kgo.Fetches {
	partitions := make([]kgo.FetchPartition, 0, len(records))
	for _, r := range records {
		partitions = append(partitions, kgo.FetchPartition{Partition: r.Partition, Records: []*kgo.Record{r}})
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: ResultsTopic, Partitions: partitions}}}}
}

func awaitPipeline[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for pipeline event")
		var zero T
		return zero
	}
}

func TestKafkaPipelineRunsNextPollOnFreeHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := KafkaConsumerConfig{IntakeMode: KafkaIntakePipeline, Diagnostics: true}.WithDefaults()
	metrics := observability.New()
	releaseSlow := make(chan struct{})
	releaseOthers := make(chan struct{})
	started := make(chan *kgo.Record, 7)
	finished := make(chan []*kgo.Record, 1)
	done := make(chan error, 1)
	var mu sync.Mutex
	active := map[int32]bool{}
	calls := map[int32][]int64{}
	handle := func(ctx context.Context, r *kgo.Record, _ []byte) error {
		mu.Lock()
		if active[r.Partition] {
			mu.Unlock()
			return fmt.Errorf("partition %d already active", r.Partition)
		}
		active[r.Partition] = true
		calls[r.Partition] = append(calls[r.Partition], r.Offset)
		mu.Unlock()
		defer func() {
			mu.Lock()
			delete(active, r.Partition)
			mu.Unlock()
		}()
		started <- r
		var release <-chan struct{}
		switch {
		case r.Partition == 0 && r.Offset == 0:
			release = releaseSlow
		case r.Partition >= 1 && r.Partition <= 3:
			release = releaseOthers
		default:
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	}
	first := pipelineFetch(batchRecord("a01", 0), batchRecord("a02", 0), batchRecord("a03", 0), batchRecord("a04", 0))
	second := pipelineFetch(batchRecord("a01", 1), batchRecord("a05", 7), batchRecord("a05", 8))
	polls := 0
	poll := func(ctx context.Context, limit int) kgo.Fetches {
		if limit != 16 {
			return kgo.NewErrFetch(fmt.Errorf("poll limit=%d, want 16", limit))
		}
		polls++
		switch polls {
		case 1:
			return first
		case 2:
			return second
		default:
			<-ctx.Done()
			return kgo.NewErrFetch(ctx.Err())
		}
	}
	go func() {
		done <- runKafkaIntake(ctx, cfg, poll, func() map[int32]int64 {
			snapshot := batchExpected("a01", "a02", "a03", "a04")
			if polls >= 2 {
				snapshot[4] = 7
			}
			return snapshot
		}, func(lasts []*kgo.Record) error {
			finished <- lasts
			cancel()
			return nil
		}, nil, handle, metrics)
	}()
	for range 4 {
		if r := awaitPipeline(t, started); r.Partition >= 4 || r.Offset != 0 {
			t.Fatalf("started %d@%d while all four handlers were occupied", r.Partition, r.Offset)
		}
	}
	close(releaseOthers)
	for offset := int64(7); offset <= 8; offset++ {
		if r := awaitPipeline(t, started); r.Partition != 4 || r.Offset != offset {
			t.Fatalf("started %d@%d during slow handler, want 4@%d", r.Partition, r.Offset, offset)
		}
	}
	close(releaseSlow)
	if r := awaitPipeline(t, started); r.Partition != 0 || r.Offset != 1 {
		t.Fatalf("started %d@%d after slow handler, want 0@1", r.Partition, r.Offset)
	}
	lasts := awaitPipeline(t, finished)
	if err := awaitPipeline(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(lasts) != 5 || lasts[0].Offset != 1 || lasts[4].Offset != 8 {
		t.Fatalf("wrong durable prefix: %v", lasts)
	}
	if !reflect.DeepEqual(calls[0], []int64{0, 1}) || !reflect.DeepEqual(calls[4], []int64{7, 8}) {
		t.Fatalf("wrong FIFO order: %v", calls)
	}
	data := metrics.Snapshot()
	if data.Counters["results_kafka_durable_confirmations"] != 7 ||
		data.Gauges["results_kafka_peak_partitions"] != 4 ||
		data.Gauges["results_kafka_active_partitions"] != 0 ||
		data.Histograms["results_kafka_partition_handler_work"].Count != 1 {
		t.Fatalf("unexpected pipeline diagnostics: %v", data)
	}
}

func TestKafkaPipelineBackpressure(t *testing.T) {
	for _, tc := range []struct {
		name        string
		largeRecord bool
		blockedPoll int
	}{
		{name: "records", blockedPoll: 3},
		{name: "bytes", largeRecord: true, blockedPoll: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cfg := KafkaConsumerConfig{MaxPollRecords: 4, Concurrency: 1, BatchTimeout: 5 * time.Second}
			batch := func(first int64) kafkaPoll {
				records := make([]*kgo.Record, cfg.MaxPollRecords)
				for i := range records {
					r := batchRecord("a01", first+int64(i))
					if tc.largeRecord {
						r.Headers = []kgo.RecordHeader{{Key: "large", Value: make([]byte, 4<<20)}}
					}
					records[i] = r
				}
				return kafkaPoll{records: records, at: time.Now()}
			}
			polls := make(chan int, 4)
			calls := 1
			poll := func(ctx context.Context) kafkaPoll {
				calls++
				polls <- calls
				if calls < tc.blockedPoll {
					return batch(4)
				}
				<-ctx.Done()
				return kafkaPoll{err: ctx.Err()}
			}
			started := make(chan int64, 8)
			release := make(chan struct{}, 8)
			done := make(chan error, 1)
			go func() {
				_, err := processKafkaPipeline(ctx, cfg, batch(0), func() map[int32]int64 { return batchExpected("a01") }, poll, nil, func(ctx context.Context, r *kgo.Record, _ []byte) error {
					started <- r.Offset
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-release:
						return nil
					}
				}, observability.New())
				done <- err
			}()
			if tc.blockedPoll == 3 && awaitPipeline(t, polls) != 2 {
				t.Fatal("second poll should fit before the record limit")
			}
			// Free all four records for the record limit, or three for the
			// 8 MiB of headroom (two ~4 MiB records already exceed it).
			free := 4
			if tc.largeRecord {
				free = 3
			}
			for offset := range free {
				if got := awaitPipeline(t, started); got != int64(offset) {
					t.Fatalf("started offset %d, want %d", got, offset)
				}
				select {
				case n := <-polls:
					t.Fatalf("poll %d started before capacity was available", n)
				case <-time.After(15 * time.Millisecond):
				}
				release <- struct{}{}
			}
			if got := awaitPipeline(t, polls); got != tc.blockedPoll {
				t.Fatalf("resumed at poll %d, want %d", got, tc.blockedPoll)
			}
			cancel()
			if err := awaitPipeline(t, done); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}

func TestKafkaPipelineDrainsBeforeReleasingAssignment(t *testing.T) {
	for _, reason := range []string{"rebalance", "admission deadline"} {
		t.Run(reason, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cfg := KafkaConsumerConfig{IntakeMode: KafkaIntakePipeline, Concurrency: 1, BatchTimeout: 5 * time.Second}.WithDefaults()
			rebalance := make(chan struct{}, 1)
			secondStarted := make(chan struct{})
			secondReturned := make(chan struct{})
			release := make(chan struct{})
			done := make(chan error, 1)
			stop := errors.New("assignment released")
			var next int64
			polls := 0
			poll := func(ctx context.Context, _ int) kgo.Fetches {
				polls++
				switch polls {
				case 1:
					return pipelineFetch(batchRecord("a01", 0), batchRecord("a01", 1))
				case 2:
					close(secondStarted)
					<-ctx.Done()
					close(secondReturned)
					// A successful fetch may race with the local poll cancellation.
					return pipelineFetch(batchRecord("a01", 2))
				default:
					return kgo.NewErrFetch(fmt.Errorf("polled again before assignment release"))
				}
			}
			go func() {
				done <- runKafkaIntake(ctx, cfg, poll, func() map[int32]int64 { return map[int32]int64{0: next} },
					func(lasts []*kgo.Record) error {
						if len(lasts) != 1 || lasts[0].Offset != 2 || next != 3 {
							return fmt.Errorf("released assignment at SQL next=%d with commit %v", next, lasts)
						}
						return stop
					}, rebalance, func(ctx context.Context, r *kgo.Record, _ []byte) error {
						if r.Offset != next {
							return fmt.Errorf("handled %d, want SQL next=%d", r.Offset, next)
						}
						if r.Offset == 0 {
							select {
							case <-ctx.Done():
								return ctx.Err()
							case <-release:
							}
						}
						next++
						return nil
					}, observability.New())
			}()
			awaitPipeline(t, secondStarted)
			if reason == "rebalance" {
				rebalance <- struct{}{}
			}
			awaitPipeline(t, secondReturned)
			close(release)
			if err := awaitPipeline(t, done); !errors.Is(err, stop) {
				t.Fatal(err)
			}
		})
	}
}

func TestKafkaPipelineFailureClosesIntakeAndRestartsAtSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := KafkaConsumerConfig{IntakeMode: KafkaIntakePipeline}.WithDefaults()
	buffered := make(chan struct{})
	siblingStarted := make(chan struct{})
	siblingCanceled := make(chan struct{})
	releaseSibling := make(chan struct{})
	done := make(chan error, 1)
	sqlPositions := [2]int64{}
	load := func() map[int32]int64 { return map[int32]int64{0: sqlPositions[0], 1: sqlPositions[1]} }
	failure := errors.New("SQL transaction failed")
	polls := 0
	poll := func(ctx context.Context, _ int) kgo.Fetches {
		polls++
		switch polls {
		case 1:
			return pipelineFetch(batchRecord("a01", 0), batchRecord("a02", 0), batchRecord("a01", 1))
		case 2:
			return pipelineFetch(batchRecord("a01", 2), batchRecord("a02", 1))
		case 3:
			close(buffered)
			<-ctx.Done()
			return kgo.NewErrFetch(ctx.Err())
		default:
			return kgo.NewErrFetch(fmt.Errorf("continued on the same client after SQL failure"))
		}
	}
	go func() {
		done <- runKafkaIntake(ctx, cfg, poll, load, func(lasts []*kgo.Record) error {
			return fmt.Errorf("attempted to commit or release a failed window: %v", lasts)
		}, nil, func(handlerCtx context.Context, r *kgo.Record, _ []byte) error {
			switch {
			case r.Partition == 0 && r.Offset == 0:
				sqlPositions[0]++
				return nil
			case r.Partition == 0 && r.Offset == 1:
				select {
				case <-buffered:
				case <-handlerCtx.Done():
					return handlerCtx.Err()
				}
				select {
				case <-siblingStarted:
					return failure
				case <-handlerCtx.Done():
					return handlerCtx.Err()
				}
			case r.Partition == 1 && r.Offset == 0:
				close(siblingStarted)
				<-handlerCtx.Done()
				close(siblingCanceled)
				select {
				case <-releaseSibling:
				case <-ctx.Done():
				}
				return handlerCtx.Err()
			default:
				return fmt.Errorf("processed queued record %d@%d after failure", r.Partition, r.Offset)
			}
		}, observability.New())
	}()
	awaitPipeline(t, siblingCanceled)
	select {
	case err := <-done:
		t.Fatalf("returned before active handler joined: %v", err)
	case <-time.After(15 * time.Millisecond):
	}
	close(releaseSibling)
	if err := awaitPipeline(t, done); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if sqlPositions != [2]int64{1, 0} {
		t.Fatalf("SQL positions after failure: %v", sqlPositions)
	}

	replay := []kgo.Fetches{
		pipelineFetch(batchRecord("a01", sqlPositions[0]), batchRecord("a02", sqlPositions[1])),
		pipelineFetch(batchRecord("a01", 2), batchRecord("a02", 1)),
	}
	restarted := errors.New("restored SQL prefix confirmed")
	slots := 0
	err := runKafkaIntake(ctx, cfg, func(ctx context.Context, _ int) kgo.Fetches {
		if slots < len(replay) {
			batch := replay[slots]
			slots++
			return batch
		}
		<-ctx.Done()
		return kgo.NewErrFetch(ctx.Err())
	}, load, func(lasts []*kgo.Record) error {
		if sqlPositions == [2]int64{3, 2} && len(lasts) == 2 && lasts[0].Offset == 2 && lasts[1].Offset == 1 {
			return restarted
		}
		return nil
	}, nil, func(_ context.Context, r *kgo.Record, _ []byte) error {
		if r.Offset != sqlPositions[r.Partition] {
			return fmt.Errorf("replayed %d@%d, want %d", r.Partition, r.Offset, sqlPositions[r.Partition])
		}
		sqlPositions[r.Partition]++
		return nil
	}, observability.New())
	if !errors.Is(err, restarted) {
		t.Fatal(err)
	}
}

func TestKafkaPipelineProcessingDeadlineCancelsActiveAndLeavesQueueUnconfirmed(t *testing.T) {
	cfg := KafkaConsumerConfig{Concurrency: 1, MaxPollRecords: 1, BatchTimeout: 100 * time.Millisecond}
	expected := batchExpected("a01")
	polled := make(chan struct{})
	var seen []int64
	lasts, err := processKafkaPipeline(context.Background(), cfg,
		kafkaPoll{records: []*kgo.Record{batchRecord("a01", 0)}, at: time.Now()}, func() map[int32]int64 { return expected },
		func(context.Context) kafkaPoll {
			close(polled)
			return kafkaPoll{records: []*kgo.Record{batchRecord("a01", 1)}, at: time.Now()}
		}, nil, func(ctx context.Context, r *kgo.Record, _ []byte) error {
			seen = append(seen, r.Offset)
			<-ctx.Done()
			return ctx.Err()
		}, observability.New())
	if !errors.Is(err, context.DeadlineExceeded) || len(lasts) != 0 || expected[0] != 0 ||
		!reflect.DeepEqual(seen, []int64{0}) {
		t.Fatalf("lasts=%v err=%v SQL snapshot=%v handlers=%v", lasts, err, expected, seen)
	}
	awaitPipeline(t, polled)
}
