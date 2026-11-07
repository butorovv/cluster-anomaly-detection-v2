package resulttransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaSQLRig struct {
	seeds               []string
	sender              *KafkaSender
	store               repo.MasterStore
	master              usecase.Master
	execution, consumer string
	starts              map[int32]int64
	raw                 map[int32][][]byte
}

func newKafkaSQLRig(t *testing.T) *kafkaSQLRig {
	t.Helper()
	seeds := producerIdentityBroker(t)
	pool := postgrestest.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sender, err := NewKafka(ctx, seeds, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sender.Close() })
	ends, err := kadm.NewClient(sender.client).ListEndOffsets(ctx, ResultsTopic)
	if err != nil {
		t.Fatal(err)
	}
	f := workerfixture.New(t, 3, 30)
	f.Options.Groups = 4
	f.Options.Binding.ExecutionID = uuid.NewString()
	r := &kafkaSQLRig{seeds: seeds, sender: sender, store: repo.MasterStore{DB: pool}, execution: f.Options.Binding.ExecutionID, consumer: "p15-results-" + uuid.NewString(), starts: map[int32]int64{}, raw: map[int32][][]byte{}}
	starts := map[string]int64{}
	for p := int32(0); p < 16; p++ {
		entry, ok := ends[ResultsTopic][p]
		if !ok || entry.Err != nil {
			t.Fatal("missing end", p, entry.Err)
		}
		r.starts[p] = entry.Offset
		starts[fmt.Sprint(p)] = entry.Offset
	}
	for g := 1; g <= 4; g++ {
		opts := f.Options
		opts.GroupID = fmt.Sprintf("a%02d", g)
		processor, err := group.NewGroupProcessor(f.Bundle, opts)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := processor.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if g == 1 {
			registration := checkpoints.Registration{Identity: snapshot.Identity, Binding: opts.Binding, ClusterID: "p15-input", TopicID: "p15-input", ConsumerGroup: "p15-input-" + r.execution, OutputTransport: "kafka", Diagnostics: true, StartOffsets: starts}
			if err := checkpoints.Register(ctx, pool, registration); err != nil {
				t.Fatal(err)
			}
			if err := r.store.BindResults(ctx, r.execution, sender.Identity.ClusterID, sender.Identity.TopicID, r.consumer, starts); err != nil {
				t.Fatal(err)
			}
		}
		for tick := 0; tick < 3; tick++ {
			at := opts.Window.Start.Add(time.Duration(tick) * telemetry.GridStep)
			missing := []string{}
			for h := 1; h <= 18; h++ {
				missing = append(missing, fmt.Sprintf("%sn%02d", opts.GroupID, h))
			}
			result, err := processor.Step(telemetry.GroupTick{Binding: opts.Binding, GroupID: opts.GroupID, Timestamp: at, Records: map[string]telemetry.TelemetryRecord{}, Missing: missing})
			if err != nil {
				t.Fatal(err)
			}
			e, err := domain.TickFragment(r.execution, snapshot.Identity, result)
			if err != nil {
				t.Fatal(err)
			}
			b, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			r.raw[int32(g-1)] = append(r.raw[int32(g-1)], b)
			if err := sender.Send(ctx, b, opts.GroupID); err != nil {
				t.Fatal(err)
			}
		}
	}
	r.master = usecase.Master{Store: r.store, Metrics: observability.New()}
	return r
}
func (r *kafkaSQLRig) run(ctx context.Context, handle func(context.Context, *kgo.Record, []byte) error) error {
	return ConsumeKafkaWithConfig(ctx, r.seeds, r.consumer, r.sender.Identity, func(ctx context.Context, p int32) (int64, error) { return r.store.ResultOffset(ctx, r.execution, p) }, handle, KafkaConsumerConfig{Concurrency: 4, MaxPollRecords: 128}, r.master.Metrics)
}
func (r *kafkaSQLRig) accept(ctx context.Context, record *kgo.Record, b []byte) error {
	return r.master.AcceptKafka(ctx, r.execution, r.sender.Identity.TopicID, record.Partition, record.Offset, b)
}
func waitKafkaSQL(t *testing.T, condition func() bool) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-tick.C:
		case <-timer.C:
			t.Fatal("Kafka/SQL progress barrier not reached")
		}
	}
}
func (r *kafkaSQLRig) offset(p int32) int64 {
	offset, _ := r.store.ResultOffset(context.Background(), r.execution, p)
	return offset
}
func (r *kafkaSQLRig) finals() int {
	var n int
	_ = r.store.DB.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_results`).Scan(&n)
	return n
}

func TestKafkaPartitionsCommitBeforeSlowPartitionAndRestart(t *testing.T) {
	r := newKafkaSQLRig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	done := make(chan error, 1)
	go func() {
		done <- r.run(ctx, func(ctx context.Context, record *kgo.Record, b []byte) error {
			if record.Partition == 0 {
				once.Do(func() { close(entered) })
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return r.accept(ctx, record, b)
		})
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("slow partition not entered")
	}
	waitKafkaSQL(t, func() bool {
		return r.offset(1) == r.starts[1]+3 && r.offset(2) == r.starts[2]+3 && r.offset(3) == r.starts[3]+3
	})
	if r.offset(0) != r.starts[0] || r.finals() != 0 {
		t.Fatal("slow partition advanced or incomplete timestamp finalized")
	}
	commits, err := kadm.NewClient(r.sender.client).FetchOffsets(context.Background(), r.consumer)
	if err != nil {
		t.Fatal(err)
	}
	for p, entry := range commits[ResultsTopic] {
		if entry.At > r.starts[p] {
			t.Fatal("Kafka acknowledged incomplete wave before durable prefix", p, entry.At)
		}
	}
	releaseOnce.Do(func() { close(release) })
	waitKafkaSQL(t, func() bool { return r.finals() == 12 })
	waitKafkaSQL(t, func() bool {
		commits, err := kadm.NewClient(r.sender.client).FetchOffsets(context.Background(), r.consumer)
		if err != nil {
			return false
		}
		for p := int32(0); p < 4; p++ {
			if commits[ResultsTopic][p].At != r.starts[p]+3 {
				return false
			}
		}
		return true
	})
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("consumer leaked on shutdown")
	}
	// Replay an actual duplicate at a new broker offset after a fresh consumer.
	if err := r.sender.Send(context.Background(), r.raw[0][2], "a01"); err != nil {
		t.Fatal(err)
	}
	restart, stop := context.WithCancel(context.Background())
	defer stop()
	go func() { done <- r.run(restart, r.accept) }()
	waitKafkaSQL(t, func() bool { return r.offset(0) == r.starts[0]+4 })
	if r.finals() != 12 {
		t.Fatal("replay duplicated final results")
	}
	stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restart consumer did not stop")
	}
}

func TestKafkaSQLFailureAndLostCommitReceiptRestoreDurablePrefixes(t *testing.T) {
	for _, afterCommit := range []bool{false, true} {
		t.Run(fmt.Sprint(afterCommit), func(t *testing.T) {
			r := newKafkaSQLRig(t)
			if !afterCommit {
				_, err := r.store.DB.Exec(context.Background(), `CREATE FUNCTION public.p15_fail_intake() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.group_id='a01' AND NEW.event_time=(SELECT min(event_time)+interval '10 seconds' FROM cad_v2.master_tick_fragments) THEN RAISE EXCEPTION 'p15 SQL fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER p15_fail_intake BEFORE INSERT ON cad_v2.master_tick_fragments FOR EACH ROW EXECUTE FUNCTION public.p15_fail_intake()`)
				if err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- r.run(ctx, func(ctx context.Context, record *kgo.Record, b []byte) error {
					if record.Partition == 0 && record.Offset == r.starts[0] {
						for r.offset(1) != r.starts[1]+3 || r.offset(2) != r.starts[2]+3 || r.offset(3) != r.starts[3]+3 {
							select {
							case <-ctx.Done():
								return ctx.Err()
							case <-time.After(5 * time.Millisecond):
							}
						}
					}
					err := r.accept(ctx, record, b)
					if err == nil && afterCommit && record.Partition == 0 {
						return fmt.Errorf("injected loss after confirmed COMMIT")
					}
					return err
				})
			}()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("fault not returned")
				}
			case <-time.After(12 * time.Second):
				t.Fatal("failure did not cancel/join handlers")
			}
			if r.offset(0) != r.starts[0]+1 || r.offset(1) != r.starts[1]+3 || r.offset(2) != r.starts[2]+3 || r.offset(3) != r.starts[3]+3 {
				t.Fatal("durable prefixes lost", r.offset(0), r.offset(1), r.offset(2), r.offset(3))
			}
			if !afterCommit {
				if _, err := r.store.DB.Exec(context.Background(), `DROP TRIGGER p15_fail_intake ON cad_v2.master_tick_fragments; DROP FUNCTION public.p15_fail_intake()`); err != nil {
					t.Fatal(err)
				}
			}
			restart, stop := context.WithCancel(context.Background())
			defer stop()
			first := make(chan int64, 1)
			var once sync.Once
			go func() {
				done <- r.run(restart, func(ctx context.Context, record *kgo.Record, b []byte) error {
					if record.Partition == 0 {
						once.Do(func() { first <- record.Offset })
					}
					return r.accept(ctx, record, b)
				})
			}()
			select {
			case offset := <-first:
				if offset != r.starts[0]+1 {
					t.Fatal("restart trusted Kafka commit instead of SQL", offset)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("restart did not restore SQL position")
			}
			waitKafkaSQL(t, func() bool { return r.finals() == 12 })
			stop()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("restart did not join")
			}
		})
	}
}

func TestKafkaRebalanceWaitsForActiveSQLWave(t *testing.T) {
	r := newKafkaSQLRig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	done := make(chan error, 2)
	go func() {
		done <- r.run(ctx, func(ctx context.Context, record *kgo.Record, b []byte) error {
			if record.Partition == 0 {
				once.Do(func() { close(entered) })
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return r.accept(ctx, record, b)
		})
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("first consumer did not enter wave")
	}
	go func() { done <- r.run(ctx, r.accept) }()
	admin := kadm.NewClient(r.sender.client)
	waitKafkaSQL(t, func() bool {
		groups, err := admin.DescribeGroups(ctx, r.consumer)
		return err == nil && groups[r.consumer].State == "PreparingRebalance"
	})
	if r.offset(0) != r.starts[0] {
		t.Fatal("assignment moved while SQL wave was held")
	}
	close(release)
	waitKafkaSQL(t, func() bool { return r.finals() == 12 })
	waitKafkaSQL(t, func() bool {
		groups, err := admin.DescribeGroups(ctx, r.consumer)
		return err == nil && groups[r.consumer].State == "Stable" && len(groups[r.consumer].Members) == 2
	})
	cancel()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("consumer did not join on rebalance/shutdown")
		}
	}
}
