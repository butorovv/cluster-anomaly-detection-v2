package resulttransport

import (
	"context"
	"errors"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const ResultsTopic = "group.results"
const resultPartitions = 16

type SourceIdentity struct{ ClusterID, TopicID string }
type KafkaSender struct {
	client   *kgo.Client
	Identity SourceIdentity
	metrics  *observability.Registry
}

func NewKafka(ctx context.Context, seeds []string, create bool, metrics ...*observability.Registry) (*KafkaSender, error) {
	if len(seeds) == 0 {
		return nil, fmt.Errorf("Kafka seeds required")
	}
	cl, err := kgo.NewClient(kgo.Dialer(resultDialer(metrics)), kgo.SeedBrokers(seeds...), kgo.DialTimeout(5*time.Second), kgo.RequestTimeoutOverhead(5*time.Second), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		return nil, err
	}
	if create {
		deletePolicy := "delete"
		created, e := kadm.NewClient(cl).CreateTopics(ctx, resultPartitions, 1, map[string]*string{"cleanup.policy": &deletePolicy}, ResultsTopic)
		if e != nil {
			cl.Close()
			return nil, e
		}
		response, ok := created[ResultsTopic]
		if !ok {
			cl.Close()
			return nil, fmt.Errorf("Kafka did not return create response for %s", ResultsTopic)
		}
		if err := response.Err; err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			cl.Close()
			return nil, err
		}
	}
	cluster, topic, err := kafkainput.DescribeSourceWithClient(ctx, cl, ResultsTopic)
	if err != nil {
		cl.Close()
		return nil, err
	}
	var registry *observability.Registry
	if len(metrics) > 0 {
		registry = metrics[0]
	}
	return &KafkaSender{client: cl, Identity: SourceIdentity{ClusterID: cluster, TopicID: topic}, metrics: registry}, nil
}

func (s *KafkaSender) Send(ctx context.Context, raw []byte, groupID string) error {
	e, err := domain.Decode(raw)
	if err != nil {
		return err
	}
	if groupID == "" || strings.TrimSpace(groupID) != groupID || len(groupID) > 256 || e.GroupID != groupID {
		return fmt.Errorf("invalid or mismatched result group ID")
	}
	p, err := topology.PartitionForGroup(groupID)
	if err != nil {
		return err
	}
	checkStarted := time.Now()
	cluster, topic, err := kafkainput.DescribeSourceWithClient(ctx, s.client, ResultsTopic)
	s.metrics.Observe("results_kafka_identity_check", time.Since(checkStarted))
	if err != nil {
		return err
	}
	if cluster != s.Identity.ClusterID || topic != s.Identity.TopicID {
		return fmt.Errorf("results topic incarnation changed")
	}
	r := &kgo.Record{Topic: ResultsTopic, Partition: int32(p), Key: []byte(groupID), Value: raw}
	publishStarted := time.Now()
	err = s.client.ProduceSync(ctx, r).FirstErr()
	s.metrics.Observe("results_kafka_publish_ack", time.Since(publishStarted))
	return err
}

func (s *KafkaSender) Close() error { s.client.Close(); return nil }

// ConsumeKafka processes output records synchronously. Durable PostgreSQL
// positions are loaded before each assignment is allowed to fetch records.
func ConsumeKafka(ctx context.Context, seeds []string, consumerGroup string, pin SourceIdentity, load func(context.Context, int32) (int64, error), handle func(context.Context, *kgo.Record, []byte) error, metrics ...*observability.Registry) (retErr error) {
	var registry *observability.Registry
	if len(metrics) > 0 {
		registry = metrics[0]
	}
	if len(seeds) == 0 || strings.TrimSpace(consumerGroup) == "" || pin.ClusterID == "" || pin.TopicID == "" || load == nil || handle == nil {
		return fmt.Errorf("Kafka consumer configuration incomplete")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	failure := make(chan error, 1)
	fail := func(err error) {
		if err == nil {
			return
		}
		select {
		case failure <- err:
		default:
		}
		cancel()
	}
	admin, err := kgo.NewClient(kgo.Dialer(resultDialer(metrics)), kgo.SeedBrokers(seeds...), kgo.DialTimeout(5*time.Second), kgo.RequestTimeoutOverhead(5*time.Second))
	if err != nil {
		return err
	}
	defer admin.Close()
	checkIdentity := func(c context.Context) error {
		bounded, done := context.WithTimeout(c, 10*time.Second)
		defer done()
		cluster, topic, e := kafkainput.DescribeSourceWithClient(bounded, admin, ResultsTopic)
		if e != nil {
			return e
		}
		if cluster != pin.ClusterID || topic != pin.TopicID {
			return fmt.Errorf("results topic incarnation changed")
		}
		return nil
	}
	if err := checkIdentity(ctx); err != nil {
		return err
	}
	var mu sync.Mutex
	expected := map[int32]int64{}
	assigned := map[int32]bool{}
	callback := func(c context.Context, _ *kgo.Client, parts map[string][]int32) {
		if err := checkIdentity(c); err != nil {
			fail(err)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		for topic, ps := range parts {
			if topic != ResultsTopic {
				fail(fmt.Errorf("unexpected assignment topic %s", topic))
				return
			}
			for _, p := range ps {
				if p < 0 || p >= resultPartitions {
					fail(fmt.Errorf("invalid result partition %d", p))
					return
				}
				assigned[p] = true
			}
		}
	}
	clear := func(_ context.Context, _ *kgo.Client, parts map[string][]int32) {
		mu.Lock()
		defer mu.Unlock()
		for topic, ps := range parts {
			if topic != ResultsTopic {
				continue
			}
			for _, p := range ps {
				delete(assigned, p)
				delete(expected, p)
			}
		}
	}
	adjust := func(session context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
		bounded, done := context.WithTimeout(session, 10*time.Second)
		defer done()
		if err := checkIdentity(bounded); err != nil {
			fail(err)
			return nil, err
		}
		mu.Lock()
		for topic, ps := range offsets {
			if topic != ResultsTopic {
				mu.Unlock()
				err := fmt.Errorf("unexpected restore topic %s", topic)
				fail(err)
				return nil, err
			}
			for p := range ps {
				if !assigned[p] {
					mu.Unlock()
					err := fmt.Errorf("restore without assignment for %d", p)
					fail(err)
					return nil, err
				}
				next, err := load(bounded, p)
				if err != nil {
					mu.Unlock()
					fail(err)
					return nil, err
				}
				if next < 0 {
					mu.Unlock()
					err = fmt.Errorf("negative durable offset for partition %d", p)
					fail(err)
					return nil, err
				}
				ps[p] = kgo.NoResetOffset().At(next).WithEpoch(-1)
				expected[p] = next
			}
		}
		mu.Unlock()
		// Check against fresh bounds only after loading the database positions.
		start, err := kadm.NewClient(admin).ListStartOffsets(bounded, ResultsTopic)
		if err != nil {
			fail(err)
			return nil, err
		}
		end, err := kadm.NewClient(admin).ListEndOffsets(bounded, ResultsTopic)
		if err != nil {
			fail(err)
			return nil, err
		}
		mu.Lock()
		defer mu.Unlock()
		for topic, ps := range offsets {
			for p := range ps {
				lo, lok := start[topic][p]
				hi, hik := end[topic][p]
				next := expected[p]
				if !lok || !hik || lo.Err != nil || hi.Err != nil || next < lo.Offset || next > hi.Offset {
					err := fmt.Errorf("durable offset %d outside current bounds for %s/%d", next, topic, p)
					fail(err)
					return nil, err
				}
			}
		}
		return offsets, nil
	}
	cl, err := kgo.NewClient(kgo.Dialer(resultDialer(metrics)), kgo.SeedBrokers(seeds...), kgo.DialTimeout(5*time.Second), kgo.RequestTimeoutOverhead(5*time.Second), kgo.ConsumerGroup(consumerGroup), kgo.ConsumeTopics(ResultsTopic), kgo.DisableAutoCommit(), kgo.Balancers(kgo.CooperativeStickyBalancer()), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.AdjustFetchOffsetsFn(adjust), kgo.OnPartitionsAssigned(callback), kgo.OnPartitionsRevoked(clear), kgo.OnPartitionsLost(clear), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		return err
	}
	defer func() {
		cl.CloseAllowingRebalance()
		select {
		case e := <-failure:
			retErr = errors.Join(retErr, e)
		default:
		}
	}()
	for {
		pollStarted := time.Now()
		fetches := cl.PollRecords(ctx, 128)
		polledAt := time.Now()
		registry.Observe("results_kafka_poll_wait", polledAt.Sub(pollStarted))
		if err := ctx.Err(); err != nil {
			return err
		}
		if e := fetches.Errors(); len(e) != 0 {
			return fmt.Errorf("Kafka output fetch %s/%d: %w", e[0].Topic, e[0].Partition, e[0].Err)
		}
		records := fetches.Records()
		registry.Set("results_kafka_poll_batch_records", float64(len(records)))
		for _, r := range records {
			if err := ctx.Err(); err != nil {
				return err
			}
			mu.Lock()
			next, ok := expected[r.Partition]
			mu.Unlock()
			if r.Topic != ResultsTopic || !ok || r.Offset != next {
				return fmt.Errorf("unexpected result record position at %s/%d@%d", r.Topic, r.Partition, r.Offset)
			}
			e, err := domain.Decode(r.Value)
			if err != nil {
				return fmt.Errorf("decode output %d@%d: %w", r.Partition, r.Offset, err)
			}
			p, err := topology.PartitionForGroup(e.GroupID)
			if err != nil || p != int(r.Partition) || string(r.Key) != e.GroupID {
				return fmt.Errorf("physical/logical output routing mismatch at %d@%d", r.Partition, r.Offset)
			}
			intakeAt := time.Now()
			registry.Observe("results_kafka_poll_to_intake", intakeAt.Sub(polledAt))
			// Record CreateTime is a processing clock assigned by this producer,
			// never Summit's historical scientific event_time. This combined interval
			// still includes producer transit, broker and client prefetch queue.
			if !r.Timestamp.IsZero() && !polledAt.Before(r.Timestamp) {
				registry.Observe("results_kafka_create_to_poll", polledAt.Sub(r.Timestamp))
			}
			if err := handle(ctx, r, r.Value); err != nil {
				return fmt.Errorf("handle output %d@%d: %w", r.Partition, r.Offset, err)
			}
			registry.Observe("results_kafka_intake_service", time.Since(intakeAt))
			mu.Lock()
			expected[r.Partition] = r.Offset + 1
			mu.Unlock()
		}
		// PostgreSQL output positions are already durable per record. Kafka's
		// auxiliary commit can acknowledge the successfully processed poll batch;
		// a crash in its middle restores SQL positions, never Kafka progress.
		if len(records) > 0 {
			commitStarted := time.Now()
			if err := cl.CommitRecords(ctx, records...); err != nil {
				return fmt.Errorf("commit durable output batch: %w", err)
			}
			registry.Observe("results_kafka_aux_commit", time.Since(commitStarted))
		}
		cl.AllowRebalance()
	}
}

func resultDialer(metrics []*observability.Registry) func(context.Context, string, string) (net.Conn, error) {
	var r *observability.Registry
	if len(metrics) > 0 {
		r = metrics[0]
	}
	return observability.CountDial((&net.Dialer{Timeout: 5 * time.Second}).DialContext, r, "results_kafka")
}
