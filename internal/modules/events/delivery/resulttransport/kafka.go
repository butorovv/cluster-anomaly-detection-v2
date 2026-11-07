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
	return newKafka(ctx, seeds, create, nil, metrics...)
}

func newKafka(ctx context.Context, seeds []string, create bool, extra []kgo.Opt, metrics ...*observability.Registry) (*KafkaSender, error) {
	if len(seeds) == 0 {
		return nil, fmt.Errorf("Kafka seeds required")
	}
	opts := []kgo.Opt{kgo.Dialer(resultDialer(metrics)), kgo.SeedBrokers(seeds...), kgo.DialTimeout(5 * time.Second), kgo.RequestTimeoutOverhead(5 * time.Second), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.RequiredAcks(kgo.AllISRAcks())}
	cl, err := kgo.NewClient(append(opts, extra...)...)
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
	cluster, topic, err := describeProducerSource(ctx, kadm.NewClient(s.client), ResultsTopic)
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

type producerTopicAdmin interface {
	Metadata(context.Context, ...string) (kadm.Metadata, error)
	DescribeTopicConfigs(context.Context, ...string) (kadm.ResourceConfigs, error)
}

// describeProducerSource deliberately stays local to the producer: input and
// result consumers retain their existing identity checks. Every send still
// checks fresh metadata and effective policy; no identity is cached here.
func describeProducerSource(ctx context.Context, admin producerTopicAdmin, topic string) (string, string, error) {
	if admin == nil || topic == "" {
		return "", "", fmt.Errorf("Kafka admin/topic required")
	}
	m, err := admin.Metadata(ctx, topic)
	if err != nil {
		return "", "", err
	}
	if m.Cluster == "" {
		return "", "", fmt.Errorf("Kafka cluster ID required")
	}
	t, ok := m.Topics[topic]
	if !ok || t.Err != nil {
		return "", "", fmt.Errorf("topic %s unavailable: %v", topic, t.Err)
	}
	if len(t.Partitions) != resultPartitions {
		return "", "", fmt.Errorf("topic %s: require 16 partitions, got %d", topic, len(t.Partitions))
	}
	for p := int32(0); p < resultPartitions; p++ {
		d, ok := t.Partitions[p]
		if !ok || d.Err != nil || d.Leader < 0 {
			return "", "", fmt.Errorf("partition %d unavailable: %v", p, d.Err)
		}
	}
	if t.ID == (kadm.TopicID{}) {
		return "", "", fmt.Errorf("topic ID required for recovery identity")
	}
	configs, err := admin.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return "", "", fmt.Errorf("describe configuration for topic %s: %w", topic, err)
	}
	if len(configs) != 1 || configs[0].Name != topic {
		return "", "", fmt.Errorf("topic %s configuration unavailable: require exactly one resource", topic)
	}
	config := configs[0]
	if config.Err != nil {
		return "", "", fmt.Errorf("topic %s configuration unavailable: %w", topic, config.Err)
	}
	var policy *string
	seen := false
	for _, c := range config.Configs {
		if c.Key == "cleanup.policy" {
			if seen {
				return "", "", fmt.Errorf("topic %s has duplicate cleanup.policy configurations", topic)
			}
			seen, policy = true, c.Value
		}
	}
	if policy == nil {
		return "", "", fmt.Errorf("topic %s cleanup.policy unavailable", topic)
	}
	if *policy != "delete" {
		return "", "", fmt.Errorf("topic %s has unsafe cleanup.policy %q: require delete", topic, *policy)
	}
	return m.Cluster, t.ID.String(), nil
}

// ConsumeKafka uses bounded partition-aware processing with default limits.
func ConsumeKafka(ctx context.Context, seeds []string, consumerGroup string, pin SourceIdentity, load func(context.Context, int32) (int64, error), handle func(context.Context, *kgo.Record, []byte) error, metrics ...*observability.Registry) (retErr error) {
	return ConsumeKafkaWithConfig(ctx, seeds, consumerGroup, pin, load, handle, KafkaConsumerConfig{}, metrics...)
}

// ConsumeKafkaWithConfig loads SQL positions before fetch and retains the
// rebalance barrier until every handler in the bounded wave has returned.
func ConsumeKafkaWithConfig(ctx context.Context, seeds []string, consumerGroup string, pin SourceIdentity, load func(context.Context, int32) (int64, error), handle func(context.Context, *kgo.Record, []byte) error, cfg KafkaConsumerConfig, metrics ...*observability.Registry) (retErr error) {
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
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
	cl, err := kgo.NewClient(kgo.Dialer(resultDialer(metrics)), kgo.SeedBrokers(seeds...), kgo.DialTimeout(5*time.Second), kgo.RequestTimeoutOverhead(5*time.Second), kgo.ConsumerGroup(consumerGroup), kgo.ConsumeTopics(ResultsTopic), kgo.DisableAutoCommit(), kgo.Balancers(kgo.CooperativeStickyBalancer()), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.AdjustFetchOffsetsFn(adjust), kgo.OnPartitionsAssigned(callback), kgo.OnPartitionsRevoked(clear), kgo.OnPartitionsLost(clear), kgo.FetchMaxWait(100*time.Millisecond), kgo.FetchMaxBytes(8<<20), kgo.FetchMaxPartitionBytes(domain.MaxMessageBytes+1024), kgo.MaxConcurrentFetches(0), kgo.SessionTimeout(10*time.Second), kgo.HeartbeatInterval(time.Second), kgo.RebalanceTimeout(30*time.Second))
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
		fetches := cl.PollRecords(ctx, cfg.MaxPollRecords)
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
		mu.Lock()
		positions := make(map[int32]int64, len(expected))
		for p, next := range expected {
			positions[p] = next
		}
		mu.Unlock()
		lasts, err := processKafkaBatch(ctx, cfg, records, positions, polledAt, handle, registry)
		if err != nil {
			return err
		} // SQL prefixes remain durable; restart loads them.
		mu.Lock()
		for _, r := range lasts {
			expected[r.Partition] = r.Offset + 1
		}
		mu.Unlock()
		// PostgreSQL output positions are already durable per record. Kafka's
		// auxiliary commit can acknowledge the successfully processed poll batch;
		// a crash in its middle restores SQL positions, never Kafka progress.
		if len(lasts) > 0 {
			commitStarted := time.Now()
			commitCtx, commitCancel := context.WithTimeout(ctx, time.Second)
			err := cl.CommitRecords(commitCtx, lasts...)
			commitCancel()
			if err != nil {
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
