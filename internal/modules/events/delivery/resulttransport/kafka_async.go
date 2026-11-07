package resulttransport

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaAsyncOptions struct {
	MaxRecords int
	MaxBytes   int64
	Linger     time.Duration
}

// KafkaAsyncSender owns a distinct bounded client. NewKafka and its synchronous
// Send retain the B1 contract for Master bootstrap and control measurements.
type KafkaAsyncSender struct {
	sender *KafkaSender
	once   sync.Once
}

func NewKafkaAsync(ctx context.Context, seeds []string, create bool, opts KafkaAsyncOptions, metrics ...*observability.Registry) (*KafkaAsyncSender, error) {
	if opts.MaxRecords < 1 || opts.MaxRecords > resultPartitions || opts.MaxBytes < domain.MaxMessageBytes+256 || opts.MaxBytes > resultPartitions*(domain.MaxMessageBytes+256) || opts.Linger < 0 || opts.Linger > time.Second {
		return nil, fmt.Errorf("invalid bounded Kafka producer options")
	}
	s, err := newKafka(ctx, seeds, create, []kgo.Opt{kgo.MaxBufferedRecords(opts.MaxRecords), kgo.MaxBufferedBytes(int(opts.MaxBytes)), kgo.ProducerLinger(opts.Linger)}, metrics...)
	if err != nil {
		return nil, err
	}
	return &KafkaAsyncSender{sender: s}, nil
}

// SendAsync borrows immutable raw until done. Error means no callback; nil means
// exactly one callback, possibly before return. Acceptance is not a Kafka ACK.
func (s *KafkaAsyncSender) SendAsync(ctx context.Context, raw []byte, groupID string, done func(error)) error {
	if done == nil {
		return fmt.Errorf("Kafka completion required")
	}
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
	started := time.Now()
	cluster, topic, err := describeProducerSource(ctx, kadm.NewClient(s.sender.client), ResultsTopic)
	s.sender.metrics.Observe("results_kafka_identity_check", time.Since(started))
	if err != nil {
		return err
	}
	if cluster != s.sender.Identity.ClusterID || topic != s.sender.Identity.TopicID {
		return fmt.Errorf("results topic incarnation changed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r := &kgo.Record{Topic: ResultsTopic, Partition: int32(p), Key: []byte(groupID), Value: raw}
	started = time.Now()
	s.sender.client.Produce(ctx, r, func(_ *kgo.Record, err error) {
		// No SQL, lifecycle operations, or dispatcher locks in this callback.
		s.sender.metrics.Observe("results_kafka_publish_ack", time.Since(started))
		done(err)
	})
	return nil
}

func (s *KafkaAsyncSender) Flush(ctx context.Context) error { return s.sender.client.Flush(ctx) }
func (s *KafkaAsyncSender) Close() error {
	s.once.Do(func() { s.sender.client.Close() })
	return nil
}
