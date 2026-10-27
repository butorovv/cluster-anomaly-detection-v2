package kafkainput

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/inputcodec"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

type ProducerConfig struct {
	Broker
	Binding         domain.InputBinding
	Window          domain.InputWindow
	DeliveryTimeout time.Duration
}

// Producer is called sequentially by Replay. Send queues at most 18 records;
// Complete is an acknowledgement barrier before publishing the marker. No
// successful replay summary is possible until every record and marker is acked.
// On any failure the stream latches; a retry requires an explicit new execution.
type Producer struct {
	client  *kgo.Client
	cfg     ProducerConfig
	mu      sync.Mutex
	failed  error
	pending int
	group   string
	at      time.Time
	closed  bool
}

func NewProducer(ctx context.Context, cfg ProducerConfig) (*Producer, error) {
	if err := cfg.Binding.Validate(); err != nil {
		return nil, err
	}
	if err := cfg.Window.Validate(); err != nil {
		return nil, err
	}
	if len(cfg.Seeds) == 0 {
		return nil, fmt.Errorf("Kafka seeds required")
	}
	if cfg.DeliveryTimeout == 0 {
		cfg.DeliveryTimeout = 30 * time.Second
	}
	if cfg.DeliveryTimeout < time.Second {
		return nil, fmt.Errorf("delivery timeout must be >=1s")
	}
	opts := append(cfg.options(), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxBufferedRecords(18), kgo.MaxBufferedBytes(2<<20), kgo.ProducerBatchMaxBytes(1<<20),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout), kgo.ProduceRequestTimeout(5*time.Second))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	if _, err = CheckTopic(ctx, cl, cfg.topic()); err != nil {
		cl.Close()
		return nil, err
	}
	return &Producer{client: cl, cfg: cfg}, nil
}

func (p *Producer) failure() error { p.mu.Lock(); defer p.mu.Unlock(); return p.failed }
func (p *Producer) fail(err error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed == nil {
		p.failed = err
	}
	return p.failed
}

func (p *Producer) encode(m domain.InputMessage) (*kgo.Record, error) {
	if p.closed {
		return nil, fmt.Errorf("producer closed")
	}
	if err := p.failure(); err != nil {
		return nil, err
	}
	b, err := inputcodec.Encode(m, p.cfg.Window)
	if err != nil {
		return nil, p.fail(err)
	}
	partition, err := topology.PartitionForGroup(m.GroupID)
	if err != nil {
		return nil, p.fail(err)
	}
	return &kgo.Record{Topic: p.cfg.topic(), Partition: int32(partition), Key: []byte(m.GroupID), Value: b}, nil
}

func (p *Producer) Send(ctx context.Context, r domain.TelemetryRecord) error {
	if err := ctx.Err(); err != nil {
		return p.fail(err)
	}
	if p.pending >= 18 || (p.pending > 0 && (p.group != r.GroupID || !p.at.Equal(r.Timestamp))) {
		return p.fail(fmt.Errorf("marker required before next group/timestamp or >18 records"))
	}
	m := domain.InputMessage{Version: domain.InputVersion, Type: domain.RecordMessage, Binding: p.cfg.Binding, GroupID: r.GroupID, Timestamp: r.Timestamp, Record: &r}
	record, err := p.encode(m)
	if err != nil {
		return err
	}
	p.group, p.at, p.pending = r.GroupID, r.Timestamp, p.pending+1
	p.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		if err != nil {
			p.fail(fmt.Errorf("record acknowledgement failed (delivery may be ambiguous): %w", err))
		}
	})
	return p.failure()
}

func (p *Producer) Complete(ctx context.Context, c domain.TimestampComplete) error {
	if err := ctx.Err(); err != nil {
		return p.fail(err)
	}
	if c.Records != p.pending || (p.pending > 0 && (p.group != c.GroupID || !p.at.Equal(c.Timestamp))) {
		return p.fail(fmt.Errorf("marker does not match pending records"))
	}
	m := domain.InputMessage{Version: domain.InputVersion, Type: domain.CompleteMessage, Binding: p.cfg.Binding, GroupID: c.GroupID, Timestamp: c.Timestamp, Completion: &c}
	record, err := p.encode(m)
	if err != nil {
		return err
	}
	deadline, cancel := context.WithTimeout(ctx, p.cfg.DeliveryTimeout)
	defer cancel()
	if err := p.client.Flush(deadline); err != nil {
		return p.fail(err)
	}
	if err := p.failure(); err != nil {
		return err
	}
	if err := p.client.ProduceSync(deadline, record).FirstErr(); err != nil {
		return p.fail(fmt.Errorf("marker acknowledgement failed (delivery may be ambiguous): %w", err))
	}
	p.pending = 0
	return nil
}

// Close never turns an unmarked prefix into a successful delivery. There are no
// pending sends after a successful Complete; aborted batches are not retried here.
func (p *Producer) Close() error {
	if p.closed {
		return p.failure()
	}
	p.closed = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := p.client.AbortBufferedRecords(ctx)
	p.client.Close()
	if err != nil {
		p.fail(err)
	}
	if p.pending != 0 {
		return errors.Join(p.failure(), fmt.Errorf("producer closed with uncompleted tick (%d records)", p.pending))
	}
	return p.failure()
}
