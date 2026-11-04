package kafkainput

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/inputcodec"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Identity prevents using an offset from another topic incarnation, partition,
// logical group, execution, source, profile or time window. Fixtures must persist
// this entire identity, not just a naked integer. The Worker layer supplies
// PostgreSQL ownership fencing and mathematical state restoration through Load.
type Identity struct {
	Topic, TopicID string
	Partition      int32
	GroupID        string
	Binding        domain.InputBinding
	Window         domain.InputWindow
}

type Position struct {
	Identity   Identity
	NextOffset int64
}

type OffsetLoader func(context.Context, Identity) (Position, error)
type Handler func(context.Context, *kgo.Record, domain.InputMessage) error
type LifecycleHook func(context.Context, []Identity) error

type ConsumerConfig struct {
	Broker
	Binding domain.InputBinding
	Window  domain.InputWindow
	Group   string
	Load    OffsetLoader
	// Lifecycle hooks are synchronous, error-returning ownership barriers.
	// Load remains the acquire/restore barrier before fetch. Hooks and Load are
	// serialized; Observe is still diagnostic and does not authorize processing.
	Assigned       LifecycleHook
	Revoked        LifecycleHook
	Lost           LifecycleHook
	RestoreTimeout time.Duration
	// Observe is diagnostic only, must be fast and concurrency-safe. Assigned is
	// protocol assignment, not permission to process before Load/validation.
	Observe func(event string, partitions []int32)
	Report  func(error)
	// FetchProgress is diagnostic high-watermark sampling; it never authorizes progress.
	FetchProgress func(partition int32, highWatermark int64)
}

func (c ConsumerConfig) identity(id string, partition int32) Identity {
	return Identity{c.topic(), id, partition, fmt.Sprintf("a%02d", partition+1), c.Binding, c.Window}
}

func (c ConsumerConfig) validate() error {
	if len(c.Seeds) == 0 || c.Load == nil {
		return fmt.Errorf("Kafka seeds and external offset loader required")
	}
	if err := c.Binding.Validate(); err != nil {
		return err
	}
	return c.Window.Validate()
}

// ConsumeGroup always overwrites Kafka commits with a validated external
// next_offset in AdjustFetchOffsetsFn, BEFORE fetching each new assignment.
// It deliberately neither commits to Kafka nor saves checkpoints itself.
func ConsumeGroup(ctx context.Context, cfg ConsumerConfig, handle Handler) (retErr error) {
	if err := cfg.validate(); err != nil {
		return err
	}
	if cfg.Group == "" || handle == nil {
		return fmt.Errorf("consumer group and handler required")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	failure := make(chan error, 1)
	fail := func(err error) {
		select {
		case failure <- err:
		default:
		}
		cancel()
	}
	var lock sync.Mutex
	expected := map[int32]int64{}
	assigned := map[int32]bool{}
	restoreTimeout := cfg.RestoreTimeout
	if restoreTimeout == 0 {
		restoreTimeout = 10 * time.Second
	}
	if restoreTimeout < time.Second || restoreTimeout > 5*time.Minute {
		return fmt.Errorf("invalid restore timeout")
	}
	admin, err := kgo.NewClient(cfg.options()...)
	if err != nil {
		return err
	}
	defer admin.Close()
	topicID, err := CheckTopic(ctx, admin, cfg.topic())
	if err != nil {
		return err
	}
	observe := func(event string, hook LifecycleHook) func(context.Context, *kgo.Client, map[string][]int32) {
		return func(callbackCtx context.Context, _ *kgo.Client, partitions map[string][]int32) {
			lock.Lock()
			identities := make([]Identity, 0, len(partitions[cfg.topic()]))
			for topic, ps := range partitions {
				if topic != cfg.topic() {
					fail(fmt.Errorf("unexpected lifecycle topic %s", topic))
					continue
				}
				for _, p := range ps {
					if p < 0 || p >= Partitions {
						fail(fmt.Errorf("invalid assigned partition %d", p))
						continue
					}
					if event == "assigned" {
						assigned[p] = true
					} else {
						delete(assigned, p)
						delete(expected, p)
					}
					identities = append(identities, cfg.identity(topicID, p))
				}
			}
			if hook != nil {
				bounded, done := context.WithTimeout(context.WithoutCancel(callbackCtx), restoreTimeout)
				if err := hook(bounded, identities); err != nil {
					fail(err)
				}
				done()
			}
			lock.Unlock()
			if cfg.Observe != nil {
				cfg.Observe(event, append([]int32(nil), partitions[cfg.topic()]...))
			}
		}
	}
	adjust := func(session context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
		bounded, done := context.WithTimeout(session, restoreTimeout)
		defer done()
		id, err := CheckTopic(bounded, admin, cfg.topic())
		if err == nil && id != topicID {
			err = fmt.Errorf("topic incarnation changed")
		}
		if err != nil {
			fail(err)
			return nil, err
		}
		for topic, partitions := range offsets {
			if topic != cfg.topic() {
				err = fmt.Errorf("unexpected assigned topic %s", topic)
				fail(err)
				return nil, err
			}
			for partition := range partitions {
				lock.Lock()
				if !assigned[partition] {
					lock.Unlock()
					err = fmt.Errorf("restore without assignment for %d", partition)
					fail(err)
					return nil, err
				}
				identity := cfg.identity(topicID, partition)
				position, loadErr := cfg.Load(bounded, identity)
				if loadErr == nil && position.Identity != identity {
					loadErr = fmt.Errorf("durable identity mismatch for partition %d", partition)
				}
				if loadErr != nil {
					lock.Unlock()
					fail(loadErr)
					return nil, loadErr
				}
				partitions[partition] = kgo.NoResetOffset().At(position.NextOffset).WithEpoch(-1)
				expected[partition] = position.NextOffset
				lock.Unlock()
			}
		}
		// Ownership acquisition may wait for an old lease. That owner can still
		// commit while we wait, so bounds sampled BEFORE Load can be stale. Check
		// the restored positions against current bounds only after every Load.
		start, end, err := offsetBounds(bounded, admin, cfg.topic())
		if err != nil {
			fail(err)
			return nil, err
		}
		lock.Lock()
		defer lock.Unlock()
		for topic, partitions := range offsets {
			for partition := range partitions {
				if err := checkOffset(start, end, topic, partition, expected[partition]); err != nil {
					fail(err)
					return nil, err
				}
			}
		}
		return offsets, nil
	}
	opts := append(cfg.options(), kgo.ConsumerGroup(cfg.Group), kgo.ConsumeTopics(cfg.topic()), kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		kgo.FetchMaxBytes(4<<20), kgo.FetchMaxPartitionBytes(1<<20), kgo.FetchMaxWait(100*time.Millisecond),
		kgo.SessionTimeout(10*time.Second), kgo.RebalanceTimeout(max(20*time.Second, restoreTimeout+5*time.Second)), kgo.HeartbeatInterval(time.Second),
		kgo.OnPartitionsAssigned(observe("assigned", cfg.Assigned)), kgo.OnPartitionsRevoked(observe("revoked", cfg.Revoked)), kgo.OnPartitionsLost(observe("lost", cfg.Lost)),
		kgo.AdjustFetchOffsetsFn(adjust))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer func() {
		cl.CloseAllowingRebalance()
		// Final revoke may fail during Close; do not silently lose that failure.
		select {
		case fatal := <-failure:
			retErr = errors.Join(retErr, fatal)
		default:
		}
	}()
	err = consume(ctx, cl, cfg, handle, func(r *kgo.Record) error {
		lock.Lock()
		defer lock.Unlock()
		next, ok := expected[r.Partition]
		if !ok || r.Offset != next {
			return fmt.Errorf("unexpected offset for partition %d: got %d want %d", r.Partition, r.Offset, next)
		}
		expected[r.Partition] = r.Offset + 1
		return nil
	})
	select {
	case fatal := <-failure:
		return fatal
	default:
		return err
	}
}

// ConsumePartition exercises direct partition reading using the same validation
// and no-reset contract. Future workers use ConsumeGroup, never static routing.
func ConsumePartition(ctx context.Context, cfg ConsumerConfig, position Position, handle Handler) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if handle == nil {
		return fmt.Errorf("handler required")
	}
	admin, err := kgo.NewClient(cfg.options()...)
	if err != nil {
		return err
	}
	defer admin.Close()
	id, err := CheckTopic(ctx, admin, cfg.topic())
	if err != nil {
		return err
	}
	p := position.Identity.Partition
	if p < 0 || p >= Partitions || position.Identity != cfg.identity(id, p) {
		return fmt.Errorf("durable identity mismatch")
	}
	start, end, err := offsetBounds(ctx, admin, cfg.topic())
	if err != nil {
		return err
	}
	if err := checkOffset(start, end, cfg.topic(), p, position.NextOffset); err != nil {
		return err
	}
	opts := append(cfg.options(), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.topic(): {p: kgo.NoResetOffset().At(position.NextOffset)}}),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchMaxBytes(4<<20), kgo.FetchMaxPartitionBytes(1<<20))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()
	next := position.NextOffset
	return consume(ctx, cl, cfg, handle, func(r *kgo.Record) error {
		if r.Partition != p || r.Offset != next {
			return fmt.Errorf("partition/offset discontinuity: got %d/%d expected %d/%d", r.Partition, r.Offset, p, next)
		}
		next++
		return nil
	})
}

func consume(ctx context.Context, cl *kgo.Client, cfg ConsumerConfig, handle Handler, position func(*kgo.Record) error) error {
	for {
		fetches := cl.PollRecords(ctx, 128)
		for _, failure := range fetches.Errors() {
			var session *kgo.ErrGroupSession
			var network *net.OpError
			if errors.As(failure.Err, &session) && (kgo.IsRetryableBrokerErr(session.Err) || kerr.IsRetriable(session.Err) || errors.As(session.Err, &network)) {
				if cfg.Report != nil {
					cfg.Report(failure.Err)
				} else {
					slog.WarnContext(ctx, "Kafka group session interrupted; retrying assignment from external offset", "error", failure.Err)
				}
				continue
			}
			return fmt.Errorf("Kafka fetch %s/%d: %w", failure.Topic, failure.Partition, failure.Err)
		}
		if cfg.FetchProgress != nil {
			fetches.EachPartition(func(p kgo.FetchTopicPartition) { cfg.FetchProgress(p.Partition, p.HighWatermark) })
		}
		for _, r := range fetches.Records() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := position(r); err != nil {
				return err
			}
			m, err := inputcodec.Decode(r.Value, cfg.Window)
			if err != nil {
				return fmt.Errorf("decode %s/%d@%d: %w", r.Topic, r.Partition, r.Offset, err)
			}
			p, err := topology.PartitionForGroup(m.GroupID)
			if err != nil || r.Topic != cfg.topic() || int32(p) != r.Partition || string(r.Key) != m.GroupID {
				return fmt.Errorf("physical/logical routing mismatch at %s/%d@%d", r.Topic, r.Partition, r.Offset)
			}
			if m.Binding != cfg.Binding {
				return fmt.Errorf("input binding mismatch at %d@%d", r.Partition, r.Offset)
			}
			if err := handle(ctx, r, m); err != nil {
				return fmt.Errorf("handle %s/%d@%d: %w", r.Topic, r.Partition, r.Offset, err)
			}
		}
		cl.AllowRebalance()
	}
}
