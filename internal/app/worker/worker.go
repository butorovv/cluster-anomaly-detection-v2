// Package worker owns process lifecycle and connects transport/store adapters to
// the pure per-partition use case. Ownership always follows actual group assignment.
package worker

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	events "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	delivery "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Bundle        *group.Bundle
	Options       group.GroupProcessorOptions
	Broker        kafkainput.Broker
	ConsumerGroup string
	OwnerID       string
	LeaseTTL      time.Duration
	Logger        *slog.Logger
	Output        *delivery.Dispatcher
}

type assignedPartition struct {
	lease   repo.Lease
	core    *core.Partition
	loading bool
}
type Runner struct {
	cfg      Config
	store    *repo.Store
	mu       sync.Mutex
	assigned map[int32]kafkainput.Identity
	parts    map[int32]*assignedPartition
	closed   bool
}

func NewOwnerID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func New(cfg Config, store *repo.Store) (*Runner, error) {
	if store == nil || store.DB == nil || cfg.ConsumerGroup == "" || (cfg.Options.Groups != 4 && cfg.Options.Groups != 8 && cfg.Options.Groups != 16) || len(cfg.Broker.Seeds) == 0 || (cfg.Broker.Topic != "" && cfg.Broker.Topic != kafkainput.Topic) {
		return nil, fmt.Errorf("Worker requires registered store, Kafka source and 4/8/16-group bundle")
	}
	if cfg.Output != nil && (cfg.Output.ExecutionID != cfg.Options.Binding.ExecutionID || cfg.Output.Mode != store.Registration.OutputTransport) {
		return nil, fmt.Errorf("dispatcher differs from registered execution transport")
	}
	if cfg.LeaseTTL == 0 {
		cfg.LeaseTTL = repo.DefaultLease
	}
	if cfg.LeaseTTL < 3*time.Second || cfg.LeaseTTL > 15*time.Second {
		return nil, fmt.Errorf("lease TTL must be 3..15 seconds")
	}
	if cfg.OwnerID == "" {
		var err error
		cfg.OwnerID, err = NewOwnerID()
		if err != nil {
			return nil, err
		}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	opts := cfg.Options
	opts.GroupID = "a01"
	p, err := group.NewGroupProcessor(cfg.Bundle, opts)
	if err != nil {
		return nil, err
	}
	s, err := p.Snapshot()
	if err != nil {
		return nil, err
	}
	a, _ := json.Marshal(s.Identity)
	b, _ := json.Marshal(store.Registration.Identity)
	if string(a) != string(b) || cfg.Options.Binding != store.Registration.Binding || cfg.ConsumerGroup != store.Registration.ConsumerGroup {
		return nil, fmt.Errorf("Worker differs from registered frozen execution")
	}
	return &Runner{cfg: cfg, store: store, assigned: map[int32]kafkainput.Identity{}, parts: map[int32]*assignedPartition{}}, nil
}

func (r *Runner) assignedHook(ctx context.Context, ids []kafkainput.Identity) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("Worker closed")
	}
	for _, id := range ids {
		if id.Topic != kafkainput.Topic || id.TopicID != r.store.Registration.TopicID || id.Binding != r.cfg.Options.Binding || id.Window.Start != r.cfg.Options.Window.Start || id.Window.End != r.cfg.Options.Window.End {
			return fmt.Errorf("assigned source identity mismatch")
		}
		r.assigned[id.Partition] = id
		r.cfg.Logger.InfoContext(ctx, "partition assigned", "partition", id.Partition, "group", id.GroupID, "owner", r.cfg.OwnerID)
	}
	return nil
}

func stale(err error) bool { var e *pgconn.PgError; return errors.As(err, &e) && e.Code == "55000" }

func (r *Runner) revokeHook(ctx context.Context, ids []kafkainput.Identity) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result error
	for _, id := range ids {
		delete(r.assigned, id.Partition)
		state := r.parts[id.Partition]
		delete(r.parts, id.Partition) // discard partial/uncommitted RAM before release
		if state != nil && r.cfg.Output != nil {
			r.cfg.Output.Forget(state.lease.Group, state.lease.Epoch)
		}
		if state != nil && state.lease.Epoch > 0 {
			if err := r.store.Release(ctx, state.lease); err != nil && !stale(err) {
				result = errors.Join(result, err)
			}
		}
		r.cfg.Logger.InfoContext(ctx, "partition revoked/lost", "partition", id.Partition, "group", id.GroupID)
	}
	return result
}

func (r *Runner) load(ctx context.Context, id kafkainput.Identity) (kafkainput.Position, error) {
	if int(id.Partition) >= r.cfg.Options.Groups {
		return kafkainput.Position{Identity: id, NextOffset: r.store.Registration.StartOffsets[strconv.Itoa(int(id.Partition))]}, nil
	}
	r.mu.Lock()
	active, ok := r.assigned[id.Partition]
	if r.closed || !ok || active != id {
		r.mu.Unlock()
		return kafkainput.Position{}, fmt.Errorf("restore without actual assignment")
	}
	state := r.parts[id.Partition]
	if state != nil {
		state.loading = true
	}
	r.mu.Unlock()
	// Acquire/restore may wait for an expired crashed owner's lease. Do not hold
	// the manager mutex here: retained partitions must continue renewing leases.
	var lease repo.Lease
	var saved *core.Checkpoint
	var err error
	if state == nil {
		for {
			lease, saved, err = r.store.Acquire(ctx, id.GroupID, r.cfg.OwnerID, r.cfg.LeaseTTL)
			if !errors.Is(err, repo.ErrLeased) {
				break
			}
			select {
			case <-ctx.Done():
				return kafkainput.Position{}, ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		if err != nil {
			return kafkainput.Position{}, err
		}
		state = &assignedPartition{lease: lease, loading: true}
		r.mu.Lock()
		r.parts[id.Partition] = state
		r.mu.Unlock()
	} else {
		lease = state.lease
		if err = r.store.Renew(ctx, lease, r.cfg.LeaseTTL); err != nil {
			return kafkainput.Position{}, err
		}
		saved, err = r.store.Load(ctx, id.GroupID)
		if err != nil {
			return kafkainput.Position{}, err
		}
	}
	options := r.cfg.Options
	options.GroupID = id.GroupID
	start := r.store.Registration.StartOffsets[strconv.Itoa(int(id.Partition))]
	processor, err := core.NewPartition(r.cfg.Bundle, options, start, saved)
	if err != nil {
		return kafkainput.Position{}, err
	}
	if err = r.store.Renew(ctx, lease, r.cfg.LeaseTTL); err != nil {
		return kafkainput.Position{}, err
	}
	next := start
	if saved != nil {
		next = saved.NextOffset
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.assigned[id.Partition] != id {
		return kafkainput.Position{}, fmt.Errorf("assignment disappeared during restore")
	}
	if r.cfg.Output != nil {
		if err := r.cfg.Output.Track(ctx, events.Owner{ExecutionID: r.cfg.Options.Binding.ExecutionID, GroupID: lease.Group, OwnerID: lease.Owner, Epoch: lease.Epoch}); err != nil {
			return kafkainput.Position{}, err
		}
	}
	state.core, state.loading = processor, false
	r.cfg.Logger.InfoContext(ctx, "partition restored", "partition", id.Partition, "group", id.GroupID, "epoch", lease.Epoch, "next_offset", next, "recovered", saved != nil)
	return kafkainput.Position{Identity: id, NextOffset: next}, nil
}

func (r *Runner) handle(ctx context.Context, record *kgo.Record, message domain.InputMessage) error {
	if int(record.Partition) >= r.cfg.Options.Groups {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.parts[record.Partition]
	if r.closed || state == nil || state.loading || state.core == nil {
		return fmt.Errorf("record without restored owner")
	}
	if _, ok := r.assigned[record.Partition]; !ok {
		return fmt.Errorf("record after lost assignment")
	}
	transition, err := state.core.Accept(record.Offset, message)
	if err != nil {
		return err
	}
	r.cfg.Logger.DebugContext(ctx, "input accepted", "partition", record.Partition, "offset", record.Offset, "complete", transition != nil)
	if transition == nil {
		return nil
	}
	bounded, done := context.WithTimeout(ctx, 5*time.Second)
	defer done()
	revision, err := r.store.Save(bounded, state.lease, *transition)
	if err != nil {
		return state.core.Fail(err)
	}
	if err = state.core.Ack(revision); err != nil {
		return err
	}
	r.cfg.Logger.InfoContext(ctx, "checkpoint committed", "partition", record.Partition, "group", message.GroupID, "epoch", state.lease.Epoch, "revision", revision, "next_offset", transition.Checkpoint.NextOffset, "tick", message.Timestamp, "finalized", transition.Checkpoint.Finalized, "events", len(transition.Result.Events))
	return nil
}

func (r *Runner) renew(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	for _, state := range r.parts {
		bounded, done := context.WithTimeout(ctx, min(5*time.Second, r.cfg.LeaseTTL/2))
		err := r.store.Renew(bounded, state.lease, r.cfg.LeaseTTL)
		done()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) close() error {
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	var result error
	for partition, state := range r.parts {
		delete(r.parts, partition)
		if r.cfg.Output != nil {
			r.cfg.Output.Forget(state.lease.Group, state.lease.Epoch)
		}
		delete(r.assigned, partition)
		if err := r.store.Release(ctx, state.lease); err != nil && !stale(err) {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (r *Runner) Run(ctx context.Context) (retErr error) {
	cluster, topic, err := kafkainput.DescribeSource(ctx, r.cfg.Broker)
	if err != nil {
		return err
	}
	if cluster != r.store.Registration.ClusterID || topic != r.store.Registration.TopicID {
		return fmt.Errorf("Kafka source incarnation differs from registration")
	}
	if err := repo.ValidateRegistration(ctx, r.store.DB, r.store.Registration); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() { retErr = errors.Join(retErr, r.close()) }()
	outputStopped := make(chan struct{})
	outputFailure := make(chan error, 1)
	if r.cfg.Output != nil {
		go func() {
			defer close(outputStopped)
			if err := r.cfg.Output.Run(ctx); err != nil {
				outputFailure <- err
				cancel()
			}
		}()
	} else {
		close(outputStopped)
	}
	heartbeat := make(chan error, 1)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(r.cfg.LeaseTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.renew(ctx); err != nil {
					heartbeat <- err
					cancel()
					return
				}
			}
		}
	}()
	cfg := kafkainput.ConsumerConfig{Broker: r.cfg.Broker, Binding: r.cfg.Options.Binding, Window: domain.InputWindow{Start: r.cfg.Options.Window.Start, End: r.cfg.Options.Window.End}, Group: r.cfg.ConsumerGroup, Load: r.load, Assigned: r.assignedHook, Revoked: r.revokeHook, Lost: r.revokeHook, RestoreTimeout: r.cfg.LeaseTTL + 5*time.Second, Report: func(e error) { r.cfg.Logger.WarnContext(ctx, "Kafka session interrupted", "error", e) }}
	retErr = kafkainput.ConsumeGroup(ctx, cfg, r.handle)
	cancel()
	<-stopped
	<-outputStopped
	select {
	case err := <-outputFailure:
		retErr = errors.Join(retErr, err)
	default:
	}
	select {
	case err := <-heartbeat:
		retErr = errors.Join(retErr, err)
	default:
	}
	return retErr
}
