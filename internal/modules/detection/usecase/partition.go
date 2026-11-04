// Package usecase connects completed input ticks to frozen detection. It has no
// Kafka or PostgreSQL dependency. Every pending transition needs a durable ACK.
package usecase

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/usecase"
)

// Checkpoint contains only a complete mathematical boundary. SHA256 hashes exact
// Snapshot bytes including the mathematical DTO's own canonical checksum.
type Checkpoint struct {
	Revision   int64
	NextOffset int64
	LastTick   time.Time
	Finalized  bool
	Snapshot   []byte
	SHA256     string
}

type Transition struct {
	Checkpoint Checkpoint
	Result     group.GroupProcessorResult   // local only; never a final Master group_result
	Results    []group.GroupProcessorResult // completed ticks staged in this durable transition
}

type Partition struct {
	processor          *group.GroupProcessor
	assembler          *telemetry.TickAssembler
	options            group.GroupProcessorOptions
	revision, readNext int64
	pending            *Transition
	batch              []group.GroupProcessorResult
	cadence            int
	lastResult         group.GroupProcessorResult
	lastCompute        time.Duration
	lastComputedAt     time.Time
	finished           bool
	failed             error
}

func SnapshotHash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func DecodeCheckpoint(c Checkpoint) (group.GroupProcessorSnapshot, error) {
	var s group.GroupProcessorSnapshot
	if c.Revision < 1 || c.NextOffset < 0 || len(c.Snapshot) == 0 || SnapshotHash(c.Snapshot) != c.SHA256 {
		return s, fmt.Errorf("invalid durable snapshot bytes/hash/revision")
	}
	if err := json.Unmarshal(c.Snapshot, &s); err != nil {
		return s, err
	}
	if !s.Journal.Last.Equal(c.LastTick) || s.Finished != c.Finalized {
		return s, fmt.Errorf("durable position differs from mathematical snapshot")
	}
	return s, nil
}

func NewPartition(bundle *group.Bundle, options group.GroupProcessorOptions, startOffset int64, saved *Checkpoint) (*Partition, error) {
	return NewPartitionWithCadence(bundle, options, startOffset, saved, 1)
}

// NewPartitionWithCadence changes only the durable boundary; Step/Finish remain
// exactly once per complete tick. Uncommitted batches are always replayed.
func NewPartitionWithCadence(bundle *group.Bundle, options group.GroupProcessorOptions, startOffset int64, saved *Checkpoint, cadence int) (*Partition, error) {
	if cadence != 1 && cadence != 3 && cadence != 6 {
		return nil, fmt.Errorf("checkpoint interval must be 1, 3 or 6 ticks")
	}
	if startOffset < 0 {
		return nil, fmt.Errorf("explicit nonnegative start offset required")
	}
	p, err := group.NewGroupProcessor(bundle, options)
	if err != nil {
		return nil, err
	}
	nextTime := options.Window.Start
	revision, nextOffset, finished := int64(0), startOffset, false
	if saved != nil {
		s, err := DecodeCheckpoint(*saved)
		if err != nil {
			return nil, err
		}
		if saved.NextOffset < startOffset {
			return nil, fmt.Errorf("durable offset precedes registration")
		}
		p, err = group.RestoreGroupProcessor(bundle, options, s)
		if err != nil {
			return nil, err
		}
		nextTime, revision, nextOffset, finished = s.Next, saved.Revision, saved.NextOffset, s.Finished
	}
	var a *telemetry.TickAssembler
	if !finished {
		// Restoring a complete boundary leaves no partial assembler buffer. Replay
		// begins at the first message of the next tick, not a prefetched RAM offset.
		a, err = telemetry.NewTickAssembler(options.GroupID, options.Binding, domain.InputWindow{Start: nextTime, End: options.Window.End})
		if err != nil {
			return nil, err
		}
	}
	return &Partition{processor: p, assembler: a, options: options, revision: revision, readNext: nextOffset, finished: finished, cadence: cadence}, nil
}

func (p *Partition) Fail(err error) error {
	if p.failed == nil {
		p.failed = err
	}
	return p.failed
}

// Accept is sequential. Record acceptance changes RAM only; a marker creates
// a complete-tick batch proposal at cadence (or finalization), then waits for Ack.
func (p *Partition) Accept(offset int64, message domain.InputMessage) (*Transition, error) {
	if p.failed != nil {
		return nil, p.failed
	}
	if p.finished || p.pending != nil {
		return nil, p.Fail(fmt.Errorf("partition finalized or awaiting durable acknowledgement"))
	}
	if offset != p.readNext {
		return nil, p.Fail(fmt.Errorf("input offset discontinuity: got %d want %d", offset, p.readNext))
	}
	tick, err := p.assembler.Accept(message)
	if err != nil {
		return nil, p.Fail(err)
	}
	p.readNext++
	if tick == nil {
		return nil, nil
	}
	computeStart := time.Now()
	result, err := p.processor.Step(*tick)
	if err != nil {
		return nil, p.Fail(err)
	}
	final := tick.Timestamp.Add(domain.GridStep).Equal(p.options.Window.End)
	if final {
		if err := p.assembler.Finish(); err != nil {
			return nil, p.Fail(err)
		}
		events, err := p.processor.Finish()
		if err != nil {
			return nil, p.Fail(err)
		}
		result.Events = append(result.Events, events...)
	}
	p.lastComputedAt = time.Now()
	p.lastResult, p.lastCompute = result, p.lastComputedAt.Sub(computeStart)
	p.batch = append(p.batch, result)
	if !final && len(p.batch) < p.cadence {
		return nil, nil
	}
	s, err := p.processor.Snapshot()
	if err != nil {
		return nil, p.Fail(err)
	}
	bytes, err := json.Marshal(s)
	if err != nil {
		return nil, p.Fail(err)
	}
	// Preserve all parents closed anywhere in the batch, including final Flush.
	// Per-tick Results retain the original local diagnostics and decision order.
	aggregate := result
	aggregate.Events = nil
	for _, tickResult := range p.batch {
		aggregate.Events = append(aggregate.Events, tickResult.Events...)
	}
	p.pending = &Transition{Checkpoint: Checkpoint{Revision: p.revision + 1, NextOffset: p.readNext, LastTick: tick.Timestamp, Finalized: final, Snapshot: bytes, SHA256: SnapshotHash(bytes)}, Result: aggregate, Results: append([]group.GroupProcessorResult(nil), p.batch...)}
	return p.pending, nil
}

func (p *Partition) Ack(revision int64) error {
	if p.failed != nil {
		return p.failed
	}
	if p.pending == nil || revision != p.pending.Checkpoint.Revision {
		return p.Fail(fmt.Errorf("invalid checkpoint acknowledgement"))
	}
	p.revision, p.finished = revision, p.pending.Checkpoint.Finalized
	p.pending = nil
	p.batch = nil
	return nil
}

func (p *Partition) Finished() bool { return p.finished && p.failed == nil }

// LastComputation exposes operational timing only; callers use it after a completion marker.
func (p *Partition) LastComputation() (group.GroupProcessorResult, time.Duration) {
	return p.lastResult, p.lastCompute
}

func (p *Partition) ComputationFinishedAt() time.Time { return p.lastComputedAt }
