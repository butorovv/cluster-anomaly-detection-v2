package group

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

var (
	// ErrProcessorInput marks a validation rejection before any mathematical transition.
	// The same processor can accept a corrected tick afterwards.
	ErrProcessorInput = errors.New("invalid group processor input")
	// ErrProcessorFailed marks an unusable processor after a computation/Journal error.
	// Its mathematical state is uncertain; recreate it (or use a future Restore).
	ErrProcessorFailed   = errors.New("group processor failed")
	ErrProcessorFinished = errors.New("group processor finished")
)

type GroupProcessorOptions struct {
	GroupID      string
	Groups       int
	Window       analysis.Window
	DeltaSeconds int
	Binding      domain.InputBinding
}

// GroupProcessorResult is a local, caller-owned result, not a final Master
// group_result. Outside IF diagnostics in Raw and Evidence.Raw are uncomputed
// (serialized as null). Full offline Raw/Evidence golden parity requires a future
// cross-group AnnotateOutside join; Temporal and Events already use frozen math.
type GroupProcessorResult struct {
	Raw      Raw
	Temporal RBI2TemporalResult
	Evidence *RBI2Evidence
	Events   []RBI2Event
}

// GroupProcessor owns one group's sequential in-memory Journal. It is not
// goroutine-safe or copyable after construction. The loaded Bundle may be shared
// read-only. Caller-owned input must not be mutated concurrently with Step.
// Snapshot/Restore, input assembly and durable progress belong to later layers.
type GroupProcessor struct {
	bundle   *Bundle
	journal  *RBI2Journal
	group    string
	hosts    []string
	binding  domain.InputBinding
	window   domain.InputWindow
	next     time.Time
	finished bool
	failed   error
}

func NewGroupProcessor(bundle *Bundle, options GroupProcessorOptions) (*GroupProcessor, error) {
	if bundle == nil || bundle.profile == nil || bundle.local == nil || bundle.hardware == nil {
		return nil, fmt.Errorf("%w: loaded frozen bundle required", ErrProcessorInput)
	}
	if err := bundle.profile.Validate(); err != nil {
		return nil, fmt.Errorf("%w: frozen profile: %w", ErrProcessorInput, err)
	}
	w := domain.InputWindow{Start: options.Window.Start, End: options.Window.End}
	if err := w.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrProcessorInput, err)
	}
	w.Start, w.End = w.Start.UTC(), w.End.UTC()
	if err := options.Binding.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrProcessorInput, err)
	}
	if bundle.preparedKey == "" || options.Binding.ProfileSHA256 != bundle.profileSHA ||
		options.Binding.InputSHA256 != bundle.profile.Binding.PreparedSHA256 ||
		options.Binding.InputSHA256 != bundle.profile.InputSHA[bundle.preparedKey] {
		return nil, fmt.Errorf("%w: input/profile binding differs from frozen bundle", ErrProcessorInput)
	}
	// Both scientific identities come from actual loaded model hashes, never the
	// execution ID or physical file locations. NewRBI2Identity validates groups/delta.
	identity, err := NewRBI2Identity(bundle.profileSHA, bundle.profile.InputSHA, options.Groups,
		analysis.Window{Start: w.Start, End: w.End}, options.DeltaSeconds)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrProcessorInput, err)
	}
	partition, err := topology.PartitionForGroup(options.GroupID)
	if err != nil || partition >= options.Groups {
		return nil, fmt.Errorf("%w: group outside selected frozen universe", ErrProcessorInput)
	}
	hosts := bundle.profile.Groups[partition].Hosts // Validate checked canonical order.
	journal, err := NewRBI2Journal(identity, options.GroupID, hosts)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrProcessorInput, err)
	}
	return &GroupProcessor{bundle: bundle, journal: journal, group: options.GroupID,
		hosts: slices.Clone(hosts), binding: options.Binding, window: w, next: w.Start}, nil
}

func (p *GroupProcessor) usable() error {
	if p == nil || p.journal == nil {
		return fmt.Errorf("%w: uninitialized processor", ErrProcessorFailed)
	}
	if p.failed != nil {
		return p.failed
	}
	if p.finished {
		return ErrProcessorFinished
	}
	return nil
}

func (p *GroupProcessor) validate(t domain.GroupTick) error {
	if t.GroupID != p.group || t.Binding != p.binding || !p.window.Contains(t.Timestamp) || !t.Timestamp.Equal(p.next) {
		return fmt.Errorf("%w: group/binding/window mismatch or nonconsecutive tick (expected %s)", ErrProcessorInput, p.next)
	}
	valid := 0
	for host, record := range t.Records {
		if !slices.Contains(p.hosts, host) || record.Hostname != host || record.GroupID != p.group ||
			!domain.OnGrid(record.Timestamp) || !record.Timestamp.Equal(t.Timestamp) {
			return fmt.Errorf("%w: record identity differs from group tick: %s", ErrProcessorInput, host)
		}
		if err := topology.ValidateSelectedNode(host, record.GroupID, record.NodeIndex); err != nil {
			return fmt.Errorf("%w: %w", ErrProcessorInput, err)
		}
		if err := record.Validate(); err != nil {
			return fmt.Errorf("%w: %w", ErrProcessorInput, err)
		}
		if record.Valid {
			valid++
		}
	}
	missing := make([]string, 0, HostsPerGroup)
	for _, host := range p.hosts {
		if _, ok := t.Records[host]; !ok {
			missing = append(missing, host)
		}
	}
	if valid != t.ValidRecords || !slices.Equal(missing, t.Missing) {
		return fmt.Errorf("%w: Records/Missing/ValidRecords disagree", ErrProcessorInput)
	}
	return nil
}

// Step accepts exactly the next complete tick, starting at the window start.
// Missing cells must arrive as explicit empty/partial ticks, never skipped time.
// Input rejection does not advance state; any evaluator/Journal error latches a
// failure, without attempting to roll back private mathematical state.
func (p *GroupProcessor) Step(t domain.GroupTick) (GroupProcessorResult, error) {
	if err := p.usable(); err != nil {
		return GroupProcessorResult{}, err
	}
	if err := p.validate(t); err != nil {
		return GroupProcessorResult{}, err
	}
	raw, err := p.bundle.EvaluateGroup(p.group, Tick{Timestamp: t.Timestamp, Records: t.Records}, nil)
	if err != nil {
		p.failed = fmt.Errorf("%w: evaluate: %w", ErrProcessorFailed, err)
		return GroupProcessorResult{}, p.failed
	}
	tr, evidence, events, err := p.journal.Step(raw)
	if err != nil {
		p.failed = fmt.Errorf("%w: journal: %w", ErrProcessorFailed, err)
		return GroupProcessorResult{}, p.failed
	}
	p.next = p.next.Add(domain.GridStep)
	return GroupProcessorResult{Raw: raw, Temporal: tr, Evidence: evidence, Events: events}, nil
}

// Finish seals the declared window through the existing Journal. An early call
// retains frozen missing_tail semantics; callers requiring a complete input grid
// must separately finish their TickAssembler. Repeated successful Finish returns
// no events, so a closed event is never emitted twice. Failed processors stay failed.
func (p *GroupProcessor) Finish() ([]RBI2Event, error) {
	if err := p.usable(); err != nil {
		if errors.Is(err, ErrProcessorFinished) {
			return nil, nil
		}
		return nil, err
	}
	events, err := p.journal.Flush(p.window.End)
	if err != nil {
		p.failed = fmt.Errorf("%w: flush: %w", ErrProcessorFailed, err)
		return nil, p.failed
	}
	p.finished = true
	return events, nil
}
