package group

import (
	"fmt"
	"slices"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

// GroupProcessorSnapshot is the complete healthy mathematical state at a tick
// boundary (including fresh/finished states). It contains no model trees, Kafka
// offset, partially assembled tick or previously emitted output history.
type GroupProcessorSnapshot struct {
	SchemaVersion int                 `json:"schema_version"`
	Group         string              `json:"group"`
	Binding       domain.InputBinding `json:"binding"`
	Identity      RBI2Identity        `json:"identity"`
	Window        analysis.Window     `json:"window"`
	DeltaSeconds  int                 `json:"delta_seconds"`
	Next          time.Time           `json:"next"`
	Finished      bool                `json:"finished"`
	Journal       RBI2JournalSnapshot `json:"journal"`
	SHA256        string              `json:"sha256,omitempty"`
}

// Snapshot must not race Step/Finish. Both are synchronous; consistency between
// next and the full Journal verifies the completed-tick boundary. Failed state
// is never serialized as healthy and is never rolled back here.
func (p *GroupProcessor) Snapshot() (GroupProcessorSnapshot, error) {
	if p == nil || p.bundle == nil || p.journal == nil || p.failed != nil {
		return GroupProcessorSnapshot{}, fmt.Errorf("cannot snapshot uninitialized/failed processor")
	}
	journal, err := p.journal.Snapshot()
	if err != nil {
		return GroupProcessorSnapshot{}, err
	}
	identity, err := copyMathSnapshot(p.journal.identity)
	if err != nil {
		return GroupProcessorSnapshot{}, err
	}
	s := GroupProcessorSnapshot{SchemaVersion: MathSnapshotSchemaVersion, Group: p.group, Binding: p.binding, Identity: identity,
		Window: analysis.Window{Start: p.window.Start, End: p.window.End}, DeltaSeconds: identity.DeltaSeconds, Next: p.next, Finished: p.finished, Journal: journal}
	s.SHA256, err = mathSnapshotHash(s)
	if err != nil {
		return GroupProcessorSnapshot{}, err
	}
	opts := GroupProcessorOptions{GroupID: p.group, Groups: identity.Groups, Window: s.Window, DeltaSeconds: s.DeltaSeconds, Binding: p.binding}
	restored, err := RestoreGroupProcessor(p.bundle, opts, s)
	if err != nil {
		return GroupProcessorSnapshot{}, err
	}
	if !slices.Equal(p.hosts, restored.hosts) {
		return GroupProcessorSnapshot{}, fmt.Errorf("processor inventory differs from bundle")
	}
	return s, nil
}

func RestoreGroupProcessor(bundle *Bundle, options GroupProcessorOptions, snapshot GroupProcessorSnapshot) (*GroupProcessor, error) {
	s, err := copyMathSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	want := s.SHA256
	s.SHA256 = ""
	if err := checkMathSnapshotHash(s, want); err != nil {
		return nil, err
	}
	p, err := NewGroupProcessor(bundle, options)
	if err != nil {
		return nil, err
	}
	if s.SchemaVersion != MathSnapshotSchemaVersion || s.Group != options.GroupID || s.Binding != options.Binding || s.DeltaSeconds != options.DeltaSeconds ||
		!s.Window.Start.Equal(p.window.Start) || !s.Window.End.Equal(p.window.End) {
		return nil, fmt.Errorf("incompatible processor snapshot configuration")
	}
	a, err := mathSnapshotHash(s.Identity)
	if err != nil {
		return nil, err
	}
	b, err := mathSnapshotHash(p.journal.identity)
	if err != nil {
		return nil, err
	}
	if a != b {
		return nil, fmt.Errorf("processor snapshot differs from loaded frozen bundle identity")
	}
	journal, err := RestoreRBI2Journal(s.Journal, p.journal.identity, p.group, p.hosts)
	if err != nil {
		return nil, err
	}
	next := p.window.Start
	if !journal.last.IsZero() {
		next = journal.last.Add(analysis.Step)
	}
	if !s.Next.Equal(next) || next.After(p.window.End) || s.Finished != !journal.clock.end.IsZero() {
		return nil, fmt.Errorf("processor next/finished does not match journal boundary")
	}
	p.journal, p.next, p.finished = journal, s.Next, s.Finished
	return p, nil
}

func (s *GroupProcessorSnapshot) UnmarshalJSON(data []byte) error {
	f, err := mathSnapshotFields(data, "schema_version group binding identity window delta_seconds next finished journal sha256", "")
	if err != nil {
		return err
	}
	if _, err := mathSnapshotFields(f["binding"], "execution_id input_sha256 profile_sha256", ""); err != nil {
		return err
	}
	if _, err := mathSnapshotFields(f["window"], "start end_exclusive", ""); err != nil {
		return err
	}
	if err := snapshotIdentityJSON(f["identity"]); err != nil {
		return err
	}
	type plain GroupProcessorSnapshot
	var next plain
	if err := decodeMathSnapshot(data, &next); err != nil {
		return err
	}
	*s = GroupProcessorSnapshot(next)
	return nil
}
