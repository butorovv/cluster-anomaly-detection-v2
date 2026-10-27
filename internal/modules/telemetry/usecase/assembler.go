package usecase

import (
	"fmt"
	"math"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

// InputError separates an atomic validation reject from a permanently stopped stream.
type InputError struct {
	Fatal bool
	Code  string
	Cause error
}

func (e *InputError) Error() string {
	return fmt.Sprintf("input %s (fatal=%t): %v", e.Code, e.Fatal, e.Cause)
}
func (e *InputError) Unwrap() error { return e.Cause }

// TickAssembler owns only one group's current tick. It is sequential, not goroutine-safe.
type TickAssembler struct {
	group    string
	binding  domain.InputBinding
	window   domain.InputWindow
	next     time.Time
	records  map[string]domain.TelemetryRecord
	lastHost string
	stopped  error
	finished bool
}

func NewTickAssembler(group string, binding domain.InputBinding, window domain.InputWindow) (*TickAssembler, error) {
	if _, err := topology.PartitionForGroup(group); err != nil {
		return nil, err
	}
	if err := binding.Validate(); err != nil {
		return nil, err
	}
	if err := window.Validate(); err != nil {
		return nil, err
	}
	return &TickAssembler{group: group, binding: binding, window: window, next: window.Start, records: make(map[string]domain.TelemetryRecord, 18)}, nil
}

func (a *TickAssembler) reject(fatal bool, code string, cause error) (*domain.GroupTick, error) {
	err := &InputError{Fatal: fatal, Code: code, Cause: cause}
	if fatal {
		a.stopped = err
	}
	return nil, err
}

// Accept validates before changing accepted data. Fatal errors latch until restart;
// recoverable rejects (e.g. an availability or count mismatch) leave data untouched.
func (a *TickAssembler) Accept(m domain.InputMessage) (*domain.GroupTick, error) {
	if a.stopped != nil {
		return nil, a.stopped
	}
	if a.finished || !a.next.Before(a.window.End) {
		return a.reject(true, "after_window", fmt.Errorf("input after completed window"))
	}
	if m.Version != domain.InputVersion || m.Binding != a.binding || m.GroupID != a.group || (m.Type != domain.RecordMessage && m.Type != domain.CompleteMessage) {
		return a.reject(true, "stream_identity", fmt.Errorf("version/type/execution/source/profile/group binding mismatch"))
	}
	if !a.window.Contains(m.Timestamp) || !m.Timestamp.Equal(a.next) {
		return a.reject(true, "position", fmt.Errorf("expected %s, got %s; missing marker, duplicate or late input", a.next, m.Timestamp))
	}
	if r := m.Record; r != nil {
		if r.GroupID != m.GroupID || r.Timestamp != m.Timestamp {
			return a.reject(true, "payload_identity", fmt.Errorf("record/envelope mismatch"))
		}
		if err := topology.ValidateSelectedNode(r.Hostname, r.GroupID, r.NodeIndex); err != nil {
			return a.reject(true, "payload_identity", err)
		}
		if prior, ok := a.records[r.Hostname]; ok {
			reason := "exact_duplicate_host"
			if !sameRecord(prior, *r) {
				reason = "conflicting_duplicate_host"
			}
			return a.reject(true, reason, fmt.Errorf("host %s at %s", r.Hostname, r.Timestamp))
		}
		if r.Hostname <= a.lastHost {
			return a.reject(true, "host_order", fmt.Errorf("non-increasing hostname %s", r.Hostname))
		}
	}
	if c := m.Completion; c != nil && (c.GroupID != m.GroupID || c.Timestamp != m.Timestamp) {
		return a.reject(true, "payload_identity", fmt.Errorf("marker/envelope mismatch"))
	}
	if err := m.Validate(a.window); err != nil {
		return a.reject(false, "validation", err)
	}
	if m.Record != nil {
		r := *m.Record // value arrays detach accepted data from caller-owned payload
		a.records[r.Hostname] = r
		a.lastHost = r.Hostname
		return nil, nil
	}
	c := *m.Completion
	valid := 0
	for _, r := range a.records {
		if r.Valid {
			valid++
		}
	}
	if c.Records != len(a.records) || c.ValidRecords != valid {
		return a.reject(false, "completion_counts", fmt.Errorf("got %d/%d, accepted %d/%d", c.Records, c.ValidRecords, len(a.records), valid))
	}
	result := &domain.GroupTick{Binding: a.binding, GroupID: a.group, Timestamp: a.next, Records: a.records, Missing: []string{}, ValidRecords: valid}
	for i := 1; i <= 18; i++ {
		h := fmt.Sprintf("%sn%02d", a.group, i)
		if _, ok := a.records[h]; !ok {
			result.Missing = append(result.Missing, h)
		}
	}
	// Transfer ownership of this map; the next tick gets a fresh bounded buffer.
	a.records = make(map[string]domain.TelemetryRecord, 18)
	a.lastHost = ""
	a.next = a.next.Add(domain.GridStep)
	return result, nil
}

// Finish is mandatory at EOF. No inferred markers or silent missing tail.
func (a *TickAssembler) Finish() error {
	if a.stopped != nil {
		return a.stopped
	}
	if a.next.Before(a.window.End) {
		_, err := a.reject(true, "incomplete_tail", fmt.Errorf("EOF before completion at %s (%d accepted records)", a.next, len(a.records)))
		return err
	}
	a.finished = true
	return nil
}

func sameRecord(a, b domain.TelemetryRecord) bool {
	if a.Timestamp != b.Timestamp || a.Hostname != b.Hostname || a.GroupID != b.GroupID || a.NodeIndex != b.NodeIndex || a.Valid != b.Valid || a.AvailabilityMask != b.AvailabilityMask {
		return false
	}
	af, bf := a.Features(), b.Features()
	for i := range af {
		if math.Float64bits(af[i]) != math.Float64bits(bf[i]) {
			return false
		}
	}
	return true
}
