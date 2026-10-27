package domain

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

const InputVersion = "telemetry-input-v1"
const RecordMessage = "telemetry_record"
const CompleteMessage = "timestamp_complete"
const GridStep = 10 * time.Second

// InputBinding identifies an input execution, not a mathematical RunID/ConfigID.
// The caller supplies immutable source/profile bindings; no new scientific ID is derived.
// v1 fixes F18-v1 and the full canonical a01..a16 inventory/mapping.
type InputBinding struct {
	ExecutionID   string `json:"execution_id"`
	InputSHA256   string `json:"input_sha256"`
	ProfileSHA256 string `json:"profile_sha256"`
}

func (b InputBinding) Validate() error {
	if strings.TrimSpace(b.ExecutionID) != b.ExecutionID || b.ExecutionID == "" || len(b.ExecutionID) > 128 {
		return fmt.Errorf("explicit execution_id of 1..128 characters required")
	}
	for _, s := range []string{b.InputSHA256, b.ProfileSHA256} {
		if v, err := hex.DecodeString(s); err != nil || len(v) != 32 || s != strings.ToLower(s) {
			return fmt.Errorf("input/profile binding must be lowercase SHA256")
		}
	}
	return nil
}

type InputWindow struct{ Start, End time.Time }

func OnGrid(t time.Time) bool {
	_, offset := t.Zone()
	return !t.IsZero() && offset == 0 && t.Equal(t.Truncate(GridStep))
}

func (w InputWindow) Validate() error {
	if !OnGrid(w.Start) || !OnGrid(w.End) || !w.End.After(w.Start) {
		return fmt.Errorf("explicit UTC 10-second grid half-open window required")
	}
	if !w.Start.Add(w.End.Sub(w.Start)).Equal(w.End) {
		return fmt.Errorf("input window exceeds duration range")
	}
	return nil
}

func (w InputWindow) Contains(t time.Time) bool {
	return OnGrid(t) && !t.Before(w.Start) && t.Before(w.End)
}

// InputMessage reuses the existing payloads. Exactly one payload must be present.
type InputMessage struct {
	Version    string
	Type       string
	Binding    InputBinding
	GroupID    string
	Timestamp  time.Time
	Record     *TelemetryRecord
	Completion *TimestampComplete
}

func (m InputMessage) Validate(w InputWindow) error {
	if err := w.Validate(); err != nil {
		return err
	}
	if m.Version != InputVersion {
		return fmt.Errorf("unknown input version %q", m.Version)
	}
	if err := m.Binding.Validate(); err != nil {
		return err
	}
	if _, err := topology.PartitionForGroup(m.GroupID); err != nil {
		return err
	}
	if !w.Contains(m.Timestamp) {
		return fmt.Errorf("timestamp outside UTC grid/window")
	}
	switch m.Type {
	case RecordMessage:
		if m.Record == nil || m.Completion != nil {
			return fmt.Errorf("record payload required exclusively")
		}
		r := *m.Record
		if r.GroupID != m.GroupID || r.Timestamp != m.Timestamp {
			return fmt.Errorf("envelope/record identity mismatch")
		}
		if err := topology.ValidateSelectedNode(r.Hostname, r.GroupID, r.NodeIndex); err != nil {
			return err
		}
		return r.Validate()
	case CompleteMessage:
		if m.Completion == nil || m.Record != nil {
			return fmt.Errorf("completion payload required exclusively")
		}
		c := *m.Completion
		if c.GroupID != m.GroupID || c.Timestamp != m.Timestamp || c.Records > 18 {
			return fmt.Errorf("envelope/completion identity or inventory count mismatch")
		}
		return c.Validate()
	default:
		return fmt.Errorf("unknown input message_type %q", m.Type)
	}
}

// GroupTick is an assembled input boundary, not a second mathematical tick.
// A future adapter may pass Timestamp/Records directly to detection/group.Tick.
// Missing contains absent records only; partial/invalid records remain in Records.
type GroupTick struct {
	Binding      InputBinding
	GroupID      string
	Timestamp    time.Time
	Records      map[string]TelemetryRecord
	Missing      []string
	ValidRecords int
}
