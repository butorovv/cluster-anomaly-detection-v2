package group

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

type LegacyOccurrenceSnapshot struct {
	Positive time.Time `json:"positive"`
	Unknown  time.Time `json:"unknown"`
	Hosts    []string  `json:"hosts"`
	Zero     bool      `json:"zero"`
}

// EngineSnapshot includes all legacy R/Y/B memory and the complete pending Event.
// WindowEnd is caller-supplied metadata: Engine has no stored Flush boundary.
type EngineSnapshot struct {
	SchemaVersion int                         `json:"schema_version"`
	MathVersion   string                      `json:"math_version"`
	Config        string                      `json:"config"`
	Group         string                      `json:"group"`
	DeltaSeconds  int                         `json:"delta_seconds"`
	Split         time.Time                   `json:"split"`
	Last          time.Time                   `json:"last"`
	WindowEnd     time.Time                   `json:"window_end"`
	Finished      bool                        `json:"finished"`
	States        [3]LegacyOccurrenceSnapshot `json:"states"`
	Open          *Event                      `json:"open"`
	SHA256        string                      `json:"sha256,omitempty"`
}

type EngineRestoreOptions struct {
	Config, Group string
	DeltaSeconds  int
	Window        analysis.Window
}

func (e *Engine) Snapshot(windowEnd time.Time) (EngineSnapshot, error) {
	if e == nil {
		return EngineSnapshot{}, fmt.Errorf("nil legacy engine")
	}
	open, err := copyMathSnapshot(e.open)
	if err != nil {
		return EngineSnapshot{}, err
	}
	s := EngineSnapshot{SchemaVersion: MathSnapshotSchemaVersion, MathVersion: Version, Config: e.config,
		Group: e.group, DeltaSeconds: e.delta, Split: e.split, Last: e.last, WindowEnd: windowEnd, Finished: e.finished, Open: open}
	for i, state := range e.states {
		s.States[i] = LegacyOccurrenceSnapshot{state.positive, state.unknown, slices.Clone(state.hosts), state.zero}
	}
	s.SHA256, err = mathSnapshotHash(s)
	if err == nil {
		_, err = RestoreEngine(s, EngineRestoreOptions{e.config, e.group, e.delta, analysis.Window{Start: e.split, End: windowEnd}})
	}
	if err != nil {
		return EngineSnapshot{}, err
	}
	return s, nil
}

func RestoreEngine(snapshot EngineSnapshot, expected EngineRestoreOptions) (*Engine, error) {
	s, err := copyMathSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	want := s.SHA256
	s.SHA256 = ""
	if err := checkMathSnapshotHash(s, want); err != nil {
		return nil, err
	}
	if s.SchemaVersion != MathSnapshotSchemaVersion || s.MathVersion != Version || s.Config != expected.Config || s.Group != expected.Group ||
		s.DeltaSeconds != expected.DeltaSeconds || !s.Split.Equal(expected.Window.Start) || !s.WindowEnd.Equal(expected.Window.End) {
		return nil, fmt.Errorf("incompatible legacy snapshot configuration")
	}
	if err := snapshotPosition(s.Split, s.Last, s.WindowEnd); err != nil {
		return nil, err
	}
	e, err := NewEngine(s.Config, s.Group, s.DeltaSeconds, s.Split)
	if err != nil {
		return nil, err
	}
	e.last, e.finished, e.open = s.Last, s.Finished, s.Open
	for i, state := range s.States {
		if err := validateSnapshotOccurrence(state.Positive, state.Unknown, s.Split, s.Last, s.Finished); err != nil {
			return nil, err
		}
		if (state.Positive.IsZero() && (len(state.Hosts) != 0 || state.Zero)) || (i == 2 && (len(state.Hosts) != 0 || state.Zero)) || (i == 1 && state.Zero) {
			return nil, fmt.Errorf("unreachable legacy occurrence metadata")
		}
		if err := snapshotHostList(state.Hosts, s.Group, false); err != nil {
			return nil, err
		}
		e.states[i] = occurrenceState{state.Positive, state.Unknown, state.Hosts, state.Zero}
	}
	if err := validateLegacyOpen(e); err != nil {
		return nil, err
	}
	return e, nil
}

// Validation uses the exact historical ID namespace from Engine.Step.
func snapshotLegacyEventID(config, group string, delta int, at time.Time) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%d/%s", config, group, delta, at.UTC().Format(time.RFC3339Nano))))
	return hex.EncodeToString(h[:])
}

func validateLegacyOpen(e *Engine) error {
	if e.finished || e.last.IsZero() {
		if e.open != nil {
			return fmt.Errorf("open legacy event without active engine")
		}
		return nil
	}
	r, y, b := e.active(0, e.last), e.active(1, e.last), e.active(2, e.last)
	preliminary := y.State == Unknown
	positive := vote(r.State, y.State, b.State) == Positive
	if preliminary {
		positive = r.State == Positive && b.State == Positive
	}
	if positive != (e.open != nil) {
		return fmt.Errorf("legacy event does not match current temporal vote")
	}
	if e.open == nil {
		return nil
	}
	v := e.open
	if !snapshotGrid(v.Detected) || !snapshotGrid(v.FirstEvidence) || v.Detected.Before(e.split) || v.FirstEvidence.Before(e.split) || v.FirstEvidence.After(v.Detected) ||
		v.Detected.Sub(v.FirstEvidence) > time.Duration(e.delta)*time.Second || !v.LastPositive.Equal(e.last) || v.Detected.After(v.LastPositive) ||
		v.ID != snapshotLegacyEventID(e.config, e.group, e.delta, v.Detected) || v.RunID != e.config || v.Group != e.group || v.DeltaSeconds != e.delta ||
		!v.Closed.IsZero() || v.DurationSeconds != 0 || v.DelaySeconds != 0 || v.RawSpanSeconds != 0 || v.CensoredMissing || v.CensoredWindow ||
		v.LeftCensoredWindow != v.Detected.Equal(e.split) || v.PositiveTicks != int(v.LastPositive.Sub(v.Detected)/analysis.Step)+1 ||
		v.RawCoincidentTicks < 0 || v.TTLExtensionTicks < 0 || v.RawCoincidentTicks+v.TTLExtensionTicks != v.PositiveTicks ||
		v.IFSupportedTicks < 0 || v.IFSupportedTicks > v.PositiveTicks || v.ZeroMADPairTicks < 0 || v.ZeroMADPairTicks > v.PositiveTicks {
		return fmt.Errorf("invalid open legacy event")
	}
	if (preliminary && (v.Mode != "preliminary" || !v.CriteriaIncomplete || v.HardwareConfirmation != "insufficient_criteria")) ||
		(!preliminary && (v.Mode != "full" || v.CriteriaIncomplete || v.HardwareConfirmation != "criteria_available")) {
		return fmt.Errorf("invalid legacy event mode")
	}
	if v.RawCoincidentTicks == 0 {
		if v.RawFirst != nil || v.RawLast != nil {
			return fmt.Errorf("raw span without coincident ticks")
		}
	} else if v.RawFirst == nil || v.RawLast == nil || !snapshotGrid(*v.RawFirst) || !snapshotGrid(*v.RawLast) ||
		v.RawFirst.Before(v.Detected) || v.RawLast.After(v.LastPositive) || v.RawLast.Before(*v.RawFirst) ||
		v.RawCoincidentTicks > int(v.RawLast.Sub(*v.RawFirst)/analysis.Step)+1 {
		return fmt.Errorf("invalid legacy raw coincidence span")
	}
	return snapshotHostList(v.WitnessHosts, e.group, true)
}

func (s *LegacyOccurrenceSnapshot) UnmarshalJSON(data []byte) error {
	if _, err := mathSnapshotFields(data, "positive unknown hosts zero", "hosts"); err != nil {
		return err
	}
	type plain LegacyOccurrenceSnapshot
	var next plain
	if err := decodeMathSnapshot(data, &next); err != nil {
		return err
	}
	*s = LegacyOccurrenceSnapshot(next)
	return nil
}

func (s *EngineSnapshot) UnmarshalJSON(data []byte) error {
	f, err := mathSnapshotFields(data, "schema_version math_version config group delta_seconds split last window_end finished states open sha256", "open")
	if err != nil {
		return err
	}
	var states []json.RawMessage
	if err := json.Unmarshal(f["states"], &states); err != nil {
		return err
	}
	if len(states) != 3 {
		return fmt.Errorf("legacy snapshot requires three occurrences")
	}
	if _, err := nullableSnapshotObject(f["open"], legacyEventSnapshotFields, "first_raw_coincident_time last_raw_coincident_time witness_hosts"); err != nil {
		return err
	}
	type plain EngineSnapshot
	var next plain
	if err := decodeMathSnapshot(data, &next); err != nil {
		return err
	}
	*s = EngineSnapshot(next)
	return nil
}
