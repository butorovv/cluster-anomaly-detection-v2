package group

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

const RBI2SnapshotSchemaVersion = 1

// RBI2OccurrenceSnapshot holds the only occurrence fields used by RBI2Engine.
// Legacy occurrenceState.hosts/zero are never set or read by this engine.
type RBI2OccurrenceSnapshot struct {
	Positive time.Time `json:"positive"`
	Unknown  time.Time `json:"unknown"`
}

// RBI2EngineSnapshot contains temporal memory only, not Journal/legacy events,
// model assets or a complete Worker checkpoint. States are ordered R, B, I2.
// Zero Last means no accepted tick; zero End means Flush has not occurred.
// WindowEnd binds the caller's declared run boundary even before Flush sets End.
// No mathematical RunID or transport ExecutionID is invented here.
type RBI2EngineSnapshot struct {
	SchemaVersion int                       `json:"schema_version"`
	MathVersion   string                    `json:"math_version"`
	Group         string                    `json:"group"`
	Hosts         []string                  `json:"hosts"`
	DeltaSeconds  int                       `json:"delta_seconds"`
	Split         time.Time                 `json:"split"`
	Last          time.Time                 `json:"last"`
	End           time.Time                 `json:"end"`
	WindowEnd     time.Time                 `json:"window_end"`
	States        [3]RBI2OccurrenceSnapshot `json:"states"`
}

// RBI2EngineRestoreOptions is trusted configuration supplied separately from the
// snapshot. Window is half-open; an empty window is allowed, matching the frozen
// Engine's Flush(split) before any tick. Bounds are checked on snapshot/restore;
// callers still control input bounds and when to Flush the unchanged engine.
type RBI2EngineRestoreOptions struct {
	Group        string
	Hosts        []string
	DeltaSeconds int
	Window       analysis.Window
}

// Snapshot returns independent temporal state. windowEnd must be supplied because
// an unflushed RBI2Engine knows its split but has no declared run-end field.
// The engine must not be mutated concurrently with this call.
func (e *RBI2Engine) Snapshot(windowEnd time.Time) (RBI2EngineSnapshot, error) {
	if e == nil {
		return RBI2EngineSnapshot{}, fmt.Errorf("cannot snapshot nil R/B/I2 engine")
	}
	s := RBI2EngineSnapshot{SchemaVersion: RBI2SnapshotSchemaVersion, MathVersion: RBI2Version,
		Group: e.group, Hosts: slices.Clone(e.hosts), DeltaSeconds: e.delta,
		Split: e.split, Last: e.last, End: e.end, WindowEnd: windowEnd}
	for i, state := range e.states {
		if state.hosts != nil || state.zero {
			return RBI2EngineSnapshot{}, fmt.Errorf("unexpected legacy occurrence fields in R/B/I2 engine")
		}
		s.States[i] = RBI2OccurrenceSnapshot{Positive: state.positive, Unknown: state.unknown}
	}
	if err := s.validate(); err != nil {
		return RBI2EngineSnapshot{}, err
	}
	return s, nil
}

func (s RBI2EngineSnapshot) validate() error {
	if s.SchemaVersion != RBI2SnapshotSchemaVersion || s.MathVersion != RBI2Version {
		return fmt.Errorf("unsupported R/B/I2 snapshot schema or mathematical version")
	}
	// Reuse the frozen constructor's canonical inventory, delta and split checks.
	if _, err := NewRBI2Engine(s.Group, s.Hosts, s.DeltaSeconds, s.Split); err != nil {
		return fmt.Errorf("invalid R/B/I2 snapshot configuration: %w", err)
	}
	if s.WindowEnd.IsZero() || !s.WindowEnd.Equal(s.WindowEnd.Truncate(analysis.Step)) || s.WindowEnd.Before(s.Split) {
		return fmt.Errorf("invalid R/B/I2 snapshot window boundary")
	}
	if !s.Last.IsZero() && (!s.Last.Equal(s.Last.Truncate(analysis.Step)) || s.Last.Before(s.Split) || !s.Last.Before(s.WindowEnd)) {
		return fmt.Errorf("R/B/I2 snapshot last tick outside declared grid window")
	}
	if !s.End.IsZero() && !s.End.Equal(s.WindowEnd) {
		return fmt.Errorf("R/B/I2 snapshot Flush boundary differs from declared window")
	}
	for i, state := range s.States {
		// Step only writes tick times (or t-Step on a gap); all lie in
		// [split,last]. Unknown clears positive; a later positive can retain an
		// older unknown. TTL expiry does not clear stored timestamps.
		for _, at := range []time.Time{state.Positive, state.Unknown} {
			if at.IsZero() {
				continue
			}
			if s.Last.IsZero() || !s.End.IsZero() || !at.Equal(at.Truncate(analysis.Step)) || at.Before(s.Split) || at.After(s.Last) {
				return fmt.Errorf("invalid R/B/I2 occurrence position at index %d", i)
			}
		}
		if !state.Positive.IsZero() && !state.Unknown.IsZero() && !state.Positive.After(state.Unknown) {
			return fmt.Errorf("R/B/I2 positive does not follow unknown barrier at index %d", i)
		}
	}
	return nil
}

// RestoreRBI2Engine validates the complete DTO and trusted expected configuration
// before returning a new engine. Failure never falls back to an empty state.
// Hosts are copied; DTO/expected configuration and the new engine do not alias.
func RestoreRBI2Engine(s RBI2EngineSnapshot, expected RBI2EngineRestoreOptions) (*RBI2Engine, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if s.Group != expected.Group || !slices.Equal(s.Hosts, expected.Hosts) || s.DeltaSeconds != expected.DeltaSeconds ||
		!s.Split.Equal(expected.Window.Start) || !s.WindowEnd.Equal(expected.Window.End) {
		return nil, fmt.Errorf("R/B/I2 snapshot differs from expected group/hosts/delta/window")
	}
	e, err := NewRBI2Engine(s.Group, s.Hosts, s.DeltaSeconds, s.Split)
	if err != nil {
		return nil, err
	}
	e.last, e.end = s.Last, s.End
	for i, state := range s.States {
		e.states[i] = occurrenceState{positive: state.Positive, unknown: state.Unknown}
	}
	return e, nil
}

// UnmarshalJSON requires explicit zero timestamps and delta=0 to remain distinct
// from missing fields. Fixed-length memory must not silently accept short arrays.
// Decode is atomic: a malformed document leaves the receiver unchanged.
func (s *RBI2EngineSnapshot) UnmarshalJSON(data []byte) error {
	fields, err := rbi2SnapshotObject(data, "schema_version", "math_version", "group", "hosts", "delta_seconds", "split", "last", "end", "window_end", "states")
	if err != nil {
		return err
	}
	var states []json.RawMessage
	if err := json.Unmarshal(fields["states"], &states); err != nil {
		return err
	}
	if len(states) != 3 {
		return fmt.Errorf("R/B/I2 snapshot requires exactly three occurrence states")
	}
	type plain RBI2EngineSnapshot
	var next plain
	if err := json.Unmarshal(data, &next); err != nil {
		return err
	}
	*s = RBI2EngineSnapshot(next)
	return nil
}

func (s *RBI2OccurrenceSnapshot) UnmarshalJSON(data []byte) error {
	if _, err := rbi2SnapshotObject(data, "positive", "unknown"); err != nil {
		return err
	}
	type plain RBI2OccurrenceSnapshot
	var next plain
	if err := json.Unmarshal(data, &next); err != nil {
		return err
	}
	*s = RBI2OccurrenceSnapshot(next)
	return nil
}

// Keep the small DTO decoder explicit: reject unknown/duplicate/missing/null
// fields rather than accepting an ambiguous or silently zero-filled checkpoint.
func rbi2SnapshotObject(data []byte, required ...string) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	opening, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if opening != json.Delim('{') {
		return nil, fmt.Errorf("R/B/I2 snapshot object required")
	}
	fields := make(map[string]json.RawMessage, len(required))
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok || !slices.Contains(required, key) {
			return nil, fmt.Errorf("unknown R/B/I2 snapshot field %v", token)
		}
		if _, exists := fields[key]; exists {
			return nil, fmt.Errorf("duplicate R/B/I2 snapshot field %s", key)
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("null R/B/I2 snapshot field %s", key)
		}
		fields[key] = value
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	if len(fields) != len(required) {
		return nil, fmt.Errorf("missing required R/B/I2 snapshot fields")
	}
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("trailing R/B/I2 snapshot data")
	}
	return fields, nil
}
