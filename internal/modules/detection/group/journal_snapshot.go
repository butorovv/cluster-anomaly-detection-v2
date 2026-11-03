package group

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

// RBI2JournalSnapshot preserves pending aggregates, not previously emitted output
// history. Evidence.Raw is an output; its accumulated maxima/unions and causal
// witness hosts are retained here. Durable output storage belongs to checkpoint/outbox.
type RBI2JournalSnapshot struct {
	SchemaVersion int                `json:"schema_version"`
	Identity      RBI2Identity       `json:"identity"`
	Clock         RBI2EngineSnapshot `json:"clock"`
	Legacy        EngineSnapshot     `json:"legacy"`
	Open          *RBI2Event         `json:"open"`
	Last          time.Time          `json:"last"`
	RHosts        []string           `json:"r_hosts"`
	IHosts        []string           `json:"i_hosts"`
	SHA256        string             `json:"sha256,omitempty"`
}

func (j *RBI2Journal) Snapshot() (RBI2JournalSnapshot, error) {
	if j == nil || j.clock == nil || j.legacy == nil {
		return RBI2JournalSnapshot{}, fmt.Errorf("uninitialized RBI2 journal")
	}
	clock, err := j.clock.Snapshot(j.identity.Window.End)
	if err != nil {
		return RBI2JournalSnapshot{}, err
	}
	legacy, err := j.legacy.Snapshot(j.identity.Window.End)
	if err != nil {
		return RBI2JournalSnapshot{}, err
	}
	identity, err := copyMathSnapshot(j.identity)
	if err != nil {
		return RBI2JournalSnapshot{}, err
	}
	open, err := copyMathSnapshot(j.open)
	if err != nil {
		return RBI2JournalSnapshot{}, err
	}
	s := RBI2JournalSnapshot{SchemaVersion: MathSnapshotSchemaVersion, Identity: identity, Clock: clock, Legacy: legacy,
		Open: open, Last: j.last, RHosts: slices.Clone(j.rHosts), IHosts: slices.Clone(j.iHosts)}
	s.SHA256, err = mathSnapshotHash(s)
	if err == nil {
		_, err = RestoreRBI2Journal(s, j.identity, j.clock.group, j.clock.hosts)
	}
	if err != nil {
		return RBI2JournalSnapshot{}, err
	}
	return s, nil
}

func RestoreRBI2Journal(snapshot RBI2JournalSnapshot, expected RBI2Identity, group string, hosts []string) (*RBI2Journal, error) {
	s, err := copyMathSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	want := s.SHA256
	s.SHA256 = ""
	if err := checkMathSnapshotHash(s, want); err != nil {
		return nil, err
	}
	j, err := NewRBI2Journal(expected, group, hosts)
	if err != nil {
		return nil, err
	}
	a, err := mathSnapshotHash(s.Identity)
	if err != nil {
		return nil, err
	}
	b, err := mathSnapshotHash(j.identity)
	if err != nil {
		return nil, err
	}
	if s.SchemaVersion != MathSnapshotSchemaVersion || a != b {
		return nil, fmt.Errorf("incompatible journal identity/version")
	}
	clock, err := RestoreRBI2Engine(s.Clock, RBI2EngineRestoreOptions{Group: group, Hosts: hosts, DeltaSeconds: expected.DeltaSeconds, Window: expected.Window})
	if err != nil {
		return nil, err
	}
	legacy, err := RestoreEngine(s.Legacy, EngineRestoreOptions{Config: expected.LegacyRunID, Group: group, DeltaSeconds: expected.DeltaSeconds, Window: expected.Window})
	if err != nil {
		return nil, err
	}
	if !s.Last.Equal(clock.last) || !s.Last.Equal(legacy.last) || legacy.finished != !clock.end.IsZero() {
		return nil, fmt.Errorf("journal clocks/finished positions disagree")
	}
	// Legacy is exactly the R+B projection with Y=Unknown. Its R/B timestamps
	// must match the independent RBI2 clock, including gap resets and Flush.
	for _, pair := range [][2]int{{0, 0}, {2, 1}} {
		left, right := legacy.states[pair[0]], clock.states[pair[1]]
		if !left.positive.Equal(right.positive) || !left.unknown.Equal(right.unknown) {
			return nil, fmt.Errorf("journal R/B memories disagree")
		}
	}
	if !legacy.finished && !s.Last.IsZero() && (!legacy.states[1].unknown.Equal(s.Last) || !legacy.states[1].positive.IsZero()) {
		return nil, fmt.Errorf("journal legacy Y is not Unknown")
	}
	if err := snapshotHostList(s.RHosts, group, false); err != nil {
		return nil, err
	}
	if err := snapshotHostList(s.IHosts, group, true); err != nil {
		return nil, err
	}
	if (s.Last.IsZero() && (s.Open != nil || len(s.RHosts) != 0 || len(s.IHosts) != 0)) ||
		(!clock.states[2].positive.IsZero() && len(s.IHosts) < 2) ||
		(!legacy.states[0].positive.IsZero() && !slices.Equal(s.RHosts, legacy.states[0].hosts)) {
		return nil, fmt.Errorf("journal witness caches disagree")
	}
	j.clock, j.legacy, j.open, j.last, j.rHosts, j.iHosts = clock, legacy, s.Open, s.Last, s.RHosts, s.IHosts
	if err := validateJournalOpen(j); err != nil {
		return nil, err
	}
	return j, nil
}

func validateJournalOpen(j *RBI2Journal) error {
	finished := !j.clock.end.IsZero()
	if finished || j.last.IsZero() {
		if j.open != nil || j.legacy.open != nil {
			return fmt.Errorf("open journal event without active clock")
		}
		return nil
	}
	// Engine.active is the existing causal occurrence lookup; using a temporary
	// view of R/B/I2 memory validates the last vote without replaying any tick.
	view := Engine{delta: j.clock.delta, states: j.clock.states}
	states := [3]State{view.active(0, j.last).State, view.active(1, j.last).State, view.active(2, j.last).State}
	vote, err := VoteRBI2(states[0], states[1], states[2])
	if err != nil {
		return err
	}
	if (vote == Positive) != (j.open != nil) {
		return fmt.Errorf("parent event disagrees with last vote")
	}
	if j.legacy.open != nil && (j.open == nil || j.legacy.open.Mode != "preliminary" || j.legacy.open.Detected.Before(j.open.Detected)) {
		return fmt.Errorf("legacy open event is not nested in parent")
	}
	if j.open == nil {
		return nil
	}
	e := j.open
	id := j.identity
	if e.Version != RBI2Version || e.RunID != id.RunID || e.KernelConfigID != id.ConfigID || e.Group != j.clock.group || e.DeltaSeconds != id.DeltaSeconds ||
		!snapshotGrid(e.Detected) || !snapshotGrid(e.FirstEvidence) || e.Detected.Before(id.Window.Start) || e.FirstEvidence.Before(id.Window.Start) ||
		e.FirstEvidence.After(e.Detected) || e.Detected.Sub(e.FirstEvidence) > time.Duration(id.DeltaSeconds)*time.Second || !e.LastPositive.Equal(j.last) || e.Detected.After(e.LastPositive) ||
		e.ID != RBI2EventID(id.RunID, e.Group, e.Detected.UTC().Format(time.RFC3339Nano)) || !e.Closed.IsZero() || e.ClosureReason != "" ||
		e.DurationSeconds != 0 || e.DelaySeconds != 0 || e.CensoredMissing || e.CensoredWindow || e.NewDisjointFromRB ||
		e.LeftCensoredWindow != e.Detected.Equal(id.Window.Start) || e.PositiveTicks != int(e.LastPositive.Sub(e.Detected)/analysis.Step)+1 ||
		e.IFSimultaneousMax < 0 || e.IFSimultaneousMax > HostsPerGroup || len(e.IFHosts) < e.IFSimultaneousMax {
		return fmt.Errorf("invalid open RBI2 parent")
	}
	for _, value := range []*float64{e.IFScoreMax, e.RRatioMax, e.BRatioMax} {
		if value != nil && !analysis.Finite(*value) {
			return fmt.Errorf("nonfinite parent aggregate")
		}
	}
	if err := snapshotHostList(e.IFHosts, e.Group, true); err != nil {
		return err
	}
	if err := snapshotHostList(e.WitnessHosts, e.Group, true); err != nil {
		return err
	}
	next := e.Detected
	lastLabel := ""
	for _, segment := range e.VoteSegments {
		if !segment.Start.Equal(next) || !snapshotGrid(segment.End) || !segment.End.After(segment.Start) || segment.Votes == lastLabel ||
			!slices.Contains([]string{"R+B", "R+I2", "B+I2", "R+B+I2"}, segment.Votes) {
			return fmt.Errorf("invalid parent vote segments")
		}
		next, lastLabel = segment.End, segment.Votes
	}
	labels := []string{}
	for i, name := range []string{"R", "B", "I2"} {
		if states[i] == Positive {
			labels = append(labels, name)
		}
	}
	if !next.Equal(j.last.Add(analysis.Step)) || lastLabel != strings.Join(labels, "+") {
		return fmt.Errorf("parent segments do not cover current vote")
	}
	previous := e.Detected
	for _, span := range e.RBSubspans {
		if !snapshotGrid(span.Start) || !snapshotGrid(span.End) || span.Start.Before(previous) || !span.End.After(span.Start) ||
			span.End.After(j.last.Add(analysis.Step)) || span.HistoricalRunID != id.LegacyRunID ||
			span.HistoricalEventID != snapshotLegacyEventID(id.LegacyRunID, e.Group, id.DeltaSeconds, span.Start) {
			return fmt.Errorf("invalid nested historical RB span")
		}
		previous = span.End
	}
	if j.legacy.open != nil && j.legacy.open.Detected.Before(previous) {
		return fmt.Errorf("open RB span overlaps completed history")
	}
	return nil
}

func (s *RBI2JournalSnapshot) UnmarshalJSON(data []byte) error {
	f, err := mathSnapshotFields(data, "schema_version identity clock legacy open last r_hosts i_hosts sha256", "open r_hosts i_hosts")
	if err != nil {
		return err
	}
	if err := snapshotIdentityJSON(f["identity"]); err != nil {
		return err
	}
	parent, err := nullableSnapshotObject(f["open"], parentEventSnapshotFields, "if_score_max r_ratio_max b_ratio_max if_hosts witness_hosts vote_segments rb_subspans")
	if err != nil {
		return err
	}
	if parent != nil {
		for _, item := range []struct{ key, fields string }{{"vote_segments", "start end votes"}, {"rb_subspans", "start end historical_event_id historical_run_id"}} {
			var rows []json.RawMessage
			if err := json.Unmarshal(parent[item.key], &rows); err != nil {
				return err
			}
			for _, row := range rows {
				if _, err := mathSnapshotFields(row, item.fields, ""); err != nil {
					return err
				}
			}
		}
	}
	type plain RBI2JournalSnapshot
	var next plain
	if err := decodeMathSnapshot(data, &next); err != nil {
		return err
	}
	*s = RBI2JournalSnapshot(next)
	return nil
}
