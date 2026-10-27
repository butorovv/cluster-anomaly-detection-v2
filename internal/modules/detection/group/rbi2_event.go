package group

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

type RBI2VoteSegment struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	Votes string    `json:"votes"`
}

type RBI2RBSubspan struct {
	Start             time.Time `json:"start"`
	End               time.Time `json:"end"`
	HistoricalEventID string    `json:"historical_event_id"`
	HistoricalRunID   string    `json:"historical_run_id"`
}

type RBI2Event struct {
	Version            string            `json:"version"`
	ID                 string            `json:"id"`
	RunID              string            `json:"run_id"`
	KernelConfigID     string            `json:"kernel_config_id"`
	Group              string            `json:"group"`
	DeltaSeconds       int               `json:"delta_seconds"`
	Detected           time.Time         `json:"detected"`
	LastPositive       time.Time         `json:"last_positive"`
	Closed             time.Time         `json:"closed"`
	FirstEvidence      time.Time         `json:"first_evidence"`
	DurationSeconds    int               `json:"duration_seconds"`
	DelaySeconds       int               `json:"confirmation_delay_seconds"`
	PositiveTicks      int               `json:"positive_ticks"`
	ClosureReason      string            `json:"closure_reason"`
	CensoredMissing    bool              `json:"right_censored_by_missing"`
	CensoredWindow     bool              `json:"right_censored_by_window"`
	LeftCensoredWindow bool              `json:"left_censored_by_window"`
	VoteSegments       []RBI2VoteSegment `json:"vote_segments"`
	RBSubspans         []RBI2RBSubspan   `json:"rb_subspans"`
	NewDisjointFromRB  bool              `json:"new_disjoint_from_rb"`
	IFHosts            []string          `json:"if_hosts"`
	WitnessHosts       []string          `json:"witness_hosts"`
	IFSimultaneousMax  int               `json:"if_simultaneous_max"`
	IFScoreMax         *float64          `json:"if_score_max"`
	RRatioMax          *float64          `json:"r_ratio_max"`
	BRatioMax          *float64          `json:"b_ratio_max"`
}

type RBI2WitnessRef struct {
	Criterion string    `json:"criterion"`
	Timestamp time.Time `json:"timestamp"`
}

// RBI2Evidence owns a full diagnostic Raw snapshot. WitnessRefs locate earlier
// causal observations in the same run's raw stream (including pre-event ticks).
// Raw contains local scores/thresholds, signed R witnesses and physical channels.
type RBI2Evidence struct {
	Version     string             `json:"version"`
	RunID       string             `json:"run_id"`
	EventID     string             `json:"event_id"`
	Raw         Raw                `json:"raw"`
	Temporal    RBI2TemporalResult `json:"temporal"`
	WitnessRefs []RBI2WitnessRef   `json:"witness_refs"`
}

// RBI2Journal materializes only parent events. Its private legacy Engine is an
// explicit R+B projection (Y unknown), used exclusively to retain legacy IDs;
// no second event stream is exposed. Y remains present in diagnostic Raw.
type RBI2Journal struct {
	identity RBI2Identity
	clock    *RBI2Engine
	legacy   *Engine
	open     *RBI2Event
	last     time.Time
	rHosts   []string
	iHosts   []string
}

func NewRBI2Journal(identity RBI2Identity, group string, hosts []string) (*RBI2Journal, error) {
	want, err := NewRBI2Identity(identity.ProfileSHA256, identity.InputSHA256, identity.Groups, identity.Window, identity.DeltaSeconds)
	if err != nil || !reflect.DeepEqual(identity, want) {
		return nil, fmt.Errorf("invalid R/B/I2 identity")
	}
	selected := false
	for i := 1; i <= identity.Groups; i++ {
		selected = selected || group == fmt.Sprintf("a%02d", i)
	}
	if !selected {
		return nil, fmt.Errorf("group outside the frozen run universe")
	}
	clock, err := NewRBI2Engine(group, hosts, identity.DeltaSeconds, identity.Window.Start)
	if err != nil {
		return nil, err
	}
	legacy, err := NewEngine(identity.LegacyRunID, group, identity.DeltaSeconds, identity.Window.Start)
	if err != nil {
		return nil, err
	}
	return &RBI2Journal{identity: want, clock: clock, legacy: legacy}, nil
}

func (j *RBI2Journal) addRB(events []Event) error {
	for _, e := range events {
		if j.open == nil || e.Detected.Before(j.open.Detected) || e.Closed.After(j.open.LastPositive.Add(analysis.Step)) {
			return fmt.Errorf("legacy R+B interval is not nested in a parent")
		}
		j.open.RBSubspans = append(j.open.RBSubspans, RBI2RBSubspan{e.Detected, e.Closed, e.ID, e.RunID})
	}
	return nil
}

func (j *RBI2Journal) close(at time.Time, reason string) []RBI2Event {
	if j.open == nil {
		return nil
	}
	e := j.open
	e.Closed, e.ClosureReason = at, reason
	e.DurationSeconds = int(e.LastPositive.Sub(e.Detected).Seconds()) + 10
	e.DelaySeconds = int(e.Detected.Sub(e.FirstEvidence).Seconds())
	e.CensoredMissing = reason == "unknown" || reason == "gap" || reason == "missing_tail"
	e.CensoredWindow = reason == "window"
	e.NewDisjointFromRB = len(e.RBSubspans) == 0
	j.open = nil
	return []RBI2Event{*e}
}

func (j *RBI2Journal) Step(raw Raw) (RBI2TemporalResult, *RBI2Evidence, []RBI2Event, error) {
	if j == nil || !j.identity.Window.Contains(raw.Timestamp) {
		return RBI2TemporalResult{}, nil, nil, fmt.Errorf("R/B/I2 tick outside run window")
	}
	next := *j.clock
	tr, err := next.Step(raw)
	if err != nil {
		return RBI2TemporalResult{}, nil, nil, err
	}
	var snapshot Raw
	if tr.Vote == Positive {
		b, err := json.Marshal(raw)
		if err != nil {
			return RBI2TemporalResult{}, nil, nil, err
		}
		if err = json.Unmarshal(b, &snapshot); err != nil {
			return RBI2TemporalResult{}, nil, nil, err
		}
	}
	projected := raw
	projected.Y = YResult{Criterion: Criterion{State: Unknown}}
	_, rbClosed, err := j.legacy.Step(projected)
	if err != nil {
		return RBI2TemporalResult{}, nil, nil, err
	}
	j.clock = &next
	if err = j.addRB(rbClosed); err != nil {
		return RBI2TemporalResult{}, nil, nil, err
	}
	var events []RBI2Event
	if tr.GapStart != nil {
		events = append(events, j.close(*tr.GapStart, "gap")...)
		j.rHosts, j.iHosts = nil, nil
	}
	j.last = raw.Timestamp
	if raw.R.State == Positive {
		j.rHosts = nil
		for _, w := range append(slices.Clone(raw.R.Pair), raw.R.Witnesses...) {
			j.rHosts = append(j.rHosts, w.Hostname)
		}
	}
	if tr.LocalI2.State == Positive {
		j.iHosts = slices.Clone(tr.LocalI2.PositiveHosts)
	}
	if tr.Vote != Positive {
		reason := "negative"
		if tr.Vote == Unknown {
			reason = "unknown"
		}
		return tr, nil, append(events, j.close(raw.Timestamp, reason)...), nil
	}
	if j.open == nil {
		j.open = &RBI2Event{Version: RBI2Version, ID: RBI2EventID(j.identity.RunID, raw.Group, raw.Timestamp.UTC().Format(time.RFC3339Nano)),
			RunID: j.identity.RunID, KernelConfigID: j.identity.ConfigID, Group: raw.Group, DeltaSeconds: j.identity.DeltaSeconds,
			Detected: raw.Timestamp, FirstEvidence: *tr.FirstEvidence, LeftCensoredWindow: raw.Timestamp.Equal(j.identity.Window.Start),
			VoteSegments: []RBI2VoteSegment{}, RBSubspans: []RBI2RBSubspan{}, IFHosts: []string{}, WitnessHosts: []string{}}
	}
	e := j.open
	e.LastPositive = raw.Timestamp
	e.PositiveTicks++
	label := strings.Join(tr.ActiveVotes, "+")
	if len(e.VoteSegments) == 0 || e.VoteSegments[len(e.VoteSegments)-1].Votes != label {
		e.VoteSegments = append(e.VoteSegments, RBI2VoteSegment{raw.Timestamp, raw.Timestamp.Add(analysis.Step), label})
	} else {
		e.VoteSegments[len(e.VoteSegments)-1].End = raw.Timestamp.Add(analysis.Step)
	}
	if tr.LocalI2.Anomalous > e.IFSimultaneousMax {
		e.IFSimultaneousMax = tr.LocalI2.Anomalous
	}
	e.IFHosts = rbi2HostUnion(e.IFHosts, tr.LocalI2.PositiveHosts)
	if tr.R.State == Positive {
		e.WitnessHosts = rbi2HostUnion(e.WitnessHosts, j.rHosts)
	}
	if tr.I2.State == Positive {
		e.WitnessHosts = rbi2HostUnion(e.WitnessHosts, j.iHosts)
	}
	for _, lr := range raw.Local {
		if lr.AnomalyScore != nil {
			rbi2Max(&e.IFScoreMax, *lr.AnomalyScore)
		}
	}
	for _, v := range []struct {
		c      Criterion
		target **float64
	}{{raw.R.Criterion, &e.RRatioMax}, {raw.B, &e.BRatioMax}} {
		if v.c.Score != nil && v.c.Threshold != nil && *v.c.Threshold > 0 {
			rbi2Max(v.target, *v.c.Score / *v.c.Threshold)
		}
	}
	evidence := &RBI2Evidence{Version: RBI2Version, RunID: e.RunID, EventID: e.ID, Raw: snapshot, Temporal: tr, WitnessRefs: []RBI2WitnessRef{}}
	for i, occ := range []Occurrence{tr.R, tr.B, tr.I2} {
		if occ.State == Positive {
			evidence.WitnessRefs = append(evidence.WitnessRefs, RBI2WitnessRef{[]string{"R", "B", "I2"}[i], *occ.Witness})
		}
	}
	return tr, evidence, events, nil
}

func rbi2HostUnion(a, b []string) []string {
	a = append(a, b...)
	slices.Sort(a)
	return slices.Compact(a)
}

func rbi2Max(target **float64, value float64) {
	if analysis.Finite(value) && (*target == nil || value > **target) {
		*target = ptr(value)
	}
}

func (j *RBI2Journal) Flush(end time.Time) ([]RBI2Event, error) {
	if j == nil || !end.Equal(j.identity.Window.End) {
		return nil, fmt.Errorf("journal flush must seal the declared run window")
	}
	if err := j.clock.Flush(end); err != nil {
		return nil, err
	}
	rb, err := j.legacy.Flush(end)
	if err != nil {
		return nil, err
	}
	if err = j.addRB(rb); err != nil {
		return nil, err
	}
	if !j.last.IsZero() && end.After(j.last.Add(analysis.Step)) {
		return j.close(j.last.Add(analysis.Step), "missing_tail"), nil
	}
	return j.close(end, "window"), nil
}
