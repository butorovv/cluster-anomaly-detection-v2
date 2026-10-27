package group

import (
	"fmt"
	"slices"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

// RBI2Version identifies the complete-coverage I2, two-of-three, causal TTL policy.
const RBI2Version = "group-rbi2-v1"

type RBI2TemporalResult struct {
	Timestamp     time.Time  `json:"timestamp"`
	Group         string     `json:"group"`
	DeltaSeconds  int        `json:"delta_seconds"`
	R             Occurrence `json:"r"`
	B             Occurrence `json:"b"`
	I2            Occurrence `json:"i2"`
	LocalI2       I2Result   `json:"local_i2"`
	Vote          State      `json:"vote"`
	ActiveVotes   []string   `json:"active_votes"`
	FirstEvidence *time.Time `json:"first_evidence"`
	GapStart      *time.Time `json:"gap_start,omitempty"`
}

// VoteRBI2 evaluates all 27 three-valued combinations without a Y shortcut.
func VoteRBI2(r, b, i2 State) (State, error) {
	for _, s := range []State{r, b, i2} {
		if s != Unknown && s != Negative && s != Positive {
			return Unknown, fmt.Errorf("invalid R/B/I2 state %q", s)
		}
	}
	return vote(r, b, i2), nil
}

// RBI2Engine owns only one ordered group's occurrence times. Event materialization
// is separate; the historical R/Y/B Engine is never modified by this engine.
type RBI2Engine struct {
	group  string
	hosts  []string
	delta  int
	split  time.Time
	last   time.Time
	end    time.Time
	states [3]occurrenceState // R,B,I2; only positive and unknown times are used.
}

func NewRBI2Engine(group string, hosts []string, deltaSeconds int, splitStart time.Time) (*RBI2Engine, error) {
	if deltaSeconds != 0 && deltaSeconds != 10 && deltaSeconds != 30 && deltaSeconds != 60 {
		return nil, fmt.Errorf("unsupported R/B/I2 delta")
	}
	if _, err := EvaluateI2(group, splitStart, hosts, nil); err != nil {
		return nil, err
	}
	return &RBI2Engine{group: group, hosts: slices.Clone(hosts), delta: deltaSeconds, split: splitStart}, nil
}

func (e *RBI2Engine) Step(raw Raw) (RBI2TemporalResult, error) {
	if e == nil {
		return RBI2TemporalResult{}, fmt.Errorf("nil R/B/I2 engine")
	}
	t := raw.Timestamp
	if !e.end.IsZero() || raw.Group != e.group || t.Before(e.split) || (!e.last.IsZero() && !t.After(e.last)) {
		return RBI2TemporalResult{}, fmt.Errorf("invalid identity, duplicate, unordered or finalized R/B/I2 tick")
	}
	i2, err := EvaluateI2(e.group, t, e.hosts, raw.Local)
	if err != nil {
		return RBI2TemporalResult{}, err
	}
	if _, err := VoteRBI2(raw.R.State, raw.B.State, i2.State); err != nil {
		return RBI2TemporalResult{}, err
	}
	// All input validation precedes mutation, so a rejected tick can be retried.
	out := RBI2TemporalResult{Timestamp: t, Group: e.group, DeltaSeconds: e.delta, LocalI2: i2, ActiveVotes: []string{}}
	previous := e.last
	if previous.IsZero() {
		previous = e.split.Add(-analysis.Step)
	}
	if t.Sub(previous) > analysis.Step {
		out.GapStart = timePtr(previous.Add(analysis.Step))
		for i := range e.states {
			e.states[i] = occurrenceState{unknown: t.Add(-analysis.Step)}
		}
	}
	e.last = t
	var active [3]Occurrence
	for i, s := range []State{raw.R.State, raw.B.State, i2.State} {
		if s == Unknown {
			e.states[i].unknown, e.states[i].positive = t, time.Time{}
		} else if s == Positive {
			e.states[i].positive = t
		}
		v := e.states[i]
		cut := t.Add(-time.Duration(e.delta) * time.Second)
		active[i].State = Negative
		if !v.positive.IsZero() && !v.positive.Before(cut) && v.positive.After(v.unknown) {
			active[i] = Occurrence{State: Positive, Witness: timePtr(v.positive)}
			out.ActiveVotes = append(out.ActiveVotes, []string{"R", "B", "I2"}[i])
		} else if !v.unknown.IsZero() && !v.unknown.Before(cut) {
			active[i].State = Unknown
		}
	}
	out.R, out.B, out.I2 = active[0], active[1], active[2]
	out.Vote, _ = VoteRBI2(out.R.State, out.B.State, out.I2.State)
	if out.Vote == Positive {
		bestSpan := time.Duration(1<<63 - 1)
		var bestLo, bestHi time.Time
		for i := 0; i < 3; i++ {
			for j := i + 1; j < 3; j++ {
				if active[i].State != Positive || active[j].State != Positive {
					continue
				}
				lo, hi := *active[i].Witness, *active[j].Witness
				if lo.After(hi) {
					lo, hi = hi, lo
				}
				span := hi.Sub(lo)
				if span < bestSpan || (span == bestSpan && hi.After(bestHi)) {
					bestSpan, bestLo, bestHi = span, lo, hi
				}
			}
		}
		out.FirstEvidence = timePtr(bestLo)
	}
	return out, nil
}

func (e *RBI2Engine) Flush(end time.Time) error {
	if e == nil {
		return fmt.Errorf("nil R/B/I2 engine")
	}
	if !e.end.IsZero() {
		if end.Equal(e.end) {
			return nil
		}
		return fmt.Errorf("R/B/I2 engine already finalized at a different boundary")
	}
	if end.IsZero() || end.Before(e.split) || !end.Equal(end.Truncate(analysis.Step)) || (!e.last.IsZero() && !end.After(e.last)) {
		return fmt.Errorf("exclusive grid flush boundary must follow the last tick")
	}
	e.end = end
	e.states = [3]occurrenceState{}
	return nil
}
