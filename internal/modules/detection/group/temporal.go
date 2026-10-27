package group

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"
)

type occurrenceState struct {
	positive time.Time
	unknown  time.Time
	hosts    []string
	zero     bool
}
type Engine struct {
	config, group string
	delta         int
	split, last   time.Time
	states        [3]occurrenceState // R,Y,B; constant bounded memory
	open          *Event
	finished      bool
}

func NewEngine(configID, group string, deltaSeconds int, splitStart time.Time) (*Engine, error) {
	if configID == "" || group == "" || splitStart.IsZero() || !splitStart.Equal(splitStart.Truncate(10*time.Second)) {
		return nil, fmt.Errorf("config, group and grid split start required")
	}
	if deltaSeconds != 0 && deltaSeconds != 10 && deltaSeconds != 30 && deltaSeconds != 60 {
		return nil, fmt.Errorf("unsupported delta")
	}
	return &Engine{config: configID, group: group, delta: deltaSeconds, split: splitStart}, nil
}
func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
func vote(states ...State) State {
	p, u := 0, 0
	for _, s := range states {
		if s == Positive {
			p++
		} else if s == Unknown {
			u++
		}
	}
	if p >= 2 {
		return Positive
	}
	if p+u < 2 {
		return Negative
	}
	return Unknown
}
func (e *Engine) active(i int, t time.Time) Occurrence {
	s := e.states[i]
	cut := t.Add(-time.Duration(e.delta) * time.Second)
	if !s.positive.IsZero() && !s.positive.Before(cut) && s.positive.After(s.unknown) {
		return Occurrence{State: Positive, Witness: timePtr(s.positive)}
	}
	if !s.unknown.IsZero() && !s.unknown.Before(cut) {
		return Occurrence{State: Unknown}
	}
	return Occurrence{State: Negative}
}
func (e *Engine) close(t time.Time, missing, window bool) []Event {
	if e.open == nil {
		return nil
	}
	v := *e.open
	v.Closed = t
	v.CensoredMissing = missing
	v.CensoredWindow = window
	v.DurationSeconds = int(v.LastPositive.Sub(v.Detected).Seconds()) + 10
	v.DelaySeconds = int(v.Detected.Sub(v.FirstEvidence).Seconds())
	if v.RawFirst != nil && v.RawLast != nil {
		v.RawSpanSeconds = int(v.RawLast.Sub(*v.RawFirst).Seconds()) + 10
	}
	e.open = nil
	return []Event{v}
}
func (e *Engine) Step(raw Raw) (TemporalResult, []Event, error) {
	t := raw.Timestamp
	if e.finished || raw.Group != e.group || t.IsZero() || t.Before(e.split) || !t.Equal(t.Truncate(10*time.Second)) || (!e.last.IsZero() && !t.After(e.last)) {
		return TemporalResult{}, nil, fmt.Errorf("invalid identity, duplicate or unordered tick")
	}
	states := []State{raw.R.State, raw.Y.State, raw.B.State}
	for _, s := range states {
		if s != Positive && s != Negative && s != Unknown {
			return TemporalResult{}, nil, fmt.Errorf("invalid criterion state")
		}
	}
	var closed []Event
	previous := e.last
	if previous.IsZero() {
		previous = e.split.Add(-10 * time.Second)
	}
	if t.Sub(previous) > 10*time.Second {
		closed = append(closed, e.close(previous.Add(10*time.Second), true, false)...)
		for i := range e.states {
			e.states[i] = occurrenceState{unknown: t.Add(-10 * time.Second)}
		}
	}
	e.last = t
	for i, s := range states {
		if s == Unknown {
			e.states[i].unknown = t
			e.states[i].positive = time.Time{}
			e.states[i].hosts = nil
			e.states[i].zero = false
		}
		if s == Positive {
			e.states[i].positive = t
			if i == 1 {
				e.states[i].hosts = nil
				for host, state := range raw.Y.Hosts {
					if state == Positive {
						e.states[i].hosts = append(e.states[i].hosts, host)
					}
				}
			}
			if i == 0 {
				e.states[i].hosts = nil
				e.states[i].zero = false
				for _, w := range raw.R.Pair {
					e.states[i].hosts = append(e.states[i].hosts, w.Hostname)
					e.states[i].zero = e.states[i].zero || w.ZeroMAD
				}
				for _, w := range raw.R.Witnesses {
					e.states[i].hosts = append(e.states[i].hosts, w.Hostname)
				}
			}
		}
	}
	r, y, b := e.active(0, t), e.active(1, t), e.active(2, t)
	tr := TemporalResult{Timestamp: t, Group: e.group, DeltaSeconds: e.delta, R: r, Y: y, B: b, Vote: vote(r.State, y.State, b.State), Candidate: Unknown}
	if y.State == Unknown {
		tr.Mode = "preliminary"
		tr.CriteriaIncomplete = true
		tr.HardwareConfirmation = "insufficient_criteria"
		if r.State != Unknown && b.State != Unknown {
			tr.Candidate = Negative
			if r.State == Positive && b.State == Positive {
				tr.Candidate = Positive
			}
		}
	} else {
		tr.Mode = "full"
		tr.HardwareConfirmation = "criteria_available"
		tr.Candidate = tr.Vote
	}
	if e.open != nil && tr.Candidate == Positive && e.open.Mode != tr.Mode {
		closed = append(closed, e.close(t, false, false)...)
	}
	if tr.Candidate != Positive {
		// A missing required statistical method is distinct from absent Y applicability.
		closed = append(closed, e.close(t, tr.Candidate == Unknown, false)...)
		return tr, closed, nil
	}
	// Choose minimum span, most recent later witness, then lexical B/R/Y.
	occ := []Occurrence{b, r, y}
	bestSpan := time.Duration(1<<63 - 1)
	var bestLo, bestHi time.Time
	for i := 0; i < 3; i++ {
		for j := i + 1; j < 3; j++ {
			if occ[i].State != Positive || occ[j].State != Positive {
				continue
			}
			lo, hi := *occ[i].Witness, *occ[j].Witness
			if lo.After(hi) {
				lo, hi = hi, lo
			}
			span := hi.Sub(lo)
			if span < bestSpan || (span == bestSpan && hi.After(bestHi)) {
				bestSpan, bestLo, bestHi = span, lo, hi
			}
		}
	}
	if bestLo.IsZero() || bestSpan > time.Duration(e.delta)*time.Second {
		return TemporalResult{}, nil, fmt.Errorf("positive candidate without causal witness pair")
	}
	tr.FirstEvidence = timePtr(bestLo)
	if e.open == nil {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%d/%s", e.config, e.group, e.delta, t.UTC().Format(time.RFC3339Nano))))
		e.open = &Event{RunID: e.config, ID: hex.EncodeToString(sum[:]), Group: e.group, DeltaSeconds: e.delta, Mode: tr.Mode, CriteriaIncomplete: tr.CriteriaIncomplete, HardwareConfirmation: tr.HardwareConfirmation, FirstEvidence: bestLo, Detected: t, LeftCensoredWindow: t.Equal(e.split)}
	}
	ev := e.open
	ev.LastPositive = t
	ev.PositiveTicks++
	if raw.IF.Available > 0 && raw.IF.Anomalous > 0 {
		ev.IFSupportedTicks++
	}
	if r.State == Positive && e.states[0].zero {
		ev.ZeroMADPairTicks++
	}
	rawCoincident := raw.R.State == Positive && raw.B.State == Positive
	if tr.Mode == "full" {
		rawCoincident = vote(raw.R.State, raw.Y.State, raw.B.State) == Positive
	}
	if rawCoincident {
		ev.RawCoincidentTicks++
		if ev.RawFirst == nil {
			ev.RawFirst = timePtr(t)
		}
		ev.RawLast = timePtr(t)
	} else {
		ev.TTLExtensionTicks++
	}
	if r.State == Positive || y.State == Positive {
		if r.State == Positive {
			ev.WitnessHosts = append(ev.WitnessHosts, e.states[0].hosts...)
		}
		if y.State == Positive {
			ev.WitnessHosts = append(ev.WitnessHosts, e.states[1].hosts...)
		}
		sort.Strings(ev.WitnessHosts)
		ev.WitnessHosts = unique(ev.WitnessHosts)
	}
	return tr, closed, nil
}
func unique(xs []string) []string {
	o := xs[:0]
	for _, x := range xs {
		if x != "" && (len(o) == 0 || o[len(o)-1] != x) {
			o = append(o, x)
		}
	}
	return o
}

// Flush seals the split at an exclusive grid boundary. A missing tail closes
// at the first unobserved cell. Further Step calls require a new Engine.
func (e *Engine) Flush(end time.Time) ([]Event, error) {
	if end.IsZero() || end.Before(e.split) || !end.Equal(end.Truncate(10*time.Second)) || (!e.last.IsZero() && !end.After(e.last)) {
		return nil, fmt.Errorf("flush must be on grid and follow last tick within split")
	}
	if e.finished {
		return nil, nil
	}
	e.finished = true
	e.states = [3]occurrenceState{}
	if !e.last.IsZero() && end.After(e.last.Add(10*time.Second)) {
		// Unobserved tail cells are missing, just like a gap inside Step.
		return e.close(e.last.Add(10*time.Second), true, false), nil
	}
	return e.close(end, false, true), nil
}
