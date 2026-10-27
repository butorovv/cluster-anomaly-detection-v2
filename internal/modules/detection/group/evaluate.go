package group

import (
	"fmt"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/local"
)

// EvaluateGroup evaluates one fixed-inventory tick using validated frozen models.
// Records belonging to other groups are ignored. The caller retains ownership of
// models and tick records and must not mutate them during this call. No temporal
// state or outside-group context is calculated; Timing may be nil.
func EvaluateGroup(g *GroupProfile, t Tick, hw *Hardware, ld *local.Detector, timing *Timing) (Raw, error) {
	if g == nil || len(g.Hosts) != HostsPerGroup {
		return Raw{}, fmt.Errorf("fixed 18-host group profile required")
	}
	if t.Timestamp.IsZero() || !t.Timestamp.Equal(t.Timestamp.Truncate(analysis.Step)) {
		return Raw{}, fmt.Errorf("group tick must be on the 10-second grid")
	}
	for h, host := range g.Hosts {
		if host != fmt.Sprintf("%sn%02d", g.Group, h+1) {
			return Raw{}, fmt.Errorf("group inventory order mismatch")
		}
		if r, ok := t.Records[host]; ok && (r.Hostname != host || !r.Timestamp.Equal(t.Timestamp)) {
			return Raw{}, fmt.Errorf("record identity differs from group tick")
		}
	}
	if timing == nil {
		timing = &Timing{}
	}
	r := Raw{Timestamp: t.Timestamp, Group: g.Group, HostValues: map[string]HostVector{}}
	var u [18]HostVector
	started := time.Now()
	for h, host := range g.Hosts {
		record, ok := t.Records[host]
		if !ok {
			r.Missing = append(r.Missing, host)
			continue
		}
		v, e := HostChannels(record)
		if e != nil {
			r.Missing = append(r.Missing, host)
			continue
		}
		u[h] = v
		r.HostValues[host] = v
		r.Complete++
	}
	if r.Complete == 18 {
		v, e := GroupVector(u)
		if e != nil {
			return Raw{}, e
		}
		r.Vector = &v
	}
	timing.FeaturesSeconds += time.Since(started).Seconds()
	started = time.Now()
	if r.Vector == nil {
		r.R = RResult{Criterion: Criterion{State: Unknown, Reason: "incomplete_group"}, Channel: -1}
		if g.Observations > 0 {
			r.R.Threshold = ptr(g.RThreshold)
		}
	} else {
		r.R = g.R(u)
	}
	timing.RSeconds += time.Since(started).Seconds()
	started = time.Now()
	if r.Vector == nil {
		r.B = Criterion{State: Unknown, Reason: "incomplete_group"}
		if g.BThreshold != nil {
			r.B.Threshold = ptr(*g.BThreshold)
		}
	} else {
		r.B = g.B(*r.Vector)
	}
	timing.BSeconds += time.Since(started).Seconds()
	started = time.Now()
	r.Y = hw.Evaluate(g.Hosts, t.Records)
	timing.YSeconds += time.Since(started).Seconds()
	started = time.Now()
	for _, host := range g.Hosts {
		record, ok := t.Records[host]
		lr := local.LocalResult{Timestamp: t.Timestamp, Hostname: host, GroupID: g.Group, DiagnosticStatus: "missing_observation"}
		if ok {
			lr = ld.Evaluate(record)
		}
		if lr.DiagnosticStatus == local.StatusScoringError {
			return Raw{}, fmt.Errorf("local score: %s", lr.Diagnostic)
		}
		r.Local = append(r.Local, lr)
		if lr.IsAvailable {
			r.IF.Available++
			if lr.IsAnomaly != nil && *lr.IsAnomaly {
				r.IF.Anomalous++
			}
		}
	}
	timing.IFSeconds += time.Since(started).Seconds()
	if r.IF.Available > 0 {
		r.IF.S = ptr(float64(r.IF.Anomalous) / float64(r.IF.Available))
	}
	return r, nil
}
