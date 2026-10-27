package group

import (
	"fmt"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/local"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

// I2Result summarizes known local decisions, including partial positive evidence
// when State is Unknown. Available counts only available, non-nil decisions.
type I2Result struct {
	State         State    `json:"state"`
	Available     int      `json:"available"`
	Anomalous     int      `json:"anomalous"`
	PositiveHosts []string `json:"positive_hosts"`
	Reason        string   `json:"reason,omitempty"`
}

// EvaluateI2 aggregates existing IF/B1 decisions for one fixed 18-host tick.
// Hosts must be the canonical inventory order group+n01 through group+n18;
// results may be in any order or omit hosts. Identity errors return an error
// and an unknown result without partial counts. Incomplete observations return
// unknown without an error, even if two or more known decisions are positive.
//
// A decision is known exactly when IsAvailable is true and IsAnomaly is non-nil.
// Scores, thresholds and diagnostic text belong to the local detector and are
// not reinterpreted here. No scoring, model loading or input mutation occurs.
func EvaluateI2(group string, timestamp time.Time, hosts []string, results []local.LocalResult) (I2Result, error) {
	invalid := I2Result{State: Unknown, Reason: "invalid_input"}
	parsed, _, err := topology.ParseHostname(group + "n01")
	if err != nil || parsed != group {
		return invalid, fmt.Errorf("I2: invalid canonical group %q", group)
	}
	if timestamp.IsZero() || !timestamp.Equal(timestamp.Truncate(analysis.Step)) {
		return invalid, fmt.Errorf("I2: timestamp must be on the 10-second grid")
	}
	if len(hosts) != HostsPerGroup {
		return invalid, fmt.Errorf("I2: fixed 18-host inventory required")
	}
	for i, host := range hosts {
		if host != fmt.Sprintf("%sn%02d", group, i+1) {
			return invalid, fmt.Errorf("I2: canonical inventory mismatch at slot %d", i+1)
		}
	}
	var slots [HostsPerGroup]*local.LocalResult
	for i := range results {
		r := &results[i]
		g, index, err := topology.ParseHostname(r.Hostname)
		if err != nil || g != group || r.GroupID != group || r.Hostname != hosts[index-1] {
			return invalid, fmt.Errorf("I2: result host/group outside canonical inventory: %q/%q", r.Hostname, r.GroupID)
		}
		if !r.Timestamp.Equal(timestamp) {
			return invalid, fmt.Errorf("I2: result timestamp mismatch for %q", r.Hostname)
		}
		if slots[index-1] != nil {
			return invalid, fmt.Errorf("I2: duplicate result for %q", r.Hostname)
		}
		slots[index-1] = r
	}
	out := I2Result{State: Unknown, PositiveHosts: make([]string, 0, HostsPerGroup)}
	missing, unavailable, undecided := false, false, false
	for i, r := range slots {
		switch {
		case r == nil:
			missing = true
		case !r.IsAvailable:
			unavailable = true
		case r.IsAnomaly == nil:
			undecided = true
		default:
			out.Available++
			if *r.IsAnomaly {
				out.Anomalous++
				out.PositiveHosts = append(out.PositiveHosts, hosts[i])
			}
		}
	}
	// Use fixed diagnostic precedence when several kinds of incompleteness coexist.
	switch {
	case missing:
		out.Reason = "missing_local_results"
	case unavailable:
		out.Reason = "unavailable_local_results"
	case undecided:
		out.Reason = "missing_local_decisions"
	default:
		out.State = Negative
		if out.Anomalous >= 2 {
			out.State = Positive
		}
	}
	return out, nil
}
