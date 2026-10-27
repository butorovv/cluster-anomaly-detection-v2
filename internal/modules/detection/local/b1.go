package local

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

const (
	Version                  = "local-b1-v1"
	Quantile                 = .99
	Comparison               = "score > threshold"
	StatusCorruptProfile     = "corrupt_profile"
	StatusIncompatibleModel  = "incompatible_model"
	StatusNormal             = "normal"
	StatusAnomaly            = "anomaly"
	StatusIncompleteF18      = "incomplete_f18"
	StatusUnknownHost        = "unknown_host"
	StatusMissingThreshold   = "missing_threshold"
	StatusInvalidObservation = "invalid_observation"
	StatusScoringError       = "scoring_error"
)

type Binding struct {
	DatasetID         string   `json:"dataset_id"`
	PreparedSHA256    string   `json:"prepared_sha256"`
	StatisticalSHA256 string   `json:"statistical_sha256"`
	IsolationSHA256   string   `json:"isolation_sha256"`
	FeatureOrder      []string `json:"feature_order"`
	Hosts             []string `json:"hosts"`
	AssumedTimezone   string   `json:"assumed_timezone"`
	TimezoneVerified  bool     `json:"timezone_verified"`
}

type Profile struct {
	Version        string             `json:"version"`
	Binding        Binding            `json:"binding"`
	TrainingWindow analysis.Window    `json:"training_window"`
	Quantile       float64            `json:"quantile"`
	HostCount      int                `json:"host_count"`
	Thresholds     map[string]float64 `json:"thresholds"`
	Observations   map[string]int     `json:"observations"`
	Provenance     map[string]string  `json:"provenance,omitempty"`
	Comparison     string             `json:"comparison"`
}

type LoadError struct {
	Status string
	Err    error
}

func (e *LoadError) Error() string {
	if e == nil {
		return ""
	}
	return e.Status + ": " + e.Err.Error()
}
func (e *LoadError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
func StatusOf(err error) string {
	var e *LoadError
	if errors.As(err, &e) {
		return e.Status
	}
	return ""
}
func fail(status string, err error) error { return &LoadError{Status: status, Err: err} }

func (p *Profile) Validate() error {
	if p == nil {
		return fmt.Errorf("nil profile")
	}
	if p.Provenance != nil {
		for k, v := range p.Provenance {
			if k == "" || v == "" {
				return fmt.Errorf("invalid provenance entry")
			}
		}
	}
	if p.Version != Version || p.TrainingWindow != analysis.NIRWindows()[0] || p.Quantile != Quantile || p.Comparison != Comparison {
		return fmt.Errorf("profile metadata mismatch")
	}
	b := p.Binding
	if b.DatasetID == "" {
		return fmt.Errorf("empty dataset id")
	}
	for _, s := range []string{b.PreparedSHA256, b.StatisticalSHA256, b.IsolationSHA256} {
		raw, e := hex.DecodeString(s)
		if e != nil || len(raw) != 32 || len(s) != 64 {
			return fmt.Errorf("invalid SHA-256 binding")
		}
	}
	if !slices.Equal(b.FeatureOrder, domain.FeatureNames[:]) {
		return fmt.Errorf("feature order mismatch")
	}
	if p.HostCount != len(b.Hosts) || p.HostCount == 0 {
		return fmt.Errorf("host count mismatch")
	}
	hosts := make(map[string]bool, len(b.Hosts))
	for _, h := range b.Hosts {
		if _, _, e := topology.ParseHostname(h); e != nil || hosts[h] {
			return fmt.Errorf("invalid or duplicate host %q", h)
		}
		hosts[h] = true
	}
	if len(p.Observations) != len(hosts) {
		return fmt.Errorf("observation host table incomplete")
	}
	for h, n := range p.Observations {
		if !hosts[h] || n < 0 {
			return fmt.Errorf("invalid observation count for %q", h)
		}
		theta, ok := p.Thresholds[h]
		if n > 0 && !ok {
			return fmt.Errorf("missing threshold for observed host %q", h)
		}
		if n == 0 && ok {
			return fmt.Errorf("threshold for unobserved host %q", h)
		}
		if ok && (!analysis.Finite(theta) || theta <= 0 || theta > 1) {
			return fmt.Errorf("invalid threshold for %q", h)
		}
	}
	for h := range p.Thresholds {
		if !hosts[h] {
			return fmt.Errorf("unknown threshold host %q", h)
		}
	}
	return nil
}

func NewProfile(binding Binding, thresholds map[string]float64, observations map[string]int) (*Profile, error) {
	p := &Profile{Version: Version, Binding: cloneBinding(binding), TrainingWindow: analysis.NIRWindows()[0], Quantile: Quantile, HostCount: len(binding.Hosts), Thresholds: cloneFloatMap(thresholds), Observations: cloneIntMap(observations), Provenance: nil, Comparison: Comparison}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}
func cloneBinding(b Binding) Binding {
	b.FeatureOrder = append([]string(nil), b.FeatureOrder...)
	b.Hosts = append([]string(nil), b.Hosts...)
	return b
}
func cloneFloatMap(m map[string]float64) map[string]float64 {
	if m == nil {
		return nil
	}
	n := make(map[string]float64, len(m))
	for k, v := range m {
		n[k] = v
	}
	return n
}
func cloneIntMap(m map[string]int) map[string]int {
	if m == nil {
		return nil
	}
	n := make(map[string]int, len(m))
	for k, v := range m {
		n[k] = v
	}
	return n
}

func LoadProfile(path string) (*Profile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var p Profile
	if err = dec.Decode(&p); err == nil {
		var extra any
		if e := dec.Decode(&extra); e != io.EOF {
			if e == nil {
				err = fmt.Errorf("trailing JSON value")
			} else {
				err = e
			}
		}
	}
	if err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	if err = p.Validate(); err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	return &p, nil
}
func SaveProfile(path string, p *Profile) error {
	if err := p.Validate(); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	err = enc.Encode(p)
	cerr := f.Close()
	if err != nil {
		return err
	}
	return cerr
}

type LocalResult struct {
	Timestamp           time.Time `json:"timestamp"`
	Hostname            string    `json:"hostname"`
	GroupID             string    `json:"group_id"`
	AnomalyScore        *float64  `json:"anomaly_score"`
	IndividualThreshold *float64  `json:"individual_threshold"`
	IsAnomaly           *bool     `json:"is_anomaly"`
	IsAvailable         bool      `json:"is_available"`
	DiagnosticStatus    string    `json:"diagnostic_status"`
	Diagnostic          string    `json:"diagnostic,omitempty"`
}
type Detector struct {
	profile Profile
	forest  *analysis.IsolationForest
	hosts   map[string]bool
}

func bindingEqual(a, b Binding) bool {
	return a.DatasetID == b.DatasetID && a.PreparedSHA256 == b.PreparedSHA256 && a.StatisticalSHA256 == b.StatisticalSHA256 && a.IsolationSHA256 == b.IsolationSHA256 && slices.Equal(a.FeatureOrder, b.FeatureOrder) && slices.Equal(a.Hosts, b.Hosts) && a.AssumedTimezone == b.AssumedTimezone && a.TimezoneVerified == b.TimezoneVerified
}
func NewDetector(p *Profile, forest *analysis.IsolationForest, expected Binding) (*Detector, error) {
	if err := p.Validate(); err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	if !bindingEqual(p.Binding, expected) {
		return nil, fail(StatusIncompatibleModel, fmt.Errorf("profile binding mismatch"))
	}
	if forest == nil {
		return nil, fail(StatusCorruptProfile, fmt.Errorf("nil isolation forest"))
	}
	data, err := json.Marshal(forest)
	if err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	var copyForest analysis.IsolationForest
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&copyForest); err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	if err = copyForest.Validate(); err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	cp := *p
	cp.Binding = cloneBinding(p.Binding)
	cp.Thresholds = cloneFloatMap(p.Thresholds)
	cp.Observations = cloneIntMap(p.Observations)
	if p.Provenance != nil {
		cp.Provenance = make(map[string]string, len(p.Provenance))
		for k, v := range p.Provenance {
			cp.Provenance[k] = v
		}
	}
	h := make(map[string]bool, len(cp.Binding.Hosts))
	for _, host := range cp.Binding.Hosts {
		h[host] = true
	}
	return &Detector{profile: cp, forest: &copyForest, hosts: h}, nil
}
func (d *Detector) Evaluate(r domain.TelemetryRecord) LocalResult {
	out := LocalResult{Timestamp: r.Timestamp, Hostname: r.Hostname, GroupID: r.GroupID}
	if d == nil {
		out.DiagnosticStatus = StatusScoringError
		out.Diagnostic = "nil detector"
		return out
	}
	if !d.hosts[r.Hostname] {
		out.DiagnosticStatus = StatusUnknownHost
		return out
	}
	group, index, err := topology.ParseHostname(r.Hostname)
	if err != nil || r.Timestamp.IsZero() || group != r.GroupID || index != r.NodeIndex {
		out.DiagnosticStatus = StatusInvalidObservation
		out.Diagnostic = "inconsistent telemetry identity"
		return out
	}
	features := r.Features()
	var mask uint32
	for i, v := range features {
		if !math.IsNaN(v) && !math.IsInf(v, 0) {
			mask |= 1 << i
		}
	}
	if r.AvailabilityMask != mask || r.Valid != (mask == domain.FullAvailabilityMask) {
		out.DiagnosticStatus = StatusInvalidObservation
		out.Diagnostic = "inconsistent F18 availability metadata"
		return out
	}
	if mask != domain.FullAvailabilityMask {
		out.DiagnosticStatus = StatusIncompleteF18
		return out
	}
	score, err := d.forest.Score(features)
	if err != nil || !analysis.Finite(score) {
		out.DiagnosticStatus = StatusScoringError
		if err != nil {
			out.Diagnostic = err.Error()
		}
		return out
	}
	out.AnomalyScore = &score
	if n := d.profile.Observations[r.Hostname]; n == 0 {
		out.DiagnosticStatus = StatusMissingThreshold
		return out
	}
	threshold := d.profile.Thresholds[r.Hostname]
	out.IndividualThreshold = &threshold
	anomaly := score > threshold
	out.IsAnomaly = &anomaly
	out.IsAvailable = true
	if anomaly {
		out.DiagnosticStatus = StatusAnomaly
	} else {
		out.DiagnosticStatus = StatusNormal
	}
	return out
}
