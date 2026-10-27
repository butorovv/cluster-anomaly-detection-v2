package local

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/experiment"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

// Models binds existing immutable files. Opening models never fits a forest.
type Models struct {
	Source  *experiment.Source
	Binding Binding
	Forest  *analysis.IsolationForest
}

func digest(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func fileDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, readErr := io.Copy(h, f)
	closeErr := f.Close()
	if readErr != nil {
		return "", readErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func OpenModels(input, manifest, statisticalPath, isolationPath string) (*Models, error) {
	s, err := experiment.OpenSource(input, manifest)
	if err != nil {
		return nil, fail(StatusIncompatibleModel, fmt.Errorf("prepared source: %w", err))
	}
	return openModelsForSource(s, statisticalPath, isolationPath)
}

// openModelsForSource shares all model-envelope and byte-binding checks between
// the offline source path and the explicitly metadata-only frozen loader.
func openModelsForSource(s *experiment.Source, statisticalPath, isolationPath string) (*Models, error) {
	stat, err := experiment.LoadProfile(statisticalPath)
	if err != nil {
		return nil, fail(StatusCorruptProfile, fmt.Errorf("statistical profile: %w", err))
	}
	// Distinguish structurally corrupt models from valid but incompatible bindings.
	bytes, err := os.ReadFile(isolationPath)
	if err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	var envelope experiment.IsolationRun
	if err = json.Unmarshal(bytes, &envelope); err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	if err = envelope.Profile.Validate(); err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	loaded, err := experiment.LoadIsolationProfile(isolationPath, s, statisticalPath)
	if err != nil {
		return nil, fail(StatusIncompatibleModel, err)
	}
	statBytes, err := os.ReadFile(statisticalPath)
	if err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	if stat.Median != loaded.Profile.Median || stat.Scale != loaded.Profile.Scale {
		return nil, fail(StatusIncompatibleModel, fmt.Errorf("normalizer changed while loading"))
	}
	forestHash, err := fileDigest(isolationPath)
	if err != nil || forestHash != digest(bytes) || digest(statBytes) != loaded.StatisticalReference.SHA256 {
		return nil, fail(StatusIncompatibleModel, fmt.Errorf("model files changed while loading"))
	}
	binding := Binding{
		DatasetID: "prepared-sha256:" + s.SHA256, PreparedSHA256: s.SHA256,
		StatisticalSHA256: digest(statBytes), IsolationSHA256: digest(bytes),
		FeatureOrder: append([]string(nil), domain.FeatureNames[:]...),
		Hosts:        append([]string(nil), s.Hosts...), AssumedTimezone: s.AssumedTimezone,
		TimezoneVerified: s.TimezoneVerified,
	}
	return &Models{Source: s, Binding: binding, Forest: loaded.Profile}, nil
}

// HostData is replayable telemetry, not an already normalized feature matrix.
type HostData func(context.Context, func(domain.TelemetryRecord) error) error

// FitProfile computes exact per-host Q99 scores from D1 only. It never trains IF.
func FitProfile(ctx context.Context, binding Binding, forest *analysis.IsolationForest, data HostData) (*Profile, error) {
	if ctx == nil || data == nil || forest == nil {
		return nil, fmt.Errorf("D1 calibration requires context, data and a frozen forest")
	}
	counts := make(map[string]int, len(binding.Hosts))
	scores := make(map[string][]float64, len(binding.Hosts))
	for _, host := range binding.Hosts {
		counts[host] = 0
	}
	// Validate the metadata before collecting observations.
	p, err := NewProfile(binding, nil, counts)
	if err != nil {
		return nil, err
	}
	detector, err := NewDetector(p, forest, binding)
	if err != nil {
		return nil, err
	}
	total := 0
	previous := map[string]time.Time{}
	err = data(ctx, func(r domain.TelemetryRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !analysis.NIRWindows()[0].Contains(r.Timestamp) {
			return fmt.Errorf("B1 calibration accepts D1 only")
		}
		if r.Timestamp.Sub(analysis.NIRWindows()[0].Start)%analysis.Step != 0 ||
			(!previous[r.Hostname].IsZero() && !r.Timestamp.After(previous[r.Hostname])) {
			return fmt.Errorf("D1 host timestamps must be unique, increasing and on the 10-second grid")
		}
		previous[r.Hostname] = r.Timestamp
		result := detector.Evaluate(r)
		switch result.DiagnosticStatus {
		case StatusIncompleteF18:
			return nil
		case StatusMissingThreshold:
			// The temporary detector has no thresholds but returns the frozen score.
			if result.AnomalyScore == nil {
				return fmt.Errorf("missing calibration score")
			}
		default:
			return fmt.Errorf("invalid D1 observation: %s (%s)", result.DiagnosticStatus, result.Diagnostic)
		}
		scores[r.Hostname] = append(scores[r.Hostname], *result.AnomalyScore)
		counts[r.Hostname]++
		total++
		return nil
	})
	if err != nil {
		return nil, err
	}
	if total == 0 {
		return nil, fmt.Errorf("no complete finite D1 states")
	}
	thresholds := make(map[string]float64, len(scores))
	for _, host := range binding.Hosts {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if counts[host] == 0 {
			continue // Known host, legitimately unavailable threshold.
		}
		thresholds[host], err = analysis.EmpiricalQuantile(scores[host], Quantile)
		if err != nil {
			return nil, err
		}
	}
	p, err = NewProfile(binding, thresholds, counts)
	if err == nil {
		p.Provenance = map[string]string{"method": "d1_score_quantile"}
	}
	return p, err
}

func (m *Models) BuildProfile(ctx context.Context) (*Profile, experiment.Audit, error) {
	var audit experiment.Audit
	if m == nil || m.Source == nil {
		return nil, audit, fmt.Errorf("missing models/source")
	}
	data := func(ctx context.Context, visit func(domain.TelemetryRecord) error) error {
		var err error
		audit, err = m.Source.Scan(ctx, analysis.NIRWindows()[0], visit)
		return err
	}
	p, err := FitProfile(ctx, m.Binding, m.Forest, data)
	return p, audit, err
}

// ImportContextProfile reuses checked research thresholds without rescoring D1.
// Its evidence is bound to the same source, normalizer and A0 forest and to the
// preserved D1 binary. No D2/D3 value is used to choose or adjust thresholds.
func (m *Models) ImportContextProfile(directory string) (*Profile, error) {
	if m == nil || m.Source == nil {
		return nil, fmt.Errorf("missing models/source")
	}
	metaBytes, err := os.ReadFile(filepath.Join(directory, "metadata.json"))
	if err != nil {
		return nil, err
	}
	var meta struct {
		Source   *experiment.Source `json:"source"`
		Features []string           `json:"feature_order"`
		D1       experiment.Audit   `json:"d1_audit"`
		Inputs   map[string]string  `json:"input_sha256_before"`
		After    map[string]string  `json:"input_sha256_after"`
		Binary   map[string]string  `json:"binary_sha256"`
	}
	if err = json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	w := analysis.NIRWindows()[0]
	samples := int(w.End.Sub(w.Start) / analysis.Step)
	if meta.Source == nil || meta.Source.SHA256 != m.Binding.PreparedSHA256 ||
		!slices.Equal(meta.Source.Hosts, m.Binding.Hosts) || !slices.Equal(meta.Features, m.Binding.FeatureOrder) ||
		meta.Source.AssumedTimezone != m.Binding.AssumedTimezone || meta.Source.TimezoneVerified != m.Binding.TimezoneVerified {
		return nil, fail(StatusIncompatibleModel, fmt.Errorf("research export source/F18 binding mismatch"))
	}
	for _, want := range []string{m.Binding.PreparedSHA256, m.Binding.StatisticalSHA256, m.Binding.IsolationSHA256} {
		found := false
		for path, value := range meta.Inputs {
			if value == want && meta.After[path] == value {
				found = true
			}
		}
		if !found {
			return nil, fail(StatusIncompatibleModel, fmt.Errorf("research export model identity not verified"))
		}
	}
	if meta.D1.Window != w || meta.D1.States != samples*len(m.Binding.Hosts) ||
		meta.D1.Valid != meta.D1.States || meta.D1.Timestamps != samples ||
		meta.D1.Hosts != len(m.Binding.Hosts) || meta.D1.MinimumStatesPerTimestamp != len(m.Binding.Hosts) {
		return nil, fail(StatusCorruptProfile, fmt.Errorf("research D1 is not a complete finite host/time grid"))
	}
	thresholdBytes, err := os.ReadFile(filepath.Join(directory, "thresholds.json"))
	if err != nil {
		return nil, err
	}
	var saved struct {
		B1 map[string]float64 `json:"B1"`
	}
	if err = json.Unmarshal(thresholdBytes, &saved); err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	d1Hash, err := verifyResearchD1(filepath.Join(directory, "D1.bin"), m.Binding.Hosts, saved.B1, meta.Binary["D1.bin"])
	if err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	counts := make(map[string]int, len(m.Binding.Hosts))
	for _, host := range m.Binding.Hosts {
		counts[host] = samples
	}
	p, err := NewProfile(m.Binding, saved.B1, counts)
	if err != nil {
		return nil, fail(StatusCorruptProfile, err)
	}
	p.Provenance = map[string]string{
		"method": "import_verified_context", "metadata_sha256": digest(metaBytes),
		"thresholds_sha256": digest(thresholdBytes), "d1_export_sha256": d1Hash,
	}
	return p, nil
}

// Verify already exported A0 scores and their Q99, without rescoring telemetry,
// fitting models or consulting D2. The binary schema belongs to research 4.2.
func verifyResearchD1(path string, hosts []string, thresholds map[string]float64, expectedHash string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	hash := sha256.New()
	reader := bufio.NewReaderSize(io.TeeReader(f, hash), 1<<20)
	w := analysis.NIRWindows()[0]
	samples := int(w.End.Sub(w.Start) / analysis.Step)
	if len(hosts) == 0 || len(thresholds) != len(hosts) {
		return "", fmt.Errorf("research threshold table incomplete")
	}
	scores := make([][]float64, len(hosts))
	var row [216]byte
	for i := 0; i < samples*len(hosts); i++ {
		if _, err = io.ReadFull(reader, row[:]); err != nil {
			return "", fmt.Errorf("D1 export length: %w", err)
		}
		h := i % len(hosts)
		timestamp := int64(binary.LittleEndian.Uint64(row[:8]))
		if timestamp != w.Start.UnixMicro()+int64(i/len(hosts))*analysis.Step.Microseconds() ||
			int(binary.LittleEndian.Uint32(row[8:12])) != h ||
			binary.LittleEndian.Uint32(row[12:16]) != domain.FullAvailabilityMask {
			return "", fmt.Errorf("research D1 identity/grid/availability mismatch at row %d", i)
		}
		score := math.Float64frombits(binary.LittleEndian.Uint64(row[160:168]))
		if !analysis.Finite(score) || score <= 0 || score > 1 {
			return "", fmt.Errorf("invalid exported A0 score")
		}
		scores[h] = append(scores[h], score)
	}
	if _, err = reader.ReadByte(); err != io.EOF {
		return "", fmt.Errorf("research D1 has extra bytes or read error")
	}
	actual := hex.EncodeToString(hash.Sum(nil))
	if expectedHash == "" || actual != expectedHash {
		return "", fmt.Errorf("research D1 checksum mismatch")
	}
	for i, host := range hosts {
		want, err := analysis.EmpiricalQuantile(scores[i], Quantile)
		threshold, ok := thresholds[host]
		if err != nil || !ok || threshold != want {
			return "", fmt.Errorf("research threshold differs from D1 Q99 for %s", host)
		}
	}
	return actual, nil
}

type Counts struct {
	States    int            `json:"states"`
	Available int            `json:"available"`
	Anomalies int            `json:"anomalies"`
	Statuses  map[string]int `json:"statuses"`
}

func (c *Counts) add(r LocalResult) {
	if c.Statuses == nil {
		c.Statuses = map[string]int{}
	}
	c.States++
	c.Statuses[r.DiagnosticStatus]++
	if r.IsAvailable {
		c.Available++
	}
	if r.IsAnomaly != nil && *r.IsAnomaly {
		c.Anomalies++
	}
}

type Summary struct {
	Type           string             `json:"type"`
	Version        string             `json:"version"`
	Binding        Binding            `json:"binding"`
	ProfileSHA256  string             `json:"b1_profile_sha256"`
	Window         analysis.Window    `json:"window"`
	Audit          experiment.Audit   `json:"audit"`
	Counts         Counts             `json:"counts"`
	PerHost        map[string]*Counts `json:"per_host"`
	ElapsedSeconds float64            `json:"elapsed_seconds"`
}

// Run is an offline local inference pass, not a group detector.
func (m *Models) Run(ctx context.Context, profilePath string, window analysis.Window, visit func(LocalResult) error) (*Summary, error) {
	if m == nil || m.Source == nil || ctx == nil || !window.Valid() {
		return nil, fmt.Errorf("missing source/context or invalid inference window")
	}
	p, err := LoadProfile(profilePath)
	if err != nil {
		return nil, err
	}
	detector, err := NewDetector(p, m.Forest, m.Binding)
	if err != nil {
		return nil, err
	}
	hash, err := fileDigest(profilePath)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	result := &Summary{Type: "local_summary", Version: "local-b1-run-v1", Binding: cloneBinding(m.Binding), ProfileSHA256: hash, Window: window, PerHost: map[string]*Counts{}}
	result.Audit, err = m.Source.Scan(ctx, window, func(r domain.TelemetryRecord) error {
		local := detector.Evaluate(r)
		result.Counts.add(local)
		if result.PerHost[r.Hostname] == nil {
			result.PerHost[r.Hostname] = &Counts{}
		}
		result.PerHost[r.Hostname].add(local)
		if local.DiagnosticStatus == StatusScoringError {
			return fmt.Errorf("local scoring error: %s", local.Diagnostic)
		}
		if visit != nil {
			return visit(local)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	result.ElapsedSeconds = time.Since(start).Seconds()
	return result, nil
}
