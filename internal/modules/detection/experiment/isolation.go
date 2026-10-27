package experiment

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"slices"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

type StatisticalReference struct {
	Path    string `json:"path"`
	SHA256  string `json:"file_sha256"`
	Version string `json:"version"`
}

type IsolationScores struct {
	Count  int     `json:"count"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	StdDev float64 `json:"population_stddev"`
	Q01    float64 `json:"q01"`
	Q50    float64 `json:"q50"`
	Q95    float64 `json:"q95"`
	Q99    float64 `json:"q99"`
}

type IsolationEvaluation struct {
	Audit          Audit           `json:"audit"`
	Scores         IsolationScores `json:"scores"`
	ElapsedSeconds float64         `json:"scan_and_score_seconds"`
	ScoreSeconds   float64         `json:"score_compute_seconds"`
}

type IsolationResources struct {
	TrainingSeconds      float64 `json:"training_scan_and_build_seconds"`
	ElapsedSeconds       float64 `json:"elapsed_seconds"`
	PeakSampledHeapBytes uint64  `json:"peak_sampled_go_heap_bytes"`
	PeakSampledSysBytes  uint64  `json:"peak_sampled_go_sys_bytes"`
	SampleIntervalMS     int     `json:"memory_sample_interval_ms"`
}

type IsolationRun struct {
	Source               *Source                   `json:"source"`
	FeatureOrder         []string                  `json:"feature_order"`
	StatisticalReference StatisticalReference      `json:"statistical_reference"`
	Profile              *analysis.IsolationForest `json:"isolation_forest"`
	TrainingAudit        Audit                     `json:"training_audit"`
	D1                   IsolationEvaluation       `json:"d1_diagnostics"`
	Resources            IsolationResources        `json:"resources"`
}

func sameIsolationSource(a, b *Source) bool {
	return a != nil && b != nil && a.SHA256 != "" && a.SHA256 == b.SHA256 && slices.Equal(a.Hosts, b.Hosts) && slices.Equal(a.Groups, b.Groups) && a.AssumedTimezone == b.AssumedTimezone && a.TimezoneVerified == b.TimezoneVerified
}

// Bind the existing Stage A envelope to this immutable dataset before reusing
// its D1 normalization. The file checksum identifies the exact saved profile.
func isolationNormalizer(path string, s *Source) (*analysis.StatisticalProfile, StatisticalReference, error) {
	var ref StatisticalReference
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, ref, err
	}
	var run StatisticalRun
	if err = json.Unmarshal(b, &run); err != nil {
		return nil, ref, err
	}
	if err = run.Profile.Validate(); err != nil {
		return nil, ref, err
	}
	if !sameIsolationSource(s, run.Source) {
		return nil, ref, fmt.Errorf("statistical profile source/topology mismatch")
	}
	h := sha256.Sum256(b)
	ref = StatisticalReference{path, hex.EncodeToString(h[:]), run.Profile.Version}
	return run.Profile, ref, nil
}

func completeIsolationD1(a Audit) error {
	if a.Window != analysis.NIRWindows()[0] || a.States != 829440 || a.Timestamps != 2880 || a.Hosts != 288 || a.MinimumStatesPerTimestamp != 288 {
		return fmt.Errorf("incomplete D1 node/time grid: %+v", a)
	}
	return nil
}

// LoadIsolationProfile validates the frozen forest envelope and D1 binding.
func LoadIsolationProfile(path string, s *Source, statisticalPath string) (*IsolationRun, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var run IsolationRun
	if err = json.Unmarshal(b, &run); err != nil {
		return nil, err
	}
	if err = run.Profile.Validate(); err != nil {
		return nil, err
	}
	if !sameIsolationSource(s, run.Source) || !slices.Equal(run.FeatureOrder, domain.FeatureNames[:]) {
		return nil, fmt.Errorf("isolation profile source/F18 mismatch")
	}
	p, ref, err := isolationNormalizer(statisticalPath, s)
	if err != nil {
		return nil, err
	}
	if ref.SHA256 != run.StatisticalReference.SHA256 || ref.Version != run.StatisticalReference.Version || !reflect.DeepEqual(p.Median, run.Profile.Median) || !reflect.DeepEqual(p.MAD, run.Profile.MAD) || !reflect.DeepEqual(p.Scale, run.Profile.Scale) || p.Observations != run.Profile.Observations {
		return nil, fmt.Errorf("isolation statistical reference/normalization mismatch")
	}
	if err = completeIsolationD1(run.TrainingAudit); err != nil {
		return nil, err
	}
	if run.TrainingAudit.Valid != run.Profile.Observations || run.TrainingAudit != run.D1.Audit || run.D1.Scores.Count != run.TrainingAudit.Valid {
		return nil, fmt.Errorf("isolation training/evaluation count mismatch")
	}
	return &run, nil
}
