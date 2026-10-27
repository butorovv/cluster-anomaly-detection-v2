package experiment

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

type StatisticalEvaluation struct {
	Audit           Audit   `json:"audit"`
	ControlPositive int     `json:"control_positive"`
	DistanceMin     float64 `json:"distance_squared_min"`
	DistanceMax     float64 `json:"distance_squared_max"`
	DistanceMean    float64 `json:"distance_squared_mean"`
}

type StatisticalRun struct {
	Source  *Source                      `json:"source"`
	Profile *analysis.StatisticalProfile `json:"statistical_profile"`
	Windows [3]StatisticalEvaluation     `json:"windows"`
}

// LoadProfile reads the profile from the existing Stage A result envelope and
// validates its numerical parameters before allowing inference.
func LoadProfile(path string) (*analysis.StatisticalProfile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var run StatisticalRun
	if err := json.Unmarshal(b, &run); err != nil {
		return nil, err
	}
	if err := run.Profile.Validate(); err != nil {
		return nil, fmt.Errorf("load statistical profile: %w", err)
	}
	return run.Profile, nil
}

func SaveNew(path string, v any) error {
	bytes, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(append(bytes, '\n'))
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
