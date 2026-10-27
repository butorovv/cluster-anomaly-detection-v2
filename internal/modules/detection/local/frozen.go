package local

import (
	"fmt"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/experiment"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

// LoadFrozenDetector validates immutable model files against an externally
// trusted binding without opening telemetry. It does not attest that a replay
// source has these bytes: validate that source separately with OpenSource.
// The returned Detector owns copies of the forest, thresholds and binding.
func LoadFrozenDetector(statisticalPath, isolationPath, profilePath string, expected Binding) (*Detector, error) {
	p, err := LoadProfile(profilePath)
	if err != nil {
		return nil, err
	}
	if !bindingEqual(p.Binding, expected) {
		return nil, fail(StatusIncompatibleModel, fmt.Errorf("frozen B1 binding mismatch"))
	}
	// A metadata-only descriptor is used only by the existing envelope validators;
	// it is never returned to a caller as a scan-capable Source.
	s := &experiment.Source{
		SHA256: expected.PreparedSHA256, Hosts: append([]string(nil), expected.Hosts...),
		AssumedTimezone: expected.AssumedTimezone, TimezoneVerified: expected.TimezoneVerified,
	}
	seen := map[string]bool{}
	for _, host := range s.Hosts {
		g, _, err := topology.ParseHostname(host)
		if err != nil {
			return nil, fail(StatusIncompatibleModel, err)
		}
		if !seen[g] {
			seen[g] = true
			s.Groups = append(s.Groups, g)
		}
	}
	m, err := openModelsForSource(s, statisticalPath, isolationPath)
	if err != nil {
		return nil, err
	}
	if !bindingEqual(m.Binding, expected) {
		return nil, fail(StatusIncompatibleModel, fmt.Errorf("frozen model bytes/binding mismatch"))
	}
	return NewDetector(p, m.Forest, expected)
}
