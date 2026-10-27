package group

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

type RBI2Identity struct {
	Version       string            `json:"version"`
	ConfigID      string            `json:"config_id"`
	RunID         string            `json:"run_id"`
	LegacyRunID   string            `json:"legacy_run_id"`
	ProfileSHA256 string            `json:"profile_sha256"`
	InputSHA256   map[string]string `json:"input_sha256"`
	Groups        int               `json:"groups"`
	DeltaSeconds  int               `json:"delta_seconds"`
	Window        analysis.Window   `json:"window"`
}

func rbi2Hash(value any) string {
	b, _ := json.Marshal(value) // Only validated strings, integers, maps and times.
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// NewRBI2Identity binds frozen bytes and the complete mathematical policy. The
// legacy identity is retained separately; physical paths never enter either ID.
func NewRBI2Identity(profileSHA string, inputs map[string]string, groups int, window analysis.Window, delta int) (RBI2Identity, error) {
	legacy, err := RunID(profileSHA, inputs, groups, window)
	if err != nil {
		return RBI2Identity{}, err
	}
	if !window.Start.Equal(window.Start.Truncate(analysis.Step)) || !window.End.Equal(window.End.Truncate(analysis.Step)) || (delta != 0 && delta != 10 && delta != 30 && delta != 60) {
		return RBI2Identity{}, fmt.Errorf("R/B/I2 identity requires grid window and supported delta")
	}
	config := rbi2Hash([]any{RBI2Version, profileSHA, inputs, delta,
		"I2:18-known-unique-hosts;count>=2", "vote:p>=2;p+unknown<2;otherwise-unknown",
		"TTL:causal-inclusive;unknown-barrier;gap-reset", "pair:min-span;latest-upper",
		"events:half-open;keep-short;vote-segments;nested-legacy-preliminary-rb"})
	run := rbi2Hash([]any{RBI2Version, config, legacy, groups, window})
	return RBI2Identity{RBI2Version, config, run, legacy, profileSHA, maps.Clone(inputs), groups, delta, window}, nil
}

// RBI2EventID remains stable as an event grows or closes. It is deliberately in
// a different namespace from research candidate IDs and historical R/B IDs.
func RBI2EventID(runID, group string, detected string) string {
	return rbi2Hash([]string{RBI2Version, runID, group, detected})
}
