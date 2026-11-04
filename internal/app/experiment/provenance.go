package experiment

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

func fileSHA(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// A saved PASS describes its original execution, never a new run of current code.
// Bind reuse to global inputs as well as RunSpec, and retain the exact binaries.
func configurationHash(cfg Config) (string, error) {
	for path, want := range map[string]string{
		filepath.Join(cfg.Assets, "data/prepared/summit-D1-pilot/D1.parquet"): cfg.InputSHA,
		filepath.Join(cfg.Assets, "configs/group-profile.summit-v1.json"):     cfg.ProfileSHA,
	} {
		got, err := fileSHA(path)
		if err != nil {
			return "", err
		}
		if got != want {
			return "", fmt.Errorf("configured input/profile SHA mismatch: %s", path)
		}
	}
	manifest, err := fileSHA(cfg.GoldenManifest)
	if err != nil {
		return "", err
	}
	value := struct {
		Assets, Brokers, Profile, Input, GoldenManifest string
		Measure                                         bool
	}{filepath.Clean(cfg.Assets), cfg.Brokers, cfg.ProfileSHA, cfg.InputSHA, manifest, cfg.Measure}
	b, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return domain.Hash(b), nil
}

func reusable(saved Result, cfg Config, provenance string) error {
	if saved.Provenance == "" {
		return fmt.Errorf("legacy PASS lacks configuration provenance; audit saved artifacts separately, do not silently replay")
	}
	if saved.Provenance != provenance {
		return fmt.Errorf("global input/golden/measurement configuration differs")
	}
	reference := filepath.Join(cfg.GoldenRoot, fmt.Sprintf("g%d-d%d", saved.Spec.Groups, saved.Spec.Delta))
	if err := verifyReference(cfg.GoldenManifest, reference, saved.Spec.Groups, saved.Spec.Delta); err != nil {
		return err
	}
	b, err := os.ReadFile(filepath.Join(reference, "summary.json"))
	if err != nil {
		return err
	}
	var summary group.RBI2RunSummary
	if err := json.Unmarshal(b, &summary); err != nil {
		return err
	}
	if summary.Identity.InputSHA256["data/prepared/summit-D1-pilot/D1.parquet"] != cfg.InputSHA {
		return fmt.Errorf("reference input identity differs")
	}
	id, err := group.NewRBI2Identity(cfg.ProfileSHA, summary.Identity.InputSHA256, saved.Spec.Groups, analysis.NIRWindows()[1], saved.Spec.Delta)
	if err != nil {
		return err
	}
	if saved.RunID != id.RunID || saved.ConfigID != id.ConfigID {
		return fmt.Errorf("saved scientific identity differs")
	}
	for _, name := range []string{"worker", "master", "replayer"} {
		actual, err := fileSHA(saved.BinaryPaths[name])
		if err != nil {
			return fmt.Errorf("saved %s binary unavailable: %w", name, err)
		}
		if actual != saved.BinarySHA256[name] {
			return fmt.Errorf("saved %s binary SHA mismatch", name)
		}
	}
	return nil
}
