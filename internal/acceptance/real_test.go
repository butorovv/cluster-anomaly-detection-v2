package acceptance

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Explicit real acceptance fails closed; ordinary go test stays self-contained.
// Existing mathematical comparators are executed without a second detector.
func TestRealAcceptance(t *testing.T) {
	if os.Getenv("P03_REAL_ACCEPTANCE") != "1" {
		t.Skip("opt-in: P03_REAL_ACCEPTANCE=1 requires every real acceptance prerequisite")
	}
	required := []string{"OFFLINE_REAL_ROOT", "OFFLINE_V1_SOURCE", "GROUP_REAL_ROOT", "GROUP_REAL_REFERENCE", "GROUP_RBI2_RESEARCH", "P03_REAL_ROOT"}
	missing := []string{}
	for _, key := range required {
		if os.Getenv(key) == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("real acceptance missing environment: %s", strings.Join(missing, ", "))
	}
	root := os.Getenv("OFFLINE_REAL_ROOT")
	if filepath.Clean(root) != filepath.Clean(os.Getenv("GROUP_REAL_ROOT")) || filepath.Clean(root) != filepath.Clean(os.Getenv("P03_REAL_ROOT")) {
		t.Fatal("real comparators must use the same assets root")
	}
	files := map[string]string{
		"data/prepared/summit-D1-pilot/D1.parquet":    "cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7",
		"data/prepared/summit-D1-pilot/manifest.json": "6d603a5445d1a76be24408eed84933e96a4a10af6c3bfd32471d93ca7dd9f291",
		"results/statistical-20220118-8h-v1.json":     "36da27a4a4f485baa4b1b1b899c26a36efc15401327fd85abc3d63ea3c1ade80",
		"results/isolation-20220118-v1.json":          "6aa1ca4f2f76b56418aafaa1ea25e6812a3b9fdcc0d309aeed44dc1db03f2da3",
		"configs/group-profile.summit-v1.json":        "a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36",
	}
	for name, want := range files {
		if err := verifySHA(filepath.Join(root, name), want); err != nil {
			missing = append(missing, err.Error())
		}
	}
	for _, path := range []string{filepath.Join(os.Getenv("GROUP_REAL_REFERENCE"), "raw.ndjson"), filepath.Join(os.Getenv("GROUP_RBI2_RESEARCH"), "journal-ticks.csv"), filepath.Join(os.Getenv("GROUP_RBI2_RESEARCH"), "event-tick-evidence.csv"), filepath.Join(os.Getenv("GROUP_RBI2_RESEARCH"), "two-level-journal.ndjson")} {
		if _, err := os.Stat(path); err != nil {
			missing = append(missing, path)
		}
	}
	// Remaining frozen inputs are anchored by the original group profile, not by
	// any newly computed expectation. All are checked before expensive inference.
	b, err := os.ReadFile(filepath.Join(root, "configs/group-profile.summit-v1.json"))
	if err == nil {
		var profile struct {
			InputSHA map[string]string `json:"input_sha256"`
		}
		if err = json.Unmarshal(b, &profile); err != nil {
			missing = append(missing, err.Error())
		} else {
			if len(profile.InputSHA) == 0 {
				missing = append(missing, "profile input_sha256 absent")
			}
			for name, want := range profile.InputSHA {
				if err := verifySHA(filepath.Join(root, name), want); err != nil {
					missing = append(missing, err.Error())
				}
			}
		}
	}
	if len(missing) > 0 {
		t.Fatalf("real acceptance missing/mismatched prerequisites:\n%s", strings.Join(missing, "\n"))
	}
	for _, tc := range []struct{ pkg, pattern string }{
		{"./internal/app/offline", "^TestRealD2AllDeltaCanonicalParity$"},
		{"./internal/modules/detection/group", "^TestRealRBI2D2Parity$"},
		{"./internal/app/replayer", "^TestRealD2InputPipeline$"},
	} {
		t.Run(tc.pattern, func(t *testing.T) {
			cmd := exec.Command("go", "test", tc.pkg, "-run", tc.pattern, "-count=1", "-timeout=30m", "-v")
			cmd.Dir = "../.."
			output, err := cmd.CombinedOutput()
			t.Log(string(output))
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func verifySHA(path, want string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("missing %s", path)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	if fmt.Sprintf("%x", h.Sum(nil)) != want {
		return fmt.Errorf("SHA256 mismatch %s", path)
	}
	return nil
}
