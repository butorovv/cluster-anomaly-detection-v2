package experiment

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
)

func streamReference[T any](root, name string, observe func(T)) (string, error) {
	f, err := os.Open(filepath.Join(root, name+".ndjson"))
	if err != nil {
		return "", err
	}
	defer f.Close()
	d := json.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	d.DisallowUnknownFields()
	h := sha256.New()
	for {
		var v T
		err := d.Decode(&v)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		observe(v)
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		h.Write(b)
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Recompute canonical digests from independently pinned V1 outputs, without
// invoking a V2 detector or trusting the counters in a saved run's report.
func referenceDigest(root string) (Comparison, group.RBI2Identity, error) {
	c := Comparison{Status: "PASS", CanonicalSHA256: map[string]string{}}
	var err error
	c.CanonicalSHA256["raw"], err = streamReference(root, "raw", func(v group.Raw) { c.Raw++ })
	if err != nil {
		return c, group.RBI2Identity{}, err
	}
	c.CanonicalSHA256["temporal"], err = streamReference(root, "temporal", func(v group.RBI2TemporalResult) {
		c.Temporal++
		if v.Vote == group.Positive {
			c.Positive++
		}
		if v.LocalI2.State == group.Positive {
			c.I2Positive++
		}
	})
	if err != nil {
		return c, group.RBI2Identity{}, err
	}
	c.CanonicalSHA256["evidence"], err = streamReference(root, "evidence", func(v group.RBI2Evidence) { c.Evidence++ })
	if err != nil {
		return c, group.RBI2Identity{}, err
	}
	events := []group.RBI2Event{}
	if _, err := streamReference(root, "events", func(v group.RBI2Event) { events = append(events, v) }); err != nil {
		return c, group.RBI2Identity{}, err
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].Group != events[j].Group {
			return events[i].Group < events[j].Group
		}
		return events[i].Detected.Before(events[j].Detected)
	})
	h := sha256.New()
	for _, e := range events {
		b, err := json.Marshal(e)
		if err != nil {
			return c, group.RBI2Identity{}, err
		}
		h.Write(b)
		h.Write([]byte{'\n'})
		c.Events++
		c.RBSubspans += len(e.RBSubspans)
		if e.NewDisjointFromRB {
			c.NewEvents++
		} else {
			c.RBParents++
		}
	}
	c.CanonicalSHA256["events"] = hex.EncodeToString(h.Sum(nil))
	b, err := os.ReadFile(filepath.Join(root, "summary.json"))
	if err != nil {
		return c, group.RBI2Identity{}, err
	}
	var s group.RBI2RunSummary
	if err := json.Unmarshal(b, &s); err != nil {
		return c, s.Identity, err
	}
	if c.Raw != s.Ticks || c.Positive != s.PositiveTicks || c.Events != s.Events || c.NewEvents != s.NewEvents || c.RBSubspans != s.RBIntervals || c.RBParents != s.RBSupportedEvents {
		return c, s.Identity, fmt.Errorf("reference counts disagree with pinned summary")
	}
	return c, s.Identity, nil
}

func AuditSaved(root, golden, manifest, output string) error {
	if root == "" || golden == "" || manifest == "" || output == "" {
		return fmt.Errorf("audit root, golden root, manifest and output required")
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		return fmt.Errorf("new audit output required")
	}
	type reference struct {
		Comparison Comparison
		Identity   group.RBI2Identity
	}
	refs := map[string]reference{}
	manifestBytes, err := os.ReadFile(manifest)
	if err != nil {
		return err
	}
	var declared []struct {
		Groups int `json:"groups"`
		Delta  int `json:"delta"`
	}
	if err := json.Unmarshal(manifestBytes, &declared); err != nil {
		return err
	}
	for _, entry := range declared {
		key := fmt.Sprintf("g%d-d%d", entry.Groups, entry.Delta)
		if _, exists := refs[key]; exists {
			return fmt.Errorf("duplicate reference manifest entry %s", key)
		}
		directory := filepath.Join(golden, key)
		if err := verifyReference(manifest, directory, entry.Groups, entry.Delta); err != nil {
			return err
		}
		comparison, identity, err := referenceDigest(directory)
		if err != nil {
			return err
		}
		refs[key] = reference{comparison, identity}
	}
	type row struct {
		Path         string `json:"path"`
		SHA          string `json:"sha256"`
		Name         string `json:"scenario"`
		Status       string `json:"status"`
		RunID        string `json:"run_id"`
		BinaryRehash string `json:"binary_rehash"`
	}
	rows := []row{}
	historicalFailures := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if d.Name() == "bin" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".json" {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var saved Result
		if json.Unmarshal(b, &saved) != nil || saved.Spec.Name == "" || saved.SchemaVersion != 1 {
			return nil
		}
		if saved.Status != "PASS" {
			historicalFailures++
			return nil
		}
		key := fmt.Sprintf("g%d-d%d", saved.Spec.Groups, saved.Spec.Delta)
		ref, ok := refs[key]
		if !ok {
			directory := filepath.Join(golden, key)
			if err := verifyReference(manifest, directory, saved.Spec.Groups, saved.Spec.Delta); err != nil {
				return err
			}
			c, id, err := referenceDigest(directory)
			if err != nil {
				return err
			}
			ref = reference{c, id}
			refs[key] = ref
		}
		if saved.RunID != ref.Identity.RunID || saved.ConfigID != ref.Identity.ConfigID || !reflect.DeepEqual(saved.Comparison, ref.Comparison) {
			return fmt.Errorf("saved parity differs from independently rehashed V1: %s", path)
		}
		hash, err := fileSHA(path)
		if err != nil {
			return err
		}
		binaryStatus := "NOT_VERIFIED_LEGACY_PATHS_NOT_RECORDED"
		if len(saved.BinaryPaths) > 0 {
			binaryStatus = "PASS"
			for name, path := range saved.BinaryPaths {
				actual, err := fileSHA(path)
				if err != nil || actual != saved.BinarySHA256[name] {
					return fmt.Errorf("saved binary differs: %s", path)
				}
			}
		}
		rows = append(rows, row{path, hash, saved.Spec.Name, "PASS", saved.RunID, binaryStatus})
		return nil
	})
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return fmt.Errorf("no saved independent comparisons")
	}
	manifestSHA, err := fileSHA(manifest)
	if err != nil {
		return err
	}
	return saveJSON(output, map[string]any{"schema_version": 1, "status": "PASS", "method": "rehashed pinned V1 source files and canonical typed streams compared to saved SQL comparator results; prior SQL databases are not retained, this is not a fresh distributed replay", "golden_manifest_sha256": manifestSHA, "verified_runs": len(rows), "historical_failures_preserved": historicalFailures, "references": refs, "runs": rows, "historical_input_lag": "NOT_VERIFIED: old sampler prefix defect; historical numeric zeros must not be used"})
}
