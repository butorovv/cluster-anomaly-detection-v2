package experiment

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
)

func TestSavedAuditRehashesStreamsAndRejectsChangedReports(t *testing.T) {
	root := t.TempDir()
	golden := filepath.Join(root, "golden")
	reference := filepath.Join(golden, "g4-d0")
	runs := filepath.Join(root, "runs")
	for _, path := range []string{reference, runs} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	id, err := group.NewRBI2Identity(strings.Repeat("a", 64), map[string]string{"input": strings.Repeat("b", 64)}, 4, analysis.NIRWindows()[1], 0)
	if err != nil {
		t.Fatal(err)
	}
	write := func(path string, value any) {
		t.Helper()
		b, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(b, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(reference, "raw.ndjson"), group.Raw{Group: "a01", Timestamp: id.Window.Start})
	write(filepath.Join(reference, "temporal.ndjson"), group.RBI2TemporalResult{Group: "a01", Timestamp: id.Window.Start, Vote: group.Unknown})
	for _, name := range []string{"evidence.ndjson", "events.ndjson"} {
		if err := os.WriteFile(filepath.Join(reference, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(reference, "summary.json"), group.RBI2RunSummary{Identity: id, Ticks: 1})
	files := []map[string]string{}
	for _, name := range []string{"raw.ndjson", "temporal.ndjson", "evidence.ndjson", "events.ndjson", "summary.json"} {
		path := filepath.Join(reference, name)
		hash, err := fileSHA(path)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, map[string]string{"path": path, "sha256": hash})
	}
	manifest := filepath.Join(root, "manifest.json")
	write(manifest, []any{map[string]any{"groups": 4, "delta": 0, "source_commit": "c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc", "files": files}})
	comparison, _, err := referenceDigest(reference)
	if err != nil {
		t.Fatal(err)
	}
	saved := Result{SchemaVersion: 1, Status: "PASS", Spec: RunSpec{Name: "scenario", Groups: 4}, RunID: id.RunID, ConfigID: id.ConfigID, Comparison: comparison}
	path := filepath.Join(runs, "scenario.json")
	write(path, saved)
	if err := AuditSaved(runs, golden, manifest, filepath.Join(root, "audit.json")); err != nil {
		t.Fatal(err)
	}
	saved.Comparison.Raw++
	write(path, saved)
	if err := AuditSaved(runs, golden, manifest, filepath.Join(root, "bad-report.json")); err == nil {
		t.Fatal("altered saved counters accepted")
	}
	saved.Comparison.Raw--
	write(path, saved)
	if err := os.WriteFile(filepath.Join(reference, "raw.ndjson"), []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := AuditSaved(runs, golden, manifest, filepath.Join(root, "bad-reference.json")); err == nil {
		t.Fatal("altered golden source accepted")
	}
}
