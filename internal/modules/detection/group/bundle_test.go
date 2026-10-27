package group

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/experiment"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/local"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

// A small synthetic model envelope, not a simulated real training experiment.
// The source bytes test SHA/manifest boundaries only; no test scans them.
func bundleFixture(t *testing.T) (BundleFiles, string, string) {
	t.Helper()
	dir := t.TempDir()
	write := func(name string, value any) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := SaveJSON(path, value); err != nil {
			t.Fatal(err)
		}
		return path
	}
	input := filepath.Join(dir, "D1.parquet")
	if err := os.WriteFile(input, []byte("synthetic immutable source checksum fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	source := &experiment.Source{Path: input, SHA256: mustSHA(t, input), AssumedTimezone: "UTC"}
	for g := 1; g <= 16; g++ {
		source.Groups = append(source.Groups, fmt.Sprintf("a%02d", g))
		for h := 1; h <= 18; h++ {
			source.Hosts = append(source.Hosts, fmt.Sprintf("a%02dn%02d", g, h))
		}
	}
	manifest := write("manifest.json", map[string]any{
		"schema_version": "F18-v1", "feature_order": domain.FeatureNames,
		"selected_hosts": source.Hosts, "selected_groups": source.Groups,
		"prepared_file_hashes": map[string]string{"D1.parquet": source.SHA256},
		"assumed_timezone":     "UTC", "timezone_verified": false,
	})
	data := func(ctx context.Context, visit func(analysis.Observation) error) error {
		for i := 0; i < 2; i++ {
			if err := visit(analysis.Observation{Timestamp: analysis.NIRWindows()[0].Start}); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	stat, err := analysis.FitStatistical(context.Background(), data, analysis.NIRWindows()[0], analysis.StatisticalConfig{SVDRCond: RCond, CovarianceDDOF: 1})
	if err != nil {
		t.Fatal(err)
	}
	statPath := write("stat.json", experiment.StatisticalRun{Source: source, Profile: stat})
	forest, err := analysis.FitIsolation(context.Background(), data, stat, analysis.NIRWindows()[0], analysis.IsolationConfig{Trees: 1, SampleSize: 2, MaxDepth: 0, Seed: 42})
	if err != nil {
		t.Fatal(err)
	}
	audit := experiment.Audit{Window: analysis.NIRWindows()[0], States: 829440, Valid: 2, Timestamps: 2880, Hosts: 288, MinimumStatesPerTimestamp: 288}
	forestPath := write("forest.json", experiment.IsolationRun{
		Source: source, FeatureOrder: append([]string(nil), domain.FeatureNames[:]...), Profile: forest,
		StatisticalReference: experiment.StatisticalReference{Path: "original/stat.json", SHA256: mustSHA(t, statPath), Version: stat.Version},
		TrainingAudit:        audit, D1: experiment.IsolationEvaluation{Audit: audit, Scores: experiment.IsolationScores{Count: 2}},
	})
	binding := local.Binding{
		DatasetID: "prepared-sha256:" + source.SHA256, PreparedSHA256: source.SHA256,
		StatisticalSHA256: mustSHA(t, statPath), IsolationSHA256: mustSHA(t, forestPath),
		FeatureOrder: append([]string(nil), domain.FeatureNames[:]...), Hosts: source.Hosts, AssumedTimezone: "UTC",
	}
	counts := map[string]int{}
	for _, host := range source.Hosts {
		counts[host] = 0
	}
	counts["a01n01"] = 2
	lp, err := local.NewProfile(binding, map[string]float64{"a01n01": .49}, counts)
	if err != nil {
		t.Fatal(err)
	}
	b1Path := write("b1.json", lp)
	hwPath := write("hardware.json", map[string]any{"approved_sensor_bounds": []Bound{}})
	configPath := write("config.json", Config{Version: Version, Floor: Floor, RCond: RCond, Dimensions: Dimensions, DDOF: 1, Quantile: .99, Delta: []int{0, 10, 30, 60}, Policy: "preliminary_rb"})
	files := BundleFiles{
		PreparedKey: "data/D1.parquet", ManifestKey: "data/manifest.json",
		Statistical: BundleFile{statPath, "models/stat.json"}, Isolation: BundleFile{forestPath, "models/forest.json"},
		B1: BundleFile{b1Path, "models/b1.json"}, Hardware: BundleFile{hwPath, "models/hardware.json"}, Config: BundleFile{configPath, "models/config.json"},
	}
	profile := &Profile{Version: Version, Binding: binding, TrainingWindow: analysis.NIRWindows()[0], Floor: Floor, RCond: RCond, DDOF: 1,
		InputSHA: map[string]string{files.PreparedKey: source.SHA256, files.ManifestKey: mustSHA(t, manifest)}}
	for _, f := range []BundleFile{files.Statistical, files.Isolation, files.B1, files.Hardware, files.Config} {
		profile.InputSHA[f.InputKey] = mustSHA(t, f.Path)
	}
	for g, group := range source.Groups {
		profile.Groups = append(profile.Groups, GroupProfile{Group: group, Hosts: source.Hosts[g*18 : (g+1)*18], BReason: "insufficient_d1"})
	}
	files.ProfilePath = write("group.json", profile)
	files.ProfileSHA256 = mustSHA(t, files.ProfilePath)
	return files, input, manifest
}

func mustSHA(t *testing.T, path string) string {
	t.Helper()
	hash, err := FileSHA(path)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func overwriteJSON(t *testing.T, path string, value any) {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestBundleLoadsWithoutSourceAndOwnsModels(t *testing.T) {
	files, input, manifest := bundleFixture(t)
	bundle, err := LoadBundle(files)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bundle.OpenSource(input, manifest); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(input); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(manifest); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadBundle(files)
	if err != nil {
		t.Fatalf("frozen loading accessed absent telemetry: %v", err)
	}
	w := analysis.NIRWindows()[1]
	wantID, _ := bundle.RunID(16, w)
	gotID, _ := loaded.RunID(16, w)
	if gotID != wantID {
		t.Fatal("identity changed without source scan")
	}
	r := validTestRecord()
	r.Timestamp = w.Start
	tick := Tick{Timestamp: r.Timestamp, Records: map[string]domain.TelemetryRecord{r.Hostname: r}}
	before, err := loaded.EvaluateGroup("a01", tick, nil)
	if err != nil || before.IF.Available != 1 || before.IF.Anomalous != 1 || before.IF.S == nil || *before.IF.S != 1 {
		t.Fatalf("loaded local model did not score: %+v / %v", before.IF, err)
	}
	// Mutation of source files and returned evidence cannot mutate private models.
	for _, path := range []string{files.ProfilePath, files.Statistical.Path, files.Isolation.Path, files.B1.Path, files.Hardware.Path, files.Config.Path} {
		if err := os.WriteFile(path, []byte("corrupted after loading"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	*before.Local[0].IndividualThreshold = .99
	after, err := loaded.EvaluateGroup("a01", tick, nil)
	if err != nil || after.Local[0].IsAnomaly == nil || !*after.Local[0].IsAnomaly || *after.Local[0].IndividualThreshold != .49 {
		t.Fatal("private model state leaked", after, err)
	}
	if _, err := loaded.EvaluateGroup("a17", tick, nil); err == nil {
		t.Fatal("unknown group accepted")
	}
}

func TestBundleRejectsChecksumAndLogicalKeyMismatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*BundleFiles)
	}{
		{"profile_sha", func(f *BundleFiles) { f.ProfileSHA256 = "" }},
		{"missing_key", func(f *BundleFiles) { f.B1.InputKey = "other" }},
		{"duplicate_key", func(f *BundleFiles) { f.Hardware.InputKey = f.B1.InputKey }},
		{"source_role", func(f *BundleFiles) { f.PreparedKey, f.ManifestKey = f.ManifestKey, f.PreparedKey }},
		{"wrong_stat_file", func(f *BundleFiles) { f.Statistical.Path = f.Hardware.Path }},
		{"wrong_b1_file", func(f *BundleFiles) { f.B1.Path = f.Config.Path }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files, _, _ := bundleFixture(t)
			tc.mutate(&files)
			if _, err := LoadBundle(files); err == nil {
				t.Fatal("incompatible bundle accepted")
			}
		})
	}
}

func TestBundleRejectsInvalidProfileAndSource(t *testing.T) {
	files, input, manifest := bundleFixture(t)
	loaded, err := LoadBundle(files)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, []byte("another source"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loaded.OpenSource(input, manifest); err == nil {
		t.Fatal("changed replay source accepted")
	}
	if err := os.WriteFile(manifest, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loaded.OpenSource(input, manifest); err == nil {
		t.Fatal("changed manifest accepted")
	}
	p, err := LoadProfile(files.ProfilePath)
	if err != nil {
		t.Fatal(err)
	}
	p.Groups[0].Hosts[0], p.Groups[0].Hosts[1] = p.Groups[0].Hosts[1], p.Groups[0].Hosts[0]
	overwriteJSON(t, files.ProfilePath, p)
	files.ProfileSHA256 = mustSHA(t, files.ProfilePath)
	if _, err := LoadBundle(files); err == nil {
		t.Fatal("invalid inventory accepted even with expected file hash")
	}
	for _, bundle := range []*Bundle{nil, {}} {
		if _, err := bundle.EvaluateGroup("a01", Tick{}, nil); err == nil {
			t.Fatal("empty bundle accepted")
		}
		if _, err := bundle.RunID(16, analysis.NIRWindows()[1]); err == nil {
			t.Fatal("empty identity accepted")
		}
	}
}

func TestFrozenDetectorRejectsBindingsBeyondChecksums(t *testing.T) {
	for _, field := range []string{"timezone", "feature_order", "forest_audit", "stat_reference", "b1_binding", "config_policy", "hardware_bounds"} {
		t.Run(field, func(t *testing.T) {
			files, _, _ := bundleFixture(t)
			p, err := LoadProfile(files.ProfilePath)
			if err != nil {
				t.Fatal(err)
			}
			switch field {
			case "timezone":
				p.Binding.AssumedTimezone = "Europe/Moscow"
			case "feature_order":
				p.Binding.FeatureOrder[0], p.Binding.FeatureOrder[1] = p.Binding.FeatureOrder[1], p.Binding.FeatureOrder[0]
			case "forest_audit", "stat_reference":
				var run experiment.IsolationRun
				data, _ := os.ReadFile(files.Isolation.Path)
				if err := json.Unmarshal(data, &run); err != nil {
					t.Fatal(err)
				}
				if field == "forest_audit" {
					run.TrainingAudit.States--
				} else {
					run.StatisticalReference.SHA256 = p.Binding.PreparedSHA256
				}
				overwriteJSON(t, files.Isolation.Path, run)
				p.Binding.IsolationSHA256 = mustSHA(t, files.Isolation.Path)
				// Rebind the B1 expectation too, so the envelope check is exercised.
				lp, _ := local.LoadProfile(files.B1.Path)
				lp.Binding.IsolationSHA256 = p.Binding.IsolationSHA256
				overwriteJSON(t, files.B1.Path, lp)
			case "b1_binding":
				lp, _ := local.LoadProfile(files.B1.Path)
				lp.Binding.TimezoneVerified = true
				overwriteJSON(t, files.B1.Path, lp)
			case "config_policy":
				config, err := LoadConfig(files.Config.Path)
				if err != nil {
					t.Fatal(err)
				}
				config.Policy = "unknown_means_negative"
				overwriteJSON(t, files.Config.Path, config)
			case "hardware_bounds":
				overwriteJSON(t, files.Hardware.Path, map[string]any{"approved_sensor_bounds": []Bound{{ID: "unverified"}}})
			}
			p.InputSHA[files.Isolation.InputKey] = mustSHA(t, files.Isolation.Path)
			p.InputSHA[files.B1.InputKey] = mustSHA(t, files.B1.Path)
			p.InputSHA[files.Config.InputKey] = mustSHA(t, files.Config.Path)
			p.InputSHA[files.Hardware.InputKey] = mustSHA(t, files.Hardware.Path)
			overwriteJSON(t, files.ProfilePath, p)
			files.ProfileSHA256 = mustSHA(t, files.ProfilePath)
			if _, err := LoadBundle(files); err == nil {
				t.Fatal("compatible bytes accepted incompatible model metadata")
			}
		})
	}
}

func TestBundlePhysicalRelocationPreservesIdentityAndInference(t *testing.T) {
	files, _, _ := bundleFixture(t)
	before, err := LoadBundle(files)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, path := range []*string{&files.ProfilePath, &files.Statistical.Path, &files.Isolation.Path, &files.B1.Path, &files.Hardware.Path, &files.Config.Path} {
		data, err := os.ReadFile(*path)
		if err != nil {
			t.Fatal(err)
		}
		*path = filepath.Join(dir, filepath.Base(*path))
		if err := os.WriteFile(*path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	after, err := LoadBundle(files)
	if err != nil {
		t.Fatal(err)
	}
	for _, groups := range []int{4, 8, 16} {
		want, _ := before.RunID(groups, analysis.NIRWindows()[1])
		got, _ := after.RunID(groups, analysis.NIRWindows()[1])
		if got != want {
			t.Fatal("physical paths changed mathematical identity")
		}
	}
	tick := Tick{Timestamp: analysis.NIRWindows()[1].Start}
	a, _ := before.EvaluateGroup("a01", tick, nil)
	b, _ := after.EvaluateGroup("a01", tick, nil)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("relocation changed inference")
	}
}
