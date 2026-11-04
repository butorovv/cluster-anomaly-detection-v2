// Package workerfixture provides a small frozen model bundle for worker tests.
package workerfixture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/experiment"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/local"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

// Fixture contains frozen models and one processor's default options.
type Fixture struct {
	Bundle  *group.Bundle
	Options group.GroupProcessorOptions
	Files   group.BundleFiles
	Root    string
}

// New creates a synthetic frozen bundle and an aligned D2 window in t.TempDir.
func New(t testing.TB, ticks int, delta int) Fixture {
	t.Helper()
	if ticks < 1 {
		t.Fatal("fixture requires at least one tick")
	}
	root := t.TempDir()
	write := func(name string, value any) string {
		t.Helper()
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		b, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(b, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	sha := func(path string) string {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:])
	}

	inputKey := "data/prepared/summit-D1-pilot/D1.parquet"
	manifestKey := "data/prepared/summit-D1-pilot/manifest.json"
	inputPath := filepath.Join(root, filepath.FromSlash(inputKey))
	if err := os.MkdirAll(filepath.Dir(inputPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inputPath, []byte("synthetic prepared source checksum fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	inputSHA := sha(inputPath)
	groups := make([]string, 16)
	hosts := make([]string, 0, 288)
	for gi := range groups {
		groups[gi] = fmt.Sprintf("a%02d", gi+1)
		for hi := 1; hi <= 18; hi++ {
			hosts = append(hosts, fmt.Sprintf("%sn%02d", groups[gi], hi))
		}
	}
	manifestPath := write(manifestKey, map[string]any{
		"schema_version": "F18-v1", "feature_order": domain.FeatureNames[:],
		"selected_hosts": hosts, "selected_groups": groups,
		"prepared_file_hashes": map[string]string{"D1.parquet": inputSHA},
		"assumed_timezone":     "UTC", "timezone_verified": false,
	})
	source := &experiment.Source{Path: inputPath, SHA256: inputSHA, Hosts: hosts, Groups: groups, AssumedTimezone: "UTC"}
	data := func(ctx context.Context, visit func(analysis.Observation) error) error {
		for i := 0; i < 2; i++ {
			var features analysis.Vector
			for j := range features {
				features[j] = float64(i)
			}
			if err := visit(analysis.Observation{Timestamp: analysis.NIRWindows()[0].Start, Features: features}); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	stat, err := analysis.FitStatistical(context.Background(), data, analysis.NIRWindows()[0], analysis.StatisticalConfig{SVDRCond: group.RCond, CovarianceDDOF: 1})
	if err != nil {
		t.Fatal(err)
	}
	statPath := write("results/statistical-20220118-8h-v1.json", experiment.StatisticalRun{Source: source, Profile: stat})
	forest, err := analysis.FitIsolation(context.Background(), data, stat, analysis.NIRWindows()[0], analysis.IsolationConfig{Trees: 1, SampleSize: 2, MaxDepth: 0, Seed: 42})
	if err != nil {
		t.Fatal(err)
	}
	audit := experiment.Audit{Window: analysis.NIRWindows()[0], States: 829440, Valid: 2, Timestamps: 2880, Hosts: 288, MinimumStatesPerTimestamp: 288}
	forestPath := write("results/isolation-20220118-v1.json", experiment.IsolationRun{
		Source: source, FeatureOrder: append([]string(nil), domain.FeatureNames[:]...), Profile: forest,
		StatisticalReference: experiment.StatisticalReference{Path: "original/stat.json", SHA256: sha(statPath), Version: stat.Version},
		TrainingAudit:        audit, D1: experiment.IsolationEvaluation{Audit: audit, Scores: experiment.IsolationScores{Count: 2}},
	})
	binding := local.Binding{DatasetID: "prepared-sha256:" + inputSHA, PreparedSHA256: inputSHA,
		StatisticalSHA256: sha(statPath), IsolationSHA256: sha(forestPath),
		FeatureOrder: append([]string(nil), domain.FeatureNames[:]...), Hosts: hosts, AssumedTimezone: "UTC"}
	counts, thresholds := map[string]int{}, map[string]float64{}
	for i, host := range hosts {
		counts[host], thresholds[host] = 2, .5
		if i%18 < 2 {
			thresholds[host] = .49
		}
	}
	lp, err := local.NewProfile(binding, thresholds, counts)
	if err != nil {
		t.Fatal(err)
	}
	b1Path := write("configs/local-b1.summit-20220118.json", lp)
	hwPath := write("configs/summit-hardware-reference.json", map[string]any{"approved_sensor_bounds": []group.Bound{}})
	configPath := write("configs/group-detection.summit-v1.json", group.Config{Version: group.Version, Floor: group.Floor, RCond: group.RCond, Dimensions: group.Dimensions, DDOF: 1, Quantile: .99, Delta: []int{0, 10, 30, 60}, Policy: "preliminary_rb"})
	files := group.BundleFiles{
		PreparedKey: inputKey, ManifestKey: manifestKey,
		Statistical: group.BundleFile{Path: statPath, InputKey: "results/statistical-20220118-8h-v1.json"},
		Isolation:   group.BundleFile{Path: forestPath, InputKey: "results/isolation-20220118-v1.json"},
		B1:          group.BundleFile{Path: b1Path, InputKey: "configs/local-b1.summit-20220118.json"},
		Hardware:    group.BundleFile{Path: hwPath, InputKey: "configs/summit-hardware-reference.json"},
		Config:      group.BundleFile{Path: configPath, InputKey: "configs/group-detection.summit-v1.json"},
	}
	profile := &group.Profile{Version: group.Version, Binding: binding, TrainingWindow: analysis.NIRWindows()[0], Floor: group.Floor, RCond: group.RCond, DDOF: 1,
		InputSHA: map[string]string{inputKey: inputSHA, manifestKey: sha(manifestPath)}}
	for _, file := range []group.BundleFile{files.Statistical, files.Isolation, files.B1, files.Hardware, files.Config} {
		profile.InputSHA[file.InputKey] = sha(file.Path)
	}
	baseline, err := HostChannels(Record("a01", 1, analysis.NIRWindows()[1].Start, 0))
	if err != nil {
		t.Fatal(err)
	}
	for gi, groupID := range groups {
		g := group.GroupProfile{Group: groupID, Hosts: append([]string(nil), hosts[gi*18:(gi+1)*18]...), Observations: 20, RThreshold: 1,
			Rank: group.Dimensions, BThreshold: floatPtr(1), Eigen: make([]float64, group.Dimensions)}
		for h := range g.HostMedian {
			g.HostMedian[h] = baseline
			for k := range g.HostScale[h] {
				g.HostScale[h][k] = group.Floor
			}
		}
		copy(g.Median[:], baseline[:])
		for k := range g.Scale {
			g.Scale[k], g.Eigen[k] = group.Floor, 1
			g.Covariance[k][k], g.Inverse[k][k] = 1, 1
		}
		profile.Groups = append(profile.Groups, g)
	}
	profilePath := write("configs/group-profile.summit-v1.json", profile)
	files.ProfilePath, files.ProfileSHA256 = profilePath, sha(profilePath)
	files = group.SummitRBI2Files(root, files.ProfileSHA256)
	bundle, err := group.LoadBundle(files)
	if err != nil {
		t.Fatal(err)
	}
	start := analysis.NIRWindows()[1].Start
	return Fixture{Bundle: bundle, Files: files, Root: root, Options: group.GroupProcessorOptions{
		GroupID: "a01", Groups: 16, Window: analysis.Window{Start: start, End: start.Add(time.Duration(ticks) * 10 * time.Second)}, DeltaSeconds: delta,
		Binding: domain.InputBinding{ExecutionID: "00000000-0000-4000-8000-000000000071", InputSHA256: inputSHA, ProfileSHA256: files.ProfileSHA256},
	}}
}

// Record returns a complete canonical F18 sample with the worker test baseline.
func Record(groupID string, node int, at time.Time, base float64) domain.TelemetryRecord {
	host := fmt.Sprintf("%sn%02d", groupID, node)
	g, n, _ := topology.ParseHostname(host)
	r := domain.TelemetryRecord{Timestamp: at, Hostname: host, GroupID: g, NodeIndex: n, Valid: true, AvailabilityMask: domain.FullAvailabilityMask}
	for i := range r.GPUPower {
		r.GPUPower[i] = base + float64(i+1)
	}
	for i := range r.CPUPower {
		r.CPUPower[i] = base + float64(i+1)
	}
	for i := range r.PSUPower {
		r.PSUPower[i] = base + float64(i+1)
	}
	for i := range r.GPUTemp {
		r.GPUTemp[i] = base + float64(30+i)
	}
	r.CPUTemp = [2]float64{base + 40, base + 50}
	return r
}

func HostChannels(r domain.TelemetryRecord) (group.HostVector, error) { return group.HostChannels(r) }
func floatPtr(v float64) *float64                                     { return &v }
