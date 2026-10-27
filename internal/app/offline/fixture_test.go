package offline

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/experiment"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/local"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/parquet-go/parquet-go"
)

type fixtureRow struct {
	Timestamp        int64    `parquet:"timestamp,timestamp(microsecond:utc)"`
	Hostname         string   `parquet:"hostname"`
	GroupID          string   `parquet:"group_id"`
	NodeIndex        int32    `parquet:"node_index,int(32)"`
	PSU0Power        *float64 `parquet:"psu0_power,optional"`
	PSU1Power        *float64 `parquet:"psu1_power,optional"`
	CPU0Power        *float64 `parquet:"cpu0_power,optional"`
	CPU1Power        *float64 `parquet:"cpu1_power,optional"`
	GPU0Power        *float64 `parquet:"gpu0_power,optional"`
	GPU1Power        *float64 `parquet:"gpu1_power,optional"`
	GPU2Power        *float64 `parquet:"gpu2_power,optional"`
	GPU3Power        *float64 `parquet:"gpu3_power,optional"`
	GPU4Power        *float64 `parquet:"gpu4_power,optional"`
	GPU5Power        *float64 `parquet:"gpu5_power,optional"`
	CPU0Temp         *float64 `parquet:"cpu0_mean_temp,optional"`
	CPU1Temp         *float64 `parquet:"cpu1_mean_temp,optional"`
	GPU0Temp         *float64 `parquet:"gpu0_core_temp,optional"`
	GPU1Temp         *float64 `parquet:"gpu1_core_temp,optional"`
	GPU2Temp         *float64 `parquet:"gpu2_core_temp,optional"`
	GPU3Temp         *float64 `parquet:"gpu3_core_temp,optional"`
	GPU4Temp         *float64 `parquet:"gpu4_core_temp,optional"`
	GPU5Temp         *float64 `parquet:"gpu5_core_temp,optional"`
	Valid            bool     `parquet:"is_valid"`
	AvailabilityMask uint32   `parquet:"availability_mask"`
	CPU0ValidCores   int32    `parquet:"cpu0_valid_cores,int(32)"`
	CPU1ValidCores   int32    `parquet:"cpu1_valid_cores,int(32)"`
}

// syntheticBundle makes deliberately tiny, frozen test models. It is never a
// production fallback and makes no statement about Summit model accuracy.
func syntheticBundle(t *testing.T) (string, string, analysis.Window) {
	t.Helper()
	root := t.TempDir()
	write := func(key string, value any) string {
		t.Helper()
		path := filepath.Join(root, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := group.SaveJSON(path, value); err != nil {
			t.Fatal(err)
		}
		return path
	}
	hash := func(path string) string {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("%x", sha256.Sum256(b))
	}
	w := analysis.Window{Start: analysis.NIRWindows()[1].Start, End: analysis.NIRWindows()[1].Start.Add(8 * analysis.Step)}
	const inputKey = "data/prepared/summit-D1-pilot/D1.parquet"
	input := filepath.Join(root, filepath.FromSlash(inputKey))
	if err := os.MkdirAll(filepath.Dir(input), 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(input)
	if err != nil {
		t.Fatal(err)
	}
	writer := parquet.NewGenericWriter[fixtureRow](f, parquet.KeyValueMetadata("schema_version", "F18-v1"))
	var rows []fixtureRow
	for _, tick := range []int{0, 1, 3, 4, 5, 6} {
		for _, g := range topology.CanonicalGroups() {
			if g.Code == "a03" {
				continue
			} // Entirely absent group, plus entirely absent ticks 2/7.
			for _, node := range g.Nodes {
				if tick == 4 && g.Code == "a02" && node.NodeIndex == 18 {
					continue
				}
				r := fixtureRow{Timestamp: w.Start.Add(time.Duration(tick) * analysis.Step).UnixMicro(), Hostname: node.Hostname, GroupID: g.Code, NodeIndex: int32(node.NodeIndex), Valid: true, AvailabilityMask: telemetry.FullAvailabilityMask, CPU0ValidCores: -1, CPU1ValidCores: -1}
				values := reflect.ValueOf(&r).Elem()
				for i := 0; i < 18; i++ {
					value := 0.
					if tick == 1 || tick == 3 || tick == 4 || tick == 6 {
						value = 10
					}
					values.Field(i + 4).Set(reflect.ValueOf(&value))
				}
				if tick == 5 && g.Code == "a01" && node.NodeIndex == 1 {
					r.PSU0Power = nil
					r.Valid = false
					r.AvailabilityMask &^= 1
				}
				rows = append(rows, r)
			}
		}
	}
	if _, err = writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	source := &experiment.Source{Path: input, SHA256: hash(input), AssumedTimezone: "UTC"}
	for _, g := range topology.CanonicalGroups() {
		source.Groups = append(source.Groups, g.Code)
		for _, node := range g.Nodes {
			source.Hosts = append(source.Hosts, node.Hostname)
		}
	}
	manifest := write("data/prepared/summit-D1-pilot/manifest.json", map[string]any{"feature_order": telemetry.FeatureNames, "selected_hosts": source.Hosts, "selected_groups": source.Groups, "prepared_file_hashes": map[string]string{"D1.parquet": source.SHA256}, "schema_version": "F18-v1", "assumed_timezone": "UTC", "timezone_verified": false})
	data := func(ctx context.Context, visit func(analysis.Observation) error) error {
		for i := 0; i < 2; i++ {
			if err := visit(analysis.Observation{Timestamp: analysis.NIRWindows()[0].Start.Add(time.Duration(i) * analysis.Step)}); err != nil {
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
	forestPath := write("results/isolation-20220118-v1.json", experiment.IsolationRun{Source: source, FeatureOrder: append([]string(nil), telemetry.FeatureNames[:]...), Profile: forest, StatisticalReference: experiment.StatisticalReference{Path: "synthetic/stat.json", SHA256: hash(statPath), Version: stat.Version}, TrainingAudit: audit, D1: experiment.IsolationEvaluation{Audit: audit, Scores: experiment.IsolationScores{Count: 2}}})
	binding := local.Binding{DatasetID: "prepared-sha256:" + source.SHA256, PreparedSHA256: source.SHA256, StatisticalSHA256: hash(statPath), IsolationSHA256: hash(forestPath), FeatureOrder: append([]string(nil), telemetry.FeatureNames[:]...), Hosts: source.Hosts, AssumedTimezone: "UTC"}
	thresholds, counts := map[string]float64{}, map[string]int{}
	for i, h := range source.Hosts {
		thresholds[h] = .5
		if i%18 < 2 {
			thresholds[h] = .49
		}
		counts[h] = 2
	}
	lp, err := local.NewProfile(binding, thresholds, counts)
	if err != nil {
		t.Fatal(err)
	}
	b1 := write("configs/local-b1.summit-20220118.json", lp)
	hw := write("configs/summit-hardware-reference.json", map[string]any{"approved_sensor_bounds": []group.Bound{}})
	cfg := write("configs/group-detection.summit-v1.json", group.Config{Version: group.Version, Floor: group.Floor, RCond: group.RCond, Dimensions: group.Dimensions, DDOF: 1, Quantile: .99, Delta: []int{0, 10, 30, 60}, Policy: "preliminary_rb"})
	profile := &group.Profile{Version: group.Version, Binding: binding, TrainingWindow: analysis.NIRWindows()[0], Floor: group.Floor, RCond: group.RCond, DDOF: 1, InputSHA: map[string]string{inputKey: source.SHA256, "data/prepared/summit-D1-pilot/manifest.json": hash(manifest)}}
	for _, path := range []string{statPath, forestPath, b1, hw, cfg} {
		key, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		profile.InputSHA[filepath.ToSlash(key)] = hash(path)
	}
	for i, g := range source.Groups {
		threshold := 1.
		p := group.GroupProfile{Group: g, Hosts: source.Hosts[i*18 : (i+1)*18], Observations: 20, RThreshold: 1, BThreshold: &threshold, Rank: 10, Eigen: make([]float64, 10)}
		for h := range p.HostScale {
			for k := range p.HostScale[h] {
				p.HostScale[h][k] = group.Floor
			}
		}
		for k := range p.Scale {
			p.Scale[k] = group.Floor
			p.Inverse[k][k] = 1
			p.Covariance[k][k] = 1
			p.Eigen[k] = 1
		}
		profile.Groups = append(profile.Groups, p)
	}
	if err = profile.Validate(); err != nil {
		t.Fatal(err)
	}
	profilePath := write("configs/group-profile.summit-v1.json", profile)
	return root, hash(profilePath), w
}
