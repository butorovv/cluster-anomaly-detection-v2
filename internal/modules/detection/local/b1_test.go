package local

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/experiment"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

func fixture(t *testing.T) (*Profile, *analysis.IsolationForest) {
	t.Helper()
	data := func(ctx context.Context, visit func(analysis.Observation) error) error {
		for i := 0; i < 2; i++ {
			if err := visit(analysis.Observation{Timestamp: analysis.NIRWindows()[0].Start.Add(time.Duration(i) * analysis.Step)}); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	stat, err := analysis.FitStatistical(context.Background(), data, analysis.NIRWindows()[0], analysis.StatisticalConfig{SVDRCond: 1e-12, CovarianceDDOF: 1})
	if err != nil {
		t.Fatal(err)
	}
	forest, err := analysis.FitIsolation(context.Background(), data, stat, analysis.NIRWindows()[0], analysis.IsolationConfig{Trees: 1, SampleSize: 2, MaxDepth: 0, Seed: 42})
	if err != nil {
		t.Fatal(err)
	}
	b := Binding{DatasetID: "synthetic-f18", PreparedSHA256: strings.Repeat("1", 64), StatisticalSHA256: strings.Repeat("2", 64), IsolationSHA256: strings.Repeat("3", 64), FeatureOrder: append([]string(nil), domain.FeatureNames[:]...), Hosts: []string{"a01n01", "a01n02", "a01n03"}, AssumedTimezone: "UTC"}
	p, err := NewProfile(b, map[string]float64{"a01n01": .5, "a01n02": .49}, map[string]int{"a01n01": 2, "a01n02": 2, "a01n03": 0})
	if err != nil {
		t.Fatal(err)
	}
	return p, forest
}

func record(host string) domain.TelemetryRecord {
	group, index, _ := topology.ParseHostname(host)
	return domain.TelemetryRecord{Timestamp: analysis.NIRWindows()[1].Start, Hostname: host, GroupID: group, NodeIndex: index, Valid: true, AvailabilityMask: domain.FullAvailabilityMask}
}

func TestIndividualThresholdStrictComparisonAndUnknownStates(t *testing.T) {
	p, f := fixture(t)
	d, err := NewDetector(p, f, p.Binding)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		host, status string
		available    bool
		anomaly      *bool
	}{
		{"a01n01", StatusNormal, true, new(false)},
		{"a01n02", StatusAnomaly, true, new(true)},
		{"a01n03", StatusMissingThreshold, false, nil},
		{"a02n01", StatusUnknownHost, false, nil},
	} {
		t.Run(tc.status, func(t *testing.T) {
			got := d.Evaluate(record(tc.host))
			if got.DiagnosticStatus != tc.status || got.IsAvailable != tc.available || !reflect.DeepEqual(got.IsAnomaly, tc.anomaly) {
				t.Fatalf("%+v", got)
			}
			if tc.available && (*got.AnomalyScore != .5 || *got.IndividualThreshold != p.Thresholds[tc.host]) {
				t.Fatal("wrong host threshold/score", got)
			}
			if !tc.available && got.IsAnomaly != nil {
				t.Fatal("unavailable observation became normal")
			}
		})
	}
	for _, value := range []float64{math.NaN(), math.Inf(1)} {
		r := record("a01n01")
		v := r.Features()
		v[0] = value
		r.SetFeatures(v)
		r.Valid = false
		r.AvailabilityMask ^= 1
		got := d.Evaluate(r)
		if got.DiagnosticStatus != StatusIncompleteF18 || got.IsAvailable || got.IsAnomaly != nil || got.AnomalyScore != nil {
			t.Fatal(got)
		}
		b, _ := json.Marshal(got)
		if !bytes.Contains(b, []byte(`"is_anomaly":null`)) {
			t.Fatal(string(b))
		}
	}
	for _, mutate := range []func(*domain.TelemetryRecord){
		func(r *domain.TelemetryRecord) { r.GroupID = "a02" },
		func(r *domain.TelemetryRecord) { r.Timestamp = time.Time{} },
		func(r *domain.TelemetryRecord) { r.Valid = false },
		func(r *domain.TelemetryRecord) { r.AvailabilityMask = 0 },
	} {
		r := record("a01n01")
		mutate(&r)
		if got := d.Evaluate(r); got.DiagnosticStatus != StatusInvalidObservation || got.IsAnomaly != nil {
			t.Fatal(got)
		}
	}
	if got := (*Detector)(nil).Evaluate(record("a01n01")); got.DiagnosticStatus != StatusScoringError {
		t.Fatal(got)
	}
}

func TestCorruptProfileAndBindingMismatches(t *testing.T) {
	for _, mutate := range []func(*Profile){
		func(p *Profile) { p.Version = "future" },
		func(p *Profile) { p.Quantile = .95 },
		func(p *Profile) { p.TrainingWindow = analysis.NIRWindows()[1] },
		func(p *Profile) { p.Comparison = "score >= threshold" },
		func(p *Profile) { p.HostCount++ },
		func(p *Profile) { p.Binding.FeatureOrder[0] = "wrong" },
		func(p *Profile) { p.Binding.PreparedSHA256 = "bad" },
		func(p *Profile) { p.Thresholds["a01n01"] = math.NaN() },
		func(p *Profile) { p.Thresholds["a01n01"] = 1.01 },
		func(p *Profile) { delete(p.Thresholds, "a01n01") },
		func(p *Profile) { p.Thresholds["a01n03"] = .5 },
		func(p *Profile) { p.Observations["foreign"] = 2 },
		func(p *Profile) { p.Binding.Hosts[1] = p.Binding.Hosts[0] },
	} {
		p, f := fixture(t)
		expected := cloneBinding(p.Binding)
		mutate(p)
		if _, err := NewDetector(p, f, expected); StatusOf(err) != StatusCorruptProfile {
			t.Fatal("accepted corrupt profile", err)
		}
	}
	for _, mutate := range []func(*Binding){
		func(b *Binding) { b.PreparedSHA256 = strings.Repeat("a", 64) },
		func(b *Binding) { b.StatisticalSHA256 = strings.Repeat("b", 64) },
		func(b *Binding) { b.IsolationSHA256 = strings.Repeat("c", 64) },
		func(b *Binding) { b.Hosts[0], b.Hosts[1] = b.Hosts[1], b.Hosts[0] },
		func(b *Binding) { b.AssumedTimezone = "Europe/Moscow" },
		func(b *Binding) { b.TimezoneVerified = true },
		func(b *Binding) { b.DatasetID = "other" },
	} {
		p, f := fixture(t)
		expected := cloneBinding(p.Binding)
		mutate(&expected)
		if _, err := NewDetector(p, f, expected); StatusOf(err) != StatusIncompatibleModel {
			t.Fatal("accepted mismatched binding", err)
		}
	}
	p, f := fixture(t)
	f.Trees[0].Size = 0
	if _, err := NewDetector(p, f, p.Binding); StatusOf(err) != StatusCorruptProfile {
		t.Fatal("accepted corrupt forest", err)
	}
}

func TestProfileSerializationDeterminismAndFrozenDetector(t *testing.T) {
	p, f := fixture(t)
	p.Provenance = map[string]string{"method": "test"}
	dir := t.TempDir()
	one, two := filepath.Join(dir, "one.json"), filepath.Join(dir, "two.json")
	for _, path := range []string{one, two} {
		if err := SaveProfile(path, p); err != nil {
			t.Fatal(err)
		}
	}
	a, _ := os.ReadFile(one)
	b, _ := os.ReadFile(two)
	if !bytes.Equal(a, b) {
		t.Fatal("profile bytes are not deterministic")
	}
	loaded, err := LoadProfile(one)
	if err != nil || !reflect.DeepEqual(loaded, p) {
		t.Fatal(loaded, err)
	}
	if err = SaveProfile(one, p); err == nil {
		t.Fatal("overwrote existing profile")
	}
	d, err := NewDetector(loaded, f, p.Binding)
	if err != nil {
		t.Fatal(err)
	}
	loaded.Thresholds["a01n01"] = .1
	loaded.Provenance["method"] = "changed"
	f.Scale[0] = math.NaN()
	got := d.Evaluate(record("a01n01"))
	if got.DiagnosticStatus != StatusNormal || *got.IndividualThreshold != .5 {
		t.Fatal("caller changed detector snapshot", got)
	}
	for _, data := range [][]byte{[]byte("{"), append(a, []byte("\n{}")...), bytes.Replace(a, []byte(`"version"`), []byte(`"unknown"`), 1)} {
		path := filepath.Join(t.TempDir(), "corrupt.json")
		os.WriteFile(path, data, 0600)
		if _, err = LoadProfile(path); StatusOf(err) != StatusCorruptProfile {
			t.Fatal("accepted invalid JSON", err)
		}
	}
}

func TestFitUsesD1OnlyAndPreservesMissingHost(t *testing.T) {
	p, f := fixture(t)
	rows := []domain.TelemetryRecord{}
	for i := 0; i < 4; i++ {
		r := record("a01n01")
		r.Timestamp = analysis.NIRWindows()[0].Start.Add(time.Duration(i) * analysis.Step)
		rows = append(rows, r)
	}
	missing := record("a01n02")
	missing.Timestamp = analysis.NIRWindows()[0].Start
	v := missing.Features()
	v[0] = math.NaN()
	missing.SetFeatures(v)
	missing.Valid = false
	missing.AvailabilityMask ^= 1
	rows = append(rows, missing)
	data := func(ctx context.Context, visit func(domain.TelemetryRecord) error) error {
		for _, r := range rows {
			if err := visit(r); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	a, err := FitProfile(context.Background(), p.Binding, f, data)
	if err != nil {
		t.Fatal(err)
	}
	b, err := FitProfile(context.Background(), p.Binding, f, data)
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("non-deterministic calibration", err)
	}
	if len(a.Thresholds) != 1 || a.Thresholds["a01n01"] != .5 || a.Observations["a01n01"] != 4 || a.Observations["a01n02"] != 0 {
		t.Fatal(a)
	}
	for _, when := range []time.Time{analysis.NIRWindows()[1].Start, analysis.NIRWindows()[2].Start, analysis.NIRWindows()[0].Start.Add(-analysis.Step), analysis.NIRWindows()[0].Start.Add(time.Second)} {
		rows[0].Timestamp = when
		if _, err = FitProfile(context.Background(), p.Binding, f, data); err == nil {
			t.Fatal("accepted non-D1 or off-grid state", when)
		}
	}
	rows = rows[1:3]
	rows[1].Timestamp = rows[0].Timestamp
	if _, err = FitProfile(context.Background(), p.Binding, f, data); err == nil {
		t.Fatal("accepted duplicate D1 host/time")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = FitProfile(ctx, p.Binding, f, data); err == nil {
		t.Fatal("ignored cancellation")
	}
}

func TestImportContextChecksScoresIdentityAndThresholds(t *testing.T) {
	p, f := fixture(t)
	p.Binding.Hosts = p.Binding.Hosts[:1]
	m := &Models{Source: &experiment.Source{}, Binding: p.Binding, Forest: f}
	dir := t.TempDir()
	bin := new(bytes.Buffer)
	w := analysis.NIRWindows()[0]
	for at := w.Start; at.Before(w.End); at = at.Add(analysis.Step) {
		var row [216]byte
		binary.LittleEndian.PutUint64(row[:8], uint64(at.UnixMicro()))
		binary.LittleEndian.PutUint32(row[12:16], domain.FullAvailabilityMask)
		binary.LittleEndian.PutUint64(row[160:168], math.Float64bits(.5))
		bin.Write(row[:])
	}
	os.WriteFile(filepath.Join(dir, "D1.bin"), bin.Bytes(), 0600)
	inputs := map[string]string{"data": p.Binding.PreparedSHA256, "stat": p.Binding.StatisticalSHA256, "if": p.Binding.IsolationSHA256}
	source := experiment.Source{SHA256: p.Binding.PreparedSHA256, Hosts: p.Binding.Hosts, AssumedTimezone: p.Binding.AssumedTimezone}
	meta := map[string]any{"source": source, "feature_order": p.Binding.FeatureOrder, "d1_audit": experiment.Audit{Window: w, States: 2880, Valid: 2880, Timestamps: 2880, Hosts: 1, MinimumStatesPerTimestamp: 1}, "input_sha256_before": inputs, "input_sha256_after": inputs, "binary_sha256": map[string]string{"D1.bin": digest(bin.Bytes())}}
	metaBytes, _ := json.Marshal(meta)
	os.WriteFile(filepath.Join(dir, "metadata.json"), metaBytes, 0600)
	writeThreshold := func(v float64) {
		b, _ := json.Marshal(map[string]any{"B1": map[string]float64{"a01n01": v}})
		os.WriteFile(filepath.Join(dir, "thresholds.json"), b, 0600)
	}
	writeThreshold(.5)
	imported, err := m.ImportContextProfile(dir)
	if err != nil || imported.Thresholds["a01n01"] != .5 || imported.Observations["a01n01"] != 2880 {
		t.Fatal(imported, err)
	}
	writeThreshold(.49)
	if _, err = m.ImportContextProfile(dir); StatusOf(err) != StatusCorruptProfile {
		t.Fatal("accepted threshold inconsistent with D1 score Q99", err)
	}
	writeThreshold(.5)
	inputs["if"] = strings.Repeat("e", 64)
	metaBytes, _ = json.Marshal(meta)
	os.WriteFile(filepath.Join(dir, "metadata.json"), metaBytes, 0600)
	if _, err = m.ImportContextProfile(dir); StatusOf(err) != StatusIncompatibleModel {
		t.Fatal("accepted wrong model identity", err)
	}
}
