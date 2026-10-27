package analysis

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"
)

func isolationData(vs []Vector) Data { return vectors(vs) }

func isolationProfile(t *testing.T, vs []Vector) *StatisticalProfile {
	t.Helper()
	p := fit(t, vs)
	return p
}

func fitIsolationTest(t *testing.T, vs []Vector, cfg IsolationConfig) *IsolationForest {
	t.Helper()
	p := isolationProfile(t, vs)
	f, err := FitIsolation(context.Background(), isolationData(vs), p, NIRWindows()[0], cfg)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestIsolationPathCorrectionKnownValues(t *testing.T) {
	for n, want := range map[int]float64{0: 0, 1: 0, 2: 1, 3: 5.0 / 3, 4: 13.0 / 6, 256: 10.248689925634562} {
		closeTo(t, IsolationPathCorrection(n), want, 1e-12)
	}
}

func TestIsolationScoreHandBuiltLeafAndEqualityRoutesRight(t *testing.T) {
	p := isolationProfile(t, []Vector{{0}, {2}})
	base := &IsolationForest{Version: isolationForestVersion, TrainingWindow: NIRWindows()[0], Observations: 2, EffectiveSampleSize: 2,
		Config: IsolationConfig{Trees: 1, SampleSize: 2, MaxDepth: 2}, Median: p.Median, MAD: p.MAD, Scale: p.Scale}
	base.Trees = []*IsolationNode{{Size: 2, Feature: -1}}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	leafScore, err := base.Score(Vector{0})
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, leafScore, math.Exp2(-1), 1e-12)
	base.EffectiveSampleSize, base.Observations = 3, 3
	base.Config.SampleSize = 3
	base.Trees = []*IsolationNode{{Size: 3, Feature: 0, Split: 0,
		Left:  &IsolationNode{Size: 2, Feature: 0, Split: 0, Left: &IsolationNode{Size: 1, Feature: -1}, Right: &IsolationNode{Size: 1, Feature: -1}},
		Right: &IsolationNode{Size: 1, Feature: -1}}}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	left, err := base.Score(Vector{-1})
	if err != nil {
		t.Fatal(err)
	}
	equal, err := base.Score(Vector{p.Median[0]})
	if err != nil {
		t.Fatal(err)
	}
	right, err := base.Score(Vector{1})
	if err != nil {
		t.Fatal(err)
	}
	if left >= equal {
		t.Fatalf("left branch score %g should be lower than equality/right branch score %g", left, equal)
	}
	closeTo(t, equal, right, 1e-12) // normalized equality must route right
}

func TestIsolationConstantAndDepthZeroScore(t *testing.T) {
	constant := make([]Vector, 5)
	f := fitIsolationTest(t, constant, IsolationConfig{Trees: 5, SampleSize: 8, MaxDepth: 4, Seed: 3})
	s, err := f.Score(Vector{})
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, s, 0.5, 1e-12)
	depthZero := fitIsolationTest(t, []Vector{{-1}, {1}, {2}}, IsolationConfig{Trees: 3, SampleSize: 3, MaxDepth: 0, Seed: 2})
	s, err = depthZero.Score(Vector{100})
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, s, 0.5, 1e-12)
}

func TestIsolationTrainingBoundariesAndConfig(t *testing.T) {
	for _, bad := range []IsolationConfig{{0, 2, 1, 0}, {1, 1, 1, 0}, {1, 65537, 1, 0}, {1, 2, -1, 0}, {1, 2, 65, 0}, {10000, 1001, 1, 0}} {
		if err := bad.Validate(); err == nil {
			t.Fatalf("accepted config %+v", bad)
		}
	}
	for _, vs := range [][]Vector{nil, {{}}} {
		p := isolationProfile(t, []Vector{{-1}, {1}})
		if _, err := FitIsolation(context.Background(), isolationData(vs), p, NIRWindows()[0], IsolationConfig{Trees: 1, SampleSize: 2, MaxDepth: 1}); err == nil {
			t.Fatalf("accepted %d observations", len(vs))
		}
	}
	vs := []Vector{{-2}, {-1}, {1}, {2}}
	f := fitIsolationTest(t, vs, IsolationConfig{Trees: 3, SampleSize: 10, MaxDepth: 4, Seed: 7})
	if f.EffectiveSampleSize != len(vs) || f.Observations != len(vs) {
		t.Fatalf("effective=%d observations=%d", f.EffectiveSampleSize, f.Observations)
	}
}

func TestIsolationDuplicateAndConstantCoordinates(t *testing.T) {
	vs := []Vector{{-1, 8}, {-1, 8}, {1, 8}, {1, 8}, {0, 8}}
	f := fitIsolationTest(t, vs, IsolationConfig{Trees: 8, SampleSize: 5, MaxDepth: 5, Seed: 11})
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, v := range vs {
		if _, err := f.Score(v); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIsolationOutlierScoreOrdering(t *testing.T) {
	vs := make([]Vector, 100)
	for i := range vs {
		x := float64(i%10) / 10
		vs[i] = Vector{x, x / 2}
	}
	f := fitIsolationTest(t, vs, IsolationConfig{Trees: 100, SampleSize: 64, MaxDepth: 8, Seed: 42})
	inlier, err := f.Score(Vector{0.45, 0.225})
	if err != nil {
		t.Fatal(err)
	}
	outlier, err := f.Score(Vector{100, 50})
	if err != nil {
		t.Fatal(err)
	}
	if outlier <= inlier {
		t.Fatalf("outlier score %g should exceed inlier score %g", outlier, inlier)
	}
}

func TestIsolationDeterminismSerializationAndJSONRoundTrip(t *testing.T) {
	vs := make([]Vector, 30)
	for i := range vs {
		vs[i] = Vector{float64(i % 7), float64(i * i % 13)}
	}
	cfg := IsolationConfig{Trees: 12, SampleSize: 9, MaxDepth: 6, Seed: 123}
	a := fitIsolationTest(t, vs, cfg)
	b := fitIsolationTest(t, vs, cfg)
	ja, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	jb, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ja, jb) {
		t.Fatal("same seed/input did not produce byte-identical forests")
	}
	c := fitIsolationTest(t, vs, IsolationConfig{Trees: cfg.Trees, SampleSize: cfg.SampleSize, MaxDepth: cfg.MaxDepth, Seed: cfg.Seed + 1})
	jc, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(ja, jc) {
		t.Fatal("different seed produced identical forest")
	}
	var restored IsolationForest
	if err := json.Unmarshal(ja, &restored); err != nil {
		t.Fatal(err)
	}
	if err := restored.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, v := range []Vector{{0, 0}, {3, 5}, {100, -100}} {
		before := *a
		got, err := restored.Score(v)
		if err != nil {
			t.Fatal(err)
		}
		want, err := a.Score(v)
		if err != nil {
			t.Fatal(err)
		}
		closeTo(t, got, want, 0)
		if !reflect.DeepEqual(before, *a) {
			t.Fatal("scoring mutated forest")
		}
	}
}

func TestIsolationRejectsWrongWindowRogueTimestampAndBadInput(t *testing.T) {
	vs := []Vector{{-1}, {1}}
	p := isolationProfile(t, vs)
	cfg := IsolationConfig{Trees: 1, SampleSize: 2, MaxDepth: 2}
	if _, err := FitIsolation(context.Background(), isolationData(vs), p, NIRWindows()[1], cfg); err == nil {
		t.Fatal("D2 window accepted")
	}
	windows := NIRWindows()
	rogue := func(ctx context.Context, visit func(Observation) error) error {
		for i, v := range vs {
			ts := windows[0].Start.Add(Step * time.Duration(i))
			if i == 1 {
				ts = windows[1].Start
			}
			if err := visit(Observation{Timestamp: ts, Features: v}); err != nil {
				return err
			}
		}
		return nil
	}
	if _, err := FitIsolation(context.Background(), rogue, p, windows[0], cfg); err == nil {
		t.Fatal("rogue D2 timestamp accepted")
	}
	badData := isolationData([]Vector{{math.NaN()}, {1}})
	if _, err := FitIsolation(context.Background(), badData, p, windows[0], cfg); err == nil {
		t.Fatal("nonfinite observation accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := FitIsolation(ctx, isolationData(vs), p, windows[0], cfg); err == nil {
		t.Fatal("canceled context accepted")
	}
	badProfile := *p
	badProfile.Scale[0] = 0
	if _, err := FitIsolation(context.Background(), isolationData(vs), &badProfile, windows[0], cfg); err == nil {
		t.Fatal("invalid normalizer accepted")
	}
	overflow := *p
	overflow.Median[0] = -math.MaxFloat64
	if _, err := FitIsolation(context.Background(), isolationData([]Vector{{math.MaxFloat64}, {1}}), &overflow, windows[0], cfg); err == nil {
		t.Fatal("normalization overflow accepted")
	}
}

func TestIsolationValidateRejectsCorruptTrees(t *testing.T) {
	f := fitIsolationTest(t, []Vector{{-1}, {1}, {2}, {3}}, IsolationConfig{Trees: 1, SampleSize: 4, MaxDepth: 3, Seed: 1})
	valid := f.Trees[0]
	cases := map[string]func(*IsolationNode){
		"root count": func(n *IsolationNode) { n.Size++ },
		"depth": func(n *IsolationNode) {
			n.Left = &IsolationNode{Size: n.Size, Feature: 0, Split: 0, Left: &IsolationNode{Size: n.Size, Feature: 0, Split: 0, Left: &IsolationNode{Size: 1, Feature: -1}, Right: &IsolationNode{Size: n.Size - 1, Feature: -1}}, Right: &IsolationNode{Size: 1, Feature: -1}}
		},
		"missing child": func(n *IsolationNode) {
			n.Feature = 0
			n.Split = 0
			n.Left = &IsolationNode{Size: n.Size, Feature: -1}
			n.Right = nil
		},
		"nonfinite split": func(n *IsolationNode) {
			n.Feature = 0
			n.Split = math.NaN()
			n.Left = &IsolationNode{Size: 1, Feature: -1}
			n.Right = &IsolationNode{Size: n.Size - 1, Feature: -1}
		},
		"feature": func(n *IsolationNode) {
			n.Feature = Dimensions
			n.Split = 0
			n.Left = &IsolationNode{Size: 1, Feature: -1}
			n.Right = &IsolationNode{Size: n.Size - 1, Feature: -1}
		},
		"size conservation": func(n *IsolationNode) {
			n.Feature = 0
			n.Split = 0
			n.Left = &IsolationNode{Size: 1, Feature: -1}
			n.Right = &IsolationNode{Size: n.Size - 2, Feature: -1}
		},
		"cycle": func(n *IsolationNode) {
			n.Feature = 0
			n.Split = 0
			n.Left = n
			n.Right = &IsolationNode{Size: 1, Feature: -1}
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			clone, err := json.Marshal(valid)
			if err != nil {
				t.Fatal(err)
			}
			var root IsolationNode
			if err := json.Unmarshal(clone, &root); err != nil {
				t.Fatal(err)
			}
			corrupt(&root)
			f.Trees[0] = &root
			if err := f.Validate(); err == nil {
				t.Fatal("corrupt tree accepted")
			}
			f.Trees[0] = valid
		})
	}
}

func TestIsolationOnePassAndFrozenNormalizer(t *testing.T) {
	vs := make([]Vector, 128)
	for i := range vs {
		vs[i] = Vector{float64(i), float64(i % 11)}
	}
	p := isolationProfile(t, vs)
	before := *p
	passes, visits := 0, 0
	data := func(ctx context.Context, visit func(Observation) error) error {
		passes++
		return vectors(vs)(ctx, func(o Observation) error { visits++; return visit(o) })
	}
	f, err := FitIsolation(context.Background(), data, p, NIRWindows()[0], IsolationConfig{Trees: 10, SampleSize: 8, MaxDepth: 8, Seed: 42})
	if err != nil {
		t.Fatal(err)
	}
	if passes != 1 || visits != len(vs) || f.Observations != len(vs) || f.EffectiveSampleSize != 8 {
		t.Fatal("training must sample the entire D1 stream in exactly one pass", passes, visits)
	}
	if !reflect.DeepEqual(before, *p) {
		t.Fatal("IF training modified the statistical profile")
	}
	// Copying the normalizer freezes inference even if its caller later edits it.
	want, err := f.Score(Vector{100, 1})
	if err != nil {
		t.Fatal(err)
	}
	p.Median[0] = 10000
	got, err := f.Score(Vector{100, 1})
	if err != nil || got != want {
		t.Fatal("forest retained mutable normalizer", got, want, err)
	}
	for _, v := range []Vector{{math.NaN()}, {math.Inf(1)}} {
		if _, err := f.Score(v); err == nil {
			t.Fatal("nonfinite inference accepted")
		}
	}
	// A depth limit of zero cannot contain a split even when all sizes conserve.
	f.Config.MaxDepth = 0
	if err := f.Validate(); err == nil {
		t.Fatal("tree deeper than configured maximum accepted")
	}
}
