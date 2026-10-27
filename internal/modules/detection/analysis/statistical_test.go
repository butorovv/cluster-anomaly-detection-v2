package analysis

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
	"time"
)

func vectors(vs []Vector) Data {
	return func(ctx context.Context, visit func(Observation) error) error {
		for i, v := range vs {
			if err := visit(Observation{NIRWindows()[0].Start.Add(Step * time.Duration(i)), v}); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
}
func fit(t *testing.T, vs []Vector) *StatisticalProfile {
	t.Helper()
	p, err := FitStatistical(context.Background(), vectors(vs), NIRWindows()[0], StatisticalConfig{1e-12, 1})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func closeTo(t *testing.T, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Fatalf("got %.16g want %.16g", got, want)
	}
}

func TestRobustScaleSingularMahalanobisAndFrozenInference(t *testing.T) {
	vs := make([]Vector, 4)
	for i, x := range []float64{-1, -1, 1, 1} {
		vs[i][0], vs[i][1] = x, 2*x
		vs[i][2] = 7
	}
	p := fit(t, vs)
	closeTo(t, p.Median[0], 0, 0)
	closeTo(t, p.MAD[0], 1, 0)
	closeTo(t, p.Scale[0], 1.4826, 0)
	closeTo(t, p.Scale[2], 0.0001, 0)
	if p.Rank != 1 {
		t.Fatalf("singular rank=%d", p.Rank)
	}
	for _, v := range vs {
		d, err := p.DistanceSquared(v)
		if err != nil {
			t.Fatal(err)
		}
		closeTo(t, d, 0.75, 1e-12)
	}
	closeTo(t, p.Threshold, 0.75, 1e-12)
	before := *p
	future := vs[3]
	future[0], future[1] = 5, 10
	d, label, err := p.ControlLabel(future)
	if err != nil || !label {
		t.Fatalf("d=%g label=%t error=%v", d, label, err)
	}
	closeTo(t, d, 18.75, 1e-10)
	if !reflect.DeepEqual(before, *p) {
		t.Fatal("inference changed D1 profile")
	}
	// Verify the Moore-Penrose A A+ A = A identity for the dependent pair.
	for i := 0; i < 2; i++ {
		for j := 0; j < 2; j++ {
			var got float64
			for k := 0; k < 2; k++ {
				for l := 0; l < 2; l++ {
					got += p.Covariance[i][k] * p.PseudoInverse[k][l] * p.Covariance[l][j]
				}
			}
			closeTo(t, got, p.Covariance[i][j], 1e-12)
		}
	}
}

func TestFullRankAndConstantProfile(t *testing.T) {
	vs := make([]Vector, 2*Dimensions)
	for j := 0; j < Dimensions; j++ {
		vs[2*j][j] = 1
		vs[2*j+1][j] = -1
	}
	p := fit(t, vs)
	if p.Rank != 18 {
		t.Fatal(p.Rank)
	}
	d, err := p.DistanceSquared(vs[0])
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, d, 17.5, 1e-10)
	constant := fit(t, []Vector{{}, {}, {}})
	if constant.Rank != 0 || constant.Threshold != 0 {
		t.Fatal(constant.Rank, constant.Threshold)
	}
	if _, label, err := constant.ControlLabel(Vector{100}); err != nil || label {
		t.Fatal("rank-zero pseudoinverse must remain zero", err)
	}
}

func TestQuantileBoundariesAndInvalidTraining(t *testing.T) {
	x := []float64{9, 1, 2, 3}
	q, err := EmpiricalQuantile(x, 0.5)
	if err != nil || q != 2 || x[0] != 9 {
		t.Fatal(q, err, x)
	}
	q, _ = EmpiricalQuantile(x, 0.99)
	if q != 9 {
		t.Fatal(q)
	}
	for _, bad := range [][]float64{nil, {math.NaN()}, {math.Inf(1)}} {
		if _, err := EmpiricalQuantile(bad, 0.99); err == nil {
			t.Fatal("bad quantile accepted")
		}
	}
	windows := NIRWindows()
	for _, w := range windows[1:] {
		if _, err := FitStatistical(context.Background(), vectors([]Vector{{}, {}}), w, StatisticalConfig{1e-12, 1}); err == nil {
			t.Fatal("D2/D3 training accepted")
		}
	}
	for _, vs := range [][]Vector{nil, {{}}, {{math.NaN()}, {}}} {
		if _, err := FitStatistical(context.Background(), vectors(vs), NIRWindows()[0], StatisticalConfig{1e-12, 1}); err == nil {
			t.Fatal("bad states accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := FitStatistical(ctx, vectors([]Vector{{}, {}}), NIRWindows()[0], StatisticalConfig{1e-12, 1}); err == nil {
		t.Fatal("cancellation ignored")
	}
}

func TestHalfOpenWindows(t *testing.T) {
	w := NIRWindows()
	for i, v := range w {
		if !v.Contains(v.Start) || v.Contains(v.End) || v.Contains(v.Start.Add(-Step)) {
			t.Fatal(i, v)
		}
		if v.End.Sub(v.Start) != 8*60*60*1e9 {
			t.Fatal(v)
		}
	}
	if w[0].Contains(w[1].Start) || !w[1].Contains(w[0].End) || w[2].Contains(w[2].End) {
		t.Fatal("boundary leakage")
	}
}

func TestStatisticalSerializationAndStrictThreshold(t *testing.T) {
	p := fit(t, []Vector{{-1}, {1}})
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var restored StatisticalProfile
	if err = json.Unmarshal(b, &restored); err != nil {
		t.Fatal(err)
	}
	if err = restored.Validate(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*p, restored) {
		t.Fatal("JSON round trip changed profile")
	}
	v := Vector{3}
	d, err := restored.DistanceSquared(v)
	if err != nil {
		t.Fatal(err)
	}
	restored.Threshold = d
	if _, label, err := restored.ControlLabel(v); err != nil || label {
		t.Fatal("distance == threshold must not trigger")
	}
	restored.Scale[0] = 0
	if err := restored.Validate(); err == nil {
		t.Fatal("invalid restored scale accepted")
	}
}

func TestRejectCorruptFiniteProfileAndNumericalOverflow(t *testing.T) {
	p := fit(t, []Vector{{-1}, {1}})
	for name, corrupt := range map[string]func(*StatisticalProfile){
		"rank":                  func(p *StatisticalProfile) { p.Rank = 0 },
		"inverse":               func(p *StatisticalProfile) { p.PseudoInverse[0][0] *= 2 },
		"indefinite covariance": func(p *StatisticalProfile) { p.Covariance[0][0] *= -1 },
		"nonfinite mean":        func(p *StatisticalProfile) { p.Mean[0] = math.Inf(1) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := *p
			corrupt(&bad)
			if err := bad.Validate(); err == nil {
				t.Fatal("corrupt profile accepted")
			}
		})
	}
	constant := fit(t, []Vector{{}, {}})
	constant.PseudoInverse[0][0] = 1e-100
	if err := constant.Validate(); err == nil {
		t.Fatal("rank-zero profile accepted a nonzero inverse")
	}
	// Previously -Inf <= 1e-12*Inf clamped negative overflow to zero.
	bad := *p
	bad.PseudoInverse[0][0] = -math.MaxFloat64
	if _, err := bad.DistanceSquared(Vector{3}); err == nil {
		t.Fatal("negative overflow was accepted as zero distance")
	}
	for _, threshold := range []float64{math.NaN(), math.Inf(1), -1} {
		bad = *p
		bad.Threshold = threshold
		if _, label, err := bad.ControlLabel(Vector{3}); err == nil || label {
			t.Fatal("invalid threshold produced a control label")
		}
	}
	if _, err := FitStatistical(context.Background(), vectors([]Vector{{-math.MaxFloat64}, {math.MaxFloat64}}), NIRWindows()[0], StatisticalConfig{1e-12, 1}); err == nil {
		t.Fatal("overflowing scale was accepted as trained")
	}
}

func TestNearDependentCovarianceCutoff(t *testing.T) {
	// Check the cutoff after robust scaling, including a floored small MAD.
	vs := []Vector{{1, 1e-7}, {1, -1e-7}, {-1, 1e-7}, {-1, -1e-7}}
	p := fit(t, vs)
	// Robust scaling floors the second coordinate at 1e-4; normalized
	// variance ratio is about 2.2e-6, so both dimensions remain resolved.
	if p.Rank != 2 {
		t.Fatal("resolved small variance lost", p.Rank)
	}
	for i := range vs {
		vs[i][1] *= 1e-7
	}
	p = fit(t, vs)
	if p.Rank != 1 {
		t.Fatal("below-cutoff variance retained", p.Rank)
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestTrainingRejectsActualD2D3ObservationsOnEveryPass(t *testing.T) {
	windows := NIRWindows()
	for _, w := range windows[1:] {
		for pass := 1; pass <= 3; pass++ {
			calls := 0
			data := func(ctx context.Context, visit func(Observation) error) error {
				calls++
				ts := windows[0].Start
				if calls == pass {
					ts = w.Start
				}
				for _, v := range []Vector{{-1}, {1}} {
					if err := visit(Observation{ts, v}); err != nil {
						return err
					}
				}
				return nil
			}
			if _, err := FitStatistical(context.Background(), data, windows[0], StatisticalConfig{1e-12, 1}); err == nil {
				t.Fatalf("D2/D3 accepted in pass %d", pass)
			}
		}
	}
}

// This optional read-only check uses the pre-existing six real Summit snapshots.
// It validates numerical behavior on real values, not a full experimental D1 fit.
func TestSmallRealD1Snapshots(t *testing.T) {
	path := os.Getenv("NIR_REAL_SAMPLE")
	if path == "" {
		t.Skip("NIR_REAL_SAMPLE not set")
	}
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct {
		Timestamp time.Time `json:"timestamp"`
		Features  Vector    `json:"f18"`
	}
	if err = json.Unmarshal(bytes, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 6 {
		t.Fatal("expected existing six real snapshots")
	}
	data := func(ctx context.Context, visit func(Observation) error) error {
		for _, r := range rows {
			if err := visit(Observation{r.Timestamp, r.Features}); err != nil {
				return err
			}
		}
		return nil
	}
	p, err := FitStatistical(context.Background(), data, NIRWindows()[0], StatisticalConfig{1e-12, 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Validate(); err != nil {
		t.Fatal(err)
	}
	closeTo(t, p.Median[0], 707.5500183105469, 1e-9)
	closeTo(t, p.MAD[0], 53.649993896484375, 1e-9)
	if p.Rank != 5 {
		t.Fatal("six centered observations must have rank five here", p.Rank)
	}
	for _, r := range rows {
		d, err := p.DistanceSquared(r.Features)
		if err != nil {
			t.Fatal(err)
		}
		closeTo(t, d, 25.0/6, 1e-9)
	}
	t.Logf("six real snapshots: median PSU0=%g W; MAD=%g W; rank=%d; D2/D3 not used", p.Median[0], p.MAD[0], p.Rank)
}
