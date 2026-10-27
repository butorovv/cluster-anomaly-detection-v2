package analysis

import (
	"context"
	"testing"
)

func TestStatisticalAuditMedianMADAndDDOF(t *testing.T) {
	for _, tc := range []struct {
		xs                      []float64
		median, mad, sumSquares float64
	}{
		{[]float64{-5, 1, 9}, 1, 6, 296.0 / 3},
		{[]float64{-5, 1, 3, 9}, 2, 4, 100},
	} {
		vs := make([]Vector, len(tc.xs))
		for i, x := range tc.xs {
			vs[i][0] = x
		}
		for _, ddof := range []int{0, 1} {
			p, err := FitStatistical(context.Background(), vectors(vs), NIRWindows()[0], StatisticalConfig{1e-12, ddof})
			if err != nil {
				t.Fatal(err)
			}
			closeTo(t, p.Median[0], tc.median, 0)
			closeTo(t, p.MAD[0], tc.mad, 0)
			closeTo(t, p.Covariance[0][0], tc.sumSquares/float64(len(vs)-ddof)/(p.Scale[0]*p.Scale[0]), 1e-12)
		}
	}
}

func TestStatisticalAuditRejectsChangedCountsInEachLaterPass(t *testing.T) {
	for _, changePass := range []int{2, 3} {
		calls := 0
		data := func(ctx context.Context, visit func(Observation) error) error {
			calls++
			vs := []Vector{{-1}, {0}, {1}}
			if calls == changePass {
				vs = vs[:2]
			}
			return vectors(vs)(ctx, visit)
		}
		if _, err := FitStatistical(context.Background(), data, NIRWindows()[0], StatisticalConfig{1e-12, 1}); err == nil {
			t.Fatal("changed count accepted")
		}
	}
}

// Audit evidence for the immutable Data precondition, not an obligation to
// accept mutable sources: a future explicit drift rejection is also allowed.
func TestStatisticalAuditSameCountChangedValues(t *testing.T) {
	vs := []Vector{{-1}, {0}, {1}}
	want := fit(t, vs)
	calls := 0
	data := func(ctx context.Context, visit func(Observation) error) error {
		calls++
		if calls == 3 {
			return vectors([]Vector{{-1}, {0}, {3}})(ctx, visit)
		}
		return vectors(vs)(ctx, visit)
	}
	p, err := FitStatistical(context.Background(), data, NIRWindows()[0], StatisticalConfig{1e-12, 1})
	if err != nil {
		t.Logf("mutable Data rejected: %v", err)
		return
	}
	closeTo(t, p.Threshold, 9*want.Threshold, 1e-12)
	t.Log("same-count finite drift accepted; immutable Data remains a caller precondition")
}
