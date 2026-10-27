package analysis

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"gonum.org/v1/gonum/mat"
)

const Dimensions = 18

type Vector = [Dimensions]float64
type Matrix = [Dimensions][Dimensions]float64

type Observation struct {
	Timestamp time.Time
	Features  Vector
}

// Data reopens the same immutable D1 selection on every call. Only complete,
// finite states are passed to visit. FitStatistical also enforces D1 timestamps.
type Data func(context.Context, func(Observation) error) error

type StatisticalConfig struct {
	// Engineering conventions, not numerical parameters specified by the RPZ.
	SVDRCond       float64 `json:"svd_relative_cutoff"`
	CovarianceDDOF int     `json:"covariance_ddof"`
}

type StatisticalProfile struct {
	Version        string            `json:"version"`
	TrainingWindow Window            `json:"training_window"`
	Observations   int               `json:"observations"`
	Median         Vector            `json:"median"`
	MAD            Vector            `json:"mad"`
	Scale          Vector            `json:"effective_scale"`
	Mean           Vector            `json:"normalized_mean"`
	Covariance     Matrix            `json:"covariance"`
	PseudoInverse  Matrix            `json:"pseudoinverse"`
	Rank           int               `json:"covariance_rank"`
	Threshold      float64           `json:"mahalanobis_squared_threshold_q99"`
	Config         StatisticalConfig `json:"numerical_conventions"`
}

func Finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
func FiniteVector(v Vector) bool {
	for _, x := range v {
		if !Finite(x) {
			return false
		}
	}
	return true
}

// EmpiricalQuantile implements RPZ (13): inverse empirical CDF, no interpolation.
// The caller's values are never reordered.
func EmpiricalQuantile(values []float64, q float64) (float64, error) {
	if len(values) == 0 || !Finite(q) || q <= 0 || q > 1 {
		return 0, fmt.Errorf("quantile requires observations and 0 < q <= 1")
	}
	s := append([]float64(nil), values...)
	for _, v := range s {
		if !Finite(v) {
			return 0, fmt.Errorf("nonfinite quantile observation")
		}
	}
	sort.Float64s(s)
	return s[int(math.Ceil(q*float64(len(s))))-1], nil
}

func medianSorted(s []float64) float64 {
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return s[n/2-1]/2 + s[n/2]/2
}

// FitStatistical uses RPZ (8)-(14), with three streaming passes over D1.
// TrainingWindow is fixed by the approved experiment; adapters must reject any
// observation outside that window. D2/D3 cannot be supplied as a training window.
func FitStatistical(ctx context.Context, data Data, window Window, cfg StatisticalConfig) (*StatisticalProfile, error) {
	if window != NIRWindows()[0] {
		return nil, fmt.Errorf("statistical training must use the fixed D1 window")
	}
	if data == nil || !Finite(cfg.SVDRCond) || cfg.SVDRCond <= 0 || cfg.SVDRCond >= 1 || (cfg.CovarianceDDOF != 0 && cfg.CovarianceDDOF != 1) {
		return nil, fmt.Errorf("invalid data or numerical conventions")
	}
	p := &StatisticalProfile{Version: "statistical-v1", TrainingWindow: window, Config: cfg}
	var cols [Dimensions][]float64
	if err := data(ctx, func(o Observation) error {
		if !window.Contains(o.Timestamp) {
			return fmt.Errorf("non-D1 observation in statistical training")
		}
		v := o.Features
		if !FiniteVector(v) {
			return fmt.Errorf("training requires complete finite F18")
		}
		for j, x := range v {
			cols[j] = append(cols[j], x)
		}
		p.Observations++
		return ctx.Err()
	}); err != nil {
		return nil, err
	}
	if p.Observations <= cfg.CovarianceDDOF {
		return nil, fmt.Errorf("insufficient D1 states")
	}
	for j := range cols {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		sort.Float64s(cols[j])
		p.Median[j] = medianSorted(cols[j])
		for i, v := range cols[j] {
			cols[j][i] = math.Abs(v - p.Median[j])
		}
		sort.Float64s(cols[j])
		p.MAD[j] = medianSorted(cols[j])
		p.Scale[j] = math.Max(1.4826*p.MAD[j], 0.0001)
		cols[j] = nil
	}
	// Welford's centered update avoids subtracting large raw second moments.
	n := 0
	if err := data(ctx, func(o Observation) error {
		if !window.Contains(o.Timestamp) {
			return fmt.Errorf("non-D1 observation in covariance training")
		}
		v := o.Features
		z, err := p.Normalize(v)
		if err != nil {
			return err
		}
		n++
		var delta Vector
		for j := range z {
			delta[j] = z[j] - p.Mean[j]
			p.Mean[j] += delta[j] / float64(n)
		}
		for i := range z {
			for j := 0; j <= i; j++ {
				p.Covariance[i][j] += delta[i] * (z[j] - p.Mean[j])
			}
		}
		return ctx.Err()
	}); err != nil {
		return nil, err
	}
	if n != p.Observations {
		return nil, fmt.Errorf("D1 changed between passes")
	}
	flat := make([]float64, Dimensions*Dimensions)
	for i := range p.Covariance {
		for j := 0; j <= i; j++ {
			x := p.Covariance[i][j] / float64(n-cfg.CovarianceDDOF)
			if !Finite(x) {
				return nil, fmt.Errorf("covariance overflow")
			}
			p.Covariance[i][j], p.Covariance[j][i] = x, x
			flat[i*Dimensions+j], flat[j*Dimensions+i] = x, x
		}
	}
	var svd mat.SVD
	if !svd.Factorize(mat.NewDense(Dimensions, Dimensions, flat), mat.SVDFull) {
		return nil, fmt.Errorf("covariance SVD did not converge")
	}
	p.Rank = svd.Rank(cfg.SVDRCond)
	// Rank zero has the Moore-Penrose inverse zero, not an ordinary inverse.
	if p.Rank > 0 {
		var inverse mat.Dense
		svd.SolveTo(&inverse, mat.NewDiagDense(Dimensions, ones(Dimensions)), p.Rank)
		for i := range p.PseudoInverse {
			for j := range p.PseudoInverse[i] {
				p.PseudoInverse[i][j] = inverse.At(i, j)
			}
		}
	}
	distances := make([]float64, 0, p.Observations)
	if err := data(ctx, func(o Observation) error {
		if !window.Contains(o.Timestamp) {
			return fmt.Errorf("non-D1 observation in threshold training")
		}
		v := o.Features
		d, err := p.DistanceSquared(v)
		if err != nil {
			return err
		}
		distances = append(distances, d)
		return ctx.Err()
	}); err != nil {
		return nil, err
	}
	if len(distances) != p.Observations {
		return nil, fmt.Errorf("D1 changed between passes")
	}
	var err error
	p.Threshold, err = EmpiricalQuantile(distances, 0.99)
	if err != nil {
		return nil, err
	}
	if err = p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

func ones(n int) []float64 {
	v := make([]float64, n)
	for i := range v {
		v[i] = 1
	}
	return v
}

func (p *StatisticalProfile) Normalize(v Vector) (Vector, error) {
	var z Vector
	for j, x := range v {
		if !Finite(x) || !Finite(p.Scale[j]) || p.Scale[j] <= 0 || !Finite(p.Median[j]) {
			return z, fmt.Errorf("invalid normalization coordinate %d", j)
		}
		z[j] = (x - p.Median[j]) / p.Scale[j]
		if !Finite(z[j]) {
			return z, fmt.Errorf("normalization overflow %d", j)
		}
	}
	return z, nil
}

func (p *StatisticalProfile) DistanceSquared(v Vector) (float64, error) {
	z, err := p.Normalize(v)
	if err != nil {
		return 0, err
	}
	for j := range z {
		z[j] -= p.Mean[j]
		if !Finite(z[j]) {
			return 0, fmt.Errorf("nonfinite centered coordinate %d", j)
		}
	}
	d := 0.0
	magnitude := 0.0
	for i := range z {
		for j := range z {
			term := z[i] * p.PseudoInverse[i][j] * z[j]
			d += term
			magnitude += math.Abs(term)
		}
	}
	if !Finite(d) || !Finite(magnitude) {
		return 0, fmt.Errorf("squared Mahalanobis overflow")
	}
	// Only suppress floating-point roundoff of the positive-semidefinite form.
	if d < 0 && -d <= 1e-12*math.Max(1, magnitude) {
		d = 0
	}
	if !Finite(d) || d < 0 {
		return 0, fmt.Errorf("invalid squared Mahalanobis distance")
	}
	return d, nil
}

func (p *StatisticalProfile) ControlLabel(v Vector) (float64, bool, error) {
	if !Finite(p.Threshold) || p.Threshold < 0 {
		return 0, false, fmt.Errorf("invalid Mahalanobis threshold")
	}
	d, err := p.DistanceSquared(v)
	return d, err == nil && d > p.Threshold, err
}

// Validate checks persisted numerical conventions and parameters before reuse.
func (p *StatisticalProfile) Validate() error {
	if p == nil {
		return fmt.Errorf("missing statistical profile")
	}
	if p.Version != "statistical-v1" || p.TrainingWindow != NIRWindows()[0] || p.Observations <= p.Config.CovarianceDDOF || p.Rank < 0 || p.Rank > Dimensions || !Finite(p.Threshold) || p.Threshold < 0 || !Finite(p.Config.SVDRCond) || p.Config.SVDRCond <= 0 || p.Config.SVDRCond >= 1 || (p.Config.CovarianceDDOF != 0 && p.Config.CovarianceDDOF != 1) {
		return fmt.Errorf("invalid statistical profile metadata")
	}
	for j := 0; j < Dimensions; j++ {
		if !Finite(p.Median[j]) || !Finite(p.MAD[j]) || p.MAD[j] < 0 || !Finite(p.Scale[j]) || p.Scale[j] != math.Max(1.4826*p.MAD[j], 0.0001) || !Finite(p.Mean[j]) {
			return fmt.Errorf("invalid profile coordinate %d", j)
		}
		for k := 0; k < Dimensions; k++ {
			if !Finite(p.Covariance[j][k]) || !Finite(p.PseudoInverse[j][k]) || p.Covariance[j][k] != p.Covariance[k][j] {
				return fmt.Errorf("invalid profile matrix")
			}
		}
	}
	// Finite JSON numbers alone do not establish a valid covariance model.
	// Recompute its small SVD using the persisted cutoff before accepting a load.
	cov := mat.NewSymDense(Dimensions, nil)
	for i := 0; i < Dimensions; i++ {
		for j := 0; j <= i; j++ {
			cov.SetSym(i, j, p.Covariance[i][j])
		}
	}
	var eigen mat.EigenSym
	if !eigen.Factorize(cov, false) {
		return fmt.Errorf("covariance eigenvalue check failed")
	}
	values := eigen.Values(nil)
	for _, value := range values {
		if !Finite(value) {
			return fmt.Errorf("nonfinite covariance eigenvalue")
		}
	}
	spectralScale := math.Max(math.Abs(values[0]), math.Abs(values[Dimensions-1]))
	if values[0] < -1e-12*spectralScale {
		return fmt.Errorf("covariance is not positive semidefinite")
	}
	var svd mat.SVD
	if !svd.Factorize(cov, mat.SVDFull) || svd.Rank(p.Config.SVDRCond) != p.Rank {
		return fmt.Errorf("covariance rank disagrees with profile")
	}
	var expected mat.Dense
	if p.Rank > 0 {
		svd.SolveTo(&expected, mat.NewDiagDense(Dimensions, ones(Dimensions)), p.Rank)
	}
	var difference, norm float64
	for i := 0; i < Dimensions; i++ {
		for j := 0; j < Dimensions; j++ {
			want := 0.0
			if p.Rank > 0 {
				want = expected.At(i, j)
			}
			if !Finite(want) {
				return fmt.Errorf("nonfinite covariance pseudoinverse")
			}
			norm = math.Hypot(norm, want)
			difference = math.Hypot(difference, p.PseudoInverse[i][j]-want)
		}
	}
	if !Finite(norm) || !Finite(difference) || difference > 1e-10*norm {
		return fmt.Errorf("pseudoinverse disagrees with covariance and cutoff")
	}
	return nil
}
