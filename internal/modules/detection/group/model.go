package group

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/local"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"gonum.org/v1/gonum/mat"
)

var ChannelNames = [5]string{"gpu_total_W", "cpu_total_W", "psu_ac_total_W", "gpu_mean_C", "cpu_mean_C"}

type Sample struct {
	Timestamp time.Time
	Values    [18]HostVector
	Vector    Vector
}
type GroupProfile struct {
	Group          string         `json:"group"`
	Hosts          []string       `json:"hosts"`
	Observations   int            `json:"observations"`
	ExpectedTicks  int            `json:"expected_ticks"`
	HostMedian     [18]HostVector `json:"host_median"`
	HostMAD        [18]HostVector `json:"host_mad"`
	HostScale      [18]HostVector `json:"host_scale"`
	RThreshold     float64        `json:"r_q99"`
	Median         Vector         `json:"median"`
	MAD            Vector         `json:"mad"`
	Scale          Vector         `json:"scale"`
	Mean           Vector         `json:"normalized_mean"`
	Covariance     Matrix         `json:"covariance"`
	Inverse        Matrix         `json:"pseudoinverse"`
	Singular       []float64      `json:"singular_values"`
	Eigen          []float64      `json:"eigenvalues"`
	Rank           int            `json:"effective_rank"`
	Condition      *float64       `json:"condition"`
	Reconstruction float64        `json:"reconstruction_relative_frobenius"`
	BThreshold     *float64       `json:"b_q99"`
	BReason        string         `json:"b_reason,omitempty"`
}
type Profile struct {
	Version        string            `json:"version"`
	Binding        local.Binding     `json:"binding"`
	InputSHA       map[string]string `json:"input_sha256"`
	TrainingWindow analysis.Window   `json:"training_window"`
	Floor          float64           `json:"floor"`
	RCond          float64           `json:"rcond"`
	DDOF           int               `json:"ddof"`
	Groups         []GroupProfile    `json:"groups"`
}

func HostChannels(r domain.TelemetryRecord) (HostVector, error) {
	var u HostVector
	if err := r.Validate(); err != nil {
		return u, err
	}
	if !r.Valid {
		return u, fmt.Errorf("incomplete_f18")
	}
	for _, x := range r.GPUPower {
		u[0] += x
	}
	for _, x := range r.CPUPower {
		u[1] += x
	}
	for _, x := range r.PSUPower {
		u[2] += x
	}
	for _, x := range r.GPUTemp {
		u[3] += x
	}
	u[3] /= 6
	for _, x := range r.CPUTemp {
		u[4] += x
	}
	u[4] /= 2
	for _, x := range u {
		if !analysis.Finite(x) {
			return HostVector{}, fmt.Errorf("channel_overflow")
		}
	}
	return u, nil
}
func GroupVector(u [18]HostVector) (Vector, error) {
	var v Vector
	for k := 0; k < 5; k++ {
		for h := 0; h < 18; h++ {
			if !analysis.Finite(u[h][k]) {
				return v, fmt.Errorf("nonfinite_channel")
			}
			v[k] += u[h][k]
		}
		v[k] /= 18
		for h := 0; h < 18; h++ {
			d := u[h][k] - v[k]
			v[k+5] += d * d
		}
		v[k+5] = math.Sqrt(v[k+5] / 18)
	}
	for _, x := range v {
		if !analysis.Finite(x) {
			return Vector{}, fmt.Errorf("vector_overflow")
		}
	}
	return v, nil
}
func medianMAD(x []float64) (float64, float64, float64) {
	y := slices.Clone(x)
	sort.Float64s(y)
	n := len(y)
	m := y[n/2]
	if n%2 == 0 {
		m = y[n/2-1]/2 + y[n/2]/2
	}
	for i := range y {
		y[i] = math.Abs(y[i] - m)
	}
	sort.Float64s(y)
	mad := y[n/2]
	if n%2 == 0 {
		mad = y[n/2-1]/2 + y[n/2]/2
	}
	return m, mad, math.Max(1.4826*mad, Floor)
}
func (p *GroupProfile) R(u [18]HostVector) RResult {
	out := RResult{Criterion: Criterion{State: Unknown, Reason: "missing_profile"}, Channel: -1}
	if p == nil || p.Observations == 0 {
		return out
	}
	out.Threshold = ptr(p.RThreshold)
	// Scan channels then negative/positive signs: ties retain lowest channel/sign.
	best := -1.
	var pair [2]int
	var z [18]HostVector
	for h := 0; h < 18; h++ {
		for k := 0; k < 5; k++ {
			z[h][k] = (u[h][k] - p.HostMedian[h][k]) / p.HostScale[h][k]
			if !analysis.Finite(z[h][k]) {
				out.Reason = "nonfinite_r"
				return out
			}
		}
	}
	for k := 0; k < 5; k++ {
		for _, a := range []int{-1, 1} {
			first, second := -1., -1.
			i1, i2 := -1, -1
			for h := 0; h < 18; h++ {
				x := math.Max(float64(a)*z[h][k], 0)
				if x > first {
					second, i2 = first, i1
					first, i1 = x, h
				} else if x > second {
					second, i2 = x, h
				}
			}
			if second > best {
				best = second
				pair = [2]int{i1, i2}
				out.Channel = k
				out.Direction = a
			}
		}
	}
	out.Criterion = Criterion{State: Negative, Score: ptr(best), Threshold: ptr(p.RThreshold)}
	if best > p.RThreshold {
		out.State = Positive
	}
	witness := func(h, k, a int) Witness {
		return Witness{Hostname: p.Hosts[h], Channel: k, Direction: a, Value: u[h][k], Median: p.HostMedian[h][k], Scale: p.HostScale[h][k], Z: z[h][k], Amplitude: u[h][k] - p.HostMedian[h][k], ZeroMAD: p.HostMAD[h][k] == 0}
	}
	for _, h := range pair {
		out.Pair = append(out.Pair, witness(h, out.Channel, out.Direction))
	}
	for h := 0; h < 18; h++ {
		if float64(out.Direction)*z[h][out.Channel] > p.RThreshold {
			out.Witnesses = append(out.Witnesses, witness(h, out.Channel, out.Direction))
		}
		for k := 0; k < 5; k++ {
			if p.HostMAD[h][k] == 0 && math.Abs(z[h][k]) > p.RThreshold {
				a := 1
				if z[h][k] < 0 {
					a = -1
				}
				out.ZeroMADAbove = append(out.ZeroMADAbove, witness(h, k, a))
			}
		}
	}
	return out
}
func (p *GroupProfile) Distance(v Vector) (float64, error) {
	if p == nil || p.Rank != 10 || p.BReason != "" {
		return 0, fmt.Errorf("rank_deficient_or_invalid_profile")
	}
	var d Vector
	for j := range d {
		d[j] = (v[j]-p.Median[j])/p.Scale[j] - p.Mean[j]
	}
	x := 0.
	for i := range d {
		for j := range d {
			x += d[i] * p.Inverse[i][j] * d[j]
		}
	}
	if !analysis.Finite(x) || x < 0 {
		return 0, fmt.Errorf("invalid_distance")
	}
	return x, nil
}
func (p *GroupProfile) B(v Vector) Criterion {
	if p == nil || p.BThreshold == nil {
		return Criterion{State: Unknown, Reason: "invalid_b_profile"}
	}
	x, e := p.Distance(v)
	if e != nil {
		return Criterion{State: Unknown, Reason: e.Error(), Threshold: ptr(*p.BThreshold)}
	}
	s := Negative
	if x > *p.BThreshold {
		s = Positive
	}
	return Criterion{State: s, Score: ptr(x), Threshold: ptr(*p.BThreshold)}
}
func FitGroup(group string, hosts []string, samples []Sample) (GroupProfile, error) {
	p := GroupProfile{Group: group, Hosts: slices.Clone(hosts), Observations: len(samples), ExpectedTicks: 2880}
	if len(hosts) != 18 {
		return p, fmt.Errorf("fixed 18 hosts required")
	}
	for h, host := range hosts {
		g, i, e := topology.ParseHostname(host)
		if e != nil || g != group || i != h+1 {
			return p, fmt.Errorf("host order mismatch")
		}
	}
	if len(samples) == 0 {
		p.BReason = "insufficient_d1"
		return p, nil
	}
	for i, s := range samples {
		if !analysis.NIRWindows()[0].Contains(s.Timestamp) || s.Timestamp.Sub(analysis.NIRWindows()[0].Start)%analysis.Step != 0 || (i > 0 && !s.Timestamp.After(samples[i-1].Timestamp)) {
			return p, fmt.Errorf("D1-only increasing grid required")
		}
		v, e := GroupVector(s.Values)
		if e != nil || v != s.Vector {
			return p, fmt.Errorf("inconsistent sample vector")
		}
	}
	column := make([]float64, len(samples))
	for h := 0; h < 18; h++ {
		for k := 0; k < 5; k++ {
			for i := range samples {
				column[i] = samples[i].Values[h][k]
			}
			p.HostMedian[h][k], p.HostMAD[h][k], p.HostScale[h][k] = medianMAD(column)
		}
	}
	scores := make([]float64, len(samples))
	for i, s := range samples {
		scores[i] = *p.R(s.Values).Score
	}
	p.RThreshold, _ = analysis.EmpiricalQuantile(scores, .99)
	for k := 0; k < 10; k++ {
		for i := range samples {
			column[i] = samples[i].Vector[k]
		}
		p.Median[k], p.MAD[k], p.Scale[k] = medianMAD(column)
	}
	if len(samples) < 11 {
		p.BReason = "insufficient_d1"
		return p, nil
	}
	for n, s := range samples {
		var w, delta Vector
		for j := range w {
			w[j] = (s.Vector[j] - p.Median[j]) / p.Scale[j]
			delta[j] = w[j] - p.Mean[j]
			p.Mean[j] += delta[j] / float64(n+1)
		}
		for i := 0; i < 10; i++ {
			for j := 0; j <= i; j++ {
				p.Covariance[i][j] += delta[i] * (w[j] - p.Mean[j])
			}
		}
	}
	flat := make([]float64, 100)
	sym := mat.NewSymDense(10, nil)
	for i := 0; i < 10; i++ {
		for j := 0; j <= i; j++ {
			x := p.Covariance[i][j] / float64(len(samples)-1)
			if !analysis.Finite(x) {
				return p, fmt.Errorf("covariance overflow")
			}
			p.Covariance[i][j] = x
			p.Covariance[j][i] = x
			sym.SetSym(i, j, x)
		}
	}
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			flat[i*10+j] = p.Covariance[i][j]
		}
	}
	var eigen mat.EigenSym
	if !eigen.Factorize(sym, false) {
		return p, fmt.Errorf("eigendecomposition failed")
	}
	p.Eigen = eigen.Values(nil)
	var svd mat.SVD
	if !svd.Factorize(mat.NewDense(10, 10, flat), mat.SVDFull) {
		return p, fmt.Errorf("SVD failed")
	}
	p.Singular = svd.Values(nil)
	p.Rank = svd.Rank(RCond)
	if p.Singular[9] > 0 {
		p.Condition = ptr(p.Singular[0] / p.Singular[9])
	}
	if p.Rank > 0 {
		var inv mat.Dense
		svd.SolveTo(&inv, mat.NewDiagDense(10, []float64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}), p.Rank)
		for i := 0; i < 10; i++ {
			for j := 0; j < 10; j++ {
				p.Inverse[i][j] = inv.At(i, j)
			}
		}
	}
	num, den := 0., 0.
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			a := 0.
			for k := 0; k < 10; k++ {
				for l := 0; l < 10; l++ {
					a += p.Covariance[i][k] * p.Inverse[k][l] * p.Covariance[l][j]
				}
			}
			num += (a - p.Covariance[i][j]) * (a - p.Covariance[i][j])
			den += p.Covariance[i][j] * p.Covariance[i][j]
		}
	}
	if den > 0 {
		p.Reconstruction = math.Sqrt(num / den)
	}
	if p.Rank < 10 {
		p.BReason = "rank_deficient"
		return p, nil
	}
	if p.Eigen[0] < 0 {
		p.BReason = "not_psd"
		return p, nil
	}
	for i, s := range samples {
		var e error
		scores[i], e = p.Distance(s.Vector)
		if e != nil {
			return p, e
		}
	}
	q, e := analysis.EmpiricalQuantile(scores, .99)
	p.BThreshold = ptr(q)
	return p, e
}
func (p *Profile) Validate() error {
	if p == nil || p.Version != Version || p.Floor != Floor || p.RCond != RCond || p.DDOF != 1 || p.TrainingWindow != analysis.NIRWindows()[0] || len(p.Groups) != 16 {
		return fmt.Errorf("group profile metadata mismatch")
	}
	if !slices.Equal(p.Binding.FeatureOrder, domain.FeatureNames[:]) || len(p.Binding.Hosts) != 288 {
		return fmt.Errorf("binding mismatch")
	}
	for gi, g := range p.Groups {
		if g.Group != fmt.Sprintf("a%02d", gi+1) || len(g.Hosts) != 18 || !slices.Equal(g.Hosts, p.Binding.Hosts[gi*18:gi*18+18]) || g.Observations < 0 || g.Observations > 2880 {
			return fmt.Errorf("group inventory/count mismatch")
		}
		for h, host := range g.Hosts {
			if host != fmt.Sprintf("%sn%02d", g.Group, h+1) {
				return fmt.Errorf("host identity mismatch")
			}
		}
		if g.Observations == 0 {
			if g.BThreshold != nil {
				return fmt.Errorf("threshold for no observations")
			}
			continue
		}
		if !analysis.Finite(g.RThreshold) || g.RThreshold < 0 {
			return fmt.Errorf("invalid R threshold")
		}
		for h := 0; h < 18; h++ {
			for k := 0; k < 5; k++ {
				if !analysis.Finite(g.HostMedian[h][k]) || !analysis.Finite(g.HostMAD[h][k]) || g.HostMAD[h][k] < 0 || g.HostScale[h][k] != math.Max(1.4826*g.HostMAD[h][k], Floor) {
					return fmt.Errorf("invalid host scale")
				}
			}
		}
		for j := 0; j < 10; j++ {
			if !analysis.Finite(g.Median[j]) || !analysis.Finite(g.MAD[j]) || g.MAD[j] < 0 || g.Scale[j] != math.Max(1.4826*g.MAD[j], Floor) || !analysis.Finite(g.Mean[j]) {
				return fmt.Errorf("invalid B scale")
			}
			for k := 0; k < 10; k++ {
				if !analysis.Finite(g.Covariance[j][k]) || !analysis.Finite(g.Inverse[j][k]) {
					return fmt.Errorf("nonfinite covariance/inverse")
				}
			}
		}
		if g.BThreshold != nil {
			if g.Rank != 10 || g.BReason != "" || g.Observations < 11 || !analysis.Finite(*g.BThreshold) || *g.BThreshold < 0 || len(g.Eigen) != 10 || g.Eigen[0] < 0 || g.Reconstruction > 1e-7 {
				return fmt.Errorf("invalid B model")
			}
			// Do not trust serialized rank/reconstruction diagnostics: validate the
			// loaded covariance and inverse themselves before reporting normal.
			sym := mat.NewSymDense(10, nil)
			flat := make([]float64, 100)
			for i := 0; i < 10; i++ {
				for j := 0; j < 10; j++ {
					if g.Covariance[i][j] != g.Covariance[j][i] {
						return fmt.Errorf("asymmetric covariance")
					}
					flat[i*10+j] = g.Covariance[i][j]
					sym.SetSym(i, j, g.Covariance[i][j])
				}
			}
			var eigen mat.EigenSym
			var svd mat.SVD
			if !eigen.Factorize(sym, false) || !svd.Factorize(mat.NewDense(10, 10, flat), mat.SVDNone) || svd.Rank(RCond) != 10 || eigen.Values(nil)[0] < 0 {
				return fmt.Errorf("unusable loaded covariance")
			}
			for i := 0; i < 10; i++ {
				for j := 0; j < 10; j++ {
					x := 0.
					for k := 0; k < 10; k++ {
						x += g.Covariance[i][k] * g.Inverse[k][j]
					}
					want := 0.
					if i == j {
						want = 1
					}
					if math.Abs(x-want) > 1e-7 {
						return fmt.Errorf("incompatible pseudoinverse")
					}
				}
			}
		}
	}
	return nil
}
func LoadProfile(path string) (*Profile, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	var p Profile
	if e = json.Unmarshal(b, &p); e != nil {
		return nil, e
	}
	return &p, p.Validate()
}
func SaveJSON(path string, v any) error {
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	e = enc.Encode(v)
	c := f.Close()
	if e != nil {
		return e
	}
	return c
}
