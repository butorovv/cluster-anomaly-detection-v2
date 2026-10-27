package group

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

func validTestRecord() domain.TelemetryRecord {
	r := domain.TelemetryRecord{Timestamp: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), Hostname: "a01n01", GroupID: "a01", NodeIndex: 1, Valid: true, AvailabilityMask: domain.FullAvailabilityMask}
	for i := range r.GPUPower {
		r.GPUPower[i] = float64(i + 1)
	}
	for i := range r.CPUPower {
		r.CPUPower[i] = float64(10 + i)
	}
	for i := range r.PSUPower {
		r.PSUPower[i] = float64(20 + i)
	}
	for i := range r.GPUTemp {
		r.GPUTemp[i] = float64(30 + i)
	}
	r.CPUTemp = [2]float64{40, 50}
	return r
}

func TestFitD1RankAndKnownQuadraticForm(t *testing.T) {
	hosts := make([]string, 18)
	for h := range hosts {
		hosts[h] = fmt.Sprintf("a01n%02d", h+1)
	}
	samples := make([]Sample, 80)
	rng := rand.New(rand.NewSource(41))
	for i := range samples {
		s := &samples[i]
		s.Timestamp = analysis.NIRWindows()[0].Start.Add(time.Duration(i) * 10 * time.Second)
		for h := range s.Values {
			for k := range s.Values[h] {
				s.Values[h][k] = 100 + float64(k) + rng.NormFloat64()*float64(i%3+1)
			}
		}
		s.Vector, _ = GroupVector(s.Values)
	}
	p, err := FitGroup("a01", hosts, samples)
	if err != nil {
		t.Fatal(err)
	}
	if p.Rank != 10 || p.BThreshold == nil {
		t.Fatalf("rank=%d reason=%s", p.Rank, p.BReason)
	}
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			x := 0.
			for k := 0; k < 10; k++ {
				x += p.Covariance[i][k] * p.Inverse[k][j]
			}
			want := 0.
			if i == j {
				want = 1
			}
			if math.Abs(x-want) > 1e-10 {
				t.Fatal("inverse reconstruction", i, j, x)
			}
		}
	}
	var v Vector
	for j := range v {
		v[j] = p.Median[j] + p.Scale[j]*p.Mean[j]
	}
	d, err := p.Distance(v)
	if err != nil || d > 1e-20 {
		t.Fatal("center distance", d, err)
	}
	for i := range samples {
		samples[i].Values = [18]HostVector{}
		samples[i].Vector = Vector{}
	}
	p, err = FitGroup("a01", hosts, samples)
	if err != nil {
		t.Fatal(err)
	}
	if p.Rank != 0 || p.B(Vector{}).State != Unknown {
		t.Fatal("singular B must be unknown")
	}
	samples[0].Timestamp = analysis.NIRWindows()[1].Start
	if _, err = FitGroup("a01", hosts, samples); err == nil {
		t.Fatal("D2 training accepted")
	}
}

func TestRDirectionChannelAndZeroMADPair(t *testing.T) {
	p := &GroupProfile{Observations: 20, RThreshold: 3, Hosts: make([]string, 18)}
	for h := 0; h < 18; h++ {
		p.Hosts[h] = fmtHost(h)
		for k := 0; k < 5; k++ {
			p.HostScale[h][k] = 1
			p.HostMAD[h][k] = 1
		}
	}
	var u [18]HostVector
	u[0][0] = 4
	u[1][0] = -4
	if p.R(u).State != Negative {
		t.Fatal("opposite directions counted together")
	}
	u[1][0] = 0
	u[1][1] = 4
	if p.R(u).State != Negative {
		t.Fatal("different channels counted together")
	}
	u[0][0] = 0
	u[1][1] = 0
	u[0][3] = -.01
	u[1][3] = -.02
	p.HostScale[0][3] = Floor
	p.HostMAD[0][3] = 0
	p.HostScale[1][3] = Floor
	p.HostMAD[1][3] = 0
	r := p.R(u)
	if r.State != Positive || r.Direction != -1 || r.Channel != 3 || !r.Pair[0].ZeroMAD || *r.Score != 100 {
		t.Fatalf("zero-MAD evidence lost: %+v", r)
	}
}

func TestLoadedProfileRejectsCorruptInverse(t *testing.T) {
	// This small tracked artifact needs no external prepared data or models.
	p, err := LoadProfile("../../../../configs/group-profile.summit-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	p.Groups[0].Inverse = Matrix{}
	if p.Validate() == nil {
		t.Fatal("corrupt inverse passed serialized diagnostics as valid")
	}
}

func TestHostChannelsUsesF18SumsAndMeans(t *testing.T) {
	u, err := HostChannels(validTestRecord())
	if err != nil {
		t.Fatal(err)
	}
	want := HostVector{21, 21, 41, 32.5, 45}
	if u != want {
		t.Fatalf("HostChannels()=%v, want %v", u, want)
	}
	r := validTestRecord()
	r.Valid = false
	if _, err := HostChannels(r); err == nil {
		t.Fatal("incomplete F18 record accepted")
	}
}

func TestGroupVectorUsesHostPopulationSpreadAndRejectsNonfinite(t *testing.T) {
	var hosts [18]HostVector
	for i := range hosts {
		hosts[i][0] = float64(i)
	}
	v, err := GroupVector(hosts)
	if err != nil {
		t.Fatal(err)
	}
	if v[0] != 8.5 {
		t.Fatalf("group mean=%v, want 8.5", v[0])
	}
	variance := 0.0
	for _, h := range hosts {
		variance += (h[0] - 8.5) * (h[0] - 8.5)
	}
	if math.Abs(v[5]-math.Sqrt(variance/18)) > 1e-12 {
		t.Fatalf("spread=%v", v[5])
	}
	hosts[4][2] = math.NaN()
	if _, err := GroupVector(hosts); err == nil {
		t.Fatal("nonfinite channel accepted")
	}
}

func TestRRequiresTwoHostsAndStrictThreshold(t *testing.T) {
	p := &GroupProfile{Observations: 1, Hosts: make([]string, 18), RThreshold: 3}
	for i := range p.Hosts {
		p.Hosts[i] = fmtHost(i)
	}
	for h := range p.HostScale {
		for k := range p.HostScale[h] {
			p.HostScale[h][k] = 1
		}
	}
	var u [18]HostVector
	u[0][0] = 4
	if got := p.R(u); got.State != Negative {
		t.Fatalf("single host extreme: %s", got.State)
	}
	u[1][0] = 3
	if got := p.R(u); got.State != Negative {
		t.Fatalf("threshold equality must be negative: %+v", got)
	}
	u[1][0] = 3.01
	if got := p.R(u); got.State != Positive || len(got.Pair) != 2 {
		t.Fatalf("two distinct hosts above threshold: %+v", got)
	}
}

func fmtHost(i int) string {
	return "a01n" + string([]byte{'0' + byte((i+1)/10), '0' + byte((i+1)%10)})
}
