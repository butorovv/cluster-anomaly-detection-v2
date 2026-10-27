package group

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/local"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

func evalHosts(group string) []string {
	h := make([]string, HostsPerGroup)
	for i := range h {
		h[i] = fmt.Sprintf("%sn%02d", group, i+1)
	}
	return h
}

func evalRecord(host string, ts time.Time, base float64) domain.TelemetryRecord {
	g, n, _ := topology.ParseHostname(host)
	r := domain.TelemetryRecord{Timestamp: ts, Hostname: host, GroupID: g, NodeIndex: n, Valid: true, AvailabilityMask: domain.FullAvailabilityMask}
	// These fixed per-sensor values give host channels [21, 3, 3, 32.5, 45].
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

func evalDetector(t *testing.T, groups int) *local.Detector {
	t.Helper()
	hosts := make([]string, 0, groups*18)
	for i := 1; i <= groups; i++ {
		hosts = append(hosts, evalHosts(fmt.Sprintf("a%02d", i))...)
	}
	b := local.Binding{DatasetID: "synthetic-evaluate", PreparedSHA256: strings.Repeat("1", 64), StatisticalSHA256: strings.Repeat("2", 64), IsolationSHA256: strings.Repeat("3", 64), FeatureOrder: append([]string(nil), domain.FeatureNames[:]...), Hosts: hosts, AssumedTimezone: "UTC"}
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
	forest, err := analysis.FitIsolation(context.Background(), data, stat, analysis.NIRWindows()[0], analysis.IsolationConfig{Trees: 1, SampleSize: 2, MaxDepth: 0, Seed: 9})
	if err != nil {
		t.Fatal(err)
	}
	thresholds, counts := map[string]float64{}, map[string]int{}
	for i, h := range hosts {
		thresholds[h] = .5
		if i%18 < 2 {
			thresholds[h] = .49
		}
		counts[h] = 2
	}
	p, err := local.NewProfile(b, thresholds, counts)
	if err != nil {
		t.Fatal(err)
	}
	d, err := local.NewDetector(p, forest, b)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func evalProfile(group string) *GroupProfile {
	p := &GroupProfile{Group: group, Hosts: evalHosts(group), Observations: 20, RThreshold: 100}
	for h := range p.HostScale {
		for k := range p.HostScale[h] {
			p.HostScale[h][k] = 1
		}
	}
	for k := range p.Scale {
		p.Scale[k] = 1
		p.Inverse[k][k] = 1
	}
	p.Rank = Dimensions
	p.BThreshold = ptr(1e30)
	return p
}

func evalTick(group string, ts time.Time, count int) Tick {
	r := map[string]domain.TelemetryRecord{}
	for i, h := range evalHosts(group) {
		if i < count {
			r[h] = evalRecord(h, ts, 0)
		}
	}
	return Tick{Timestamp: ts, Records: r}
}

func TestEvaluateGroupCompleteIndependentVectorAndCriteria(t *testing.T) {
	ts := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	p := evalProfile("a01")
	// Every host contributes channel vector [21,3,3,32.5,45].
	r, err := EvaluateGroup(p, evalTick("a01", ts, 18), nil, evalDetector(t, 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := Vector{21, 3, 3, 32.5, 45, 0, 0, 0, 0, 0}
	if r.Complete != 18 || len(r.Missing) != 0 || r.Vector == nil || *r.Vector != want {
		t.Fatalf("vector/completeness: %+v, want %v", r, want)
	}
	if r.R.State != Negative || r.R.Score == nil || *r.R.Score != 45 || r.R.Channel != 4 {
		t.Fatalf("R expected known nonbreach: %+v", r.R)
	}
	if r.B.State != Negative || r.B.Score == nil || *r.B.Score != 21*21+3*3+3*3+32.5*32.5+45*45 {
		t.Fatalf("B independent identity-covariance score: %+v", r.B)
	}
	if r.Y.State != Unknown || r.Y.Reason != "no_approved_sensor_bounds" {
		t.Fatalf("empty hardware model: %+v", r.Y)
	}
	if r.IF.Available != 18 || r.IF.Anomalous != 2 || r.IF.S == nil || *r.IF.S != 2.0/18 {
		t.Fatalf("local support: %+v", r.IF)
	}
	if r.IF.OutsideGroups != nil || r.IF.OutsideS != nil || r.IF.E != nil {
		t.Fatalf("single-group result got outside annotation: %+v", r.IF)
	}
	b, err := json.Marshal(r.IF)
	if err != nil {
		t.Fatal(err)
	}
	var encoded map[string]any
	if err = json.Unmarshal(b, &encoded); err != nil {
		t.Fatal(err)
	}
	if encoded["outside_available"] != nil || encoded["outside_anomalous"] != nil {
		t.Fatalf("outside counts should serialize null before annotation: %s", b)
	}
}

func TestEvaluateGroupIncompleteAndMissingSemantics(t *testing.T) {
	ts := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	p := evalProfile("a01")
	for _, tc := range []struct {
		name        string
		count       int
		nan         bool
		wantMissing int
	}{{"absent", 0, false, 18}, {"one absent", 17, false, 1}, {"one NaN", 18, true, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			tick := evalTick("a01", ts, tc.count)
			if tc.nan {
				r := tick.Records[p.Hosts[7]]
				r.GPUPower[0] = math.NaN()
				r.Valid = false
				r.AvailabilityMask &^= 1 << 4
				tick.Records[p.Hosts[7]] = r
			}
			got, err := EvaluateGroup(p, tick, nil, evalDetector(t, 1), nil)
			if err != nil {
				t.Fatal(err)
			}
			if got.Complete != 18-tc.wantMissing || len(got.Missing) != tc.wantMissing || got.Vector != nil || got.R.State != Unknown || got.B.State != Unknown {
				t.Fatalf("incomplete result: %+v", got)
			}
			for i, h := range got.Missing {
				want := evalHosts("a01")[18-tc.wantMissing+i]
				if h != want && !(tc.nan && h == p.Hosts[7]) {
					t.Fatalf("missing host order=%v", got.Missing)
				}
			}
			if tc.nan && (got.Missing[0] != p.Hosts[7] || got.Local[7].DiagnosticStatus != local.StatusIncompleteF18) {
				t.Fatalf("NaN F18 semantics: missing=%v local=%+v", got.Missing, got.Local[7])
			}
		})
	}
}

func TestEvaluateGroupRPairThresholdDirectionAndZeroMAD(t *testing.T) {
	ts := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	makeTick := func(values map[int]HostVector) Tick {
		tick := evalTick("a01", ts, 18)
		for _, host := range evalHosts("a01") {
			r := tick.Records[host]
			r.GPUPower = [6]float64{}
			r.CPUPower = [2]float64{}
			r.PSUPower = [2]float64{}
			r.GPUTemp = [6]float64{}
			r.CPUTemp = [2]float64{}
			tick.Records[host] = r
		}
		for i, v := range values {
			r := tick.Records[evalHosts("a01")[i]]
			r.GPUPower[0] = v[0]
			r.CPUPower[0] = v[1]
			r.PSUPower[0] = v[2]
			for j := range r.GPUTemp {
				r.GPUTemp[j] = v[3]
			}
			r.CPUTemp = [2]float64{v[4], v[4]}
			tick.Records[r.Hostname] = r
		}
		return tick
	}
	p := evalProfile("a01")
	p.RThreshold = 3
	for h := range p.HostMedian {
		for k := range p.HostMedian[h] {
			p.HostMedian[h][k] = 0
		}
	}
	got, err := EvaluateGroup(p, makeTick(map[int]HostVector{0: {-4}, 1: {-3}, 2: {0, 3}, 3: {0, 3}}), nil, evalDetector(t, 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.R.State != Negative || got.R.Channel != 0 || got.R.Direction != -1 || len(got.R.Pair) != 2 || got.R.Pair[0].Hostname != p.Hosts[0] || got.R.Pair[1].Hostname != p.Hosts[1] {
		t.Fatalf("negative channel/host tie and equality: %+v", got.R)
	}
	got, err = EvaluateGroup(p, makeTick(map[int]HostVector{0: {math.Nextafter(3, math.Inf(1))}, 1: {math.Nextafter(3, math.Inf(1))}}), nil, evalDetector(t, 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.R.State != Positive || got.R.Direction != 1 || got.R.Channel != 0 {
		t.Fatalf("nextafter R threshold should breach: %+v", got.R)
	}
	for h := range p.HostScale {
		p.HostScale[h][0] = Floor
		p.HostMAD[h][0] = 0
	}
	got, err = EvaluateGroup(p, makeTick(map[int]HostVector{0: {.01}, 1: {.02}}), nil, evalDetector(t, 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.R.State != Positive || len(got.R.ZeroMADAbove) != 2 || len(got.R.Pair) != 2 || !got.R.Pair[0].ZeroMAD || !got.R.Pair[1].ZeroMAD {
		t.Fatalf("zero-MAD witnesses: %+v", got.R)
	}
}

func TestEvaluateGroupBStrictThresholdAndApprovedHardwareEvidence(t *testing.T) {
	ts := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	p := evalProfile("a01")
	p.BThreshold = ptr(1)
	makeTick := func(x float64) Tick {
		tick := evalTick("a01", ts, 18)
		for _, h := range p.Hosts {
			r := tick.Records[h]
			r.GPUPower = [6]float64{x, 0, 0, 0, 0, 0}
			r.CPUPower = [2]float64{}
			r.PSUPower = [2]float64{}
			r.GPUTemp = [6]float64{}
			r.CPUTemp = [2]float64{}
			tick.Records[h] = r
		}
		return tick
	}
	got, err := EvaluateGroup(p, makeTick(1), nil, evalDetector(t, 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.B.State != Negative || got.B.Score == nil || *got.B.Score != 1 {
		t.Fatalf("B equality should be negative: %+v", got.B)
	}
	got, err = EvaluateGroup(p, makeTick(math.Nextafter(1, math.Inf(1))), nil, evalDetector(t, 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.B.State != Positive || got.B.Score == nil || *got.B.Score <= 1 {
		t.Fatalf("B nextafter should breach: %+v", got.B)
	}
	hw, err := NewHardware([]Bound{boundFor(p.Hosts[0], "gpu0_power", "DC", "W", 0, 0), boundFor(p.Hosts[1], "gpu0_power", "DC", "W", 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	got, err = EvaluateGroup(p, makeTick(1), hw, evalDetector(t, 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Y.State != Positive || got.Y.Hosts[p.Hosts[0]] != Positive || got.Y.Hosts[p.Hosts[1]] != Positive || len(got.Y.Evidence) != 2 || *got.Y.Evidence[0].Upper != 0 {
		t.Fatalf("approved bound evidence not preserved: %+v", got.Y)
	}
}

func TestEvaluateGroupRejectsInventoryIdentityAndTimestampErrors(t *testing.T) {
	ts := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	p := evalProfile("a01")
	tests := []struct {
		name    string
		profile *GroupProfile
		tick    Tick
	}{}
	tests = append(tests, struct {
		name    string
		profile *GroupProfile
		tick    Tick
	}{"nil profile", nil, evalTick("a01", ts, 0)})
	short := *p
	short.Hosts = short.Hosts[:17]
	tests = append(tests, struct {
		name    string
		profile *GroupProfile
		tick    Tick
	}{"short inventory", &short, evalTick("a01", ts, 0)})
	reordered := *p
	reordered.Hosts = append([]string(nil), p.Hosts...)
	reordered.Hosts[0], reordered.Hosts[1] = reordered.Hosts[1], reordered.Hosts[0]
	tests = append(tests, struct {
		name    string
		profile *GroupProfile
		tick    Tick
	}{"reordered inventory", &reordered, evalTick("a01", ts, 0)})
	badTick := evalTick("a01", ts, 1)
	rec := badTick.Records[p.Hosts[0]]
	rec.Timestamp = ts.Add(10 * time.Second)
	badTick.Records[p.Hosts[0]] = rec
	tests = append(tests, struct {
		name    string
		profile *GroupProfile
		tick    Tick
	}{"record timestamp mismatch", p, badTick})
	badTick = evalTick("a01", ts, 1)
	rec = badTick.Records[p.Hosts[0]]
	rec.Hostname = p.Hosts[1]
	badTick.Records[p.Hosts[0]] = rec
	tests = append(tests, struct {
		name    string
		profile *GroupProfile
		tick    Tick
	}{"record hostname mismatch", p, badTick})
	for _, bad := range []time.Time{{}, ts.Add(time.Second)} {
		tests = append(tests, struct {
			name    string
			profile *GroupProfile
			tick    Tick
		}{"invalid tick time", p, Tick{Timestamp: bad}})
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := EvaluateGroup(tc.profile, tc.tick, nil, evalDetector(t, 1), nil); err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
}

func TestEvaluateGroupWrapperParityAndWeightedOutside(t *testing.T) {
	ts := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	p := &Profile{}
	for i := 1; i <= 4; i++ {
		g := *evalProfile(fmt.Sprintf("a%02d", i))
		p.Groups = append(p.Groups, g)
	}
	ld := evalDetector(t, 4)
	tick := Tick{Timestamp: ts, Records: map[string]domain.TelemetryRecord{}}
	for gi, g := range p.Groups {
		for i, h := range g.Hosts {
			if i < []int{18, 9, 1, 0}[gi] {
				tick.Records[h] = evalRecord(h, ts, 0)
			}
		}
	}
	rows, err := Evaluate(p, tick, nil, ld, 4, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("wrapper rows=%d", len(rows))
	}
	for i, g := range p.Groups {
		one, err := EvaluateGroup(&g, tick, nil, ld, nil)
		if err != nil {
			t.Fatal(err)
		}
		withoutOutside := rows[i]
		withoutOutside.IF.OutsideAvailable, withoutOutside.IF.OutsideAnomalous = 0, 0
		withoutOutside.IF.OutsideGroups, withoutOutside.IF.OutsideS, withoutOutside.IF.E = nil, nil, nil
		if !reflect.DeepEqual(one, withoutOutside) {
			t.Fatalf("group %d single/wrapper raw mismatch", i)
		}
	}
	// Different group sizes make the outside score a weighted host rate, not an average of group rates.
	if rows[0].IF.OutsideGroups == nil || rows[0].IF.OutsideAvailable == 0 || rows[0].IF.OutsideS == nil || rows[0].IF.E == nil {
		t.Fatalf("wrapper omitted annotation: %+v", rows[0].IF)
	}
	outsideAvailable, outsideAnomalous := 0, 0
	for _, row := range rows[1:] {
		outsideAvailable += row.IF.Available
		outsideAnomalous += row.IF.Anomalous
	}
	wantOutsideS := float64(outsideAnomalous) / float64(outsideAvailable)
	if outsideAvailable != 10 || outsideAnomalous != 3 || wantOutsideS != .3 {
		t.Fatal("fixture no longer exercises unequal outside availability")
	}
	if rows[0].IF.OutsideAvailable != outsideAvailable || rows[0].IF.OutsideAnomalous != outsideAnomalous || *rows[0].IF.OutsideS != wantOutsideS || *rows[0].IF.E != *rows[0].IF.S-wantOutsideS {
		t.Fatalf("outside support must be host weighted: got %+v, outside=(%d,%d)", rows[0].IF, outsideAvailable, outsideAnomalous)
	}
	b, err := json.Marshal(rows[0].IF)
	if err != nil {
		t.Fatal(err)
	}
	var encoded map[string]any
	_ = json.Unmarshal(b, &encoded)
	if encoded["outside_available"] == nil || encoded["outside_anomalous"] == nil {
		t.Fatalf("annotated counts must serialize numerically: %s", b)
	}
}

func TestEvaluateGroupReductionUsesInventoryRatherThanArrival(t *testing.T) {
	ts := analysis.NIRWindows()[1].Start
	profile := evalProfile("a01")
	forward, reverse := Tick{Timestamp: ts, Records: map[string]domain.TelemetryRecord{}}, Tick{Timestamp: ts, Records: map[string]domain.TelemetryRecord{}}
	for i, host := range profile.Hosts {
		r := evalRecord(host, ts, 0)
		r.GPUPower = [6]float64{}
		if i < 3 {
			r.GPUPower[0] = []float64{1e16, 1, -1e16}[i]
		}
		forward.Records[host] = r
	}
	for i := len(profile.Hosts) - 1; i >= 0; i-- {
		host := profile.Hosts[i]
		reverse.Records[host] = forward.Records[host]
	}
	detector := evalDetector(t, 1)
	a, err := EvaluateGroup(profile, forward, nil, detector, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := EvaluateGroup(profile, reverse, nil, detector, nil)
	if err != nil {
		t.Fatal(err)
	}
	// ((1e16 + 1) + -1e16) is zero in the frozen reduction order.
	if a.Vector == nil || a.Vector[0] != 0 || !reflect.DeepEqual(a, b) {
		t.Fatal("arrival/map order changed float64 reduction or evidence")
	}
}
