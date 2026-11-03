package group

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/local"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

// Reuse the existing small on-disk frozen bundle fixture. Only synthetic model
// files in t.TempDir are changed; no production asset is trained or rewritten.
func processorFixture(t *testing.T) (*Bundle, GroupProcessorOptions) {
	t.Helper()
	files, _, _ := bundleFixture(t)
	lp, err := local.LoadProfile(files.B1.Path)
	if err != nil {
		t.Fatal(err)
	}
	for i, host := range lp.Binding.Hosts {
		lp.Observations[host], lp.Thresholds[host] = 2, .5
		if i%18 < 2 {
			lp.Thresholds[host] = .49
		}
	}
	overwriteJSON(t, files.B1.Path, lp)
	p, err := LoadProfile(files.ProfilePath)
	if err != nil {
		t.Fatal(err)
	}
	g := &p.Groups[0]
	g.Observations, g.RThreshold, g.Rank, g.BThreshold, g.BReason = 20, 1, Dimensions, ptr(1), ""
	g.Eigen = make([]float64, Dimensions)
	base, err := HostChannels(evalRecord("a01n01", analysis.NIRWindows()[1].Start, 0))
	if err != nil {
		t.Fatal(err)
	}
	for h := range g.HostMedian {
		g.HostMedian[h] = base
		for k := range g.HostScale[h] {
			g.HostScale[h][k] = Floor
		}
	}
	copy(g.Median[:], base[:])
	for k := range g.Scale {
		g.Scale[k], g.Eigen[k], g.Covariance[k][k], g.Inverse[k][k] = Floor, 1, 1, 1
	}
	p.InputSHA[files.B1.InputKey] = mustSHA(t, files.B1.Path)
	overwriteJSON(t, files.ProfilePath, p)
	files.ProfileSHA256 = mustSHA(t, files.ProfilePath)
	bundle, err := LoadBundle(files)
	if err != nil {
		t.Fatal(err)
	}
	start := analysis.NIRWindows()[1].Start
	return bundle, GroupProcessorOptions{GroupID: "a01", Groups: 16,
		Window:  analysis.Window{Start: start, End: start.Add(14 * analysis.Step)},
		Binding: domain.InputBinding{ExecutionID: "p05-synthetic", InputSHA256: p.Binding.PreparedSHA256, ProfileSHA256: files.ProfileSHA256}}
}

func processorTick(o GroupProcessorOptions, index, count int, base float64) domain.GroupTick {
	tick := domain.GroupTick{GroupID: o.GroupID, Binding: o.Binding, Timestamp: o.Window.Start.Add(time.Duration(index) * analysis.Step),
		Records: map[string]domain.TelemetryRecord{}, Missing: []string{}, ValidRecords: count}
	for i, host := range evalHosts(o.GroupID) {
		if i < count {
			tick.Records[host] = evalRecord(host, tick.Timestamp, base)
		} else {
			tick.Missing = append(tick.Missing, host)
		}
	}
	return tick
}

func mustProcessor(t *testing.T, b *Bundle, o GroupProcessorOptions) *GroupProcessor {
	t.Helper()
	p, err := NewGroupProcessor(b, o)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func processorEqual(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		g, _ := json.Marshal(got)
		w, _ := json.Marshal(want)
		t.Fatalf("processor differs from direct frozen composition:\ngot %s\nwant %s", g, w)
	}
}

func TestGroupProcessorFrozenParity(t *testing.T) {
	b, options := processorFixture(t)
	for _, delta := range []int{0, 10, 30, 60} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			o := options
			o.DeltaSeconds = delta
			p := mustProcessor(t, b, o)
			id, err := NewRBI2Identity(b.profileSHA, b.profile.InputSHA, o.Groups, o.Window, delta)
			if err != nil {
				t.Fatal(err)
			}
			j, err := NewRBI2Journal(id, o.GroupID, b.profile.Groups[0].Hosts)
			if err != nil {
				t.Fatal(err)
			}
			evidenceCount, eventCount, rbCount, extended := 0, 0, 0, false
			for i := 0; i < 14; i++ {
				count, base := 18, 0.
				if i < 2 || i >= 11 {
					base = 2
				}
				if i == 9 {
					count = 17
				} else if i == 10 {
					count = 0
				}
				tick := processorTick(o, i, count, base)
				raw, err := b.EvaluateGroup(o.GroupID, Tick{Timestamp: tick.Timestamp, Records: tick.Records}, nil)
				if err != nil {
					t.Fatal(err)
				}
				tr, evidence, events, err := j.Step(raw)
				if err != nil {
					t.Fatal(err)
				}
				got, err := p.Step(tick)
				if err != nil {
					t.Fatal(err)
				}
				// Exact full local payload comparison: outside is uncomputed in both.
				processorEqual(t, got, GroupProcessorResult{Raw: raw, Temporal: tr, Evidence: evidence, Events: events})
				if evidence != nil {
					evidenceCount++
				}
				extended = extended || (i == 2 && tr.Vote == Positive)
				eventCount += len(events)
				for _, e := range events {
					rbCount += len(e.RBSubspans)
				}
			}
			want, err := j.Flush(o.Window.End)
			if err != nil {
				t.Fatal(err)
			}
			got, err := p.Finish()
			if err != nil {
				t.Fatal(err)
			}
			processorEqual(t, got, want)
			for _, e := range got {
				rbCount += len(e.RBSubspans)
			}
			if evidenceCount == 0 || eventCount == 0 || rbCount == 0 || (delta > 0 && !extended) {
				t.Fatalf("fixture did not exercise evidence/events/RB/TTL: %d/%d/%d/%t", evidenceCount, eventCount, rbCount, extended)
			}
		})
	}
}

func TestGroupProcessorMissingAndOutside(t *testing.T) {
	b, o := processorFixture(t)
	p := mustProcessor(t, b, o)
	for i, count := range []int{18, 17, 18, 0} {
		tick := processorTick(o, i, count, 2)
		if i == 2 {
			r := tick.Records["a01n18"]
			r.GPUPower[0], r.Valid = math.NaN(), false
			r.AvailabilityMask &^= 1 << 4
			tick.Records[r.Hostname], tick.ValidRecords = r, 17
		}
		got, err := p.Step(tick)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if got.Temporal.Vote != Positive || got.Evidence == nil || got.Raw.Complete != 18 {
				t.Fatal("full fixture was not positive")
			}
		} else if got.Raw.R.State != Unknown || got.Raw.B.State != Unknown || got.Temporal.LocalI2.State != Unknown || got.Temporal.Vote != Unknown {
			t.Fatal("missing/partial input became a known decision", got)
		}
		if i == 2 && (len(tick.Missing) != 0 || !slices.Equal(got.Raw.Missing, []string{"a01n18"})) {
			t.Fatal("record absence was confused with unusable group channels")
		}
		if i == 3 && (len(got.Raw.Missing) != 18 || got.Raw.IF.Available != 0) {
			t.Fatal("empty tick was lost or filled with values")
		}
		payloads := []Raw{got.Raw}
		if got.Evidence != nil {
			payloads = append(payloads, got.Evidence.Raw)
		}
		for _, raw := range payloads {
			encoded, err := json.Marshal(raw.IF)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"outside_available", "outside_anomalous", "outside_groups", "outside_s", "e"} {
				value, exists := fields[key]
				if !exists || value != nil {
					t.Fatalf("uncomputed %s was not null: %s", key, encoded)
				}
			}
		}
	}
}

func TestGroupProcessorRejectsInputWithoutTransition(t *testing.T) {
	b, o := processorFixture(t)
	cases := map[string]func(*domain.GroupTick){
		"group":       func(t *domain.GroupTick) { t.GroupID = "a02" },
		"execution":   func(t *domain.GroupTick) { t.Binding.ExecutionID = "other" },
		"input_sha":   func(t *domain.GroupTick) { t.Binding.InputSHA256 = strings.Repeat("0", 64) },
		"profile_sha": func(t *domain.GroupTick) { t.Binding.ProfileSHA256 = strings.Repeat("0", 64) },
		"duplicate":   func(t *domain.GroupTick) { t.Timestamp = o.Window.Start },
		"past":        func(t *domain.GroupTick) { t.Timestamp = o.Window.Start.Add(-analysis.Step) },
		"gap":         func(t *domain.GroupTick) { t.Timestamp = t.Timestamp.Add(analysis.Step) },
		"end":         func(t *domain.GroupTick) { t.Timestamp = o.Window.End },
		"off_grid":    func(t *domain.GroupTick) { t.Timestamp = t.Timestamp.Add(time.Second) },
		"non_utc":     func(t *domain.GroupTick) { t.Timestamp = t.Timestamp.In(time.FixedZone("MSK", 3*3600)) },
		"counts":      func(t *domain.GroupTick) { t.ValidRecords-- },
		"missing_present": func(t *domain.GroupTick) {
			t.Missing = []string{"a01n01"}
		},
		"missing_omitted": func(t *domain.GroupTick) {
			delete(t.Records, "a01n18")
			t.ValidRecords--
		},
		"missing_order": func(t *domain.GroupTick) {
			delete(t.Records, "a01n17")
			delete(t.Records, "a01n18")
			t.ValidRecords -= 2
			t.Missing = []string{"a01n18", "a01n17"}
		},
		"foreign_host": func(t *domain.GroupTick) { t.Records["a02n01"] = evalRecord("a02n01", t.Timestamp, 2) },
		"record_key": func(t *domain.GroupTick) {
			r := t.Records["a01n01"]
			r.Hostname = "a01n02"
			t.Records["a01n01"] = r
		},
		"record_group": func(t *domain.GroupTick) {
			r := t.Records["a01n01"]
			r.GroupID = "a02"
			t.Records["a01n01"] = r
		},
		"record_index": func(t *domain.GroupTick) {
			r := t.Records["a01n01"]
			r.NodeIndex = 2
			t.Records["a01n01"] = r
		},
		"record_time": func(t *domain.GroupTick) {
			r := t.Records["a01n01"]
			r.Timestamp = r.Timestamp.Add(analysis.Step)
			t.Records["a01n01"] = r
		},
		"availability": func(t *domain.GroupTick) {
			r := t.Records["a01n01"]
			r.AvailabilityMask = 0
			t.Records["a01n01"] = r
		},
		"infinity": func(t *domain.GroupTick) {
			r := t.Records["a01n01"]
			r.GPUPower[0] = math.Inf(1)
			t.Records["a01n01"] = r
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			p, control := mustProcessor(t, b, o), mustProcessor(t, b, o)
			first := processorTick(o, 0, 18, 2)
			if _, err := p.Step(first); err != nil {
				t.Fatal(err)
			}
			if _, err := control.Step(first); err != nil {
				t.Fatal(err)
			}
			bad := processorTick(o, 1, 18, 2)
			corrupt(&bad)
			result, err := p.Step(bad)
			if !errors.Is(err, ErrProcessorInput) || !reflect.DeepEqual(result, GroupProcessorResult{}) {
				t.Fatal("expected atomic input rejection", result, err)
			}
			for i := 1; i < 3; i++ {
				tick := processorTick(o, i, 18, 2)
				got, err := p.Step(tick)
				if err != nil {
					t.Fatal(err)
				}
				want, err := control.Step(tick)
				if err != nil {
					t.Fatal(err)
				}
				processorEqual(t, got, want)
			}
			got, err := p.Finish()
			if err != nil {
				t.Fatal(err)
			}
			want, err := control.Finish()
			if err != nil {
				t.Fatal(err)
			}
			processorEqual(t, got, want)
		})
	}
}

func TestGroupProcessorFinish(t *testing.T) {
	b, o := processorFixture(t)
	for _, ticks := range []int{0, 2, 14} {
		t.Run(fmt.Sprint(ticks), func(t *testing.T) {
			p := mustProcessor(t, b, o)
			var eventID string
			for i := 0; i < ticks; i++ {
				got, err := p.Step(processorTick(o, i, 18, 2))
				if err != nil {
					t.Fatal(err)
				}
				if got.Evidence == nil {
					t.Fatal("expected open parent evidence")
				}
				if i == 0 {
					eventID = got.Evidence.EventID
				} else if got.Evidence.EventID != eventID {
					t.Fatal("continuation changed parent ID")
				}
			}
			events, err := p.Finish()
			if err != nil {
				t.Fatal(err)
			}
			if ticks == 0 {
				if len(events) != 0 {
					t.Fatal("empty run created an event")
				}
			} else {
				if len(events) != 1 {
					t.Fatal("expected one closed parent", events)
				}
				e := events[0]
				closed, reason := o.Window.Start.Add(time.Duration(ticks)*analysis.Step), "missing_tail"
				if ticks == 14 {
					closed, reason = o.Window.End, "window"
				}
				if e.ID != eventID || e.Closed != closed || e.ClosureReason != reason || e.PositiveTicks != ticks ||
					len(e.VoteSegments) != 1 || e.VoteSegments[0].End != closed || len(e.RBSubspans) != 1 ||
					e.RBSubspans[0].End != closed || e.RBSubspans[0].HistoricalEventID == "" {
					t.Fatal("Finish changed frozen identity/boundaries/segments/RB projection", e)
				}
			}
			if again, err := p.Finish(); err != nil || len(again) != 0 {
				t.Fatal("Finish emitted a duplicate", again, err)
			}
			if _, err := p.Step(processorTick(o, 0, 18, 2)); !errors.Is(err, ErrProcessorFinished) {
				t.Fatal("Step accepted after Finish", err)
			}
		})
	}
}

func TestGroupProcessorFailureLatches(t *testing.T) {
	b, o := processorFixture(t)
	for _, phase := range []string{"evaluate", "journal", "flush"} {
		t.Run(phase, func(t *testing.T) {
			p := mustProcessor(t, b, o)
			tick := processorTick(o, 0, 18, 2)
			var err error
			switch phase {
			case "evaluate":
				// Individually finite/valid F18, but group reduction overflows.
				for host, r := range tick.Records {
					f := r.Features()
					for i := range f {
						f[i] = 1e307
					}
					r.SetFeatures(f)
					tick.Records[host] = r
				}
				_, err = p.Step(tick)
			case "journal":
				if _, err := p.Step(tick); err != nil {
					t.Fatal(err)
				}
				// Test-only fault: legacy has an open RB event but parent is lost.
				// The next Journal transition fails after touching private state.
				p.journal.open = nil
				_, err = p.Step(processorTick(o, 1, 18, 0))
			case "flush":
				p.journal.clock.end = o.Window.End.Add(analysis.Step)
				_, err = p.Finish()
			}
			if !errors.Is(err, ErrProcessorFailed) {
				t.Fatal("expected latched computation error", err)
			}
			if _, next := p.Step(processorTick(o, 0, 18, 2)); next != err {
				t.Fatal("failed processor resumed or lost original error", next)
			}
			if _, next := p.Finish(); next != err {
				t.Fatal("failed processor flushed or lost original error", next)
			}
		})
	}
}

func TestGroupProcessorConstructor(t *testing.T) {
	b, options := processorFixture(t)
	cases := map[string]func(*Bundle, *GroupProcessorOptions){
		"group":        func(_ *Bundle, o *GroupProcessorOptions) { o.GroupID = "a17" },
		"noncanonical": func(_ *Bundle, o *GroupProcessorOptions) { o.GroupID = "a1" },
		"universe":     func(_ *Bundle, o *GroupProcessorOptions) { o.Groups, o.GroupID = 4, "a05" },
		"groups":       func(_ *Bundle, o *GroupProcessorOptions) { o.Groups = 3 },
		"delta":        func(_ *Bundle, o *GroupProcessorOptions) { o.DeltaSeconds = 20 },
		"window":       func(_ *Bundle, o *GroupProcessorOptions) { o.Window.End = o.Window.Start },
		"zero_window":  func(_ *Bundle, o *GroupProcessorOptions) { o.Window = analysis.Window{} },
		"grid":         func(_ *Bundle, o *GroupProcessorOptions) { o.Window.Start = o.Window.Start.Add(time.Second) },
		"timezone": func(_ *Bundle, o *GroupProcessorOptions) {
			o.Window.Start = o.Window.Start.In(time.FixedZone("MSK", 3*3600))
		},
		"execution": func(_ *Bundle, o *GroupProcessorOptions) { o.Binding.ExecutionID = "" },
		"profile_sha": func(_ *Bundle, o *GroupProcessorOptions) {
			o.Binding.ProfileSHA256 = strings.Repeat("0", 64)
		},
		"input_sha": func(_ *Bundle, o *GroupProcessorOptions) {
			o.Binding.InputSHA256 = strings.Repeat("0", 64)
		},
		"version":  func(b *Bundle, _ *GroupProcessorOptions) { b.profile.Version = "other" },
		"detector": func(b *Bundle, _ *GroupProcessorOptions) { b.local = nil },
		"hardware": func(b *Bundle, _ *GroupProcessorOptions) { b.hardware = nil },
		"hosts": func(b *Bundle, _ *GroupProcessorOptions) {
			b.profile.Groups = slices.Clone(b.profile.Groups)
			b.profile.Groups[0].Hosts = slices.Clone(b.profile.Groups[0].Hosts)
			slices.Reverse(b.profile.Groups[0].Hosts)
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			candidate, profile, o := *b, *b.profile, options
			candidate.profile = &profile
			corrupt(&candidate, &o)
			if p, err := NewGroupProcessor(&candidate, o); p != nil || !errors.Is(err, ErrProcessorInput) {
				t.Fatal("invalid constructor accepted", p, err)
			}
		})
	}
	for _, bad := range []*Bundle{nil, {}} {
		if p, err := NewGroupProcessor(bad, options); p != nil || !errors.Is(err, ErrProcessorInput) {
			t.Fatal("nil/unloaded bundle accepted", p, err)
		}
	}
	for _, count := range []int{4, 8, 16} {
		o := options
		o.Groups, o.GroupID = count, fmt.Sprintf("a%02d", count)
		mustProcessor(t, b, o)
	}
	other := options
	other.Binding.ExecutionID = "another-execution"
	p, q := mustProcessor(t, b, options), mustProcessor(t, b, other)
	processorEqual(t, p.journal.identity, q.journal.identity)
	for _, p := range []*GroupProcessor{nil, {}} {
		if _, err := p.Step(domain.GroupTick{}); !errors.Is(err, ErrProcessorFailed) {
			t.Fatal("uninitialized Step accepted", err)
		}
		if _, err := p.Finish(); !errors.Is(err, ErrProcessorFailed) {
			t.Fatal("uninitialized Finish accepted", err)
		}
	}
}

func TestGroupProcessorOutputOwnership(t *testing.T) {
	b, o := processorFixture(t)
	p, control := mustProcessor(t, b, o), mustProcessor(t, b, o)
	tick := processorTick(o, 0, 18, 2)
	got, err := p.Step(tick)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.Step(processorTick(o, 0, 18, 2)); err != nil {
		t.Fatal(err)
	}
	clear(tick.Records)
	got.Raw.R.Pair[0].Hostname = "corrupt"
	got.Raw.Local[0].Hostname = "corrupt"
	*got.Raw.Local[0].IsAnomaly = false
	got.Raw.IF.Available = 0
	clear(got.Raw.HostValues)
	got.Temporal.LocalI2.PositiveHosts[0] = "corrupt"
	*got.Temporal.R.Witness = o.Window.End
	*got.Temporal.FirstEvidence = o.Window.End
	got.Evidence.Raw.Local[0].Hostname = "corrupt"
	got.Evidence.Temporal.ActiveVotes[0] = "corrupt"
	for i := 1; i < 3; i++ {
		tick := processorTick(o, i, 18, 2)
		result, err := p.Step(tick)
		if err != nil {
			t.Fatal(err)
		}
		want, err := control.Step(tick)
		if err != nil {
			t.Fatal(err)
		}
		processorEqual(t, result, want)
	}
	events, err := p.Finish()
	if err != nil {
		t.Fatal(err)
	}
	want, err := control.Finish()
	if err != nil {
		t.Fatal(err)
	}
	processorEqual(t, events, want)
}
