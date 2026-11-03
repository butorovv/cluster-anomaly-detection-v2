package group

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

func processorSnapshotRoundTrip(t *testing.T, p *GroupProcessor) GroupProcessorSnapshot {
	t.Helper()
	s, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var decoded GroupProcessorSnapshot
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func processorSnapshotTick(o GroupProcessorOptions, i int) domain.GroupTick {
	count, base := 18, 0.
	if i < 2 || i >= 11 {
		base = 2
	}
	if i == 9 {
		count = 17
	} else if i == 10 {
		count = 0
	}
	return processorTick(o, i, count, base)
}

func TestProcessorSnapshotRoundTripParity(t *testing.T) {
	b, options := processorFixture(t)
	for _, delta := range []int{0, 10, 30, 60} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			o := options
			o.DeltaSeconds = delta
			continuous := mustProcessor(t, b, o)
			want := make([]GroupProcessorResult, 14)
			for i := range want {
				var err error
				want[i], err = continuous.Step(processorSnapshotTick(o, i))
				if err != nil {
					t.Fatal(err)
				}
			}
			wantFinish, err := continuous.Finish()
			if err != nil {
				t.Fatal(err)
			}
			wantFinal := processorSnapshotRoundTrip(t, continuous)
			for cut := 0; cut <= 14; cut++ {
				prefix := mustProcessor(t, b, o)
				for i := 0; i < cut; i++ {
					if _, err := prefix.Step(processorSnapshotTick(o, i)); err != nil {
						t.Fatal(err)
					}
				}
				snapshot := processorSnapshotRoundTrip(t, prefix)
				if cut == 0 && snapshot.Journal.Open != nil {
					t.Fatal("empty state unexpectedly has an open parent")
				}
				if cut == 1 && snapshot.Journal.Open == nil {
					t.Fatal("first positive tick failed to retain open parent")
				}
				resumed, err := RestoreGroupProcessor(b, o, snapshot)
				if err != nil {
					t.Fatalf("restore at cut %d: %v", cut, err)
				}
				for i := cut; i < 14; i++ {
					got, err := resumed.Step(processorSnapshotTick(o, i))
					if err != nil || !reflect.DeepEqual(got, want[i]) {
						t.Fatalf("cut %d tick %d: err=%v\ngot %#v\nwant %#v", cut, i, err, got, want[i])
					}
				}
				gotFinish, err := resumed.Finish()
				if err != nil || !reflect.DeepEqual(gotFinish, wantFinish) {
					t.Fatalf("cut %d Finish: err=%v\ngot %#v\nwant %#v", cut, err, gotFinish, wantFinish)
				}
				if got := processorSnapshotRoundTrip(t, resumed); !reflect.DeepEqual(got, wantFinal) {
					t.Fatalf("cut %d final snapshot differs", cut)
				}
			}
		})
	}
}

func TestProcessorSnapshotEarlyFinishAndEmpty(t *testing.T) {
	b, o := processorFixture(t)
	for _, ticks := range []int{0, 2, 14} {
		t.Run(fmt.Sprint(ticks), func(t *testing.T) {
			p := mustProcessor(t, b, o)
			for i := 0; i < ticks; i++ {
				if _, err := p.Step(processorSnapshotTick(o, i)); err != nil {
					t.Fatal(err)
				}
			}
			events, err := p.Finish()
			if err != nil {
				t.Fatal(err)
			}
			if ticks == 0 && len(events) != 0 {
				t.Fatal("empty finish produced events", events)
			}
			if ticks == 2 && (len(events) != 1 || events[0].ClosureReason != "missing_tail" || !events[0].Closed.Equal(o.Window.Start.Add(2*analysis.Step))) {
				t.Fatal("early finish lost missing_tail closure", events)
			}
			s := processorSnapshotRoundTrip(t, p)
			if !s.Finished {
				t.Fatal("finished processor snapshot is not marked finished")
			}
			restored, err := RestoreGroupProcessor(b, o, s)
			if err != nil {
				t.Fatal(err)
			}
			if again, err := restored.Finish(); err != nil || len(again) != 0 {
				t.Fatal("restored Finish was not idempotent", again, err)
			}
			if _, err := restored.Step(processorSnapshotTick(o, 0)); !errors.Is(err, ErrProcessorFinished) {
				t.Fatal("restored finished processor accepted Step", err)
			}
		})
	}
}

func TestProcessorSnapshotRejectsCorruptionAndConfiguration(t *testing.T) {
	b, o := processorFixture(t)
	p := mustProcessor(t, b, o)
	if _, err := p.Step(processorSnapshotTick(o, 0)); err != nil {
		t.Fatal(err)
	}
	base := processorSnapshotRoundTrip(t, p)
	mutations := map[string]func(*GroupProcessorSnapshot){
		"group": func(s *GroupProcessorSnapshot) { s.Group = "a02" },
		"binding": func(s *GroupProcessorSnapshot) { s.Binding.ExecutionID = "other" },
		"next": func(s *GroupProcessorSnapshot) { s.Next = s.Next.Add(analysis.Step) },
		"window": func(s *GroupProcessorSnapshot) { s.Window.End = s.Window.End.Add(analysis.Step) },
		"delta": func(s *GroupProcessorSnapshot) { s.DeltaSeconds = 60 },
		"identity": func(s *GroupProcessorSnapshot) { s.Identity.Groups++ },
		"journal_clock": func(s *GroupProcessorSnapshot) { s.Journal.Clock.Last = s.Journal.Clock.Last.Add(analysis.Step) },
		"nested_open": func(s *GroupProcessorSnapshot) { s.Journal.Open.LastPositive = s.Journal.Open.LastPositive.Add(analysis.Step) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			s := processorSnapshotRoundTrip(t, p)
			mutate(&s)
			if got, err := RestoreGroupProcessor(b, o, s); err == nil || got != nil {
				t.Fatal("corrupt/altered snapshot accepted", got, err)
			}
		})
	}
	other := o
	other.Binding.ExecutionID = "different-execution"
	if got, err := RestoreGroupProcessor(b, other, base); err == nil || got != nil {
		t.Fatal("different expected binding accepted", got, err)
	}
	alternateBundle, alternateOptions := processorFixture(t)
	if got, err := RestoreGroupProcessor(alternateBundle, alternateOptions, base); err == nil || got != nil {
		t.Fatal("snapshot from separately loaded bundle accepted", got, err)
	}
}

func TestProcessorSnapshotFailureAndInputRejection(t *testing.T) {
	b, o := processorFixture(t)
	p, control := mustProcessor(t, b, o), mustProcessor(t, b, o)
	first := processorSnapshotTick(o, 0)
	if _, err := p.Step(first); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Step(first); err != nil {
		t.Fatal(err)
	}
	bad := processorSnapshotTick(o, 1)
	bad.Timestamp = bad.Timestamp.Add(time.Second)
	if _, err := p.Step(bad); !errors.Is(err, ErrProcessorInput) {
		t.Fatal("invalid input was not rejected", err)
	}
	processorEqual(t, processorSnapshotRoundTrip(t, p), processorSnapshotRoundTrip(t, control))

	failed := mustProcessor(t, b, o)
	tick := processorSnapshotTick(o, 0)
	for host, record := range tick.Records {
		features := record.Features()
		for i := range features {
			features[i] = math.MaxFloat64
		}
		record.SetFeatures(features)
		tick.Records[host] = record
	}
	if _, err := failed.Step(tick); !errors.Is(err, ErrProcessorFailed) {
		t.Fatal("expected computation failure", err)
	}
	if _, err := failed.Snapshot(); err == nil {
		t.Fatal("failed processor produced a snapshot")
	}
}

func TestProcessorSnapshotCopyIndependence(t *testing.T) {
	b, o := processorFixture(t)
	source := mustProcessor(t, b, o)
	if _, err := source.Step(processorSnapshotTick(o, 0)); err != nil {
		t.Fatal(err)
	}
	before := processorSnapshotRoundTrip(t, source)
	s, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	s.Identity.InputSHA256[o.GroupID] = "mutated"
	s.Journal.RHosts[0] = "mutated"
	s.Journal.Clock.Hosts[0] = "mutated"
	s.Journal.Open.IFHosts[0] = "mutated"
	if got := processorSnapshotRoundTrip(t, source); !reflect.DeepEqual(got, before) {
		t.Fatal("mutating snapshot/nested maps/slices changed source processor")
	}

	s = processorSnapshotRoundTrip(t, source)
	restored, err := RestoreGroupProcessor(b, o, s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Step(processorSnapshotTick(o, 1)); err != nil {
		t.Fatal(err)
	}
	if got := processorSnapshotRoundTrip(t, source); !reflect.DeepEqual(got, before) {
		t.Fatal("restored Step changed source processor")
	}
	if _, err := restored.Finish(); err != nil {
		t.Fatal(err)
	}
	if got := processorSnapshotRoundTrip(t, source); !reflect.DeepEqual(got, before) {
		t.Fatal("restored Finish changed source processor")
	}
}
