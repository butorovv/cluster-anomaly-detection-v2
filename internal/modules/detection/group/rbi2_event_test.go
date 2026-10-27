package group

import (
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

func journalIdentity(t *testing.T, delta, ticks int) RBI2Identity {
	t.Helper()
	w := analysis.Window{Start: analysisTick(), End: analysisTick().Add(time.Duration(ticks) * analysis.Step)}
	id, err := NewRBI2Identity(strings.Repeat("a", 64), map[string]string{"source": strings.Repeat("b", 64)}, 16, w, delta)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func journalRaw(tick int, r, b, i State) Raw {
	at := analysisTick().Add(time.Duration(tick) * analysis.Step)
	raw := rbi2Raw(at, r, b, false)
	raw.Y = YResult{Criterion: Criterion{State: Unknown}}
	if i != Unknown {
		positive := 0
		if i == Positive {
			positive = 2
		}
		raw.Local = i2Decisions(at, 18, positive)
		for h := range raw.Local {
			score := .5
			if *raw.Local[h].IsAnomaly {
				score = .75
			}
			raw.Local[h].AnomalyScore, raw.Local[h].IndividualThreshold = ptr(score), ptr(.5)
		}
	}
	raw.R.Score, raw.R.Threshold = ptr(3), ptr(1)
	raw.B.Score, raw.B.Threshold = ptr(2), ptr(1)
	raw.R.Pair = []Witness{{Hostname: "a01n17", Value: 42, Amplitude: -2}, {Hostname: "a01n18", Value: 43, Amplitude: -1}}
	raw.HostValues = map[string]HostVector{"a01n17": {1, 2, 3, 4, 5}}
	return raw
}

func TestRBI2JournalNestedRBAndVoiceSegments(t *testing.T) {
	id := journalIdentity(t, 0, 5)
	j, err := NewRBI2Journal(id, "a01", evalHosts("a01"))
	if err != nil {
		t.Fatal(err)
	}
	legacy, _ := NewEngine(id.LegacyRunID, "a01", 0, id.Window.Start)
	var actual []RBI2Event
	var historical []Event
	triples := [][3]State{{Positive, Positive, Negative}, {Positive, Negative, Positive}, {Negative, Positive, Positive}, {Positive, Positive, Negative}, {Negative, Negative, Negative}}
	for tick, states := range triples {
		raw := journalRaw(tick, states[0], states[1], states[2])
		before := i2JSON(t, raw)
		_, evidence, events, err := j.Step(raw)
		if err != nil {
			t.Fatal(err)
		}
		_, old, err := legacy.Step(raw)
		if err != nil {
			t.Fatal(err)
		}
		historical = append(historical, old...)
		actual = append(actual, events...)
		if i2JSON(t, raw) != before {
			t.Fatal("journal mutated Raw")
		}
		if tick < 4 {
			if evidence == nil || !reflect.DeepEqual(evidence.Raw, raw) || len(evidence.WitnessRefs) != 2 {
				t.Fatalf("missing physical/local evidence: %+v", evidence)
			}
			raw.HostValues["a01n17"] = HostVector{}
			*raw.R.Score = 999
			if evidence.Raw.HostValues["a01n17"][0] != 1 || *evidence.Raw.R.Score != 3 {
				t.Fatal("evidence aliases caller data")
			}
		}
	}
	if len(actual) != 1 || len(historical) != 2 {
		t.Fatalf("one parent with two children required: %d/%d", len(actual), len(historical))
	}
	e := actual[0]
	if e.DurationSeconds != 40 || e.PositiveTicks != 4 || e.ClosureReason != "negative" || !e.LeftCensoredWindow || e.CensoredMissing || e.CensoredWindow || e.NewDisjointFromRB {
		t.Fatalf("event lifecycle: %+v", e)
	}
	if len(e.VoteSegments) != 4 || len(e.RBSubspans) != 2 || !slices.Equal(e.IFHosts, []string{"a01n01", "a01n02"}) || e.IFSimultaneousMax != 2 || *e.IFScoreMax != .75 || *e.RRatioMax != 3 || *e.BRatioMax != 2 {
		t.Fatalf("event diagnostic summary: %+v", e)
	}
	for i, span := range e.RBSubspans {
		old := historical[i]
		if span.HistoricalEventID != old.ID || span.HistoricalRunID != old.RunID || span.Start != old.Detected || span.End != old.Closed {
			t.Fatalf("historical ID/interval lost: %+v vs %+v", span, old)
		}
	}
	for tick, label := range []string{"R+B", "R+I2", "B+I2", "R+B"} {
		seg := e.VoteSegments[tick]
		if seg.Votes != label || !seg.Start.Equal(id.Window.Start.Add(time.Duration(tick)*analysis.Step)) || seg.End.Sub(seg.Start) != analysis.Step {
			t.Fatal("wrong voice segment", seg)
		}
	}
	if e.ID != RBI2EventID(id.RunID, "a01", e.Detected.UTC().Format(time.RFC3339Nano)) || e.ID == historical[0].ID {
		t.Fatal("new event identity is not deterministic/separate")
	}
	if events, err := j.Flush(id.Window.End); err != nil || len(events) != 0 {
		t.Fatal("closed parent re-emitted", events, err)
	}
}

func TestRBI2JournalGapUnknownAndWindowClosures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		next   int
		i2     State
		reason string
	}{
		{"unknown", 1, Unknown, "unknown"},
		{"gap", 2, Negative, "gap"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := journalIdentity(t, 0, 4)
			j, _ := NewRBI2Journal(id, "a01", evalHosts("a01"))
			if _, _, _, err := j.Step(journalRaw(0, Positive, Negative, Positive)); err != nil {
				t.Fatal(err)
			}
			_, evidence, events, err := j.Step(journalRaw(tc.next, Positive, Negative, tc.i2))
			if err != nil || len(events) != 1 || events[0].DurationSeconds != 10 || events[0].ClosureReason != tc.reason || !events[0].CensoredMissing || evidence != nil {
				t.Fatalf("short event lost/crossed gap: %+v, %v", events, err)
			}
		})
	}
	for _, tail := range []bool{false, true} {
		id := journalIdentity(t, 0, 1)
		if tail {
			id = journalIdentity(t, 0, 3)
		}
		j, _ := NewRBI2Journal(id, "a01", evalHosts("a01"))
		_, _, _, err := j.Step(journalRaw(0, Positive, Positive, Unknown))
		if err != nil {
			t.Fatal(err)
		}
		events, err := j.Flush(id.Window.End)
		if err != nil || len(events) != 1 || events[0].DurationSeconds != 10 || len(events[0].RBSubspans) != 1 || events[0].CensoredMissing != tail || events[0].CensoredWindow == tail {
			t.Fatalf("window/tail closure: %+v, %v", events, err)
		}
		if again, err := j.Flush(id.Window.End); err != nil || len(again) != 0 {
			t.Fatal("flush not idempotent", again, err)
		}
	}
}

func TestRBI2JournalCausalWitnessReferenceAndRetry(t *testing.T) {
	id := journalIdentity(t, 30, 4)
	j, _ := NewRBI2Journal(id, "a01", evalHosts("a01"))
	_, _, _, err := j.Step(journalRaw(0, Positive, Negative, Negative))
	if err != nil {
		t.Fatal(err)
	}
	_, evidence, _, err := j.Step(journalRaw(1, Negative, Positive, Negative))
	if err != nil || evidence == nil || evidence.WitnessRefs[0].Timestamp != id.Window.Start || evidence.WitnessRefs[0].Criterion != "R" || evidence.Temporal.FirstEvidence == nil || *evidence.Temporal.FirstEvidence != id.Window.Start {
		t.Fatalf("causal pre-event reference lost: %+v %v", evidence, err)
	}
	j2, _ := NewRBI2Journal(journalIdentity(t, 0, 2), "a01", evalHosts("a01"))
	bad := journalRaw(0, Positive, Positive, Positive)
	bad.R.Score = ptr(math.NaN())
	if _, _, _, err := j2.Step(bad); err == nil {
		t.Fatal("unserializable diagnostic accepted")
	}
	if _, evidence, _, err := j2.Step(journalRaw(0, Positive, Positive, Positive)); err != nil || evidence == nil {
		t.Fatal("rejected evidence advanced journal", err)
	}
}

func TestRBI2IdentityBindingAndInputOwnership(t *testing.T) {
	id := journalIdentity(t, 0, 2)
	legacy, _ := RunID(id.ProfileSHA256, id.InputSHA256, id.Groups, id.Window)
	if id.LegacyRunID != legacy || id.RunID == legacy || id.ConfigID == legacy {
		t.Fatal("legacy identity changed or namespace collided")
	}
	j, _ := NewRBI2Journal(id, "a01", evalHosts("a01"))
	id.InputSHA256["source"] = strings.Repeat("c", 64)
	if j.identity.InputSHA256["source"] == id.InputSHA256["source"] {
		t.Fatal("journal aliases identity input")
	}
	for _, change := range []func(*RBI2Identity){
		func(i *RBI2Identity) { i.ProfileSHA256 = strings.Repeat("d", 64) },
		func(i *RBI2Identity) { i.InputSHA256["source"] = strings.Repeat("e", 64) },
		func(i *RBI2Identity) { i.Groups = 8 },
		func(i *RBI2Identity) { i.DeltaSeconds = 10 },
		func(i *RBI2Identity) { i.Window.End = i.Window.End.Add(analysis.Step) },
	} {
		base := journalIdentity(t, 0, 2)
		change(&base)
		other, err := NewRBI2Identity(base.ProfileSHA256, base.InputSHA256, base.Groups, base.Window, base.DeltaSeconds)
		if err != nil || other.RunID == j.identity.RunID {
			t.Fatal("changed contract/source did not change run ID", err)
		}
	}
	bad := journalIdentity(t, 0, 2)
	bad.Version = "old"
	if _, err := NewRBI2Journal(bad, "a01", evalHosts("a01")); err == nil {
		t.Fatal("tampered identity accepted")
	}
	if _, err := NewRBI2Identity("bad", nil, 3, analysis.Window{}, 5); err == nil {
		t.Fatal("invalid identity accepted")
	}
}
