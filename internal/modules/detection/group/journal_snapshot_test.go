package group

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

func fullSnapshotJSON[T any](t *testing.T, s T) T {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	again, err := json.Marshal(out)
	if err != nil || !bytes.Equal(b, again) {
		t.Fatal("unstable snapshot JSON", err)
	}
	return out
}

func sealLegacyTest(t *testing.T, s *EngineSnapshot) {
	t.Helper()
	s.SHA256 = ""
	var err error
	s.SHA256, err = mathSnapshotHash(*s)
	if err != nil {
		t.Fatal(err)
	}
}
func sealJournalTest(t *testing.T, s *RBI2JournalSnapshot) {
	t.Helper()
	s.SHA256 = ""
	var err error
	s.SHA256, err = mathSnapshotHash(*s)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFullSnapshotLegacyParity(t *testing.T) {
	start := analysisTick()
	for _, delta := range []int{0, 10, 30, 60} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			opts := EngineRestoreOptions{Config: "legacy-test", Group: "a01", DeltaSeconds: delta, Window: analysis.Window{Start: start, End: start.Add(120 * time.Second)}}
			rows := []Raw{
				temporalRaw(start, Positive, Unknown, Positive),
				temporalRaw(start.Add(10*time.Second), Positive, Positive, Negative),
				temporalRaw(start.Add(20*time.Second), Negative, Negative, Positive),
				temporalRaw(start.Add(30*time.Second), Unknown, Unknown, Unknown),
				temporalRaw(start.Add(40*time.Second), Positive, Unknown, Negative),
				temporalRaw(start.Add(50*time.Second), Negative, Unknown, Positive),
				temporalRaw(start.Add(70*time.Second), Positive, Unknown, Positive),
				temporalRaw(start.Add(80*time.Second), Negative, Unknown, Negative),
				temporalRaw(start.Add(90*time.Second), Positive, Unknown, Positive),
				temporalRaw(start.Add(100*time.Second), Positive, Unknown, Positive),
			}
			for i := range rows {
				rows[i].R.Pair = []Witness{{Hostname: "a01n01", ZeroMAD: i%2 == 0}, {Hostname: "a01n02"}}
				rows[i].R.Witnesses = []Witness{{Hostname: "a01n17"}}
				rows[i].Y.Hosts = map[string]State{"a01n03": Positive, "a01n04": Positive}
				rows[i].IF = Support{Available: 18, Anomalous: 2}
			}
			e, err := NewEngine(opts.Config, opts.Group, delta, start)
			if err != nil {
				t.Fatal(err)
			}
			type result struct {
				Temporal TemporalResult
				Events   []Event
			}
			want := make([]result, len(rows))
			for i, r := range rows {
				tr, ev, err := e.Step(r)
				if err != nil {
					t.Fatal(err)
				}
				want[i] = result{tr, ev}
			}
			tail, err := e.Flush(opts.Window.End)
			if err != nil {
				t.Fatal(err)
			}
			final, err := e.Snapshot(opts.Window.End)
			if err != nil {
				t.Fatal(err)
			}
			for cut := 0; cut <= len(rows); cut++ {
				prefix, _ := NewEngine(opts.Config, opts.Group, delta, start)
				for _, r := range rows[:cut] {
					if _, _, err := prefix.Step(r); err != nil {
						t.Fatal(err)
					}
				}
				s, err := prefix.Snapshot(opts.Window.End)
				if err != nil {
					t.Fatal(err)
				}
				resumed, err := RestoreEngine(fullSnapshotJSON(t, s), opts)
				if err != nil {
					t.Fatal(err)
				}
				for i := cut; i < len(rows); i++ {
					tr, ev, err := resumed.Step(rows[i])
					if err != nil || !reflect.DeepEqual(result{tr, ev}, want[i]) {
						t.Fatalf("delta%d cut%d tick%d err=%v", delta, cut, i, err)
					}
				}
				got, err := resumed.Flush(opts.Window.End)
				if err != nil || !reflect.DeepEqual(got, tail) {
					t.Fatal("legacy tail", cut, got, tail, err)
				}
				actual, err := resumed.Snapshot(opts.Window.End)
				if err != nil || !reflect.DeepEqual(actual, final) {
					t.Fatal("legacy final state", cut, err)
				}
				sealed, err := RestoreEngine(fullSnapshotJSON(t, actual), opts)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := sealed.Step(rows[len(rows)-1]); err == nil {
					t.Fatal("finished legacy resumed")
				}
			}
		})
	}
}

func TestFullSnapshotJournalParity(t *testing.T) {
	for _, delta := range []int{0, 10, 30, 60} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			id := journalIdentity(t, delta, 12)
			rows := []Raw{
				journalRaw(0, Positive, Positive, Negative), journalRaw(1, Positive, Negative, Positive),
				journalRaw(2, Negative, Positive, Positive), journalRaw(3, Positive, Positive, Negative),
				journalRaw(4, Unknown, Unknown, Unknown), journalRaw(5, Positive, Negative, Positive),
				journalRaw(6, Negative, Positive, Unknown), journalRaw(8, Positive, Positive, Positive),
				journalRaw(9, Positive, Negative, Positive), journalRaw(10, Negative, Positive, Positive),
			}
			type result struct {
				Temporal RBI2TemporalResult
				Evidence *RBI2Evidence
				Events   []RBI2Event
			}
			baseline, err := NewRBI2Journal(id, "a01", evalHosts("a01"))
			if err != nil {
				t.Fatal(err)
			}
			want := make([]result, len(rows))
			evidenceCount, rbCount, openCuts := 0, 0, 0
			for i, r := range rows {
				tr, ev, es, err := baseline.Step(r)
				if err != nil {
					t.Fatal(err)
				}
				want[i] = result{tr, ev, es}
				if ev != nil {
					evidenceCount++
				}
				for _, e := range es {
					rbCount += len(e.RBSubspans)
				}
			}
			tail, err := baseline.Flush(id.Window.End)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range tail {
				rbCount += len(e.RBSubspans)
			}
			final, err := baseline.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			for cut := 0; cut <= len(rows); cut++ {
				prefix, err := NewRBI2Journal(id, "a01", evalHosts("a01"))
				if err != nil {
					t.Fatal(err)
				}
				for _, r := range rows[:cut] {
					if _, _, _, err := prefix.Step(r); err != nil {
						t.Fatal(err)
					}
				}
				s, err := prefix.Snapshot()
				if err != nil {
					t.Fatal("snapshot", cut, err)
				}
				if s.Open != nil {
					openCuts++
				}
				resumed, err := RestoreRBI2Journal(fullSnapshotJSON(t, s), id, "a01", evalHosts("a01"))
				if err != nil {
					t.Fatal(err)
				}
				for i := cut; i < len(rows); i++ {
					tr, ev, es, err := resumed.Step(rows[i])
					if err != nil || !reflect.DeepEqual(result{tr, ev, es}, want[i]) {
						t.Fatalf("delta%d cut%d tick%d err=%v", delta, cut, i, err)
					}
				}
				got, err := resumed.Flush(id.Window.End)
				if err != nil || !reflect.DeepEqual(got, tail) {
					t.Fatal("journal tail", cut, got, tail, err)
				}
				actual, err := resumed.Snapshot()
				if err != nil || !reflect.DeepEqual(actual, final) {
					t.Fatal("journal final", cut, err)
				}
				sealed, err := RestoreRBI2Journal(fullSnapshotJSON(t, actual), id, "a01", evalHosts("a01"))
				if err != nil {
					t.Fatal(err)
				}
				if _, _, _, err := sealed.Step(rows[len(rows)-1]); err == nil {
					t.Fatal("sealed journal resumed")
				}
			}
			if evidenceCount == 0 || rbCount == 0 || openCuts == 0 {
				t.Fatal("fixture missed evidence/RB/open parents", evidenceCount, rbCount, openCuts)
			}
		})
	}
}

func TestFullSnapshotRejectsCorruption(t *testing.T) {
	id := journalIdentity(t, 30, 12)
	j, _ := NewRBI2Journal(id, "a01", evalHosts("a01"))
	if _, _, _, err := j.Step(journalRaw(0, Positive, Positive, Positive)); err != nil {
		t.Fatal(err)
	}
	base, err := j.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*RBI2JournalSnapshot){
		"hash":           func(s *RBI2JournalSnapshot) { s.SHA256 = strings.Repeat("0", 64) },
		"version":        func(s *RBI2JournalSnapshot) { s.SchemaVersion++; sealJournalTest(t, s) },
		"identity":       func(s *RBI2JournalSnapshot) { s.Identity.ConfigID = "wrong"; sealJournalTest(t, s) },
		"position":       func(s *RBI2JournalSnapshot) { s.Last = s.Last.Add(analysis.Step); sealJournalTest(t, s) },
		"clock_delta":    func(s *RBI2JournalSnapshot) { s.Clock.DeltaSeconds = 60; sealJournalTest(t, s) },
		"clock_memory":   func(s *RBI2JournalSnapshot) { s.Clock.States[0].Positive = time.Time{}; sealJournalTest(t, s) },
		"parent_lost":    func(s *RBI2JournalSnapshot) { s.Open = nil; sealJournalTest(t, s) },
		"parent_id":      func(s *RBI2JournalSnapshot) { s.Open.ID = "wrong"; sealJournalTest(t, s) },
		"parent_counter": func(s *RBI2JournalSnapshot) { s.Open.PositiveTicks++; sealJournalTest(t, s) },
		"segment_gap": func(s *RBI2JournalSnapshot) {
			s.Open.VoteSegments[0].Start = s.Open.VoteSegments[0].Start.Add(analysis.Step)
			sealJournalTest(t, s)
		},
		"parent_host":   func(s *RBI2JournalSnapshot) { s.Open.IFHosts = []string{"a02n01"}; sealJournalTest(t, s) },
		"witness_cache": func(s *RBI2JournalSnapshot) { s.IHosts = nil; sealJournalTest(t, s) },
		"rb_id": func(s *RBI2JournalSnapshot) {
			s.Legacy.Open.ID = "wrong"
			sealLegacyTest(t, &s.Legacy)
			sealJournalTest(t, s)
		},
		"legacy_R": func(s *RBI2JournalSnapshot) {
			s.Legacy.States[0].Positive = time.Time{}
			s.Legacy.States[0].Hosts = nil
			s.Legacy.States[0].Zero = false
			s.Legacy.Open = nil
			sealLegacyTest(t, &s.Legacy)
			sealJournalTest(t, s)
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			s := fullSnapshotJSON(t, base)
			change(&s)
			if got, err := RestoreRBI2Journal(s, id, "a01", evalHosts("a01")); err == nil || got != nil {
				t.Fatal("corruption accepted", got, err)
			}
		})
	}
	legacy := base.Legacy
	opts := EngineRestoreOptions{Config: id.LegacyRunID, Group: "a01", DeltaSeconds: 30, Window: id.Window}
	for _, change := range []func(*EngineSnapshot){
		func(s *EngineSnapshot) { s.SchemaVersion++ },
		func(s *EngineSnapshot) { s.Open.RawCoincidentTicks++ },
		func(s *EngineSnapshot) { s.States[2].Hosts = []string{"a01n01"} },
		func(s *EngineSnapshot) { s.States[0].Unknown = s.Last },
		func(s *EngineSnapshot) { s.Finished = true },
	} {
		s := fullSnapshotJSON(t, legacy)
		change(&s)
		sealLegacyTest(t, &s)
		if got, err := RestoreEngine(s, opts); err == nil || got != nil {
			t.Fatal("invalid legacy accepted", got, err)
		}
	}
}

func TestFullSnapshotStrictJSON(t *testing.T) {
	id := journalIdentity(t, 0, 3)
	j, _ := NewRBI2Journal(id, "a01", evalHosts("a01"))
	if _, _, _, err := j.Step(journalRaw(0, Positive, Positive, Positive)); err != nil {
		t.Fatal(err)
	}
	s, err := j.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	reject := func(data []byte) {
		t.Helper()
		var out RBI2JournalSnapshot
		if err := json.Unmarshal(data, &out); err == nil {
			t.Fatalf("invalid JSON accepted: %s", data)
		}
	}
	reject([]byte("{"))
	reject([]byte("null"))
	reject(append([]byte(`{"last":"0001-01-01T00:00:00Z",`), encoded[1:]...))
	var root map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &root); err != nil {
		t.Fatal(err)
	}
	for key := range root {
		m := make(map[string]json.RawMessage)
		for k, v := range root {
			m[k] = v
		}
		delete(m, key)
		bad, _ := json.Marshal(m)
		reject(bad)
	}
	for _, location := range []string{"identity", "open", "legacy"} {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(root[location], &object); err != nil {
			t.Fatal(err)
		}
		for key := range object {
			obj := make(map[string]json.RawMessage)
			for k, v := range object {
				obj[k] = v
			}
			delete(obj, key)
			bad, _ := json.Marshal(obj)
			reject(bytes.Replace(encoded, root[location], bad, 1))
		}
	}
	var old map[string]json.RawMessage
	_ = json.Unmarshal(root["legacy"], &old)
	var event map[string]json.RawMessage
	_ = json.Unmarshal(old["open"], &event)
	delete(event, "positive_ticks")
	bad, _ := json.Marshal(event)
	reject(bytes.Replace(encoded, old["open"], bad, 1))
	// A duplicate map key inside input hashes must also fail, even if values agree.
	var identity map[string]json.RawMessage
	_ = json.Unmarshal(root["identity"], &identity)
	hashes := identity["input_sha256"]
	repeated := append([]byte(`{"source":"`+strings.Repeat("b", 64)+`",`), hashes[1:]...)
	reject(bytes.Replace(encoded, hashes, repeated, 1))
}

func TestFullSnapshotDeepCopies(t *testing.T) {
	id := journalIdentity(t, 30, 8)
	j, _ := NewRBI2Journal(id, "a01", evalHosts("a01"))
	if _, _, _, err := j.Step(journalRaw(0, Positive, Positive, Positive)); err != nil {
		t.Fatal(err)
	}
	before, err := j.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	s, err := j.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreRBI2Journal(s, id, "a01", evalHosts("a01"))
	if err != nil {
		t.Fatal(err)
	}
	s.Identity.InputSHA256["source"] = "changed"
	s.RHosts[0] = "changed"
	s.IHosts[0] = "changed"
	s.Open.VoteSegments[0].Votes = "changed"
	*s.Open.IFScoreMax = 99
	s.Open.IFHosts[0] = "changed"
	s.Legacy.States[0].Hosts[0] = "changed"
	s.Legacy.Open.WitnessHosts[0] = "changed"
	*s.Legacy.Open.RawFirst = id.Window.End
	actual, err := j.Snapshot()
	if err != nil || !reflect.DeepEqual(actual, before) {
		t.Fatal("DTO aliases journal", err)
	}
	if _, _, _, err := restored.Step(journalRaw(1, Negative, Negative, Negative)); err != nil {
		t.Fatal(err)
	}
	actual, err = j.Snapshot()
	if err != nil || !reflect.DeepEqual(actual, before) {
		t.Fatal("restore aliases source", err)
	}
	if _, err := restored.Flush(id.Window.End); err != nil {
		t.Fatal(err)
	}
	actual, err = j.Snapshot()
	if err != nil || !reflect.DeepEqual(actual, before) {
		t.Fatal("restored flush aliases source", err)
	}
}

func TestFullSnapshotCompletedRBInsideOpenParent(t *testing.T) {
	id := journalIdentity(t, 0, 5)
	j, _ := NewRBI2Journal(id, "a01", evalHosts("a01"))
	rows := []Raw{journalRaw(0, Positive, Positive, Negative), journalRaw(1, Positive, Negative, Positive), journalRaw(2, Positive, Positive, Negative)}
	for _, r := range rows {
		if _, _, _, err := j.Step(r); err != nil {
			t.Fatal(err)
		}
	}
	s, err := j.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if s.Open == nil || len(s.Open.RBSubspans) != 1 || s.Legacy.Open == nil || len(s.Open.VoteSegments) != 3 {
		t.Fatal("fixture did not retain finished and open RB spans", s.Open)
	}
	s.Open.RBSubspans[0].HistoricalEventID = "wrong"
	sealJournalTest(t, &s)
	if _, err := RestoreRBI2Journal(s, id, "a01", evalHosts("a01")); err == nil {
		t.Fatal("historical ID corruption accepted")
	}
}

func TestFullSnapshotInvalidSourceRejected(t *testing.T) {
	id := journalIdentity(t, 0, 2)
	j, _ := NewRBI2Journal(id, "a01", evalHosts("a01"))
	if _, _, _, err := j.Step(journalRaw(0, Positive, Positive, Positive)); err != nil {
		t.Fatal(err)
	}
	j.last = j.last.Add(analysis.Step)
	if _, err := j.Snapshot(); err == nil {
		t.Fatal("partial journal transition snapshotted")
	}
	if _, err := (*RBI2Journal)(nil).Snapshot(); err == nil {
		t.Fatal("nil journal accepted")
	}
	if _, err := (*Engine)(nil).Snapshot(id.Window.End); err == nil {
		t.Fatal("nil engine accepted")
	}
}
