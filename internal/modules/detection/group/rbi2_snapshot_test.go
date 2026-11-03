package group

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

func snapshotRaw(at time.Time, r, b, i State) Raw {
	raw := rbi2Raw(at, r, b, false)
	if i != Unknown {
		positive := 0
		if i == Positive {
			positive = 2
		}
		raw.Local = i2Decisions(at, 18, positive)
	}
	return raw
}

func snapshotJSONRoundTrip(t *testing.T, e *RBI2Engine, end time.Time) RBI2EngineSnapshot {
	t.Helper()
	s, err := e.Snapshot(end)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var decoded RBI2EngineSnapshot
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	again, err := json.Marshal(decoded)
	if err != nil || !bytes.Equal(encoded, again) {
		t.Fatal("unstable snapshot JSON", err)
	}
	return decoded
}

func snapshotOptions(delta int, start, end time.Time) RBI2EngineRestoreOptions {
	return RBI2EngineRestoreOptions{Group: "a01", Hosts: evalHosts("a01"), DeltaSeconds: delta,
		Window: analysis.Window{Start: start, End: end}}
}

func TestRBI2SnapshotRoundTripParity(t *testing.T) {
	for _, delta := range []int{0, 10, 30, 60} {
		for _, initialGap := range []int{0, 20} {
			t.Run(fmt.Sprintf("delta%d/initialGap%d", delta, initialGap), func(t *testing.T) {
				continuous, start := rbi2Engine(t, delta)
				end := start.Add(time.Duration(240+initialGap) * time.Second)
				steps := []struct {
					seconds int
					r, b, i State
				}{
					{0, Positive, Negative, Positive}, {10, Negative, Positive, Negative},
					{20, Negative, Negative, Negative}, {30, Unknown, Negative, Negative},
					{40, Negative, Unknown, Unknown}, {50, Positive, Negative, Positive},
					{60, Negative, Positive, Negative}, {80, Negative, Negative, Negative},
					{90, Positive, Positive, Positive}, {100, Negative, Negative, Negative},
					{110, Negative, Negative, Negative}, {120, Negative, Negative, Negative},
					{130, Negative, Negative, Negative}, {140, Negative, Negative, Negative},
					{150, Negative, Negative, Negative}, {160, Negative, Negative, Negative},
					{180, Unknown, Unknown, Unknown}, {200, Positive, Positive, Negative},
				}
				rows := make([]Raw, len(steps))
				want := make([]RBI2TemporalResult, len(steps))
				for i, step := range steps {
					rows[i] = snapshotRaw(start.Add(time.Duration(step.seconds+initialGap)*time.Second), step.r, step.b, step.i)
					var err error
					want[i], err = continuous.Step(rows[i])
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := continuous.Flush(end); err != nil {
					t.Fatal(err)
				}
				final := snapshotJSONRoundTrip(t, continuous, end)
				// Restore at EVERY boundary, including empty, expired TTL, Unknown,
				// and either side of gaps. Compare every output field, not counts.
				for cut := 0; cut <= len(rows); cut++ {
					prefix, _ := rbi2Engine(t, delta)
					for _, row := range rows[:cut] {
						if _, err := prefix.Step(row); err != nil {
							t.Fatal(err)
						}
					}
					s := snapshotJSONRoundTrip(t, prefix, end)
					resumed, err := RestoreRBI2Engine(s, snapshotOptions(delta, start, end))
					if err != nil {
						t.Fatal(err)
					}
					for i := cut; i < len(rows); i++ {
						got, err := resumed.Step(rows[i])
						if err != nil || !reflect.DeepEqual(got, want[i]) {
							t.Fatalf("cut %d tick %d: err=%v\ngot %#v\nwant %#v", cut, i, err, got, want[i])
						}
					}
					if err := resumed.Flush(end); err != nil {
						t.Fatal(err)
					}
					if got := snapshotJSONRoundTrip(t, resumed, end); !reflect.DeepEqual(got, final) {
						t.Fatalf("cut %d: final state differs", cut)
					}
				}
			})
		}
	}
}

func TestRBI2SnapshotRetainsPositiveWitnessOnNegative(t *testing.T) {
	e, start := rbi2Engine(t, 30)
	end := start.Add(80 * time.Second)
	if _, err := e.Step(snapshotRaw(start, Positive, Negative, Positive)); err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreRBI2Engine(snapshotJSONRoundTrip(t, e, end), snapshotOptions(30, start, end))
	if err != nil {
		t.Fatal(err)
	}
	for seconds := 10; seconds <= 40; seconds += 10 {
		raw := snapshotRaw(start.Add(time.Duration(seconds)*time.Second), Negative, Negative, Negative)
		want, err := e.Step(raw)
		if err != nil {
			t.Fatal(err)
		}
		got, err := restored.Step(raw)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("lost temporal memory on negative", got, want, err)
		}
		if seconds <= 30 {
			if got.Vote != Positive || got.R.Witness == nil || got.I2.Witness == nil || !got.R.Witness.Equal(start) ||
				!got.I2.Witness.Equal(start) || !slices.Equal(got.ActiveVotes, []string{"R", "I2"}) {
				t.Fatal("lost inclusive TTL/witness/active votes", got)
			}
		} else if got.Vote != Negative {
			t.Fatal("expired positive did not expire", got)
		}
	}
}

func TestRBI2SnapshotEmptyAndFlushed(t *testing.T) {
	for _, ticks := range []int{0, 2} {
		for _, emptyWindow := range []bool{false, true} {
			if emptyWindow && ticks > 0 {
				continue
			}
			t.Run(fmt.Sprintf("ticks%d/empty%t", ticks, emptyWindow), func(t *testing.T) {
				e, start := rbi2Engine(t, 30)
				end := start.Add(30 * time.Second)
				if emptyWindow {
					end = start
				} // Flush(split) is valid before the first tick.
				fresh := snapshotJSONRoundTrip(t, e, end)
				if !fresh.Last.IsZero() || !fresh.End.IsZero() || fresh.States != [3]RBI2OccurrenceSnapshot{} {
					t.Fatal("initial zero state was changed", fresh)
				}
				restored, err := RestoreRBI2Engine(fresh, snapshotOptions(30, start, end))
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < ticks; i++ {
					if _, err := restored.Step(snapshotRaw(start.Add(time.Duration(i)*analysis.Step), Positive, Positive, Positive)); err != nil {
						t.Fatal(err)
					}
				}
				if err := restored.Flush(end); err != nil {
					t.Fatal(err)
				}
				flushed := snapshotJSONRoundTrip(t, restored, end)
				if !flushed.End.Equal(end) || flushed.States != [3]RBI2OccurrenceSnapshot{} {
					t.Fatal("flushed memory not cleared", flushed)
				}
				sealed, err := RestoreRBI2Engine(flushed, snapshotOptions(30, start, end))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := sealed.Step(snapshotRaw(end, Positive, Positive, Positive)); err == nil {
					t.Fatal("restored flushed engine resumed")
				}
				if err := sealed.Flush(end); err != nil {
					t.Fatal("same Flush lost idempotence", err)
				}
				if err := sealed.Flush(end.Add(analysis.Step)); err == nil {
					t.Fatal("different Flush boundary accepted")
				}
			})
		}
	}
}

func TestRBI2SnapshotRejectsImpossibleStateAndConfig(t *testing.T) {
	e, start := rbi2Engine(t, 30)
	for i, raw := range []Raw{
		snapshotRaw(start, Unknown, Negative, Unknown),
		snapshotRaw(start.Add(analysis.Step), Positive, Positive, Positive),
		snapshotRaw(start.Add(2*analysis.Step), Negative, Negative, Negative),
	} {
		if _, err := e.Step(raw); err != nil {
			t.Fatal(i, err)
		}
	}
	end := start.Add(120 * time.Second)
	base := snapshotJSONRoundTrip(t, e, end)
	mutations := map[string]func(*RBI2EngineSnapshot){
		"schema":        func(s *RBI2EngineSnapshot) { s.SchemaVersion++ },
		"math_version":  func(s *RBI2EngineSnapshot) { s.MathVersion = "other" },
		"group":         func(s *RBI2EngineSnapshot) { s.Group = "a02" },
		"delta_invalid": func(s *RBI2EngineSnapshot) { s.DeltaSeconds = 20 },
		"delta_other":   func(s *RBI2EngineSnapshot) { s.DeltaSeconds = 60 },
		"hosts_short":   func(s *RBI2EngineSnapshot) { s.Hosts = s.Hosts[:17] },
		"hosts_order":   func(s *RBI2EngineSnapshot) { slices.Reverse(s.Hosts) },
		"split_zero":    func(s *RBI2EngineSnapshot) { s.Split = time.Time{} },
		"split_grid":    func(s *RBI2EngineSnapshot) { s.Split = s.Split.Add(time.Second) },
		"last_zero":     func(s *RBI2EngineSnapshot) { s.Last = time.Time{} },
		"last_before":   func(s *RBI2EngineSnapshot) { s.Last = s.Split.Add(-analysis.Step) },
		"last_grid":     func(s *RBI2EngineSnapshot) { s.Last = s.Last.Add(time.Second) },
		"last_end":      func(s *RBI2EngineSnapshot) { s.Last = s.WindowEnd },
		"end_mismatch":  func(s *RBI2EngineSnapshot) { s.End = s.WindowEnd.Add(analysis.Step) },
		"end_memory":    func(s *RBI2EngineSnapshot) { s.End = s.WindowEnd },
		"window_zero":   func(s *RBI2EngineSnapshot) { s.WindowEnd = time.Time{} },
		"window_before": func(s *RBI2EngineSnapshot) { s.WindowEnd = s.Split.Add(-analysis.Step) },
		"window_grid":   func(s *RBI2EngineSnapshot) { s.WindowEnd = s.WindowEnd.Add(time.Second) },
		"positive_grid": func(s *RBI2EngineSnapshot) { s.States[0].Positive = s.Last.Add(-time.Second) },
		"positive_past": func(s *RBI2EngineSnapshot) { s.States[0].Positive = s.Split.Add(-analysis.Step) },
		"positive_late": func(s *RBI2EngineSnapshot) { s.States[0].Positive = s.Last.Add(analysis.Step) },
		"unknown_late":  func(s *RBI2EngineSnapshot) { s.States[1].Unknown = s.Last.Add(analysis.Step) },
		"equal_barrier": func(s *RBI2EngineSnapshot) { s.States[0].Unknown = s.States[0].Positive },
		"newer_barrier": func(s *RBI2EngineSnapshot) { s.States[0].Unknown = s.Last },
	}
	for name, corrupt := range mutations {
		t.Run(name, func(t *testing.T) {
			s := base
			s.Hosts = slices.Clone(base.Hosts)
			corrupt(&s)
			if got, err := RestoreRBI2Engine(s, snapshotOptions(30, start, end)); err == nil || got != nil {
				t.Fatal("invalid snapshot restored", got, err)
			}
		})
	}
	for _, field := range []string{"group", "hosts", "delta", "start", "end"} {
		t.Run("expected_"+field, func(t *testing.T) {
			opts := snapshotOptions(30, start, end)
			switch field {
			case "group":
				opts.Group, opts.Hosts = "a02", evalHosts("a02")
			case "hosts":
				slices.Reverse(opts.Hosts)
			case "delta":
				opts.DeltaSeconds = 60
			case "start":
				opts.Window.Start = start.Add(-analysis.Step)
			case "end":
				opts.Window.End = end.Add(analysis.Step)
			}
			if got, err := RestoreRBI2Engine(base, opts); err == nil || got != nil {
				t.Fatal("incompatible restore configuration accepted", got, err)
			}
		})
	}
	if _, err := (*RBI2Engine)(nil).Snapshot(end); err == nil {
		t.Fatal("nil engine snapshot accepted")
	}
	for _, field := range []string{"hosts", "zero", "position"} {
		bad := *e
		switch field {
		case "hosts":
			bad.states[0].hosts = []string{"a01n01"}
		case "zero":
			bad.states[0].zero = true
		case "position":
			bad.states[0].positive = end
		}
		if _, err := bad.Snapshot(end); err == nil {
			t.Fatalf("invalid source %s accepted", field)
		}
	}
}

func TestRBI2SnapshotStrictJSON(t *testing.T) {
	e, start := rbi2Engine(t, 0)
	s := snapshotJSONRoundTrip(t, e, start.Add(analysis.Step))
	encoded, _ := json.Marshal(s)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	reject := func(t *testing.T, data []byte) {
		t.Helper()
		destination := s
		if err := json.Unmarshal(data, &destination); err == nil {
			t.Fatalf("malformed snapshot JSON accepted: %s", data)
		}
		if !reflect.DeepEqual(destination, s) {
			t.Fatal("failed JSON decode partially mutated receiver")
		}
	}
	for key := range fields {
		for _, null := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/null%t", key, null), func(t *testing.T) {
				m := make(map[string]json.RawMessage, len(fields))
				for k, v := range fields {
					m[k] = v
				}
				if null {
					m[key] = json.RawMessage("null")
				} else {
					delete(m, key)
				}
				data, _ := json.Marshal(m)
				reject(t, data)
			})
		}
	}
	for _, data := range [][]byte{
		[]byte("{"), []byte("null"), []byte("[]"), append(slices.Clone(encoded), []byte(" {}")...),
		append([]byte(`{"extra":1,`), encoded[1:]...), append([]byte(`{"group":"a01",`), encoded[1:]...),
		bytes.Replace(encoded, fields["states"], []byte(`[]`), 1),
		bytes.Replace(encoded, fields["states"], []byte(`[{}, {}, {}]`), 1),
		bytes.Replace(encoded, []byte(`"delta_seconds":0`), []byte(`"delta_seconds":0.5`), 1),
	} {
		reject(t, data)
	}
	for index := 0; index < 3; index++ {
		for _, key := range []string{"positive", "unknown"} {
			var memory []map[string]json.RawMessage
			if err := json.Unmarshal(fields["states"], &memory); err != nil {
				t.Fatal(err)
			}
			delete(memory[index], key)
			bad, _ := json.Marshal(memory)
			reject(t, bytes.Replace(encoded, fields["states"], bad, 1))
		}
	}
	var occurrences []json.RawMessage
	if err := json.Unmarshal(fields["states"], &occurrences); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{2, 4} {
		list := slices.Clone(occurrences[:2])
		if n == 4 {
			list = append(list, occurrences[0], occurrences[0])
		}
		bad, _ := json.Marshal(list)
		reject(t, bytes.Replace(encoded, fields["states"], bad, 1))
	}
	for _, prefix := range []string{`{"extra":1,`, `{"positive":"0001-01-01T00:00:00Z",`} {
		bad := append([]byte(prefix), occurrences[0][1:]...)
		reject(t, bytes.Replace(encoded, occurrences[0], bad, 1))
	}
}

func TestRBI2SnapshotCopyIndependence(t *testing.T) {
	e, start := rbi2Engine(t, 30)
	end := start.Add(60 * time.Second)
	if _, err := e.Step(snapshotRaw(start, Positive, Positive, Positive)); err != nil {
		t.Fatal(err)
	}
	before := snapshotJSONRoundTrip(t, e, end)
	s, err := e.Snapshot(end)
	if err != nil {
		t.Fatal(err)
	}
	s.Hosts[0], s.States[0].Positive = "mutated", end
	if got := snapshotJSONRoundTrip(t, e, end); !reflect.DeepEqual(got, before) {
		t.Fatal("mutating DTO changed source engine")
	}
	s = snapshotJSONRoundTrip(t, e, end)
	opts := snapshotOptions(30, start, end)
	restored, err := RestoreRBI2Engine(s, opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Hosts[0], opts.Hosts[0], s.States[0].Positive = "mutated", "mutated", end
	if _, err := restored.Step(snapshotRaw(start.Add(analysis.Step), Unknown, Negative, Negative)); err != nil {
		t.Fatal("restored engine aliases external hosts", err)
	}
	if got := snapshotJSONRoundTrip(t, e, end); !reflect.DeepEqual(got, before) {
		t.Fatal("restored Step changed source engine")
	}
	if err := restored.Flush(end); err != nil {
		t.Fatal(err)
	}
	if got := snapshotJSONRoundTrip(t, e, end); !reflect.DeepEqual(got, before) {
		t.Fatal("restored Flush changed source engine")
	}
}
