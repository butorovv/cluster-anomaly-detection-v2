package group

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/local"
)

func i2Decisions(timestamp time.Time, count, positive int) []local.LocalResult {
	rows := make([]local.LocalResult, count)
	for i := range rows {
		anomaly := i < positive
		rows[i] = local.LocalResult{
			Timestamp: timestamp, Hostname: evalHosts("a01")[i], GroupID: "a01",
			IsAvailable: true, IsAnomaly: &anomaly,
		}
	}
	return rows
}

func i2JSON(t *testing.T, value any) string {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestI2CompleteCoverage(t *testing.T) {
	timestamp := analysisTick()
	for _, tc := range []struct {
		name            string
		known, positive int
		state           State
		reason          string
	}{
		{"18 known 0 positive", 18, 0, Negative, ""},
		{"18 known 1 positive", 18, 1, Negative, ""},
		{"18 known 2 positive", 18, 2, Positive, ""},
		{"18 known 3 positive", 18, 3, Positive, ""},
		{"18 known 18 positive", 18, 18, Positive, ""},
		{"17 known 2 positive", 17, 2, Unknown, "missing_local_results"},
		{"17 known 17 positive", 17, 17, Unknown, "missing_local_results"},
		{"no results", 0, 0, Unknown, "missing_local_results"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := EvaluateI2("a01", timestamp, evalHosts("a01"), i2Decisions(timestamp, tc.known, tc.positive))
			if err != nil {
				t.Fatal(err)
			}
			want := I2Result{State: tc.state, Available: tc.known, Anomalous: tc.positive,
				PositiveHosts: evalHosts("a01")[:tc.positive], Reason: tc.reason}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %+v, want %+v", got, want)
			}
		})
	}
}

func TestI2UnknownDecisions(t *testing.T) {
	timestamp := analysisTick()
	for _, tc := range []struct {
		name      string
		change    func([]local.LocalResult) []local.LocalResult
		available int
		positive  int
		reason    string
	}{
		{"unavailable positive ignored", func(r []local.LocalResult) []local.LocalResult {
			r[0].IsAvailable = false
			return r
		}, 17, 2, "unavailable_local_results"},
		{"available without boolean", func(r []local.LocalResult) []local.LocalResult {
			r[0].IsAnomaly = nil
			return r
		}, 17, 2, "missing_local_decisions"},
		{"all unavailable", func(r []local.LocalResult) []local.LocalResult {
			for i := range r {
				r[i].IsAvailable = false
			}
			return r
		}, 0, 0, "unavailable_local_results"},
		{"all undecided", func(r []local.LocalResult) []local.LocalResult {
			for i := range r {
				r[i].IsAnomaly = nil
			}
			return r
		}, 0, 0, "missing_local_decisions"},
		{"unavailable precedes undecided", func(r []local.LocalResult) []local.LocalResult {
			r[0].IsAnomaly = nil
			r[1].IsAvailable = false
			return r
		}, 16, 1, "unavailable_local_results"},
		{"missing precedes other unknown", func(r []local.LocalResult) []local.LocalResult {
			r[0].IsAnomaly = nil
			r[1].IsAvailable = false
			return r[:17]
		}, 15, 1, "missing_local_results"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := EvaluateI2("a01", timestamp, evalHosts("a01"), tc.change(i2Decisions(timestamp, 18, 3)))
			if err != nil || got.State != Unknown || got.Available != tc.available || got.Anomalous != tc.positive || got.Reason != tc.reason {
				t.Fatalf("got %+v, err=%v", got, err)
			}
			if len(got.PositiveHosts) != tc.positive {
				t.Fatalf("positive hosts: %v", got.PositiveHosts)
			}
		})
	}
}

func TestI2RejectsInvalidIdentityAndInventory(t *testing.T) {
	timestamp := analysisTick()
	for _, tc := range []struct {
		name   string
		change func(*string, *time.Time, *[]string, *[]local.LocalResult)
		match  string
	}{
		{"empty group", func(g *string, _ *time.Time, _ *[]string, _ *[]local.LocalResult) { *g = "" }, "canonical group"},
		{"noncanonical group", func(g *string, _ *time.Time, _ *[]string, _ *[]local.LocalResult) { *g = "a1" }, "canonical group"},
		{"zero timestamp", func(_ *string, ts *time.Time, _ *[]string, _ *[]local.LocalResult) { *ts = time.Time{} }, "10-second grid"},
		{"off grid", func(_ *string, ts *time.Time, _ *[]string, _ *[]local.LocalResult) { *ts = ts.Add(time.Second) }, "10-second grid"},
		{"missing inventory", func(_ *string, _ *time.Time, h *[]string, _ *[]local.LocalResult) { *h = nil }, "18-host inventory"},
		{"short inventory", func(_ *string, _ *time.Time, h *[]string, _ *[]local.LocalResult) { *h = (*h)[:17] }, "18-host inventory"},
		{"duplicate inventory", func(_ *string, _ *time.Time, h *[]string, _ *[]local.LocalResult) { (*h)[17] = (*h)[0] }, "inventory mismatch"},
		{"reordered inventory", func(_ *string, _ *time.Time, h *[]string, _ *[]local.LocalResult) { slices.Reverse(*h) }, "inventory mismatch"},
		{"foreign inventory", func(_ *string, _ *time.Time, h *[]string, _ *[]local.LocalResult) { (*h)[17] = "a02n18" }, "inventory mismatch"},
		{"duplicate result", func(_ *string, _ *time.Time, _ *[]string, r *[]local.LocalResult) { (*r)[17] = (*r)[0] }, "duplicate result"},
		{"extra duplicate", func(_ *string, _ *time.Time, _ *[]string, r *[]local.LocalResult) { *r = append(*r, (*r)[0]) }, "duplicate result"},
		{"unknown host", func(_ *string, _ *time.Time, _ *[]string, r *[]local.LocalResult) { (*r)[17].Hostname = "unknown" }, "outside canonical inventory"},
		{"out of range host", func(_ *string, _ *time.Time, _ *[]string, r *[]local.LocalResult) { (*r)[17].Hostname = "a01n19" }, "outside canonical inventory"},
		{"noncanonical host", func(_ *string, _ *time.Time, _ *[]string, r *[]local.LocalResult) { (*r)[17].Hostname = "a1n18" }, "outside canonical inventory"},
		{"foreign host", func(_ *string, _ *time.Time, _ *[]string, r *[]local.LocalResult) { (*r)[17].Hostname = "a02n18" }, "outside canonical inventory"},
		{"wrong group field", func(_ *string, _ *time.Time, _ *[]string, r *[]local.LocalResult) { (*r)[17].GroupID = "a02" }, "outside canonical inventory"},
		{"wrong timestamp", func(_ *string, _ *time.Time, _ *[]string, r *[]local.LocalResult) {
			(*r)[17].Timestamp = timestamp.Add(10 * time.Second)
		}, "timestamp mismatch"},
		{"missing timestamp", func(_ *string, _ *time.Time, _ *[]string, r *[]local.LocalResult) { (*r)[17].Timestamp = time.Time{} }, "timestamp mismatch"},
		{"unavailable foreign host", func(_ *string, _ *time.Time, _ *[]string, r *[]local.LocalResult) {
			(*r)[17].Hostname, (*r)[17].IsAvailable = "a02n18", false
		}, "outside canonical inventory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, ts, hosts, rows := "a01", timestamp, evalHosts("a01"), i2Decisions(timestamp, 18, 18)
			tc.change(&g, &ts, &hosts, &rows)
			got, err := EvaluateI2(g, ts, hosts, rows)
			if err == nil || !strings.Contains(err.Error(), tc.match) {
				t.Fatalf("expected %q validation error, got %+v, %v", tc.match, got, err)
			}
			if got.State != Unknown || got.Reason != "invalid_input" || got.Available != 0 || got.Anomalous != 0 || len(got.PositiveHosts) != 0 {
				t.Fatalf("invalid input exposed a partial decision: %+v", got)
			}
		})
	}
}

func TestI2OrderAndInputOwnership(t *testing.T) {
	timestamp := analysisTick()
	hosts := evalHosts("a01")
	rows := i2Decisions(timestamp, 18, 0)
	for _, i := range []int{1, 9, 17} {
		*rows[i].IsAnomaly = true
	}
	want := I2Result{State: Positive, Available: 18, Anomalous: 3, PositiveHosts: []string{"a01n02", "a01n10", "a01n18"}}
	rng := rand.New(rand.NewSource(42))
	for repeat := 0; repeat < 30; repeat++ {
		rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
		// Exercise arbitrary map iteration order as well as slice permutations.
		byHost := map[string]local.LocalResult{}
		for _, r := range rows {
			byHost[r.Hostname] = r
		}
		rows = rows[:0]
		for _, r := range byHost {
			rows = append(rows, r)
		}
		beforeRows, beforeHosts := i2JSON(t, rows), slices.Clone(hosts)
		got, err := EvaluateI2("a01", timestamp, hosts, rows)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("permutation %d: %+v, %v", repeat, got, err)
		}
		if i2JSON(t, rows) != beforeRows || !slices.Equal(hosts, beforeHosts) {
			t.Fatal("input was mutated")
		}
		got.PositiveHosts[0] = "changed"
		if !slices.Equal(hosts, beforeHosts) || i2JSON(t, rows) != beforeRows {
			t.Fatal("result aliases input")
		}
	}
	// Concurrent callers share read-only inputs; -race also checks hidden state.
	for i := 0; i < 8; i++ {
		t.Run("concurrent", func(t *testing.T) {
			t.Parallel()
			got, err := EvaluateI2("a01", timestamp, hosts, rows)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("concurrent aggregation: %+v, %v", got, err)
			}
		})
	}
}

func TestI2ConsumesDecisionsWithoutRescoring(t *testing.T) {
	timestamp := analysisTick()
	rows := i2Decisions(timestamp, 18, 2)
	// No detector, forest, scores or thresholds are needed to aggregate decisions.
	// Diagnostic labels are not an additional mathematical vote or availability flag.
	for i := range rows {
		rows[i].DiagnosticStatus = "annotation"
	}
	rows[0].Timestamp = timestamp.In(time.FixedZone("same instant", 3*60*60))
	got, err := EvaluateI2("a01", timestamp, evalHosts("a01"), rows)
	if err != nil || got.State != Positive || got.Available != 18 || got.Anomalous != 2 {
		t.Fatalf("aggregation should use existing decisions: %+v, %v", got, err)
	}
}

func TestI2EvaluateGroupCompatibility(t *testing.T) {
	timestamp := analysisTick()
	p, detector := evalProfile("a01"), evalDetector(t, 1)
	for _, count := range []int{18, 17, 0} {
		raw, err := EvaluateGroup(p, evalTick("a01", timestamp, count), nil, detector, nil)
		if err != nil {
			t.Fatal(err)
		}
		before := i2JSON(t, raw)
		got, err := EvaluateI2(raw.Group, raw.Timestamp, p.Hosts, raw.Local)
		if err != nil || got.Available != raw.IF.Available || got.Anomalous != raw.IF.Anomalous {
			t.Fatalf("count=%d: I2=%+v, support=%+v, err=%v", count, got, raw.IF, err)
		}
		want := Unknown
		if count == 18 {
			want = Positive
		}
		if got.State != want || i2JSON(t, raw) != before {
			t.Fatalf("count=%d: state=%s, want=%s, or Raw changed", count, got.State, want)
		}
		// The fixture's score equals .5: n01/n02 use .49, the other hosts
		// use .5 and are negative under the unchanged strict local comparison.
		if count > 0 && !reflect.DeepEqual(got.PositiveHosts, []string{"a01n01", "a01n02"}) {
			t.Fatalf("local threshold/equality witnesses: %v", got.PositiveHosts)
		}
	}
}

func analysisTick() time.Time {
	return time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
}
