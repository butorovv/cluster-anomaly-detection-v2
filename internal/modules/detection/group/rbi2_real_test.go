package group

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

// This is full inference with frozen assets, not a refit or a new experiment.
// Independent research parents/ticks and pre-PR14 Raw are read-only oracles.
func TestRealRBI2D2Parity(t *testing.T) {
	root, reference, research := os.Getenv("GROUP_REAL_ROOT"), os.Getenv("GROUP_REAL_REFERENCE"), os.Getenv("GROUP_RBI2_RESEARCH")
	if root == "" || reference == "" || research == "" {
		t.Skip("GROUP_REAL_ROOT, GROUP_REAL_REFERENCE and GROUP_RBI2_RESEARCH required for full frozen R/B/I2 parity")
	}
	open := func(path string) *os.File {
		t.Helper()
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })
		return f
	}
	key := func(group string, at time.Time) string { return group + "/" + at.UTC().Format(time.RFC3339Nano) }
	readCSV := func(name string) map[string]map[string]string {
		t.Helper()
		reader := csv.NewReader(bufio.NewReader(open(filepath.Join(research, name))))
		header, err := reader.Read()
		if err != nil {
			t.Fatal(err)
		}
		rows := map[string]map[string]string{}
		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			fields := map[string]string{}
			for i, name := range header {
				fields[name] = row[i]
			}
			at, err := time.Parse(time.RFC3339Nano, fields["timestamp"])
			if err != nil {
				t.Fatal(err)
			}
			k := key(fields["group"], at)
			if _, exists := rows[k]; exists {
				t.Fatal("duplicate CSV key", k)
			}
			rows[k] = fields
		}
		return rows
	}
	integer := func(s string) int {
		v, err := strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	state := func(s string) State {
		switch s {
		case "1":
			return Positive
		case "0":
			return Negative
		case "-1":
			return Unknown
		}
		t.Fatal("invalid reference state", s)
		return Unknown
	}
	closeFloat := func(a, b float64) bool { return math.Abs(a-b) <= 1e-8+1e-8*math.Abs(b) }
	ticks, tickEvidence := readCSV("journal-ticks.csv"), readCSV("event-tick-evidence.csv")
	if len(ticks) != 46080 || len(tickEvidence) == 0 {
		t.Fatal("incomplete research ticks")
	}
	type parent struct {
		ID            string            `json:"id"`
		Group         string            `json:"group"`
		Detected      time.Time         `json:"detected"`
		Closed        time.Time         `json:"closed"`
		FirstEvidence time.Time         `json:"first_evidence"`
		Duration      int               `json:"duration_seconds"`
		Delay         int               `json:"confirmation_delay_seconds"`
		PositiveTicks int               `json:"positive_ticks"`
		New           bool              `json:"new_disjoint_from_RB"`
		Segments      []RBI2VoteSegment `json:"vote_segments"`
		RB            []RBI2RBSubspan   `json:"RB_subspans"`
		IFHosts       []string          `json:"IF_hosts"`
		IFMax         int               `json:"IF_simultaneous_max"`
		IFScore       float64           `json:"IF_score_max"`
		RRatio        float64           `json:"R_ratio_max"`
		BRatio        float64           `json:"B_ratio_max"`
		Left          bool              `json:"left_censored_by_window"`
		Missing       bool              `json:"right_censored_by_missing"`
		Window        bool              `json:"right_censored_by_window"`
	}
	parents := map[string]parent{}
	decoder := json.NewDecoder(bufio.NewReader(open(filepath.Join(research, "two-level-journal.ndjson"))))
	for {
		var p parent
		if err := decoder.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		k := key(p.Group, p.Detected)
		if _, exists := parents[k]; exists {
			t.Fatal("duplicate parent", k)
		}
		parents[k] = p
	}
	if len(parents) != 750 {
		t.Fatal("incomplete parent oracle", len(parents))
	}
	rawReference := json.NewDecoder(bufio.NewReaderSize(open(filepath.Join(reference, "raw.ndjson")), 1<<20))
	var last Raw
	var temporal RBI2TemporalResult
	rawCount, temporalCount, evidenceCount := 0, 0, 0
	seenIDs := map[string]bool{}
	profile, err := LoadProfile(filepath.Join(root, "configs/group-profile.summit-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewRBI2Identity(summitProfileSHA, profile.InputSHA, 16, analysis.NIRWindows()[1], 0)
	if err != nil || id.LegacyRunID != summitD2RunID || id.ConfigID != "b44f95bee95d00b908b30de77d89d8ae56c2a6a8a40d9d8fc8a508413b17da87" || id.RunID != "845af471d9ec5b8a503f09bb1395486cbe1658d8cb9786d94f1d6b4e6e5688ae" {
		t.Fatal("frozen production/legacy identity changed", id, err)
	}
	sink := RBI2Sink{
		Raw: func(raw Raw) error {
			var want Raw
			if err := rawReference.Decode(&want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(raw, want) {
				t.Fatal("historical raw/evidence changed", key(raw.Group, raw.Timestamp))
			}
			last = raw
			rawCount++
			return nil
		},
		Temporal: func(tr RBI2TemporalResult) error {
			k := key(tr.Group, tr.Timestamp)
			want, ok := ticks[k]
			if !ok {
				t.Fatal("unexpected temporal key", k)
			}
			if tr.R.State != state(want["R"]) || tr.B.State != state(want["B"]) || tr.I2.State != state(want["I2"]) || tr.Vote != state(want["RBI2"]) || tr.LocalI2.Available != integer(want["IF_available"]) || tr.LocalI2.Anomalous != integer(want["IF_positive_hosts"]) {
				t.Fatal("keyed research decision mismatch", k, tr, want)
			}
			voices := []string{}
			for _, name := range []string{"R", "B", "I2"} {
				if want[name] == "1" {
					voices = append(voices, name)
				}
			}
			if !reflect.DeepEqual(tr.ActiveVotes, voices) || (tr.Vote == Positive && (tr.FirstEvidence == nil || !tr.FirstEvidence.Equal(tr.Timestamp))) || (tr.Vote != Positive && tr.FirstEvidence != nil) {
				t.Fatal("voices/first evidence mismatch", k)
			}
			delete(ticks, k)
			temporal = tr
			temporalCount++
			return nil
		},
		Evidence: func(e RBI2Evidence) error {
			k := key(e.Raw.Group, e.Raw.Timestamp)
			if !reflect.DeepEqual(e.Raw, last) || !reflect.DeepEqual(e.Temporal, temporal) || e.EventID == "" || e.RunID != id.RunID {
				t.Fatal("raw/temporal evidence payload or identity changed", k)
			}
			if want, ok := tickEvidence[k]; ok {
				if strings.Join(e.Temporal.ActiveVotes, "+") != want["votes"] {
					t.Fatal("research evidence voices", k)
				}
				for field, value := range map[string]*float64{"R_score": e.Raw.R.Score, "R_threshold": e.Raw.R.Threshold, "B_score": e.Raw.B.Score, "B_threshold": e.Raw.B.Threshold} {
					v, err := strconv.ParseFloat(want[field], 64)
					if err != nil || value == nil || !closeFloat(*value, v) {
						t.Fatal("research R/B numerical evidence", k, field, err)
					}
				}
				var scores, thresholds []float64
				var states []int
				if err := json.Unmarshal([]byte(want["IF_scores"]), &scores); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(want["IF_thresholds"]), &thresholds); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(want["IF_states"]), &states); err != nil {
					t.Fatal(err)
				}
				if len(scores) != 18 || len(thresholds) != 18 || len(states) != 18 {
					t.Fatal("incomplete local oracle")
				}
				for h, lr := range e.Raw.Local {
					if lr.AnomalyScore == nil || lr.IndividualThreshold == nil || *lr.AnomalyScore != scores[h] || *lr.IndividualThreshold != thresholds[h] || lr.IsAnomaly == nil || *lr.IsAnomaly != (states[h] == 1) {
						t.Fatal("local host score/threshold/decision mismatch", k, lr.Hostname)
					}
				}
				delete(tickEvidence, k)
			}
			for _, ref := range e.WitnessRefs {
				if ref.Timestamp != e.Raw.Timestamp {
					t.Fatal("delta0 witness time", k)
				}
			}
			evidenceCount++
			return nil
		},
		Event: func(e RBI2Event) error {
			k := key(e.Group, e.Detected)
			p, ok := parents[k]
			if !ok || seenIDs[e.ID] {
				t.Fatal("duplicate/unexpected parent", k, e.ID)
			}
			seenIDs[e.ID] = true
			if e.ID != RBI2EventID(id.RunID, e.Group, e.Detected.UTC().Format(time.RFC3339Nano)) || e.ID == p.ID || e.RunID != id.RunID || e.KernelConfigID != id.ConfigID {
				t.Fatal("production/research namespace identity mismatch", k)
			}
			if e.Closed != p.Closed || e.FirstEvidence != p.FirstEvidence || e.DurationSeconds != p.Duration || e.DelaySeconds != p.Delay || e.PositiveTicks != p.PositiveTicks || e.NewDisjointFromRB != p.New || e.LeftCensoredWindow != p.Left || e.CensoredMissing != p.Missing || e.CensoredWindow != p.Window || !reflect.DeepEqual(e.VoteSegments, p.Segments) || !reflect.DeepEqual(e.RBSubspans, p.RB) || !reflect.DeepEqual(e.IFHosts, p.IFHosts) || e.IFSimultaneousMax != p.IFMax || e.IFScoreMax == nil || e.RRatioMax == nil || e.BRatioMax == nil || !closeFloat(*e.IFScoreMax, p.IFScore) || !closeFloat(*e.RRatioMax, p.RRatio) || !closeFloat(*e.BRatioMax, p.BRatio) {
				t.Fatalf("research parent interval/voices/children/diagnostics mismatch at %s\ngot=%+v\nwant=%+v", k, e, p)
			}
			delete(parents, k)
			return nil
		},
	}
	summary, err := RunRBI2Offline(context.Background(), RBI2OfflineOptions{Files: SummitRBI2Files(root, summitProfileSHA), Input: filepath.Join(root, "data/prepared/summit-D1-pilot/D1.parquet"), Manifest: filepath.Join(root, "data/prepared/summit-D1-pilot/manifest.json"), Window: analysis.NIRWindows()[1], Groups: 16, DeltaSeconds: 0, Sink: sink})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Ticks != 46080 || summary.PositiveTicks != 1284 || summary.Events != 750 || summary.NewEvents != 473 || summary.RBIntervals != 285 || summary.RBSupportedEvents != 277 || rawCount != 46080 || temporalCount != 46080 || evidenceCount != 1284 || len(ticks) != 0 || len(parents) != 0 || len(tickEvidence) != 0 {
		t.Fatal("incomplete full parity", summary, len(ticks), len(parents), len(tickEvidence))
	}
	var extra Raw
	if err := rawReference.Decode(&extra); err != io.EOF {
		t.Fatal("extra historical raw records", err)
	}
	t.Logf("exact keyed masks, raw/physical/local evidence, voices, parent intervals and legacy IDs: %+v", []int{summary.Ticks, summary.PositiveTicks, summary.Events, summary.NewEvents, summary.RBIntervals, summary.RBSupportedEvents})
}
