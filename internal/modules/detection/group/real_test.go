package group

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

// Real assets/reference outputs are opt-in and read-only. Reference files must
// come from the unchanged offline CLI on the same canonical historical input.
// There is no training and no output written into the repository.
func TestRealPerGroupD2Parity(t *testing.T) {
	root, reference := os.Getenv("GROUP_REAL_ROOT"), os.Getenv("GROUP_REAL_REFERENCE")
	if root == "" || reference == "" {
		t.Skip("GROUP_REAL_ROOT and GROUP_REAL_REFERENCE required: immutable Summit assets and pre-change offline outputs")
	}
	const inputKey = "data/prepared/summit-D1-pilot/D1.parquet"
	const manifestKey = "data/prepared/summit-D1-pilot/manifest.json"
	asset := func(key string) BundleFile { return BundleFile{Path: filepath.Join(root, key), InputKey: key} }
	files := BundleFiles{
		ProfilePath: filepath.Join(root, "configs/group-profile.summit-v1.json"), ProfileSHA256: summitProfileSHA,
		PreparedKey: inputKey, ManifestKey: manifestKey,
		Statistical: asset("results/statistical-20220118-8h-v1.json"), Isolation: asset("results/isolation-20220118-v1.json"),
		B1: asset("configs/local-b1.summit-20220118.json"), Hardware: asset("configs/summit-hardware-reference.json"), Config: asset("configs/group-detection.summit-v1.json"),
	}
	bundle, err := LoadBundle(files)
	if err != nil {
		t.Fatal(err)
	}
	source, err := bundle.OpenSource(filepath.Join(root, inputKey), filepath.Join(root, manifestKey))
	if err != nil {
		t.Fatal(err)
	}
	window := analysis.NIRWindows()[1]
	runID, err := bundle.RunID(16, window)
	if err != nil || runID != summitD2RunID {
		t.Fatal("baseline run ID mismatch", runID, err)
	}
	open := func(name string) *json.Decoder {
		t.Helper()
		file, err := os.Open(filepath.Join(reference, name))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		return json.NewDecoder(bufio.NewReaderSize(file, 1<<20))
	}
	rawReference, temporalReference := open("raw.ndjson"), open("temporal.ndjson")
	eventReference := open("events.ndjson")
	expectedEvents := map[string]Event{}
	for {
		var event Event
		if err := eventReference.Decode(&event); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if _, exists := expectedEvents[event.ID]; exists {
			t.Fatal("duplicate reference event ID", event.ID)
		}
		expectedEvents[event.ID] = event
	}
	if len(expectedEvents) != 1159 {
		t.Fatal("incomplete all-delta event reference", len(expectedEvents))
	}
	engines := map[string]*Engine{}
	deltas := []int{0, 10, 30, 60}
	for _, g := range bundle.profile.Groups {
		for _, delta := range deltas {
			e, err := NewEngine(runID, g.Group, delta, window.Start)
			if err != nil {
				t.Fatal(err)
			}
			engines[g.Group+"/"+strconv.Itoa(delta)] = e
		}
	}
	eventCounts := map[int]int{}
	checkEvents := func(events []Event) {
		t.Helper()
		for _, event := range events {
			want, exists := expectedEvents[event.ID]
			if !exists || !reflect.DeepEqual(event, want) {
				t.Fatalf("event identity/payload mismatch: %s/%s/%d/%s\ngot=%+v\nwant=%+v", event.RunID, event.Group, event.DeltaSeconds, event.ID, event, want)
			}
			delete(expectedEvents, event.ID)
			eventCounts[event.DeltaSeconds]++
		}
	}
	rPositive, bPositive, joint, ifPositive, yUnknown, rawCount, temporalCount := 0, 0, 0, 0, 0, 0, 0
	err = WalkTicks(context.Background(), source, window, func(tick Tick) error {
		rows := make([]Raw, len(bundle.profile.Groups))
		for i, g := range bundle.profile.Groups {
			row, err := bundle.EvaluateGroup(g.Group, tick, nil)
			if err != nil {
				return err
			}
			if row.IF.OutsideGroups != nil || row.IF.OutsideS != nil || row.IF.E != nil {
				t.Fatal("worker API calculated outside context")
			}
			rows[i] = row
		}
		AnnotateOutside(rows)
		for _, row := range rows {
			var want Raw
			if err := rawReference.Decode(&want); err != nil {
				t.Fatal("missing raw reference", err)
			}
			// Timestamp/group are the natural key; full comparison also covers every
			// local score/decision, R/Y witness, B vector/distance and outside count.
			if !row.Timestamp.Equal(want.Timestamp) || row.Group != want.Group || !reflect.DeepEqual(row, want) {
				t.Fatalf("raw keyed payload mismatch at %s/%s", row.Timestamp, row.Group)
			}
			rawCount++
			if row.R.State == Positive {
				rPositive++
			}
			if row.B.State == Positive {
				bPositive++
			}
			if row.R.State == Positive && row.B.State == Positive {
				joint++
			}
			if row.Y.State == Unknown {
				yUnknown++
			}
			ifPositive += row.IF.Anomalous
			for _, delta := range deltas {
				got, events, err := engines[row.Group+"/"+strconv.Itoa(delta)].Step(row)
				if err != nil {
					return err
				}
				var want TemporalResult
				if err := temporalReference.Decode(&want); err != nil {
					t.Fatal("missing temporal reference", err)
				}
				if !got.Timestamp.Equal(want.Timestamp) || got.Group != want.Group || got.DeltaSeconds != want.DeltaSeconds || !reflect.DeepEqual(got, want) {
					t.Fatalf("temporal keyed payload mismatch at %s/%s/%d", got.Timestamp, got.Group, delta)
				}
				temporalCount++
				checkEvents(events)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, engine := range engines {
		events, err := engine.Flush(window.End)
		if err != nil {
			t.Fatal(err)
		}
		checkEvents(events)
	}
	for _, decoder := range []*json.Decoder{rawReference, temporalReference} {
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			t.Fatal("extra reference record", err)
		}
	}
	if len(expectedEvents) != 0 || rawCount != 46080 || temporalCount != 184320 || rPositive != 899 || bPositive != 3260 || joint != 682 || ifPositive != 14341 || yUnknown != 46080 || !reflect.DeepEqual(eventCounts, map[int]int{0: 285, 10: 292, 30: 295, 60: 287}) {
		t.Fatalf("golden coverage/count mismatch: raw=%d temporal=%d R=%d B=%d joint=%d IF=%d Y?=%d events=%v missing=%d", rawCount, temporalCount, rPositive, bPositive, joint, ifPositive, yUnknown, eventCounts, len(expectedEvents))
	}
	t.Logf("exact float64/keyed payload parity: %d raw, %d temporal, 1159 all-delta events; event counts %v", rawCount, temporalCount, eventCounts)
}
