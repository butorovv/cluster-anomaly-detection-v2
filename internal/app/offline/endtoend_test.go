package offline

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
)

func fixtureArgs(root, sha, out string, w analysis.Window, delta int) []string {
	return []string{"--assets", root, "--profile-sha256", sha, "--output", out, "--delta", strconv.Itoa(delta), "--start", w.Start.Format(time.RFC3339Nano), "--end", w.End.Format(time.RFC3339Nano)}
}
func TestOfflineEndToEnd(t *testing.T) {
	root, sha, w := syntheticBundle(t)
	for _, delta := range []int{0, 10, 30, 60} {
		t.Run(strconv.Itoa(delta), func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "run")
			var stdout bytes.Buffer
			if err := Run(context.Background(), fixtureArgs(root, sha, out, w, delta), &stdout, io.Discard); err != nil {
				t.Fatal(err)
			}
			var summary group.RBI2RunSummary
			if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
				t.Fatal(err)
			}
			if summary.Ticks != 128 || summary.Events == 0 || summary.PositiveTicks == 0 || summary.RBIntervals == 0 {
				t.Fatal(summary)
			}
			count := func(name string, visit func([]byte)) {
				t.Helper()
				file, err := os.Open(filepath.Join(out, name))
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				scanner := bufio.NewScanner(file)
				scanner.Buffer(make([]byte, 4096), 1<<20)
				n := 0
				for scanner.Scan() {
					visit(scanner.Bytes())
					n++
				}
				if err = scanner.Err(); err != nil {
					t.Fatal(err)
				}
				if n == 0 {
					t.Fatalf("empty %s", name)
				}
			}
			rawCount, emptyTicks, partial := 0, 0, 0
			count("raw.ndjson", func(b []byte) {
				var r group.Raw
				if err := json.Unmarshal(b, &r); err != nil {
					t.Fatal(err)
				}
				rawCount++
				if r.Timestamp.Equal(w.Start.Add(2*analysis.Step)) || r.Timestamp.Equal(w.Start.Add(7*analysis.Step)) || r.Group == "a03" {
					emptyTicks++
					if r.R.State != group.Unknown || r.B.State != group.Unknown || r.IF.Available != 0 || len(r.Missing) != 18 {
						t.Fatal("empty became known", r)
					}
				}
				if r.Group == "a02" && r.Timestamp.Equal(w.Start.Add(4*analysis.Step)) {
					partial++
					if r.R.State != group.Unknown || r.B.State != group.Unknown || r.IF.Available != 17 {
						t.Fatal("partial became complete", r)
					}
				}
			})
			if rawCount != 128 || emptyTicks != 38 || partial != 1 {
				t.Fatal(rawCount, emptyTicks, partial)
			}
			count("temporal.ndjson", func(b []byte) {
				var r group.RBI2TemporalResult
				if err := json.Unmarshal(b, &r); err != nil {
					t.Fatal(err)
				}
			})
			count("evidence.ndjson", func(b []byte) {
				var r group.RBI2Evidence
				if err := json.Unmarshal(b, &r); err != nil {
					t.Fatal(err)
				}
				if r.EventID == "" || r.RunID != summary.Identity.RunID || r.Temporal.Vote != group.Positive {
					t.Fatal(r)
				}
			})
			count("events.ndjson", func(b []byte) {
				var r group.RBI2Event
				if err := json.Unmarshal(b, &r); err != nil {
					t.Fatal(err)
				}
				if !r.Closed.After(r.Detected) || r.ID != group.RBI2EventID(summary.Identity.RunID, r.Group, r.Detected.Format(time.RFC3339Nano)) {
					t.Fatal(r)
				}
			})
			saved, err := os.ReadFile(filepath.Join(out, "summary.json"))
			if err != nil {
				t.Fatal(err)
			}
			var expected group.RBI2RunSummary
			if err = json.Unmarshal(saved, &expected); err != nil {
				t.Fatal(err)
			}
			if expected.Identity.RunID != summary.Identity.RunID {
				t.Fatal("summary mismatch")
			}
			second := filepath.Join(t.TempDir(), "second")
			if err := Run(context.Background(), fixtureArgs(root, sha, second, w, delta), io.Discard, io.Discard); err != nil {
				t.Fatal(err)
			}
			compareOutputs(t, out, second)
		})
	}
}
func compareOutputs(t *testing.T, a, b string) {
	t.Helper()
	factories := map[string]func() any{
		"raw.ndjson":      func() any { return &group.Raw{} },
		"temporal.ndjson": func() any { return &group.RBI2TemporalResult{} },
		"evidence.ndjson": func() any { return &group.RBI2Evidence{} },
		"events.ndjson":   func() any { return &group.RBI2Event{} },
		"summary.json":    func() any { return &group.RBI2RunSummary{} },
	}
	for _, name := range []string{"raw.ndjson", "temporal.ndjson", "evidence.ndjson", "events.ndjson", "summary.json"} {
		left, err := os.Open(filepath.Join(a, name))
		if err != nil {
			t.Fatal(err)
		}
		right, err := os.Open(filepath.Join(b, name))
		if err != nil {
			left.Close()
			t.Fatal(err)
		}
		func() {
			defer left.Close()
			defer right.Close()
			ld, rd := json.NewDecoder(bufio.NewReaderSize(left, 1<<20)), json.NewDecoder(bufio.NewReaderSize(right, 1<<20))
			for row := 0; ; row++ {
				lv, rv := factories[name](), factories[name]()
				le, re := ld.Decode(lv), rd.Decode(rv)
				if le == io.EOF && re == io.EOF {
					break
				}
				if le != nil || re != nil {
					t.Fatalf("%s row %d: V1=%v V2=%v", name, row, le, re)
				}
				if !reflect.DeepEqual(lv, rv) {
					t.Fatalf("semantic mismatch: %s row %d", name, row)
				}
			}
			// Keep a separate canonical-byte check, with bounded memory for real D2.
			if _, err = left.Seek(0, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			if _, err = right.Seek(0, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			lh, rh := sha256.New(), sha256.New()
			if _, err = io.Copy(lh, left); err != nil {
				t.Fatal(err)
			}
			if _, err = io.Copy(rh, right); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(lh.Sum(nil), rh.Sum(nil)) {
				t.Fatalf("canonical bytes differ: %s", name)
			}
		}()
	}
}

func TestFullD2EnumeratesAbsentTicks(t *testing.T) {
	root, sha, _ := syntheticBundle(t)
	raw, temporal := 0, 0
	sink := group.RBI2Sink{Raw: func(r group.Raw) error { raw++; return nil }, Temporal: func(r group.RBI2TemporalResult) error { temporal++; return nil }, Evidence: func(group.RBI2Evidence) error { return nil }, Event: func(group.RBI2Event) error { return nil }}
	summary, err := group.RunRBI2Offline(context.Background(), group.RBI2OfflineOptions{Files: group.SummitRBI2Files(root, sha), Input: filepath.Join(root, "data/prepared/summit-D1-pilot/D1.parquet"), Manifest: filepath.Join(root, "data/prepared/summit-D1-pilot/manifest.json"), Window: analysis.NIRWindows()[1], Groups: 16, Sink: sink})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Ticks != 46080 || raw != 46080 || temporal != 46080 {
		t.Fatal(summary.Ticks, raw, temporal)
	}
	t.Log("SYNTHETIC full D2 grid: 46,080 ticks; REAL_D2_GOLDEN: NOT_RUN")
}
func TestSubwindowValidation(t *testing.T) {
	for _, pair := range [][2]string{{"bad", ""}, {"2022-01-18T08:00:01Z", "2022-01-18T08:01:00Z"}, {"2022-01-18T07:00:00Z", "2022-01-18T08:01:00Z"}} {
		args := []string{"--assets", "missing", "--profile-sha256", testSHA, "--output", "never-created", "--start", pair[0], "--end", pair[1]}
		if err := Run(context.Background(), args, io.Discard, io.Discard); err == nil {
			t.Fatal(fmt.Sprint(pair))
		}
	}
}
