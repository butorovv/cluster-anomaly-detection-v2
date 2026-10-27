package group

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Saved Raw.Local and an independent research tick table suffice: no IF scoring,
// training or telemetry scan is needed for this opt-in full-D2 aggregation check.
func TestI2RealD2SavedDecisions(t *testing.T) {
	reference, golden := os.Getenv("GROUP_REAL_REFERENCE"), os.Getenv("GROUP_I2_GOLDEN_TICKS")
	if reference == "" || golden == "" {
		t.Skip("GROUP_REAL_REFERENCE and GROUP_I2_GOLDEN_TICKS required: saved D2 raw.ndjson and research journal-ticks.csv")
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
	reader := csv.NewReader(bufio.NewReader(open(golden)))
	header, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	columns := map[string]int{}
	for i, name := range header {
		columns[name] = i
	}
	for _, name := range []string{"timestamp", "group", "I2", "IF_available", "IF_positive_hosts"} {
		if _, ok := columns[name]; !ok {
			t.Fatal("missing golden column", name)
		}
	}
	type key struct {
		group string
		time  int64
	}
	type expected struct {
		state                State
		available, anomalous int
	}
	parseInt := func(s string) int {
		t.Helper()
		v, err := strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	want := map[key]expected{}
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		ts, err := time.Parse(time.RFC3339Nano, row[columns["timestamp"]])
		if err != nil {
			t.Fatal(err)
		}
		k := key{row[columns["group"]], ts.UnixNano()}
		if _, duplicate := want[k]; duplicate {
			t.Fatal("duplicate golden key", k)
		}
		states := map[string]State{"-1": Unknown, "0": Negative, "1": Positive}
		state, ok := states[row[columns["I2"]]]
		if !ok {
			t.Fatal("invalid golden state", row[columns["I2"]])
		}
		want[k] = expected{state, parseInt(row[columns["IF_available"]]), parseInt(row[columns["IF_positive_hosts"]])}
	}
	if len(want) != 46080 {
		t.Fatal("incomplete D2 golden table", len(want))
	}
	decoder := json.NewDecoder(bufio.NewReaderSize(open(filepath.Join(reference, "raw.ndjson")), 1<<20))
	count, positive := 0, 0
	for {
		var raw Raw
		if err := decoder.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		k := key{raw.Group, raw.Timestamp.UnixNano()}
		expect, ok := want[k]
		if !ok {
			t.Fatal("extra or duplicate raw key", k)
		}
		got, err := EvaluateI2(raw.Group, raw.Timestamp, evalHosts(raw.Group), raw.Local)
		if err != nil || got.State != expect.state || got.Available != expect.available || got.Anomalous != expect.anomalous {
			t.Fatalf("D2 I2 mismatch at %s/%s: got=%+v want=%+v err=%v", raw.Timestamp, raw.Group, got, expect, err)
		}
		delete(want, k)
		count++
		if got.State == Positive {
			positive++
		}
	}
	if count != 46080 || len(want) != 0 || positive != 1408 {
		t.Fatalf("D2 count=%d, missing=%d, I2-positive=%d; want 46080/0/1408", count, len(want), positive)
	}
	t.Logf("all %d keyed states and counts exact; raw I2-positive=%d", count, positive)
}
