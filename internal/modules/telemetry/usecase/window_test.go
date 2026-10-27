package usecase

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

type windowFixture struct {
	rows []domain.TelemetryRecord
	i    int
}

func (f *windowFixture) Next(context.Context) (domain.TelemetryRecord, error) {
	if f.i == len(f.rows) {
		return domain.TelemetryRecord{}, io.EOF
	}
	v := f.rows[f.i]
	f.i++
	return v, nil
}

func TestWindowReaderHalfOpenOrderMissingAndCancellation(t *testing.T) {
	start := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	var rows []domain.TelemetryRecord
	for _, offset := range []time.Duration{-10 * time.Second, 0, 10 * time.Second, 20 * time.Second} {
		v := fixtureRecord(0, "a01n01")
		v.Timestamp = start.Add(offset)
		rows = append(rows, v)
	}
	r := WindowReader{Source: &windowFixture{rows: rows}, Start: start, End: start.Add(20 * time.Second)}
	for _, offset := range []time.Duration{0, 10 * time.Second} {
		v, err := r.Next(context.Background())
		if err != nil || !v.Timestamp.Equal(start.Add(offset)) {
			t.Fatal(v, err)
		}
	}
	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	r = WindowReader{Source: &windowFixture{rows: []domain.TelemetryRecord{rows[0], rows[0]}}, Start: start, End: start.Add(time.Hour)}
	if _, err := r.Next(context.Background()); err == nil {
		t.Fatal("duplicate in skipped range accepted")
	}
}

func TestWindowReaderStopsAtExcludedEnd(t *testing.T) {
	start := time.Date(2022, 1, 18, 0, 0, 0, 0, time.UTC)
	var rows []domain.TelemetryRecord
	for _, offset := range []time.Duration{0, 10 * time.Second, 20 * time.Second, 30 * time.Second} {
		v := fixtureRecord(0, "a01n01")
		v.Timestamp = start.Add(offset)
		rows = append(rows, v)
	}
	source := &windowFixture{rows: rows}
	r := WindowReader{Source: source, Start: start, End: start.Add(20 * time.Second)}
	for i := 0; i < 2; i++ {
		if _, err := r.Next(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
	}
	if source.i != 3 {
		t.Fatal("traversed held-out suffix", source.i)
	}
}

func TestFilteredReplayCompletesLastAllowedTimestamp(t *testing.T) {
	start := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	var rows []domain.TelemetryRecord
	for _, offset := range []time.Duration{-10 * time.Second, 0, 10 * time.Second, 20 * time.Second} {
		for _, host := range []string{"a01n01", "a01n02", "a02n01"} {
			v := fixtureRecord(0, host)
			v.Timestamp = start.Add(offset)
			rows = append(rows, v)
		}
	}
	r := WindowReader{Source: &sliceReader{records: rows}, Start: start, End: start.Add(20 * time.Second)}
	sender := &captureSender{}
	stats, err := Replay(context.Background(), &r, sender, sender, &fakeClock{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sender.records, rows[3:9]) || stats.Rows != 6 || stats.Completions != 4 {
		t.Fatal(stats, sender.records)
	}
	want := []domain.TimestampComplete{
		{Timestamp: start, GroupID: "a01", Records: 2, ValidRecords: 2},
		{Timestamp: start, GroupID: "a02", Records: 1, ValidRecords: 1},
		{Timestamp: start.Add(10 * time.Second), GroupID: "a01", Records: 2, ValidRecords: 2},
		{Timestamp: start.Add(10 * time.Second), GroupID: "a02", Records: 1, ValidRecords: 1},
	}
	if !reflect.DeepEqual(sender.complete, want) {
		t.Fatal(sender.complete)
	}
}
