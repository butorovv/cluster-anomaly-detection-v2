package parquet

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestPhysicalWindowFromSummitArchive(t *testing.T) {
	path := os.Getenv("CAD_GUI_D2_PARQUET")
	if path == "" {
		t.Skip("CAD_GUI_D2_PARQUET points to the registered Summit archive")
	}
	ctx := context.Background()
	target := time.Date(2022, 1, 18, 15, 57, 20, 0, time.UTC)
	from := target.Add(-5 * time.Minute)
	const host = "a14n04"
	sequential, err := OpenParquet(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sequential.Close()

	expected := make(map[time.Time][]*float64)
	var previous time.Time
	for {
		row, err := sequential.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if row.Timestamp.Before(previous) {
			t.Fatalf("registered archive is not timestamp-ordered at %s", row.Timestamp)
		}
		previous = row.Timestamp
		if row.Hostname != host || row.Timestamp.Before(from) || row.Timestamp.After(target) {
			continue
		}
		var channels []*float64
		for _, value := range row.Features() {
			if math.IsNaN(value) {
				channels = append(channels, nil)
			} else {
				value := value
				channels = append(channels, &value)
			}
		}
		expected[row.Timestamp] = channels
	}

	reader, err := OpenParquet(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	actual, err := reader.PhysicalWindow(ctx, host, from, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(expected) != 31 || len(actual) != len(expected) {
		t.Fatalf("physical window: actual %d, source %d", len(actual), len(expected))
	}
	for _, sample := range actual {
		if !reflect.DeepEqual(sample.Values[:], expected[sample.Time]) {
			t.Fatalf("channels differ from the source at %s", sample.Time)
		}
	}
	for _, interval := range [][2]time.Time{{target, from}, {from, target.Add(6 * time.Minute)}, {from.Add(time.Second), target}} {
		if _, err := reader.PhysicalWindow(ctx, host, interval[0], interval[1]); err == nil {
			t.Fatalf("accepted invalid window %v", interval)
		}
	}
}
