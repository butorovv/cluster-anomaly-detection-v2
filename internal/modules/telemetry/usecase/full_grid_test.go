package usecase

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

type gridReader struct {
	rows []domain.TelemetryRecord
	i    int
}

func (r *gridReader) Next(context.Context) (domain.TelemetryRecord, error) {
	if r.i == len(r.rows) {
		return domain.TelemetryRecord{}, io.EOF
	}
	v := r.rows[r.i]
	r.i++
	return v, nil
}

type gridSink struct {
	rows  int
	marks []domain.TimestampComplete
}

func (s *gridSink) Send(_ context.Context, _ domain.TelemetryRecord) error { s.rows++; return nil }
func (s *gridSink) Complete(_ context.Context, c domain.TimestampComplete) error {
	s.marks = append(s.marks, c)
	return nil
}

type gridClock struct {
	now    time.Time
	sleeps []time.Duration
}

func (c *gridClock) Now() time.Time { return c.now }
func (c *gridClock) Sleep(_ context.Context, d time.Duration) error {
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
	return nil
}
func validGridRecord(at time.Time, host string, node int) domain.TelemetryRecord {
	r := domain.TelemetryRecord{Timestamp: at, Hostname: host, GroupID: "a01", NodeIndex: node, Valid: true, AvailabilityMask: domain.FullAvailabilityMask}
	var f [domain.FeatureCount]float64
	for i := range f {
		f[i] = float64(i)
	}
	r.SetFeatures(f)
	return r
}
func TestReplayFullGridEmptyEmitsAllPairsAndTicks(t *testing.T) {
	start := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(20 * time.Second)
	sink := &gridSink{}
	clock := &gridClock{now: start}
	stats, err := ReplayFullGrid(context.Background(), &gridReader{}, sink, sink, clock, 1, start, end)
	if err != nil || stats.Completions != 32 || stats.Timestamps != 2 || stats.Rows != 0 || len(sink.marks) != 32 || len(clock.sleeps) != 1 {
		t.Fatalf("stats=%+v marks=%d sleeps=%v err=%v", stats, len(sink.marks), clock.sleeps, err)
	}
	for i, m := range sink.marks {
		if m.GroupID != fmt.Sprintf("a%02d", i%16+1) || !m.Timestamp.Equal(start.Add(time.Duration(i/16)*10*time.Second)) || m.Records != 0 {
			t.Fatal(i, m)
		}
	}
}
func TestReplayFullGridSendsRowsAtMatchingPair(t *testing.T) {
	start := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []domain.TelemetryRecord{validGridRecord(start, "a01n01", 1), validGridRecord(start.Add(10*time.Second), "a01n02", 2)}
	sink := &gridSink{}
	stats, err := ReplayFullGrid(context.Background(), &gridReader{rows: rows}, sink, sink, &gridClock{now: start}, 0, start, start.Add(20*time.Second))
	if err != nil || stats.Rows != 2 || stats.Completions != 32 || sink.marks[0].Records != 1 || sink.marks[16].Records != 1 {
		t.Fatal(stats, err)
	}
}
func TestReplayFullGridRejectsWindow(t *testing.T) {
	start := time.Date(2022, 1, 1, 0, 0, 0, 1, time.UTC)
	_, err := ReplayFullGrid(context.Background(), &gridReader{}, &gridSink{}, &gridSink{}, &gridClock{now: start}, math.NaN(), start, start.Add(time.Second))
	if err == nil {
		t.Fatal("invalid window accepted")
	}
}

func TestFullD2EmptyGridAndPacing(t *testing.T) {
	start := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	for _, speed := range []float64{0, 2} {
		sink := &gridSink{}
		clock := &gridClock{now: start}
		stats, err := ReplayFullGrid(context.Background(), &gridReader{}, sink, sink, clock, speed, start, start.Add(8*time.Hour))
		if err != nil || stats.Completions != 46080 || stats.Timestamps != 2880 || stats.Rows != 0 {
			t.Fatal(stats, err)
		}
		for i, m := range sink.marks {
			if m.GroupID != fmt.Sprintf("a%02d", i%16+1) || m.Timestamp != start.Add(time.Duration(i/16)*10*time.Second) || m.Records != 0 || m.ValidRecords != 0 {
				t.Fatal(i, m)
			}
		}
		if speed == 0 && len(clock.sleeps) != 0 || speed > 0 && len(clock.sleeps) != 2879 {
			t.Fatal("pacing repeated for each group", len(clock.sleeps))
		}
		for _, d := range clock.sleeps {
			if d != 5*time.Second {
				t.Fatal(d)
			}
		}
	}
}

func TestFullGridMissingHeadMiddleTailAndWholeGroups(t *testing.T) {
	start := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	r := validGridRecord(start.Add(10*time.Second), "a01n01", 1)
	r.PSUPower[0] = math.NaN()
	r.Valid = false
	r.AvailabilityMask &^= 1
	sink := &gridSink{}
	stats, err := ReplayFullGrid(context.Background(), &gridReader{rows: []domain.TelemetryRecord{r}}, sink, sink, &gridClock{}, 0, start, start.Add(50*time.Second))
	if err != nil || stats.Rows != 1 || stats.Invalid != 1 || stats.Completions != 80 {
		t.Fatal(stats, err)
	}
	for i, m := range sink.marks {
		if i == 16 {
			if m.Records != 1 || m.ValidRecords != 0 {
				t.Fatal(m)
			}
		} else if m.Records != 0 || m.ValidRecords != 0 {
			t.Fatal(i, m)
		}
	}
}

func TestFullGridRejectsSourceProtocolAndParameters(t *testing.T) {
	start := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	base := validGridRecord(start, "a01n01", 1)
	later := base
	later.Timestamp = later.Timestamp.Add(10 * time.Second)
	second := base
	second.Hostname = "a01n02"
	second.NodeIndex = 2
	for name, rows := range map[string][]domain.TelemetryRecord{
		"duplicate": {base, base}, "host_order": {second, base}, "time_order": {later, base},
		"offgrid":   {func() domain.TelemetryRecord { r := base; r.Timestamp = r.Timestamp.Add(time.Second); return r }()},
		"outside":   {func() domain.TelemetryRecord { r := base; r.Timestamp = r.Timestamp.Add(20 * time.Second); return r }()},
		"canonical": {func() domain.TelemetryRecord { r := base; r.Hostname = "a1n1"; return r }()},
		"mask":      {func() domain.TelemetryRecord { r := base; r.AvailabilityMask = 0; return r }()},
	} {
		t.Run(name, func(t *testing.T) {
			sink := &gridSink{}
			if _, err := ReplayFullGrid(context.Background(), &gridReader{rows: rows}, sink, sink, &gridClock{now: start}, 0, start, start.Add(20*time.Second)); err == nil {
				t.Fatal("accepted bad source")
			}
		})
	}
	for _, speed := range []float64{-1, math.NaN(), math.Inf(1)} {
		sink := &gridSink{}
		if _, err := ReplayFullGrid(context.Background(), &gridReader{}, sink, sink, &gridClock{now: start}, speed, start, start.Add(20*time.Second)); err == nil {
			t.Fatal(speed)
		}
	}
	for _, window := range [][2]time.Time{{start, start}, {start, start.Add(time.Second)}, {start.Add(time.Second), start.Add(20 * time.Second)}, {start.In(time.FixedZone("offset", 3600)), start.Add(20 * time.Second)}} {
		sink := &gridSink{}
		if _, err := ReplayFullGrid(context.Background(), &gridReader{}, sink, sink, &gridClock{now: start}, 0, window[0], window[1]); err == nil {
			t.Fatal(window)
		}
	}
}

func TestFullGridPreservesErrorsAndCancellation(t *testing.T) {
	start := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	base := validGridRecord(start, "a01n01", 1)
	boom := errors.New("source/sink failure")
	sink := &captureSender{}
	stats, err := ReplayFullGrid(context.Background(), &sliceReader{failure: boom}, sink, sink, &fakeClock{}, 0, start, start.Add(20*time.Second))
	if !errors.Is(err, boom) || stats.Completions != 0 {
		t.Fatal(stats, err)
	}
	sink = &captureSender{failure: boom}
	stats, err = ReplayFullGrid(context.Background(), &sliceReader{records: []domain.TelemetryRecord{base}}, sink, sink, &fakeClock{}, 0, start, start.Add(20*time.Second))
	if !errors.Is(err, boom) || stats.Rows != 0 {
		t.Fatal(stats, err)
	}
	sink = &captureSender{}
	stats, err = ReplayFullGrid(context.Background(), &sliceReader{}, sink, failingCompleter{boom}, &fakeClock{}, 0, start, start.Add(20*time.Second))
	if !errors.Is(err, boom) || stats.Completions != 0 {
		t.Fatal(stats, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	sink = &captureSender{onSend: cancel}
	stats, err = ReplayFullGrid(ctx, &sliceReader{records: []domain.TelemetryRecord{base}}, sink, sink, &fakeClock{}, 0, start, start.Add(20*time.Second))
	if !errors.Is(err, context.Canceled) || stats.Completions != 0 {
		t.Fatal(stats, err)
	}
	sink = &captureSender{}
	_, err = ReplayFullGrid(context.Background(), &sliceReader{}, sink, sink, canceledClock{}, 1, start, start.Add(20*time.Second))
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
