package usecase

import (
	"context"
	"errors"
	"fmt"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"io"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

type sliceReader struct {
	records []domain.TelemetryRecord
	index   int
	failure error
}

func (r *sliceReader) Next(context.Context) (domain.TelemetryRecord, error) {
	if r.index == len(r.records) {
		if r.failure != nil {
			return domain.TelemetryRecord{}, r.failure
		}
		return domain.TelemetryRecord{}, io.EOF
	}
	v := r.records[r.index]
	r.index++
	return v, nil
}

type captureSender struct {
	records  []domain.TelemetryRecord
	complete []domain.TimestampComplete
	failure  error
	onSend   func()
}

func (s *captureSender) Send(_ context.Context, r domain.TelemetryRecord) error {
	if s.failure != nil {
		return s.failure
	}
	s.records = append(s.records, r)
	if s.onSend != nil {
		s.onSend()
	}
	return nil
}
func (s *captureSender) Complete(_ context.Context, c domain.TimestampComplete) error {
	s.complete = append(s.complete, c)
	return nil
}

type fakeClock struct {
	now    time.Time
	sleeps []time.Duration
}

func (c *fakeClock) Now() time.Time { return c.now }
func (c *fakeClock) Sleep(_ context.Context, d time.Duration) error {
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
	return nil
}

func fixtureRecord(offset int, host string) domain.TelemetryRecord {
	group, index, err := topology.ParseHostname(host)
	if err != nil {
		panic(err)
	}
	r := domain.TelemetryRecord{Timestamp: time.Date(2020, 1, 1, 0, 0, offset, 0, time.UTC), Hostname: host, GroupID: group, NodeIndex: index, Valid: true, AvailabilityMask: domain.FullAvailabilityMask}
	var features [18]float64
	for i := range features {
		features[i] = float64(i + 1)
	}
	r.SetFeatures(features)
	return r
}

func TestReplayOrderLogicalTimeAndCompletion(t *testing.T) {
	records := []domain.TelemetryRecord{fixtureRecord(0, "a01n01"), fixtureRecord(0, "a01n02"), fixtureRecord(0, "a02n01"), fixtureRecord(10, "a01n01")}
	sender := &captureSender{}
	clock := &fakeClock{now: time.Now()}
	stats, err := Replay(context.Background(), &sliceReader{records: records}, sender, sender, clock, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sender.records, records) {
		t.Fatal("record fields or original timestamps changed")
	}
	if len(clock.sleeps) != 0 {
		t.Fatal("speed 0 introduced a delay")
	}
	want := []domain.TimestampComplete{
		{Timestamp: records[0].Timestamp, GroupID: "a01", Records: 2, ValidRecords: 2},
		{Timestamp: records[2].Timestamp, GroupID: "a02", Records: 1, ValidRecords: 1},
		{Timestamp: records[3].Timestamp, GroupID: "a01", Records: 1, ValidRecords: 1},
	}
	if !reflect.DeepEqual(sender.complete, want) {
		t.Fatalf("completion events: %+v", sender.complete)
	}
	if stats.Rows != 4 || stats.Timestamps != 2 || stats.Groups != 2 || stats.Hosts != 3 || stats.Completions != 3 {
		t.Fatalf("stats: %+v", stats)
	}
}

func TestSpeedScalingWithInjectedClock(t *testing.T) {
	for _, speed := range []float64{0, 1, 10, 100} {
		t.Run(fmt.Sprintf("speed_%g", speed), func(t *testing.T) {
			clock := &fakeClock{now: time.Now()}
			sender := &captureSender{}
			_, err := Replay(context.Background(), &sliceReader{records: []domain.TelemetryRecord{fixtureRecord(0, "a01n01"), fixtureRecord(10, "a01n01"), fixtureRecord(20, "a01n01")}}, sender, sender, clock, speed)
			if err != nil {
				t.Fatal(err)
			}
			if speed == 0 {
				if len(clock.sleeps) != 0 {
					t.Fatal(clock.sleeps)
				}
				return
			}
			want := time.Duration(float64(10*time.Second) / speed)
			if !reflect.DeepEqual(clock.sleeps, []time.Duration{want, want}) {
				t.Fatalf("got %v expected %v", clock.sleeps, want)
			}
		})
	}
}

func TestInvalidRecordIsSentWithAvailabilityAndNullCoordinate(t *testing.T) {
	r := fixtureRecord(0, "a01n01")
	r.PSUPower[0] = math.NaN()
	r.AvailabilityMask &^= 1
	r.Valid = false
	sender := &captureSender{}
	stats, err := Replay(context.Background(), &sliceReader{records: []domain.TelemetryRecord{r}}, sender, sender, &fakeClock{now: time.Now()}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(sender.records) != 1 || !math.IsNaN(sender.records[0].PSUPower[0]) || sender.records[0].Valid {
		t.Fatal("invalid record was dropped or coerced")
	}
	if stats.Invalid != 1 || sender.complete[0].ValidRecords != 0 || sender.complete[0].Records != 1 {
		t.Fatal("incorrect availability/completion statistics")
	}
}

func TestPacingUsesAbsoluteTimelineAndAccountsForSending(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	sender := &captureSender{onSend: func() { clock.now = clock.now.Add(3 * time.Second) }}
	_, err := Replay(context.Background(), &sliceReader{records: []domain.TelemetryRecord{
		fixtureRecord(0, "a01n01"), fixtureRecord(10, "a01n01"), fixtureRecord(20, "a01n01"),
	}}, sender, sender, clock, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(clock.sleeps, []time.Duration{7 * time.Second, 7 * time.Second}) {
		t.Fatal("sending introduced pacing drift:", clock.sleeps)
	}
}

func TestRejectsUnsortedDuplicateOrMalformedInput(t *testing.T) {
	bad := fixtureRecord(0, "a01n01")
	bad.AvailabilityMask = 0
	for _, records := range [][]domain.TelemetryRecord{
		{fixtureRecord(10, "a01n01"), fixtureRecord(0, "a01n01")},
		{fixtureRecord(0, "a02n01"), fixtureRecord(0, "a01n01")},
		{fixtureRecord(0, "a01n02"), fixtureRecord(0, "a01n01")},
		{fixtureRecord(0, "a01n01"), fixtureRecord(0, "a01n01")},
		{bad},
	} {
		sender := &captureSender{}
		if _, err := Replay(context.Background(), &sliceReader{records: records}, sender, sender, &fakeClock{}, 0); err == nil {
			t.Fatal("bad input accepted")
		}
		if len(sender.complete) != 0 {
			t.Fatal("failed/partial group marked complete")
		}
	}
}

func TestErrorsAndCancellationDoNotCompletePartialGroup(t *testing.T) {
	boom := errors.New("failure")
	sender := &captureSender{}
	_, err := Replay(context.Background(), &sliceReader{records: []domain.TelemetryRecord{fixtureRecord(0, "a01n01")}, failure: boom}, sender, sender, &fakeClock{}, 0)
	if !errors.Is(err, boom) || len(sender.complete) != 0 {
		t.Fatal("read error lost or partial completion emitted")
	}
	sender = &captureSender{failure: boom}
	_, err = Replay(context.Background(), &sliceReader{records: []domain.TelemetryRecord{fixtureRecord(0, "a01n01")}}, sender, sender, &fakeClock{}, 0)
	if !errors.Is(err, boom) || len(sender.complete) != 0 {
		t.Fatal("send error lost")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Replay(ctx, &sliceReader{}, sender, sender, &fakeClock{}, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, speed := range []float64{-1, math.NaN(), math.Inf(1)} {
		if _, err := Replay(context.Background(), &sliceReader{}, sender, sender, &fakeClock{}, speed); err == nil {
			t.Fatal("invalid speed accepted")
		}
	}
}
