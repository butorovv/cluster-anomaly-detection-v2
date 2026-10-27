package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

type failingCompleter struct{ err error }

func (s failingCompleter) Complete(context.Context, domain.TimestampComplete) error { return s.err }

type canceledClock struct{}

func (canceledClock) Now() time.Time                             { return time.Time{} }
func (canceledClock) Sleep(context.Context, time.Duration) error { return context.Canceled }

func TestCompletionFailureEmptyInputAndCancellation(t *testing.T) {
	boom := errors.New("completion failed")
	for _, records := range [][]domain.TelemetryRecord{
		{fixtureRecord(0, "a01n01")},
		{fixtureRecord(0, "a01n01"), fixtureRecord(0, "a02n01")},
	} {
		sender := &captureSender{}
		stats, err := Replay(context.Background(), &sliceReader{records: records}, sender, failingCompleter{boom}, &fakeClock{}, 0)
		if !errors.Is(err, boom) || stats.Rows != 1 || stats.Completions != 0 || len(sender.records) != 1 {
			t.Fatal(stats, err)
		}
	}
	sender := &captureSender{}
	stats, err := Replay(context.Background(), &sliceReader{}, sender, sender, &fakeClock{}, 0)
	if err != nil || stats != (Stats{}) || len(sender.complete) != 0 {
		t.Fatal(stats, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	sender = &captureSender{onSend: cancel}
	stats, err = Replay(ctx, &sliceReader{records: []domain.TelemetryRecord{fixtureRecord(0, "a01n01")}}, sender, sender, &fakeClock{}, 0)
	if !errors.Is(err, context.Canceled) || stats.Rows != 1 || len(sender.complete) != 0 {
		t.Fatal(stats, err)
	}
	sender = &captureSender{}
	_, err = Replay(context.Background(), &sliceReader{records: []domain.TelemetryRecord{fixtureRecord(0, "a01n01"), fixtureRecord(10, "a01n01")}}, sender, sender, canceledClock{}, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
