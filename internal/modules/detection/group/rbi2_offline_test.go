package group

import (
	"context"
	"testing"
)

func TestRBI2OfflineRejectsInvalidRequestBeforeLoading(t *testing.T) {
	if _, err := RunRBI2Offline(nil, RBI2OfflineOptions{}); err == nil {
		t.Fatal("nil context/sinks accepted")
	}
	sink := RBI2Sink{Raw: func(Raw) error { return nil }, Temporal: func(RBI2TemporalResult) error { return nil }, Evidence: func(RBI2Evidence) error { return nil }, Event: func(RBI2Event) error { return nil }}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RunRBI2Offline(ctx, RBI2OfflineOptions{Sink: sink}); err != context.Canceled {
		t.Fatal("cancellation did not precede I/O", err)
	}
	if _, err := RunRBI2Offline(context.Background(), RBI2OfflineOptions{Sink: sink, Groups: 3}); err == nil {
		t.Fatal("invalid group universe accepted")
	}
}
