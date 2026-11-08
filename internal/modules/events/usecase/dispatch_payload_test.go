package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

// These tests use asyncFakeOutbox: it models pending in memory and does not
// exercise a real SQL database or its durability guarantees.
type payloadSyncSender struct {
	err   error
	calls int
}

func (s *payloadSyncSender) Send(context.Context, []byte, string) error {
	s.calls++
	return s.err
}
func (*payloadSyncSender) Close() error { return nil }

func waitRun(t *testing.T, stopped <-chan error) error {
	t.Helper()
	select {
	case err := <-stopped:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher did not stop")
		return nil
	}
}

func TestSyncPermanentDeliveryStopsAndRestartReplaysPending(t *testing.T) {
	o := newAsyncFakeOutbox()
	item := resultDelivery(t, "a01", "sync-permanent", 0)
	o.put("a01", item)
	permanent := domain.PermanentDelivery(errors.New("invalid payload"))
	s1 := &payloadSyncSender{err: permanent}
	d1 := &Dispatcher{ExecutionID: testExecution, Mode: "kafka", Store: o, Sender: s1}
	trackOwner(t, d1, "a01", "first", 1)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- d1.Run(ctx) }()
	err := waitRun(t, stopped)
	cancel()
	if !domain.IsPermanentDelivery(err) {
		t.Fatalf("Run error = %v, want permanent delivery error", err)
	}
	if got := o.ackCount(); got != 0 {
		t.Fatalf("permanent failure Acked %d records", got)
	}
	if s1.calls != 1 {
		t.Fatalf("permanent failure attempts = %d, want 1", s1.calls)
	}

	s2 := &payloadSyncSender{}
	d2 := &Dispatcher{ExecutionID: testExecution, Mode: "kafka", Store: o, Sender: s2}
	trackOwner(t, d2, "a01", "second", 2)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	stopped2 := make(chan error, 1)
	go func() { stopped2 <- d2.Run(ctx2) }()
	awaitAck(t, o, 1)
	cancel2()
	if err := waitRun(t, stopped2); err != nil {
		t.Fatal(err)
	}
	if s2.calls != 1 || o.ackCount() != 1 {
		t.Fatalf("restart calls=%d acks=%d, want 1 each", s2.calls, o.ackCount())
	}
}

type payloadImmediateSender struct{ err error }

func (s *payloadImmediateSender) SendAsync(_ context.Context, _ []byte, _ string, cb func(error)) error {
	if s.err != nil {
		return s.err
	}
	cb(nil)
	return nil
}
func (*payloadImmediateSender) Flush(context.Context) error { return nil }
func (*payloadImmediateSender) Close() error                { return nil }

func TestAsyncImmediatePermanentDeliveryStopsAndRestartReplaysPending(t *testing.T) {
	o := newAsyncFakeOutbox()
	o.put("a01", resultDelivery(t, "a01", "async-immediate-permanent", 0))
	d1 := &Dispatcher{ExecutionID: testExecution, Mode: "kafka", Store: o, AsyncSender: &payloadImmediateSender{err: domain.PermanentDelivery(errors.New("oversize"))}}
	trackOwner(t, d1, "a01", "first", 1)
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	err := d1.Run(ctx1)
	if !domain.IsPermanentDelivery(err) {
		t.Fatalf("Run error = %v, want permanent delivery error", err)
	}
	if o.ackCount() != 0 {
		t.Fatal("immediate permanent failure acknowledged pending record")
	}
	d2 := &Dispatcher{ExecutionID: testExecution, Mode: "kafka", Store: o, AsyncSender: &payloadImmediateSender{}}
	trackOwner(t, d2, "a01", "second", 2)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- d2.Run(ctx) }()
	awaitAck(t, o, 1)
	cancel()
	if err := waitRun(t, stopped); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncCallbackPermanentDeliveryStopsAndRestartReplaysPending(t *testing.T) {
	o := newAsyncFakeOutbox()
	item := resultDelivery(t, "a01", "async-callback-permanent", 0)
	e0, _ := domain.Decode(item.Envelope)
	e0.Payload = json.RawMessage(`{"padding":""}`)
	base, _ := json.Marshal(e0)
	e0.Payload = json.RawMessage(`{"padding":"` + string(bytes.Repeat([]byte{'x'}, domain.MaxMessageBytes-len(base))) + `"}`)
	item.Envelope, _ = json.Marshal(e0)
	if len(item.Envelope) != domain.MaxMessageBytes {
		t.Fatal("boundary fixture")
	}
	o.put("a01", item)
	sender := newHeldAsyncSender()
	d1 := &Dispatcher{ExecutionID: testExecution, Mode: "kafka", Store: o, AsyncSender: sender, AsyncConfig: AsyncConfig{DrainTimeout: 150 * time.Millisecond}}
	trackOwner(t, d1, "a01", "first", 1)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- d1.Run(ctx) }()
	sender.call(t, 0).cb(domain.PermanentDelivery(errors.New("broker rejected record")))
	if err := waitRun(t, stopped); !domain.IsPermanentDelivery(err) {
		t.Fatalf("Run error = %v, want permanent delivery error", err)
	}
	cancel()
	if o.ackCount() != 0 {
		t.Fatal("callback permanent failure acknowledged pending record")
	}

	s2 := newHeldAsyncSender()
	d2 := &Dispatcher{ExecutionID: testExecution, Mode: "kafka", Store: o, AsyncSender: s2}
	trackOwner(t, d2, "a01", "second", 2)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	stopped2 := make(chan error, 1)
	go func() { stopped2 <- d2.Run(ctx2) }()
	replay := s2.call(t, 0)
	e, err := domain.Decode(replay.data)
	if err != nil || e.MessageID != "async-callback-permanent" || !bytes.Equal(replay.data, item.Envelope) {
		t.Fatalf("restart did not replay pending message: id=%q err=%v", e.MessageID, err)
	}
	replay.cb(nil)
	awaitAck(t, o, 1)
	cancel2()
	if err := waitRun(t, stopped2); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncTransientCallbackRetriesAndAcknowledgesAfterSuccess(t *testing.T) {
	d, o, sender, _ := setupAsync(t, 2, domain.MaxMessageBytes+256)
	owner := trackOwner(t, d, "a01", "worker", 1)
	o.put(owner.GroupID, resultDelivery(t, owner.GroupID, "transient-retry", 0))
	sender.call(t, 0).cb(errors.New("temporary broker failure"))
	sender.call(t, 1).cb(nil)
	awaitAck(t, o, 1)
	if sender.count() != 2 {
		t.Fatalf("send attempts=%d, want transient retry then success", sender.count())
	}
}
