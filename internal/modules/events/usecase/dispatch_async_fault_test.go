package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/jackc/pgx/v5/pgconn"
)

type immediateAsyncSender struct{ callback atomic.Bool }

func (s *immediateAsyncSender) SendAsync(_ context.Context, _ []byte, _ string, done func(error)) error {
	s.callback.Store(true)
	done(nil)
	done(nil)
	s.callback.Store(false)
	return nil
}
func (*immediateAsyncSender) Flush(context.Context) error { return nil }
func (*immediateAsyncSender) Close() error                { return nil }

func startAsyncCase(t *testing.T, d *Dispatcher) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- d.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = d.Close()
		select {
		case err := <-stopped:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(2 * time.Second):
			t.Error("async dispatcher did not join workers")
		}
	})
	return cancel
}

func TestAsyncImmediateDuplicateCallbackIsNotSQLExecution(t *testing.T) {
	o := newAsyncFakeOutbox()
	o.put("a01", resultDelivery(t, "a01", "once", 0))
	s := &immediateAsyncSender{}
	o.ackFn = func(context.Context, domain.Owner, string) error {
		if s.callback.Load() {
			t.Error("SQL executed within callback")
		}
		return nil
	}
	d := &Dispatcher{ExecutionID: testExecution, Mode: "kafka", Store: o, AsyncSender: s}
	trackOwner(t, d, "a01", "worker", 1)
	startAsyncCase(t, d)
	awaitAck(t, o, 1)
	if o.ackCount() != 1 {
		t.Fatal("duplicate callback acknowledged twice")
	}
}

func TestAsyncByteBudgetBackpressureAndPayloadOwnership(t *testing.T) {
	d, o, s, _ := setupAsync(t, 2, domain.MaxMessageBytes+256)
	for _, g := range []string{"a01", "a02"} {
		item := resultDelivery(t, g, g, 0)
		e, _ := domain.Decode(item.Envelope)
		e.Payload = json.RawMessage(`{"padding":"` + strings.Repeat("x", 3<<20) + `"}`)
		item.Envelope, _ = json.Marshal(e)
		o.put(g, item)
		trackOwner(t, d, g, "worker", 1)
	}
	first := s.call(t, 0)
	// Both tasks have loaded their pages; the second cannot enter Produce until
	// the first releases its byte reservation (not merely its enqueue call).
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case owner := <-o.pages:
			seen[owner.GroupID] = true
		case <-time.After(2 * time.Second):
			t.Fatal("second page was not loaded")
		}
	}
	time.Sleep(20 * time.Millisecond)
	if s.count() != 1 {
		t.Fatal("payload bytes exceeded budget")
	}
	if _, err := domain.Decode(first.data); err != nil {
		t.Fatal("borrowed payload corrupted", err)
	}
	first.cb(nil)
	second := s.call(t, 1)
	if second.key == first.key {
		t.Fatal("other group starved")
	}
	second.cb(nil)
	awaitAck(t, o, 2)
}

func TestAsyncSQLFenceRetiresEpochUntilReassignment(t *testing.T) {
	d, o, s, _ := setupAsync(t, 2, domain.MaxMessageBytes+256)
	var allow atomic.Bool
	attempted := make(chan struct{}, 1)
	o.ackFn = func(context.Context, domain.Owner, string) error {
		if !allow.Load() {
			select {
			case attempted <- struct{}{}:
			default:
			}
			return &pgconn.PgError{Code: "55000", Message: "stale owner"}
		}
		return nil
	}
	o.put("a01", resultDelivery(t, "a01", "same-id", 0))
	trackOwner(t, d, "a01", "old", 1)
	old := s.call(t, 0)
	old.cb(nil)
	select {
	case <-attempted:
	case <-time.After(2 * time.Second):
		t.Fatal("missing fenced ACK")
	}
	time.Sleep(70 * time.Millisecond)
	if s.count() != 1 || o.ackCount() != 0 {
		t.Fatal("stale epoch was reused")
	}
	allow.Store(true)
	trackOwner(t, d, "a01", "new", 2)
	fresh := s.call(t, 1)
	a, _ := domain.Decode(old.data)
	b, _ := domain.Decode(fresh.data)
	if a.MessageID != b.MessageID {
		t.Fatal("recovery changed MessageID")
	}
	fresh.cb(nil)
	awaitAck(t, o, 1)
}

func TestAsyncCrashAfterKafkaACKBeforeSQLPreservesPending(t *testing.T) {
	o := newAsyncFakeOutbox()
	o.put("a01", resultDelivery(t, "a01", "durable-id", 0))
	var dbUnavailable atomic.Bool
	dbUnavailable.Store(true)
	entered := make(chan struct{}, 1)
	o.ackFn = func(context.Context, domain.Owner, string) error {
		if dbUnavailable.Load() {
			select {
			case entered <- struct{}{}:
			default:
			}
			return errors.New("lost SQL ACK")
		}
		return nil
	}
	s := newHeldAsyncSender()
	d := &Dispatcher{ExecutionID: testExecution, Mode: "kafka", Store: o, AsyncSender: s, AsyncConfig: AsyncConfig{DrainTimeout: 50 * time.Millisecond}}
	trackOwner(t, d, "a01", "old", 1)
	cancel := startAsyncCase(t, d)
	old := s.call(t, 0)
	old.cb(nil)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("SQL ACK not reached")
	}
	cancel()
	_ = d.Close()
	if o.ackCount() != 0 {
		t.Fatal("unconfirmed SQL ACK advanced outbox")
	}
	dbUnavailable.Store(false)
	s2 := newHeldAsyncSender()
	d2 := &Dispatcher{ExecutionID: testExecution, Mode: "kafka", Store: o, AsyncSender: s2}
	trackOwner(t, d2, "a01", "new", 2)
	startAsyncCase(t, d2)
	replay := s2.call(t, 0)
	a, _ := domain.Decode(old.data)
	b, _ := domain.Decode(replay.data)
	if a.MessageID != b.MessageID {
		t.Fatal("durable replay changed ID")
	}
	replay.cb(nil)
	awaitAck(t, o, 1)
}

func TestAsyncInvalidLimitsFailBeforePublish(t *testing.T) {
	for _, cfg := range []AsyncConfig{{MaxRecords: 17}, {MaxBytes: 1}, {DrainTimeout: -1}} {
		d := &Dispatcher{Mode: "kafka", AsyncSender: newHeldAsyncSender(), AsyncConfig: cfg}
		if err := d.Run(context.Background()); err == nil {
			t.Fatal("invalid limits accepted", cfg)
		}
	}
}

func TestAsyncDrainCancelsPendingSQLAndJoins(t *testing.T) {
	o := newAsyncFakeOutbox()
	o.put("a01", resultDelivery(t, "a01", "pending-sql", 0))
	entered := make(chan struct{}, 1)
	o.ackFn = func(ctx context.Context, _ domain.Owner, _ string) error {
		entered <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	d := &Dispatcher{ExecutionID: testExecution, Mode: "kafka", Store: o, AsyncSender: &immediateAsyncSender{}, AsyncConfig: AsyncConfig{DrainTimeout: 50 * time.Millisecond}}
	trackOwner(t, d, "a01", "worker", 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- d.Run(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("SQL ACK not reached")
	}
	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel and join SQL ACK")
	}
	if o.ackCount() != 0 {
		t.Fatal("unconfirmed record was acknowledged")
	}
}
