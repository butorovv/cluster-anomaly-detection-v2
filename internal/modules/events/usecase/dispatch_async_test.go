package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

const testExecution = "123e4567-e89b-12d3-a456-426614174000"

type asyncCall struct {
	ctx  context.Context
	data []byte
	key  string
	cb   func(error)
}

// heldAsyncSender deliberately retains accepted callbacks so tests control their
// timing. Close completes any still-pending accepted sends as failed.
type heldAsyncSender struct {
	mu       sync.Mutex
	calls    []*asyncCall
	wake     chan struct{}
	closed   bool
	flushErr error
}

func newHeldAsyncSender() *heldAsyncSender { return &heldAsyncSender{wake: make(chan struct{}, 1)} }
func (s *heldAsyncSender) SendAsync(ctx context.Context, b []byte, key string, cb func(error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("closed")
	}
	s.calls = append(s.calls, &asyncCall{ctx: ctx, data: append([]byte(nil), b...), key: key, cb: cb})
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return nil
}
func (s *heldAsyncSender) Flush(ctx context.Context) error { return s.flushErr }
func (s *heldAsyncSender) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	calls := append([]*asyncCall(nil), s.calls...)
	s.mu.Unlock()
	for _, c := range calls {
		c.cb(errors.New("closed before delivery"))
	}
	return nil
}
func (s *heldAsyncSender) count() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.calls) }
func (s *heldAsyncSender) call(t *testing.T, n int) *asyncCall {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		s.mu.Lock()
		if len(s.calls) > n {
			c := s.calls[n]
			s.mu.Unlock()
			return c
		}
		s.mu.Unlock()
		select {
		case <-s.wake:
		case <-deadline.C:
			t.Fatalf("timed out waiting for async send %d", n+1)
		}
	}
}

type asyncFakeOutbox struct {
	mu    sync.Mutex
	items map[string][]domain.Delivery
	acks  []string
	ackFn func(context.Context, domain.Owner, string) error
	pages chan domain.Owner
}

func newAsyncFakeOutbox() *asyncFakeOutbox {
	return &asyncFakeOutbox{items: map[string][]domain.Delivery{}, pages: make(chan domain.Owner, 128)}
}
func (o *asyncFakeOutbox) put(group string, items ...domain.Delivery) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.items[group] = items
}
func (o *asyncFakeOutbox) Advertise(context.Context, domain.Owner, string) error { return nil }
func (o *asyncFakeOutbox) Page(ctx context.Context, owner domain.Owner, _ domain.Cursor, limit int, _ bool) ([]domain.Delivery, error) {
	select {
	case o.pages <- owner:
	default:
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	items := o.items[owner.GroupID]
	if len(items) > limit {
		items = items[:limit]
	}
	return append([]domain.Delivery(nil), items...), nil
}
func (o *asyncFakeOutbox) Ack(ctx context.Context, owner domain.Owner, id string) error {
	if o.ackFn != nil {
		if err := o.ackFn(ctx, owner, id); err != nil {
			return err
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.acks = append(o.acks, owner.GroupID+":"+id)
	items := o.items[owner.GroupID]
	for i, item := range items {
		e, _ := domain.Decode(item.Envelope)
		if e.MessageID == id {
			o.items[owner.GroupID] = append(items[:i:i], items[i+1:]...)
			break
		}
	}
	return nil
}
func (o *asyncFakeOutbox) AckCursor(context.Context, domain.Owner, domain.Cursor) error { return nil }
func (o *asyncFakeOutbox) ackCount() int                                                { o.mu.Lock(); defer o.mu.Unlock(); return len(o.acks) }

func resultDelivery(t *testing.T, group, id string, ordinal int) domain.Delivery {
	t.Helper()
	h := sha256.Sum256([]byte("id"))
	hash := hex.EncodeToString(h[:])
	value := domain.Envelope{SchemaVersion: 1, ExecutionID: testExecution, RunID: hash, ConfigID: hash, GroupID: group, MessageID: id, Kind: "group_result", Revision: int64(ordinal + 1), EventTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Payload: json.RawMessage(`{}`)}
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = domain.Decode(b); err != nil {
		t.Fatalf("invalid fixture envelope: %v", err)
	}
	return domain.Delivery{Cursor: domain.Cursor{Revision: int64(ordinal + 1), Ordinal: ordinal}, Envelope: b}
}
func setupAsync(t *testing.T, records int, bytes int64) (*Dispatcher, *asyncFakeOutbox, *heldAsyncSender, context.CancelFunc) {
	t.Helper()
	store := newAsyncFakeOutbox()
	sender := newHeldAsyncSender()
	ctx, cancel := context.WithCancel(context.Background())
	d := &Dispatcher{Store: store, ExecutionID: testExecution, Mode: "kafka", AsyncSender: sender, AsyncConfig: AsyncConfig{MaxRecords: records, MaxBytes: bytes, DrainTimeout: 150 * time.Millisecond}}
	finished := make(chan error, 1)
	go func() { finished <- d.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = d.Close()
		select {
		case err := <-finished:
			if err != nil {
				t.Error("dispatcher run", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("dispatcher goroutines did not stop")
		}
	})
	return d, store, sender, cancel
}
func trackOwner(t *testing.T, d *Dispatcher, group, owner string, epoch int64) domain.Owner {
	t.Helper()
	o := domain.Owner{ExecutionID: testExecution, GroupID: group, OwnerID: owner, Epoch: epoch}
	if err := d.Track(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	return o
}
func awaitAck(t *testing.T, o *asyncFakeOutbox, n int) {
	t.Helper()
	end := time.Now().Add(2 * time.Second)
	for time.Now().Before(end) {
		if o.ackCount() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("ack count=%d, want at least %d", o.ackCount(), n)
}

func TestAsyncDispatcherAcknowledgesOnlyAfterCallback(t *testing.T) {
	d, o, s, _ := setupAsync(t, 4, domain.MaxMessageBytes+256)
	own := trackOwner(t, d, "a01", "worker", 1)
	o.put(own.GroupID, resultDelivery(t, own.GroupID, "msg-1", 0))
	c := s.call(t, 0)
	if _, err := domain.Decode(c.data); err != nil {
		t.Fatalf("published invalid compact envelope: %v", err)
	}
	if got := o.ackCount(); got != 0 {
		t.Fatalf("Ack before delivery callback: %d", got)
	}
	c.cb(nil)
	awaitAck(t, o, 1)
}

func TestAsyncDispatcherKeepsGroupSlotUntilCallbackButServesOtherGroup(t *testing.T) {
	d, o, s, _ := setupAsync(t, 3, domain.MaxMessageBytes+256)
	a := trackOwner(t, d, "a01", "wa", 1)
	b := trackOwner(t, d, "a02", "wb", 1)
	o.put(a.GroupID, resultDelivery(t, a.GroupID, "a1", 0), resultDelivery(t, a.GroupID, "a2", 1))
	o.put(b.GroupID, resultDelivery(t, b.GroupID, "b1", 0))
	first := s.call(t, 0)
	second := s.call(t, 1)
	if first.key == second.key || (first.key != "a01" && second.key != "a01") || (first.key != "a02" && second.key != "a02") {
		t.Fatalf("unrelated group did not progress: key=%q", second.key)
	}
	if s.count() != 2 {
		t.Fatalf("same group admitted another unfinished record: %d sends", s.count())
	}
	second.cb(nil)
	first.cb(nil)
	awaitAck(t, o, 2)
}

func TestAsyncDispatcherSerializesGroupThroughSQLAckAndRetriesAckWithoutRepublish(t *testing.T) {
	d, o, s, _ := setupAsync(t, 4, domain.MaxMessageBytes+256)
	own := trackOwner(t, d, "a01", "w", 1)
	o.put(own.GroupID, resultDelivery(t, own.GroupID, "m1", 0), resultDelivery(t, own.GroupID, "m2", 1))
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	failures := 1
	o.ackFn = func(ctx context.Context, _ domain.Owner, _ string) error {
		once.Do(func() { entered <- struct{}{} })
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		if failures > 0 {
			failures--
			return errors.New("temporary SQL failure")
		}
		return nil
	}
	s.call(t, 0).cb(nil)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Ack was not attempted")
	}
	if s.count() != 1 {
		t.Fatalf("published next group record while SQL Ack unresolved: %d", s.count())
	}
	close(release)
	// An Ack retry must not publish the same delivery a second time.
	end := time.Now().Add(2 * time.Second)
	for time.Now().Before(end) && o.ackCount() == 0 {
		time.Sleep(time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, c := range s.calls {
		e, _ := domain.Decode(c.data)
		if e.MessageID == "m1" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("retry republished record: %d", count)
	}
}

func TestAsyncDispatcherIgnoresStaleCallbackAfterReassignment(t *testing.T) {
	d, o, s, _ := setupAsync(t, 4, domain.MaxMessageBytes+256)
	old := trackOwner(t, d, "a01", "old", 1)
	o.put(old.GroupID, resultDelivery(t, old.GroupID, "m1", 0))
	stale := s.call(t, 0)
	d.Forget(old.GroupID, old.Epoch)
	trackOwner(t, d, old.GroupID, "new", 2)
	stale.cb(nil)
	if got := o.ackCount(); got != 0 {
		t.Fatalf("stale callback Acked old assignment: %d", got)
	}
	fresh := s.call(t, 1)
	fresh.cb(nil)
	awaitAck(t, o, 1)
	if err := d.Track(context.Background(), domain.Owner{ExecutionID: "bad", GroupID: "z", OwnerID: "x", Epoch: 1}); err == nil {
		t.Fatal("Track accepted invalid execution")
	}
}

func TestAsyncDispatcherBoundsAcceptedRecords(t *testing.T) {
	d, o, s, _ := setupAsync(t, 2, domain.MaxMessageBytes+256)
	own := trackOwner(t, d, "a01", "w1", 1)
	other := trackOwner(t, d, "a02", "w2", 1)
	o.put(own.GroupID, resultDelivery(t, own.GroupID, "m1", 0), resultDelivery(t, own.GroupID, "m2", 1), resultDelivery(t, own.GroupID, "m3", 2))
	o.put(other.GroupID, resultDelivery(t, other.GroupID, "m4", 0))
	s.call(t, 0)
	s.call(t, 1)
	if got := s.count(); got > 2 {
		t.Fatalf("accepted records exceeded MaxRecords=2: %d", got)
	}
}

func TestAsyncDispatcherShutdownClosesSenderToResolvePendingCallbacks(t *testing.T) {
	d, o, s, cancel := setupAsync(t, 2, domain.MaxMessageBytes+256)
	own := trackOwner(t, d, "a01", "w", 1)
	o.put(own.GroupID, resultDelivery(t, own.GroupID, "m1", 0))
	s.call(t, 0)
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(fmt.Sprintf("dispatcher did not close producer to resolve pending callback (%d sends)", s.count()))
}
