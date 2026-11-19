package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/resulttransport"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

// These stores model pending records and fencing in memory. They exercise the
// dispatcher/HTTP contract, not PostgreSQL persistence or transaction durability.
type pushAck struct {
	owner domain.Owner
	id    string
}
type pushOutbox struct {
	*asyncFakeOutbox
	acked     chan pushAck
	beforeAck func(context.Context, domain.Owner, string) error
	pageHook  func(context.Context, domain.Owner) ([]domain.Delivery, error)
}

func newPushOutbox() *pushOutbox {
	return &pushOutbox{asyncFakeOutbox: newAsyncFakeOutbox(), acked: make(chan pushAck, 256)}
}
func (o *pushOutbox) Page(ctx context.Context, owner domain.Owner, c domain.Cursor, limit int, pending bool) ([]domain.Delivery, error) {
	if o.pageHook != nil {
		return o.pageHook(ctx, owner)
	}
	return o.asyncFakeOutbox.Page(ctx, owner, c, limit, pending)
}
func (o *pushOutbox) Ack(ctx context.Context, owner domain.Owner, id string) error {
	if o.beforeAck != nil {
		if err := o.beforeAck(ctx, owner, id); err != nil {
			return err
		}
	}
	if err := o.asyncFakeOutbox.Ack(ctx, owner, id); err != nil {
		return err
	}
	o.acked <- pushAck{owner, id}
	return nil
}

type pushSenderFunc func(context.Context, []byte, string) error

func (s pushSenderFunc) Send(ctx context.Context, b []byte, g string) error { return s(ctx, b, g) }
func (pushSenderFunc) Close() error                                         { return nil }

type pushRun struct {
	done   chan struct{}
	err    error
	cancel context.CancelFunc
}

func startPush(t *testing.T, d *Dispatcher) *pushRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &pushRun{done: make(chan struct{}), cancel: cancel}
	go func() { r.err = d.Run(ctx); close(r.done) }()
	t.Cleanup(func() { cancel(); _ = r.wait(t) })
	return r
}
func (r *pushRun) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-r.done:
		return r.err
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP dispatcher did not join its workers")
		return nil
	}
}
func pushAwait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP contract event did not arrive")
		var zero T
		return zero
	}
}
func pushDispatcher(o Outbox, s Sender) *Dispatcher {
	return &Dispatcher{ExecutionID: testExecution, Mode: "http_push", Store: o, Sender: s, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestHTTPPushAUD010IndependentGroupsAndFIFOThroughAck(t *testing.T) {
	for _, hold := range []string{"send", "sql-ack"} {
		t.Run(hold, func(t *testing.T) {
			o := newPushOutbox()
			o.put("a01", resultDelivery(t, "a01", "a1", 0), resultDelivery(t, "a01", "a2", 1))
			o.put("a02", resultDelivery(t, "a02", "b1", 0))
			entered, release, firstAck := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var secondBeforeAck atomic.Bool
			wait := func(ctx context.Context) error {
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			o.beforeAck = func(ctx context.Context, _ domain.Owner, id string) error {
				if id == "a1" {
					if hold == "sql-ack" {
						if err := wait(ctx); err != nil {
							return err
						}
					}
					close(firstAck)
				}
				return nil
			}
			sender := pushSenderFunc(func(ctx context.Context, b []byte, _ string) error {
				e, _ := domain.Decode(b)
				if e.MessageID == "a1" && hold == "send" {
					return wait(ctx)
				}
				if e.MessageID == "a2" {
					select {
					case <-firstAck:
					default:
						secondBeforeAck.Store(true)
					}
				}
				return nil
			})
			d := pushDispatcher(o, sender)
			trackOwner(t, d, "a01", "worker", 1)
			trackOwner(t, d, "a02", "worker", 1)
			r := startPush(t, d)
			pushAwait(t, entered)
			if ack := pushAwait(t, o.acked); ack.id != "b1" {
				t.Fatalf("a02 did not ACK while a01 was held: %+v", ack)
			}
			if o.ackCount() != 1 {
				t.Fatal("a01 ACKed before confirmation was released")
			}
			close(release)
			if ack := pushAwait(t, o.acked); ack.id != "a1" {
				t.Fatal(ack)
			}
			if ack := pushAwait(t, o.acked); ack.id != "a2" {
				t.Fatal(ack)
			}
			r.cancel()
			if err := r.wait(t); err != nil {
				t.Fatal(err)
			}
			if secondBeforeAck.Load() {
				t.Fatal("a01 second record overtook the first ACK")
			}
		})
	}
}

func TestHTTPPushLostResponseRetriesSameMessageIDWithoutEarlyAck(t *testing.T) {
	o := newPushOutbox()
	o.put("a01", resultDelivery(t, "a01", "lost-reply", 0))
	retrying, confirm := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	persisted := map[string]bool{}
	var ids []string
	var times []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		e, err := domain.Decode(b)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		ids = append(ids, e.MessageID)
		times = append(times, time.Now())
		attempt := len(ids)
		duplicate := persisted[e.MessageID]
		persisted[e.MessageID] = true
		mu.Unlock()
		if attempt == 1 {
			// The fake Master has accepted the record, but the HTTP reply is lost.
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		close(retrying)
		select {
		case <-confirm:
		case <-r.Context().Done():
			return
		}
		_ = json.NewEncoder(w).Encode(Receipt{MessageID: e.MessageID, Accepted: !duplicate})
	}))
	t.Cleanup(server.Close)
	sender, err := resulttransport.NewHTTP(server.URL, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	d := pushDispatcher(o, sender)
	trackOwner(t, d, "a01", "worker", 1)
	r := startPush(t, d)
	pushAwait(t, retrying)
	if o.ackCount() != 0 {
		t.Fatal("lost or still-unconfirmed reply produced local ACK")
	}
	mu.Lock()
	same := len(ids) == 2 && ids[0] == ids[1] && len(persisted) == 1
	pause := times[1].Sub(times[0])
	mu.Unlock()
	if !same {
		t.Fatal("retry changed MessageID or duplicated the logical result")
	}
	if pause < 90*time.Millisecond {
		t.Fatalf("retry backoff skipped: %v", pause)
	}
	close(confirm)
	if ack := pushAwait(t, o.acked); ack.id != "lost-reply" {
		t.Fatal(ack)
	}
	r.cancel()
	if err := r.wait(t); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPPushOwnerAndGenerationFenceAfterSend(t *testing.T) {
	for _, changeEpoch := range []bool{false, true} {
		t.Run(fmt.Sprint("new-epoch=", changeEpoch), func(t *testing.T) {
			o := newPushOutbox()
			o.put("a01", resultDelivery(t, "a01", "held-owner", 0))
			entered := make(chan string, 2)
			oldReply, newReply := make(chan struct{}), make(chan struct{})
			var count atomic.Int32
			d := pushDispatcher(o, pushSenderFunc(func(ctx context.Context, b []byte, _ string) error {
				e, _ := domain.Decode(b)
				entered <- e.MessageID
				gate := oldReply
				if count.Add(1) > 1 {
					gate = newReply
				}
				select {
				case <-gate:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}))
			old := trackOwner(t, d, "a01", "old", 1)
			r := startPush(t, d)
			pushAwait(t, entered)
			d.Forget("a01", old.Epoch)
			current := old
			if changeEpoch {
				current.OwnerID = "new"
				current.Epoch++
			}
			if err := d.Track(context.Background(), current); err != nil {
				t.Fatal(err)
			}
			close(oldReply)
			if id := pushAwait(t, entered); id != "held-owner" {
				t.Fatal(id)
			}
			if o.ackCount() != 0 {
				t.Fatal("stale generation ACKed after ownership changed")
			}
			close(newReply)
			if ack := pushAwait(t, o.acked); ack.owner != current {
				t.Fatalf("ACK used stale owner: %+v", ack)
			}
			r.cancel()
			if err := r.wait(t); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHTTPPushPoolLimitCancellationAndClose(t *testing.T) {
	for _, viaClose := range []bool{false, true} {
		t.Run(fmt.Sprint("close=", viaClose), func(t *testing.T) {
			o := newPushOutbox()
			started := make(chan string, 16)
			var calls, active, peak atomic.Int32
			d := pushDispatcher(o, pushSenderFunc(func(ctx context.Context, _ []byte, g string) error {
				calls.Add(1)
				n := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); n > old; old = peak.Load() {
					if peak.CompareAndSwap(old, n) {
						break
					}
				}
				started <- g
				<-ctx.Done()
				return ctx.Err()
			}))
			for i := 1; i <= 16; i++ {
				g := fmt.Sprintf("a%02d", i)
				o.put(g, resultDelivery(t, g, g, 0))
				trackOwner(t, d, g, "worker", 1)
			}
			r := startPush(t, d)
			for i := 0; i < httpPushConcurrency; i++ {
				pushAwait(t, started)
			}
			if viaClose {
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				r.cancel()
			}
			if err := r.wait(t); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 4 || peak.Load() != 4 || active.Load() != 0 || o.ackCount() != 0 {
				t.Fatalf("calls=%d peak=%d active=%d acks=%d", calls.Load(), peak.Load(), active.Load(), o.ackCount())
			}
			if err := d.Track(context.Background(), domain.Owner{ExecutionID: testExecution, GroupID: "a01", OwnerID: "late", Epoch: 2}); err == nil {
				t.Fatal("closed pool admitted an owner")
			}
		})
	}
}

func TestHTTPPushLargePageYieldsToOtherReadyGroup(t *testing.T) {
	o := newPushOutbox()
	items := make([]domain.Delivery, 64)
	for i := range items {
		items[i] = resultDelivery(t, "a01", fmt.Sprint("bulk-", i), i)
	}
	o.put("a01", items...)
	var bulk, bulkAtOtherAck atomic.Int32
	o.beforeAck = func(_ context.Context, own domain.Owner, _ string) error {
		if own.GroupID == "a05" {
			bulkAtOtherAck.Store(bulk.Load())
		}
		return nil
	}
	d := pushDispatcher(o, pushSenderFunc(func(ctx context.Context, _ []byte, g string) error {
		if g == "a01" {
			bulk.Add(1)
			return nil
		}
		if g == "a05" {
			return nil
		}
		<-ctx.Done()
		return ctx.Err()
	}))
	for i := 1; i <= 5; i++ {
		g := fmt.Sprintf("a%02d", i)
		if i > 1 {
			o.put(g, resultDelivery(t, g, g, 0))
		}
		trackOwner(t, d, g, "worker", 1)
	}
	r := startPush(t, d)
	for {
		if ack := pushAwait(t, o.acked); ack.owner.GroupID == "a05" {
			break
		}
	}
	r.cancel()
	if err := r.wait(t); err != nil {
		t.Fatal(err)
	}
	if n := bulkAtOtherAck.Load(); n > 2 {
		t.Fatalf("large page monopolized a pool slot before a05 ACK: %d records", n)
	}
}

func TestHTTPPushNoAckAfterDeadlineEvenIfSenderReturnsNil(t *testing.T) {
	o := newPushOutbox()
	o.put("a01", resultDelivery(t, "a01", "timeout", 0))
	d := pushDispatcher(o, pushSenderFunc(func(ctx context.Context, _ []byte, _ string) error { <-ctx.Done(); return nil }))
	owner := trackOwner(t, d, "a01", "worker", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	r := d.deliverHTTP(ctx, httpJob{owner: owner, generation: 1})
	if !errors.Is(r.err, context.DeadlineExceeded) || o.ackCount() != 0 {
		t.Fatalf("err=%v acks=%d", r.err, o.ackCount())
	}
}

func TestHTTPPushPermanentFailureRetainsPendingAndStops(t *testing.T) {
	o := newPushOutbox()
	o.put("a01", resultDelivery(t, "a01", "permanent", 0))
	d := pushDispatcher(o, pushSenderFunc(func(context.Context, []byte, string) error { return domain.PermanentDelivery(errors.New("HTTP 413")) }))
	trackOwner(t, d, "a01", "worker", 1)
	r := startPush(t, d)
	if err := r.wait(t); !domain.IsPermanentDelivery(err) {
		t.Fatalf("permanent failure hidden: %v", err)
	}
	if o.ackCount() != 0 {
		t.Fatal("permanent failure acknowledged")
	}
	if err := d.Track(context.Background(), domain.Owner{ExecutionID: testExecution, GroupID: "a17"}); err == nil {
		t.Fatal("invalid group admitted")
	}
}

func TestHTTPPushStaleMalformedPageDoesNotStopNewOwner(t *testing.T) {
	o := newPushOutbox()
	o.put("a01", resultDelivery(t, "a01", "fresh-owner", 0))
	reading, release := make(chan struct{}), make(chan struct{})
	o.pageHook = func(ctx context.Context, owner domain.Owner) ([]domain.Delivery, error) {
		if owner.Epoch == 1 {
			close(reading)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return []domain.Delivery{{Envelope: json.RawMessage(`{"bad":true}`)}}, nil
		}
		return o.asyncFakeOutbox.Page(ctx, owner, domain.Cursor{}, 64, true)
	}
	d := pushDispatcher(o, pushSenderFunc(func(context.Context, []byte, string) error { return nil }))
	trackOwner(t, d, "a01", "old", 1)
	r := startPush(t, d)
	pushAwait(t, reading)
	d.Forget("a01", 1)
	newOwner := trackOwner(t, d, "a01", "new", 2)
	close(release)
	if ack := pushAwait(t, o.acked); ack.owner != newOwner {
		t.Fatal(ack)
	}
	r.cancel()
	if err := r.wait(t); err != nil {
		t.Fatalf("stale Page killed the new owner: %v", err)
	}
}

func TestHTTPPushSQLAckFailureKeepsHeadAndRetryOrder(t *testing.T) {
	o := newPushOutbox()
	o.put("a01", resultDelivery(t, "a01", "head", 0), resultDelivery(t, "a01", "next", 1))
	sends := make(chan string, 4)
	secondAck, allowAck := make(chan struct{}), make(chan struct{})
	var attempts atomic.Int32
	o.beforeAck = func(ctx context.Context, _ domain.Owner, id string) error {
		if id != "head" {
			return nil
		}
		if attempts.Add(1) == 1 {
			return errors.New("SQL ACK retry")
		}
		close(secondAck)
		select {
		case <-allowAck:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	d := pushDispatcher(o, pushSenderFunc(func(_ context.Context, b []byte, _ string) error {
		e, _ := domain.Decode(b)
		sends <- e.MessageID
		return nil
	}))
	trackOwner(t, d, "a01", "worker", 1)
	r := startPush(t, d)
	pushAwait(t, secondAck)
	if a, b := pushAwait(t, sends), pushAwait(t, sends); a != "head" || b != "head" {
		t.Fatalf("head did not retry before next: %q %q", a, b)
	}
	if o.ackCount() != 0 {
		t.Fatal("failed SQL ACK advanced pending")
	}
	close(allowAck)
	if ack := pushAwait(t, o.acked); ack.id != "head" {
		t.Fatal(ack)
	}
	if id := pushAwait(t, sends); id != "next" {
		t.Fatal(id)
	}
	if ack := pushAwait(t, o.acked); ack.id != "next" {
		t.Fatal(ack)
	}
	r.cancel()
	if err := r.wait(t); err != nil {
		t.Fatal(err)
	}
}
