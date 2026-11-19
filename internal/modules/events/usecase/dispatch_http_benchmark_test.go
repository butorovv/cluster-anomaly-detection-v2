package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

// Frozen Run from 249dc0bb5890785c2821c252d5acfb8196f60751, renamed only.
// The benchmark below exercises its http_push branch, never Kafka or Polling.
type pushBenchOutbox struct {
	mu              sync.Mutex
	items           map[string][]domain.Delivery
	next            map[string]int
	left, otherLeft int
	start           time.Time
	otherAck        time.Duration
	cancel          context.CancelFunc
}

func (*pushBenchOutbox) Advertise(context.Context, domain.Owner, string) error        { return nil }
func (*pushBenchOutbox) AckCursor(context.Context, domain.Owner, domain.Cursor) error { return nil }
func (o *pushBenchOutbox) Page(_ context.Context, owner domain.Owner, _ domain.Cursor, limit int, _ bool) ([]domain.Delivery, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	items := o.items[owner.GroupID][o.next[owner.GroupID]:]
	if len(items) > limit {
		items = items[:limit]
	}
	return append([]domain.Delivery(nil), items...), nil
}
func (o *pushBenchOutbox) Ack(_ context.Context, owner domain.Owner, id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	item := o.items[owner.GroupID][o.next[owner.GroupID]]
	var e domain.Envelope
	// A cheap fake ID check avoids adding another strict decode to the measured
	// dispatcher. Real SQL ACK and HTTP receipt validation are excluded here.
	_ = json.Unmarshal(item.Envelope, &e)
	if e.MessageID != id {
		panic("benchmark FIFO/MessageID mismatch")
	}
	o.next[owner.GroupID]++
	o.left--
	if owner.GroupID != "a01" {
		o.otherLeft--
		if o.otherLeft == 0 {
			o.otherAck = time.Since(o.start)
		}
	}
	if o.left == 0 {
		o.cancel()
	}
	return nil
}

// BenchmarkHTTPPushDispatcher times one identical 16-group x 16-record batch,
// including the common initial 50ms scan and Run shutdown. Payload construction,
// Track and fixture reset are outside the timer. Page/ACK are in-memory fakes;
// Send is a no-op or an explicit 2ms delay on each a01 record. No HTTP sockets,
// SQL, golden evaluation or D2 input are included.
func BenchmarkHTTPPushDispatcher(b *testing.B) {
	const records = 16
	data := map[string][]domain.Delivery{}
	for g := 1; g <= 16; g++ {
		name := fmt.Sprintf("a%02d", g)
		for i := 0; i < records; i++ {
			e := domain.Envelope{SchemaVersion: 1, ExecutionID: testExecution, RunID: domain.Hash([]byte("id")), ConfigID: domain.Hash([]byte("id")), GroupID: name, MessageID: fmt.Sprintf("%s-%d", name, i), Kind: "group_result", Revision: int64(i + 1), EventTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Payload: json.RawMessage(`{}`)}
			raw, err := json.Marshal(e)
			if err != nil {
				b.Fatal(err)
			}
			data[name] = append(data[name], domain.Delivery{Cursor: domain.Cursor{Revision: int64(i + 1)}, Envelope: raw})
		}
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, slow := range []bool{false, true} {
		for _, after := range []bool{false, true} {
			b.Run(fmt.Sprintf("slow=%t/after=%t", slow, after), func(b *testing.B) {
				b.ReportAllocs()
				b.StopTimer()
				var other time.Duration
				for i := 0; i < b.N; i++ {
					ctx, cancel := context.WithCancel(context.Background())
					o := &pushBenchOutbox{items: data, next: map[string]int{}, left: 16 * records, otherLeft: 15 * records, cancel: cancel}
					s := pushSenderFunc(func(ctx context.Context, _ []byte, g string) error {
						if slow && g == "a01" {
							timer := time.NewTimer(2 * time.Millisecond)
							defer timer.Stop()
							select {
							case <-timer.C:
							case <-ctx.Done():
								return ctx.Err()
							}
						}
						return nil
					})
					d := &Dispatcher{ExecutionID: testExecution, Mode: "http_push", Store: o, Sender: s, Logger: logger}
					for g := 1; g <= 16; g++ {
						if err := d.Track(ctx, domain.Owner{ExecutionID: testExecution, GroupID: fmt.Sprintf("a%02d", g), OwnerID: "worker", Epoch: 1}); err != nil {
							b.Fatal(err)
						}
					}
					b.StartTimer()
					o.start = time.Now()
					var err error
					if after {
						err = d.Run(ctx)
					} else {
						err = d.runHTTPBaseline(ctx)
					}
					b.StopTimer()
					cancel()
					if err != nil || o.left != 0 {
						b.Fatalf("err=%v remaining=%d", err, o.left)
					}
					other += o.otherAck
				}
				b.ReportMetric(float64(other.Nanoseconds())/float64(b.N), "other-groups-ack-ns/batch")
				b.ReportMetric(float64(b.N*16*records)/b.Elapsed().Seconds(), "records/s")
			})
		}
	}
}

func (d *Dispatcher) runHTTPBaseline(ctx context.Context) error {
	if d.Mode == "polling" {
		<-ctx.Done()
		return nil
	}
	if d.AsyncSender != nil {
		return d.runAsync(ctx)
	}
	if d.Sender == nil {
		return fmt.Errorf("output sender required")
	}
	defer d.Close()
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	retry := map[string]int{}
	nextTry := map[string]time.Time{}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		d.mu.Lock()
		groups := make([]string, 0, len(d.owners))
		for g := range d.owners {
			groups = append(groups, g)
		}
		d.mu.Unlock()
		sort.Strings(groups)
		for _, g := range groups {
			if time.Now().Before(nextTry[g]) {
				continue
			}
			o, ok := d.Owner(g)
			if !ok {
				continue
			}
			bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
			items, err := d.Store.Page(bounded, o, domain.Cursor{}, 64, true)
			cancel()
			for _, item := range items {
				if err != nil {
					break
				}
				active, ok := d.Owner(g)
				if !ok || active != o {
					break
				}
				e, decodeErr := domain.Decode(item.Envelope)
				if decodeErr != nil {
					return decodeErr
				}
				bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
				sentAt := time.Now()
				d.Metrics.Add("output_attempts", 1)
				d.Metrics.Add("output_attempt_payload_bytes", int64(len(item.Envelope)))
				if d.Measure {
					d.Logger.InfoContext(ctx, "output attempt", "execution", o.ExecutionID, "group", g, "message_id", e.MessageID, "kind", e.Kind, "sent_at", sentAt, "bytes", len(item.Envelope))
				}
				err = d.Sender.Send(bounded, item.Envelope, g)
				d.Metrics.Observe("delivery_ack", time.Since(sentAt))
				if err == nil {
					d.Metrics.Add("output_successful_sends", 1)
				}
				if err == nil {
					err = d.Store.Ack(bounded, o, e.MessageID)
				}
				cancel()
			}
			if domain.IsPermanentDelivery(err) {
				d.Metrics.Add("output_permanent_errors", 1)
				return fmt.Errorf("permanent output failure for group %s; durable pending retained: %w", g, err)
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				d.Metrics.Add("output_retries", 1)
				retry[g] = min(retry[g]+1, 6)
				nextTry[g] = time.Now().Add(time.Duration(1<<retry[g]) * 50 * time.Millisecond)
				d.Logger.WarnContext(ctx, "output retry", "execution", o.ExecutionID, "group", g, "owner", o.OwnerID, "epoch", o.Epoch, "attempt", retry[g], "error", err)
			} else {
				retry[g] = 0
				delete(nextTry, g)
			}
		}
	}
}
