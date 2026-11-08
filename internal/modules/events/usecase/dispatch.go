package usecase

import (
	"context"
	"errors"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"log/slog"
	"sort"
	"sync"
	"time"
)

type Sender interface {
	Send(context.Context, []byte, string) error
	Close() error
}
type Outbox interface {
	// Implementations must honor context cancellation; shutdown joins all calls.
	Advertise(context.Context, domain.Owner, string) error
	Page(context.Context, domain.Owner, domain.Cursor, int, bool) ([]domain.Delivery, error)
	Ack(context.Context, domain.Owner, string) error
	AckCursor(context.Context, domain.Owner, domain.Cursor) error
}
type Dispatcher struct {
	Store                       Outbox
	ExecutionID, Mode, Endpoint string
	Sender                      Sender
	AsyncSender                 AsyncSender
	AsyncConfig                 AsyncConfig
	Logger                      *slog.Logger
	Metrics                     *observability.Registry
	Measure                     bool
	mu                          sync.Mutex
	owners                      map[string]domain.Owner
	generations                 map[string]uint64
	closed                      bool
	closeOnce                   sync.Once
	closeErr                    error
}

func (d *Dispatcher) Track(ctx context.Context, o domain.Owner) error {
	if o.ExecutionID != d.ExecutionID {
		return fmt.Errorf("dispatcher execution mismatch")
	}
	if d.AsyncSender != nil {
		if _, err := topology.PartitionForGroup(o.GroupID); err != nil {
			return err
		}
	}
	if d.Mode == "polling" {
		if err := d.Store.Advertise(ctx, o, d.Endpoint); err != nil {
			return err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.AsyncSender != nil && d.closed {
		return fmt.Errorf("async dispatcher closed")
	}
	if d.owners == nil {
		d.owners = map[string]domain.Owner{}
	}
	if d.generations == nil {
		d.generations = map[string]uint64{}
	}
	d.generations[o.GroupID]++
	d.owners[o.GroupID] = o
	return nil
}
func (d *Dispatcher) Forget(groupID string, epoch int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if o, ok := d.owners[groupID]; ok && o.Epoch == epoch {
		delete(d.owners, groupID)
		d.generations[groupID]++
	}
}
func (d *Dispatcher) Owner(groupID string) (domain.Owner, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	o, ok := d.owners[groupID]
	return o, ok
}

// Close also covers startup failures before Run has been entered.
func (d *Dispatcher) Close() error {
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		d.mu.Unlock()
		if d.AsyncSender != nil {
			d.closeErr = d.AsyncSender.Close()
		} else if d.Sender != nil {
			d.closeErr = d.Sender.Close()
		}
	})
	return d.closeErr
}
func (d *Dispatcher) Run(ctx context.Context) error {
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
