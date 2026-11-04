package usecase

import (
	"context"
	"errors"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
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
	Advertise(context.Context, domain.Owner, string) error
	Page(context.Context, domain.Owner, domain.Cursor, int, bool) ([]domain.Delivery, error)
	Ack(context.Context, domain.Owner, string) error
	AckCursor(context.Context, domain.Owner, domain.Cursor) error
}
type Dispatcher struct {
	Store                       Outbox
	ExecutionID, Mode, Endpoint string
	Sender                      Sender
	Logger                      *slog.Logger
	mu                          sync.Mutex
	owners                      map[string]domain.Owner
}

func (d *Dispatcher) Track(ctx context.Context, o domain.Owner) error {
	if o.ExecutionID != d.ExecutionID {
		return fmt.Errorf("dispatcher execution mismatch")
	}
	if d.Mode == "polling" {
		if err := d.Store.Advertise(ctx, o, d.Endpoint); err != nil {
			return err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.owners == nil {
		d.owners = map[string]domain.Owner{}
	}
	d.owners[o.GroupID] = o
	return nil
}
func (d *Dispatcher) Forget(groupID string, epoch int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.owners[groupID].Epoch == epoch {
		delete(d.owners, groupID)
	}
}
func (d *Dispatcher) Owner(groupID string) (domain.Owner, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	o, ok := d.owners[groupID]
	return o, ok
}
func (d *Dispatcher) Run(ctx context.Context) error {
	if d.Mode == "polling" {
		<-ctx.Done()
		return nil
	}
	if d.Sender == nil {
		return fmt.Errorf("output sender required")
	}
	defer d.Sender.Close()
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
				err = d.Sender.Send(bounded, item.Envelope, g)
				if err == nil {
					err = d.Store.Ack(bounded, o, e.MessageID)
				}
				cancel()
			}
			if err != nil && !errors.Is(err, context.Canceled) {
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
