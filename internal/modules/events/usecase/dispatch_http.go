package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

// A Worker owns one pool, independently of its number of leased groups.
const httpPushConcurrency = 4

type httpJob struct {
	owner      domain.Owner
	generation uint64
	items      []domain.Delivery
}

type httpResult struct {
	job                   httpJob
	items                 []domain.Delivery
	err                   error
	empty, retired, fatal bool
}

type httpGroup struct {
	owner      domain.Owner
	generation uint64
	items      []domain.Delivery
	busy       bool
	retries    int
	next       time.Time
}

func (d *Dispatcher) httpCurrent(job httpJob) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return !d.closed && d.owners[job.owner.GroupID] == job.owner && d.generations[job.owner.GroupID] == job.generation
}

// runHTTP schedules one record per turn, including its SQL ACK. A group keeps
// its slot through Send and ACK; different groups use the fixed Worker pool.
// At most one existing 64-record / 4-MiB page is retained for each of the 16
// valid groups. The unbuffered job channel cannot queue additional pages.
func (d *Dispatcher) runHTTP(ctx context.Context) (retErr error) {
	defer d.Close()
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan httpJob)
	results := make(chan httpResult, httpPushConcurrency)
	var workers sync.WaitGroup
	for i := 0; i < httpPushConcurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				results <- d.deliverHTTP(workCtx, job)
			}
		}()
	}
	groups := map[string]*httpGroup{}
	active, last := 0, ""
	apply := func(r httpResult) {
		active--
		d.Metrics.Set("http_push_active_groups", float64(active))
		g := groups[r.job.owner.GroupID]
		g.busy = false
		d.mu.Lock()
		owned := d.owners[r.job.owner.GroupID] == r.job.owner && d.generations[r.job.owner.GroupID] == r.job.generation
		d.mu.Unlock()
		if !owned {
			g.items = nil
			g.retries = 0
			g.next = time.Time{}
			return
		}
		g.items = r.items
		if r.fatal || domain.IsPermanentDelivery(r.err) {
			if domain.IsPermanentDelivery(r.err) {
				d.Metrics.Add("output_permanent_errors", 1)
			}
			retErr = errors.Join(retErr, fmt.Errorf("permanent output failure for group %s; durable pending retained: %w", r.job.owner.GroupID, r.err))
			cancel()
			return
		}
		if r.err != nil && !r.retired && !errors.Is(r.err, context.Canceled) {
			g.retries = min(g.retries+1, 6)
			g.next = time.Now().Add(time.Duration(1<<g.retries) * 50 * time.Millisecond)
			d.Metrics.Add("output_retries", 1)
			d.Logger.WarnContext(ctx, "output retry", "execution", r.job.owner.ExecutionID, "group", r.job.owner.GroupID, "owner", r.job.owner.OwnerID, "epoch", r.job.owner.Epoch, "attempt", g.retries, "error", r.err)
		} else {
			g.retries = 0
			g.next = time.Time{}
			if r.empty {
				g.next = time.Now().Add(50 * time.Millisecond)
			}
		}
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	// Keep the existing initial outbox scan cadence. Completed record turns can
	// then yield immediately to another ready group without waiting for a page.
	select {
	case <-workCtx.Done():
	case <-ticker.C:
	}
	owners := make(map[string]httpJob, 16)
	names := make([]string, 0, 16)
loop:
	for workCtx.Err() == nil {
		clear(owners)
		names = names[:0]
		d.mu.Lock()
		closed := d.closed
		for name, o := range d.owners {
			owners[name] = httpJob{owner: o, generation: d.generations[name]}
		}
		d.mu.Unlock()
		if closed {
			break
		}
		for name := range owners {
			names = append(names, name)
		}
		sort.Strings(names)
		start := sort.SearchStrings(names, last)
		if start < len(names) && names[start] == last {
			start++
		}
		for i := 0; i < len(names) && active < httpPushConcurrency; i++ {
			name := names[(start+i)%len(names)]
			job := owners[name]
			g := groups[name]
			if g == nil {
				g = &httpGroup{}
				groups[name] = g
			}
			if g.busy {
				continue
			}
			if g.owner != job.owner || g.generation != job.generation {
				*g = httpGroup{owner: job.owner, generation: job.generation}
			}
			if time.Now().Before(g.next) {
				continue
			}
			job.items = g.items
			select {
			case <-workCtx.Done():
				break loop
			case jobs <- job:
				g.busy = true
				g.items = nil
				active++
				last = name
				d.Metrics.Set("http_push_active_groups", float64(active))
			default:
			}
		}
		for name, g := range groups {
			if _, ok := owners[name]; !ok && !g.busy {
				delete(groups, name)
			}
		}
		select {
		case <-workCtx.Done():
			break loop
		case r := <-results:
			apply(r)
		case <-ticker.C:
		}
	}
	// Stop admission and cancel I/O before joining. Each worker can report into
	// the N-capacity result mailbox without needing this coordinator to run.
	cancel()
	close(jobs)
	workers.Wait()
	for active > 0 {
		apply(<-results)
	}
	d.Metrics.Set("http_push_active_groups", 0)
	return retErr
}

func (d *Dispatcher) deliverHTTP(ctx context.Context, job httpJob) (r httpResult) {
	r.job = job
	if ctx.Err() != nil || !d.httpCurrent(job) {
		r.retired = true
		return
	}
	items := job.items
	if len(items) == 0 {
		bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
		items, r.err = d.Store.Page(bounded, job.owner, domain.Cursor{}, 64, true)
		cancel()
		if r.err != nil {
			return
		}
		if ctx.Err() != nil || !d.httpCurrent(job) {
			r.retired = true
			return
		}
		size := 0
		for _, item := range items {
			size += len(item.Envelope)
		}
		if len(items) > 64 || size > domain.MaxMessageBytes {
			r.err = fmt.Errorf("outbox page exceeds HTTP staging bounds")
			r.fatal = true
			return
		}
	}
	if len(items) == 0 {
		r.empty = true
		return
	}
	r.items = items
	e, err := domain.Decode(items[0].Envelope)
	if err != nil {
		r.err = fmt.Errorf("invalid HTTP outbox envelope: %w", err)
		r.fatal = true
		return
	}
	if e.ExecutionID != job.owner.ExecutionID || e.GroupID != job.owner.GroupID {
		r.err = fmt.Errorf("HTTP outbox execution/group mismatch")
		r.fatal = true
		return
	}
	if ctx.Err() != nil || !d.httpCurrent(job) {
		r.items = nil
		r.retired = true
		return
	}
	bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	sentAt := time.Now()
	d.Metrics.Add("output_attempts", 1)
	d.Metrics.Add("output_attempt_payload_bytes", int64(len(items[0].Envelope)))
	if d.Measure {
		d.Logger.InfoContext(ctx, "output attempt", "execution", job.owner.ExecutionID, "group", e.GroupID, "message_id", e.MessageID, "kind", e.Kind, "sent_at", sentAt, "bytes", len(items[0].Envelope))
	}
	r.err = d.Sender.Send(bounded, items[0].Envelope, e.GroupID)
	d.Metrics.Observe("delivery_ack", time.Since(sentAt))
	if r.err == nil {
		r.err = bounded.Err()
	}
	if r.err != nil {
		return
	}
	d.Metrics.Add("output_successful_sends", 1)
	if !d.httpCurrent(job) {
		r.items = nil
		r.retired = true
		return
	}
	// Always pass the captured owner/epoch. SQL remains the final fence if a
	// lease changes after this check; no ownership lock is held across I/O.
	r.err = d.Store.Ack(bounded, job.owner, e.MessageID)
	if r.err == nil {
		items[0] = domain.Delivery{}
		r.items = items[1:]
	}
	return
}
