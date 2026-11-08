package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/sync/semaphore"
)

type AsyncSender interface {
	// A nil return transfers immutable payload ownership until exactly one
	// completion, which may run before SendAsync returns. An error return must
	// not call completion. Cancellation/Flush timeout is not a delivery outcome.
	SendAsync(context.Context, []byte, string, func(error)) error
	Flush(context.Context) error
	// Close must resolve all accepted attempts, including unknown broker ACKs.
	// Callbacks must finish without requiring application coordinator progress.
	Close() error
}

type AsyncConfig struct {
	MaxRecords   int
	MaxBytes     int64
	DrainTimeout time.Duration
}

func (c AsyncConfig) WithDefaults() AsyncConfig {
	if c.MaxRecords == 0 {
		c.MaxRecords = 4
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = domain.MaxMessageBytes + 256
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = 5 * time.Second
	}
	return c
}
func (c AsyncConfig) Validate() error {
	if c.MaxRecords < 1 || c.MaxRecords > 16 || c.MaxBytes < domain.MaxMessageBytes+256 || c.MaxBytes > 16*(domain.MaxMessageBytes+256) || c.DrainTimeout <= 0 || c.DrainTimeout > 15*time.Second {
		return fmt.Errorf("invalid async delivery bounds")
	}
	return nil
}

type asyncJob struct {
	owner      domain.Owner
	generation uint64
	items      []domain.Delivery
}
type asyncResult struct {
	job                   asyncJob
	items                 []domain.Delivery
	err                   error
	empty, retired, fatal bool
}
type asyncGroup struct {
	owner         domain.Owner
	generation    uint64
	items         []domain.Delivery
	busy, retired bool
	retries       int
	next          time.Time
}
type asyncOutcome struct {
	err error
	at  time.Time
}

func (d *Dispatcher) asyncCurrent(o domain.Owner, generation uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return !d.closed && d.owners[o.GroupID] == o && d.generations[o.GroupID] == generation
}

// runAsync keeps one task per group, including SQL ACK retries. Fixed workers
// own their page and completion mailbox; no goroutine is created per message.
func (d *Dispatcher) runAsync(ctx context.Context) (retErr error) {
	defer d.Close()
	cfg := d.AsyncConfig.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
	if d.Mode != "kafka" {
		return fmt.Errorf("async delivery is Kafka-only")
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	prepCtx, cancelPrep := context.WithCancel(ctx)
	defer cancelPrep()
	ackCtx, cancelAck := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelAck()
	jobs := make(chan asyncJob, cfg.MaxRecords)
	results := make(chan asyncResult, cfg.MaxRecords)
	budget := semaphore.NewWeighted(cfg.MaxBytes)
	var workers sync.WaitGroup
	var bytes, peakBytes, pendingSQL atomic.Int64
	for i := 0; i < cfg.MaxRecords; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				results <- d.deliverAsync(prepCtx, ackCtx, budget, &bytes, &peakBytes, &pendingSQL, job)
			}
		}()
	}
	// Each of the fixed 16 groups can retain at most one existing bounded page
	// (64 records / 4 MiB). MaxBytes separately bounds admitted payloads through
	// SQL ACK; it is not a process RSS or page-cache limit.
	groups := map[string]*asyncGroup{}
	active, last := 0, ""
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	apply := func(r asyncResult) {
		active--
		d.Metrics.Set("async_reserved_records", float64(active))
		g := groups[r.job.owner.GroupID]
		g.busy = false
		g.items, g.retired = r.items, r.retired
		if r.fatal || (domain.IsPermanentDelivery(r.err) && d.asyncCurrent(r.job.owner, r.job.generation)) {
			if domain.IsPermanentDelivery(r.err) {
				d.Metrics.Add("output_permanent_errors", 1)
			}
			retErr = errors.Join(retErr, fmt.Errorf("output failure for group %s; durable pending retained: %w", r.job.owner.GroupID, r.err))
			cancelPrep()
			return
		}
		if r.err != nil && !r.retired && !errors.Is(r.err, context.Canceled) {
			g.retries = min(g.retries+1, 6)
			g.next = time.Now().Add(time.Duration(1<<g.retries) * 50 * time.Millisecond)
			d.Metrics.Add("output_retries", 1)
			d.Logger.Warn("output retry", "group", r.job.owner.GroupID, "epoch", r.job.owner.Epoch, "error", r.err)
		} else {
			g.retries = 0
			g.next = time.Time{}
			if r.empty {
				g.next = time.Now().Add(50 * time.Millisecond)
			}
		}
	}
loop:
	for {
		d.mu.Lock()
		closed := d.closed
		owners := make(map[string]asyncJob, len(d.owners))
		for name, o := range d.owners {
			owners[name] = asyncJob{owner: o, generation: d.generations[name]}
		}
		d.mu.Unlock()
		if closed || prepCtx.Err() != nil {
			break
		}
		if len(owners) > 16 {
			retErr = fmt.Errorf("too many async groups")
			break
		}
		names := make([]string, 0, len(owners))
		for name := range owners {
			if _, err := topology.PartitionForGroup(name); err != nil {
				retErr = err
				break loop
			}
			names = append(names, name)
		}
		sort.Strings(names)
		start := sort.SearchStrings(names, last)
		if start < len(names) && names[start] == last {
			start++
		}
		for i := 0; i < len(names) && active < cfg.MaxRecords; i++ {
			name := names[(start+i)%len(names)]
			o := owners[name]
			g := groups[name]
			if g == nil {
				g = &asyncGroup{}
				groups[name] = g
			}
			if g.busy {
				continue
			}
			if g.owner != o.owner || g.generation != o.generation {
				g.owner = o.owner
				g.generation = o.generation
				g.items = nil
				g.retired = false
				g.next = time.Time{}
				g.retries = 0
			}
			if g.retired || time.Now().Before(g.next) {
				continue
			}
			o.items = g.items
			select {
			case jobs <- o:
				g.busy = true
				g.items = nil
				active++
				last = name
				d.Metrics.Set("async_reserved_records", float64(active))
			default:
			}
		}
		for name, g := range groups {
			if _, ok := owners[name]; !ok && !g.busy {
				delete(groups, name)
			}
		}
		select {
		case <-prepCtx.Done():
			break loop
		case r := <-results:
			apply(r)
		case <-ticker.C:
		}
	}
	// Stop admission first. Callbacks and the N-capacity results queue remain
	// usable while Flush runs: no coordinator progress is needed to report them.
	cancelPrep()
	close(jobs)
	drainCtx, done := context.WithTimeout(context.WithoutCancel(ctx), cfg.DrainTimeout)
	defer done()
	flushErr := d.AsyncSender.Flush(drainCtx)
	if flushErr != nil {
		d.Metrics.Add("async_flush_errors", 1)
	}
	for active > 0 && drainCtx.Err() == nil {
		select {
		case r := <-results:
			apply(r)
		case <-drainCtx.Done():
		}
	}
	if active > 0 {
		d.Metrics.Add("async_drain_incomplete", 1)
		d.Logger.Warn("async drain incomplete; durable outbox retained", "pending_tasks", active, "kafka_acked_pending_sql", pendingSQL.Load(), "flush_error", flushErr)
	}
	cancelAck()
	// Close forces unresolved idempotent records to complete. Joining before
	// Close would deadlock when their broker outcome is unknown.
	_ = d.Close()
	workers.Wait()
	for active > 0 {
		apply(<-results)
	}
	d.Metrics.Set("async_reserved_records", 0)
	return retErr
}

func (d *Dispatcher) deliverAsync(prepCtx, ackCtx context.Context, budget *semaphore.Weighted, bytes, peak, pendingSQL *atomic.Int64, job asyncJob) (r asyncResult) {
	r.job = job
	items := job.items
	if !d.asyncCurrent(job.owner, job.generation) {
		r.retired = true
		return
	}
	if len(items) == 0 {
		bounded, done := context.WithTimeout(prepCtx, 3*time.Second)
		items, r.err = d.Store.Page(bounded, job.owner, domain.Cursor{}, 64, true)
		done()
		if r.err != nil {
			return
		}
		size := 0
		for _, item := range items {
			size += len(item.Envelope)
		}
		if len(items) > 64 || size > domain.MaxMessageBytes {
			r.err = fmt.Errorf("outbox page exceeds async staging bounds")
			r.fatal = true
			return
		}
	}
	if len(items) == 0 {
		r.empty = true
		return
	}
	r.items = items
	item := items[0]
	e, err := domain.Decode(item.Envelope)
	if err != nil || e.GroupID != job.owner.GroupID {
		r.err = fmt.Errorf("invalid async outbox identity: %v", err)
		r.fatal = true
		return
	}
	size := int64(len(item.Envelope) + len(e.GroupID))
	started := time.Now()
	if r.err = budget.Acquire(prepCtx, size); r.err != nil {
		return
	}
	d.Metrics.Observe("async_admission_wait", time.Since(started))
	used := bytes.Add(size)
	for old := peak.Load(); used > old; old = peak.Load() {
		if peak.CompareAndSwap(old, used) {
			break
		}
	}
	d.Metrics.Set("async_reserved_bytes", float64(used))
	d.Metrics.Set("async_peak_reserved_bytes", float64(peak.Load()))
	defer func() { d.Metrics.Set("async_reserved_bytes", float64(bytes.Add(-size))); budget.Release(size) }()
	if !d.asyncCurrent(job.owner, job.generation) {
		r.items = nil
		r.retired = true
		return
	}
	bounded, done := context.WithTimeout(prepCtx, 3*time.Second)
	completion := make(chan asyncOutcome, 1)
	var once sync.Once
	sentAt := time.Now()
	d.Metrics.Add("output_attempts", 1)
	d.Metrics.Add("output_attempt_payload_bytes", int64(len(item.Envelope)))
	if d.Measure {
		d.Logger.Info("output attempt", "execution", job.owner.ExecutionID, "group", e.GroupID, "message_id", e.MessageID, "kind", e.Kind, "sent_at", sentAt, "bytes", len(item.Envelope))
	}
	err = d.AsyncSender.SendAsync(bounded, item.Envelope, e.GroupID, func(err error) { once.Do(func() { completion <- asyncOutcome{err: err, at: time.Now()} }) })
	d.Metrics.Observe("async_submit", time.Since(sentAt))
	if err != nil {
		done()
		r.err = err
		return
	}
	// Neither cancellation nor an elapsed deadline proves non-publication.
	// Only the callback (including one forced by Close) completes this attempt.
	outcome := <-completion
	done()
	d.Metrics.Observe("delivery_ack", outcome.at.Sub(sentAt))
	d.Metrics.Observe("async_completion_queue", time.Since(outcome.at))
	if outcome.err != nil {
		r.err = outcome.err
		return
	}
	d.Metrics.Add("output_successful_sends", 1)
	d.Metrics.Set("async_kafka_acked_pending_sql", float64(pendingSQL.Add(1)))
	defer func() { d.Metrics.Set("async_kafka_acked_pending_sql", float64(pendingSQL.Add(-1))) }()
	for {
		if !d.asyncCurrent(job.owner, job.generation) {
			r.items = nil
			r.retired = true
			return
		}
		if ackCtx.Err() != nil {
			r.err = ackCtx.Err()
			return
		}
		bounded, cancel := context.WithTimeout(ackCtx, 3*time.Second)
		at := time.Now()
		err = d.Store.Ack(bounded, job.owner, e.MessageID)
		cancel()
		d.Metrics.Observe("async_sql_ack", time.Since(at))
		if err == nil {
			r.items = items[1:]
			return
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55000" {
			r.items = nil
			r.retired = true
			r.err = err
			return
		}
		d.Metrics.Add("async_sql_ack_retries", 1)
		select {
		case <-ackCtx.Done():
			r.err = ackCtx.Err()
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}
