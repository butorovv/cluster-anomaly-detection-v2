package usecase

import (
	"context"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"

	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type PollStore interface {
	Discover(context.Context, string, string) (domain.Endpoint, error)
	PollCursor(context.Context, string, string) (domain.Cursor, error)
}
type Poller struct {
	ExecutionID string
	Groups      int
	Concurrency int
	Interval    time.Duration
	Store       PollStore
	Master      Master
	Client      *http.Client
	Logger      *slog.Logger
}

func (p Poller) Run(ctx context.Context) error {
	if p.Concurrency == 0 {
		p.Concurrency = 4
	}
	if !domain.ValidExecution(p.ExecutionID) || (p.Groups != 4 && p.Groups != 8 && p.Groups != 16) || p.Concurrency < 1 || p.Concurrency > 16 || p.Interval < time.Millisecond || p.Interval > time.Minute {
		return fmt.Errorf("invalid polling configuration")
	}
	ownedClient := p.Client == nil
	if ownedClient {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.DialContext = observability.CountDial(transport.DialContext, p.Master.Metrics, "results_http")
		p.Client = &http.Client{Timeout: 2 * time.Second, Transport: transport}
	}
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	concurrency := p.Concurrency
	if concurrency > p.Groups {
		concurrency = p.Groups
	}
	type job struct{ index int }
	type result struct {
		index    int
		err      error
		finished time.Time
	}
	jobs := make(chan job)
	results := make(chan result, concurrency)
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer workers.Done()
			for task := range jobs {
				g := fmt.Sprintf("a%02d", task.index+1)
				bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
				err := p.poll(bounded, g)
				cancel()
				results <- result{index: task.index, err: err, finished: time.Now()}
			}
		}()
	}
	defer func() {
		close(jobs)
		workers.Wait()
		p.Master.Metrics.Set("poll_active_groups", 0)
		if ownedClient {
			p.Client.CloseIdleConnections()
		}
	}()

	due := make([]time.Time, p.Groups)
	busy := make([]bool, p.Groups)
	firstDue := time.Now().Add(p.Interval)
	for i := range due {
		due[i] = firstDue
	}
	active := 0
	fairAfter := -1
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		now := time.Now()
		chosen, earliest := -1, time.Time{}
		if active < concurrency {
			for offset := 0; offset < p.Groups; offset++ {
				i := (fairAfter + 1 + offset) % p.Groups
				if busy[i] {
					continue
				}
				if earliest.IsZero() || due[i].Before(earliest) {
					earliest = due[i]
					chosen = i
				}
			}
		}
		var jobCh chan job
		var nextJob job
		var timerCh <-chan time.Time
		if chosen >= 0 {
			if !earliest.After(now) {
				jobCh = jobs
				nextJob = job{index: chosen}
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				d := time.Until(earliest)
				timer.Reset(d)
				timerCh = timer.C
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-timerCh:
		case jobCh <- nextJob:
			busy[chosen] = true
			active++
			p.Master.Metrics.Set("poll_active_groups", float64(active))
			fairAfter = chosen
		case completed := <-results:
			busy[completed.index] = false
			active--
			p.Master.Metrics.Set("poll_active_groups", float64(active))
			due[completed.index] = completed.finished.Add(p.Interval)
			if completed.err != nil {
				g := fmt.Sprintf("a%02d", completed.index+1)
				p.Logger.WarnContext(ctx, "polling attempt failed", "execution", p.ExecutionID, "group", g, "error", completed.err)
			}
		}
	}
}
func (p Poller) poll(ctx context.Context, g string) error {
	start := time.Now()
	defer func() { p.Master.Metrics.Observe("poll_request", time.Since(start)) }()
	owner, err := p.Store.Discover(ctx, p.ExecutionID, g)
	if err != nil {
		return err
	}
	cursor, err := p.Store.PollCursor(ctx, p.ExecutionID, g)
	if err != nil {
		return err
	}
	target, err := url.Parse(owner.URL)
	if err != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") {
		return fmt.Errorf("invalid discovered worker URL")
	}
	target.Path = "/v1/outbox"
	target.RawQuery = url.Values{"execution_id": {p.ExecutionID}, "group_id": {g}, "revision": {strconv.FormatInt(cursor.Revision, 10)}, "ordinal": {strconv.Itoa(cursor.Ordinal)}, "limit": {"64"}}.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return err
	}
	p.Master.Metrics.Add("poll_http_requests", 1)
	response, err := p.Client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fmt.Errorf("Worker page HTTP %d", response.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(response.Body, 2*domain.MaxMessageBytes+1))
	if err != nil {
		return err
	}
	if len(b) > 2*domain.MaxMessageBytes {
		return fmt.Errorf("Worker page exceeds limit")
	}
	var page domain.Page
	if err := domain.StrictJSON(b, &page); err != nil {
		return err
	}
	if page.ExecutionID != p.ExecutionID || page.GroupID != g || page.OwnerID != owner.OwnerID || page.Epoch != owner.Epoch || len(page.Items) > 64 {
		return fmt.Errorf("Worker page identity mismatch")
	}
	for _, item := range page.Items {
		if !item.Cursor.After(cursor) {
			return fmt.Errorf("non-increasing Worker page cursor")
		}
		e, err := domain.Decode(item.Envelope)
		if err != nil {
			return err
		}
		if e.ExecutionID != p.ExecutionID || e.GroupID != g {
			return fmt.Errorf("cross-execution/group polling result")
		}
		if _, err := p.Master.AcceptAt(ctx, item.Envelope, item.Cursor); err != nil {
			return err
		}
		cursor = item.Cursor
	}
	// A subsequent request carries this durable cursor. Worker ACKs it only after
	// independently verifying the PostgreSQL cursor, including after a lost reply.
	return nil
}
