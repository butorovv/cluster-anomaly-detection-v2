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
	"time"
)

type PollStore interface {
	Discover(context.Context, string, string) (domain.Endpoint, error)
	PollCursor(context.Context, string, string) (domain.Cursor, error)
}
type Poller struct {
	ExecutionID string
	Groups      int
	Interval    time.Duration
	Store       PollStore
	Master      Master
	Client      *http.Client
	Logger      *slog.Logger
}

func (p Poller) Run(ctx context.Context) error {
	if !domain.ValidExecution(p.ExecutionID) || (p.Groups != 4 && p.Groups != 8 && p.Groups != 16) || p.Interval < time.Millisecond || p.Interval > time.Minute {
		return fmt.Errorf("invalid polling configuration")
	}
	if p.Client == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.DialContext = observability.CountDial(transport.DialContext, p.Master.Metrics, "results_http")
		p.Client = &http.Client{Timeout: 2 * time.Second, Transport: transport}
		defer p.Client.CloseIdleConnections()
	}
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		for i := 1; i <= p.Groups; i++ {
			if ctx.Err() != nil {
				return nil
			}
			g := fmt.Sprintf("a%02d", i)
			bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
			err := p.poll(bounded, g)
			cancel()
			if err != nil {
				p.Logger.WarnContext(ctx, "polling attempt failed", "execution", p.ExecutionID, "group", g, "error", err)
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
