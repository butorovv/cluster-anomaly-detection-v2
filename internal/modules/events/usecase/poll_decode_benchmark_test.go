package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
)

type pollBenchmarkTransport struct{ body string }

func (t pollBenchmarkTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(t.body)), Request: r}, nil
}

// BenchmarkPollEnvelopeDecode compares the exact base-revision Poller.poll
// path against the current Poller.poll path for identical pages. It includes
// page decoding, payload validation, URL construction, request handling, and
// poll metrics. It uses an in-memory RoundTripper and a discard-mode fake store;
// it excludes sockets, PostgreSQL SQL/durability, and worker computation.
func BenchmarkPollEnvelopeDecode(b *testing.B) {
	for _, count := range []int{1, 16, 64} {
		b.Run(fmt.Sprintf("items_%d", count), func(b *testing.B) {
			page, id, execution := pollingFixture(b, count)
			wire, err := json.Marshal(page)
			if err != nil {
				b.Fatal(err)
			}
			master := Master{Store: &pollDecodeStore{id: id, discard: true}, Metrics: observability.New()}
			client := &http.Client{Transport: pollBenchmarkTransport{body: string(wire)}}
			p := Poller{ExecutionID: execution, Groups: 16, Store: pollDecodeDirectory{endpoint: "http://memory.invalid"}, Master: master, Client: client}
			p.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
			ctx := context.Background()
			for _, tc := range []struct {
				name string
				run  func() error
			}{{"legacy_double_decode", func() error { return legacyPoll(ctx, p, "a01") }}, {"strict_decoded_envelope", func() error { return p.poll(ctx, "a01") }}} {
				b.Run(tc.name, func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(wire)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := tc.run(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

// legacyPoll is copied from git base 249dc0b's Poller.poll as a free function.
// Only the receiver syntax and group argument are explicit; checks and work
// ordering remain the original implementation for an apples-to-apples baseline.
func legacyPoll(ctx context.Context, p Poller, g string) error {
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
		if _, err := p.Master.legacyAcceptAt(ctx, item.Envelope, item.Cursor); err != nil {
			return err
		}
		cursor = item.Cursor
	}
	// A subsequent request carries this durable cursor. Worker ACKs it only after
	// independently verifying the PostgreSQL cursor, including after a lost reply.
	return nil
}

// Frozen byte acceptance from the same base, renamed only. This avoids
// charging the legacy path for any checks introduced by the new byte wrapper.
func (m Master) legacyAcceptAt(ctx context.Context, b []byte, c domain.Cursor) (receipt Receipt, retErr error) {
	received := time.Now()
	var envelope domain.Envelope
	defer func() { m.observed(ctx, envelope, received, receipt.Accepted, retErr) }()
	e, err := m.validated(ctx, b)
	envelope = e
	if err != nil {
		return Receipt{}, err
	}
	store, ok := m.Store.(positionedStore)
	if !ok {
		return Receipt{}, fmt.Errorf("delivery progress store required")
	}
	accepted, err := store.AcceptAt(ctx, e, c)
	return Receipt{e.MessageID, accepted}, err
}
