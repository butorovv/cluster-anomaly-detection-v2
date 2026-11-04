package resulttransport

import (
	"bytes"
	"context"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

const receiptLimit = 64 << 10

type HTTPSender struct {
	endpoint string
	client   *http.Client
}

func NewHTTP(endpoint string, timeout time.Duration, metrics ...*observability.Registry) (*HTTPSender, error) {
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid result HTTP endpoint")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("HTTP timeout must be positive")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/results"
	u.RawPath = ""
	transport := http.DefaultTransport.(*http.Transport).Clone()
	client := &http.Client{Timeout: timeout, Transport: transport}
	if len(metrics) > 0 && metrics[0] != nil {
		transport.DialContext = observability.CountDial(transport.DialContext, metrics[0], "results_http")
		client.Transport = transport
	}
	return &HTTPSender{endpoint: u.String(), client: client}, nil
}

func (s *HTTPSender) Send(ctx context.Context, raw []byte, groupID string) error {
	e, err := domain.Decode(raw)
	if err != nil {
		return err
	}
	if groupID == "" || len(groupID) > 256 || strings.TrimSpace(groupID) != groupID || e.GroupID != groupID {
		return fmt.Errorf("invalid or mismatched result group ID")
	}
	if _, err := topology.PartitionForGroup(groupID); err != nil {
		return fmt.Errorf("invalid result group ID: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, receiptLimit))
		return fmt.Errorf("result receiver returned HTTP %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, receiptLimit+1))
	if err != nil {
		return err
	}
	if len(b) > receiptLimit {
		return fmt.Errorf("result receipt exceeds %d bytes", receiptLimit)
	}
	var receipt struct {
		MessageID string `json:"message_id"`
		Accepted  *bool  `json:"accepted"`
	}
	if err := domain.StrictJSON(b, &receipt); err != nil {
		return fmt.Errorf("invalid result receipt: %w", err)
	}
	if receipt.Accepted == nil || receipt.MessageID != e.MessageID {
		return fmt.Errorf("result receipt does not acknowledge message")
	}
	return nil
}

func (s *HTTPSender) Close() error { s.client.CloseIdleConnections(); return nil }
