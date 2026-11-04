package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/jackc/pgx/v5/pgconn"
	"log/slog"
	"time"
)

type Store interface {
	Identity(context.Context, string) (group.RBI2Identity, error)
	Accept(context.Context, domain.Envelope) (bool, error)
	Events(context.Context, string, string, string, int) ([]json.RawMessage, error)
	Results(context.Context, string, domain.ResultCursor, int) (domain.ResultPage, error)
	Summary(context.Context, string) (domain.Summary, error)
}
type Master struct {
	Store   Store
	Metrics *observability.Registry
	Logger  *slog.Logger
	Measure bool
}
type Receipt struct {
	MessageID string `json:"message_id"`
	Accepted  bool   `json:"accepted"`
}

func (m Master) Accept(ctx context.Context, b []byte) (receipt Receipt, retErr error) {
	received := time.Now()
	var envelope domain.Envelope
	defer func() { m.observed(ctx, envelope, received, receipt.Accepted, retErr) }()
	e, err := domain.Decode(b)
	envelope = e
	if err != nil {
		return Receipt{}, fmt.Errorf("invalid envelope: %w", err)
	}
	id, err := m.Store.Identity(ctx, e.ExecutionID)
	if err != nil {
		return Receipt{}, err
	}
	if err := domain.Validate(e, id); err != nil {
		return Receipt{}, fmt.Errorf("invalid payload: %w", err)
	}
	accepted, err := m.Store.Accept(ctx, e)
	return Receipt{e.MessageID, accepted}, err
}

func (m Master) observed(ctx context.Context, e domain.Envelope, received time.Time, accepted bool, err error) {
	if err != nil {
		m.Metrics.Add("master_rejected", 1)
		var p *pgconn.PgError
		if errors.As(err, &p) && (p.Code == "23505" || p.Code == "55000") {
			m.Metrics.Add("master_conflicts", 1)
		}
		return
	}
	if accepted {
		m.Metrics.Add("master_accepted", 1)
	} else {
		m.Metrics.Add("master_duplicates", 1)
	}
	m.Metrics.Observe("master_accept", time.Since(received))
	if m.Measure && m.Logger != nil {
		m.Logger.InfoContext(ctx, "result measurement", "execution", e.ExecutionID, "group", e.GroupID, "message_id", e.MessageID, "kind", e.Kind, "tick", e.EventTime, "received_at", received, "committed_at", time.Now(), "accepted", accepted)
	}
}
