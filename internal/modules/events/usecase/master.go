package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

type Store interface {
	Identity(context.Context, string) (group.RBI2Identity, error)
	Accept(context.Context, domain.Envelope) (bool, error)
	Events(context.Context, string, string, string, int) ([]json.RawMessage, error)
}
type Master struct{ Store Store }
type Receipt struct {
	MessageID string `json:"message_id"`
	Accepted  bool   `json:"accepted"`
}

func (m Master) Accept(ctx context.Context, b []byte) (Receipt, error) {
	e, err := domain.Decode(b)
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
