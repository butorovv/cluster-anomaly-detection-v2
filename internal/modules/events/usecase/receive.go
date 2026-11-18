package usecase

import (
	"context"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"time"
)

func (m Master) validated(ctx context.Context, b []byte) (domain.Envelope, error) {
	e, err := domain.Decode(b)
	if err != nil {
		return e, fmt.Errorf("invalid envelope: %w", err)
	}
	return e, m.validateEnvelope(ctx, e)
}

func (m Master) validateEnvelope(ctx context.Context, e domain.Envelope) error {
	id, err := m.Store.Identity(ctx, e.ExecutionID)
	if err != nil {
		return err
	}
	if err := domain.Validate(e, id); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}
	return nil
}

type positionedStore interface {
	AcceptAt(context.Context, domain.Envelope, domain.Cursor) (bool, error)
	AcceptKafka(context.Context, string, string, int32, int64, *domain.Envelope) (bool, error)
}

func (m Master) AcceptAt(ctx context.Context, b []byte, c domain.Cursor) (receipt Receipt, retErr error) {
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
func (m Master) AcceptKafka(ctx context.Context, execution, topic string, p int32, offset int64, b []byte) (retErr error) {
	e, err := domain.DecodeEnvelope(b)
	if err != nil {
		if e.Envelope().ExecutionID == execution {
			m.observed(ctx, e.Envelope(), time.Now(), false, err)
		}
		return err
	}
	return m.AcceptKafkaEnvelope(ctx, execution, topic, p, offset, e)
}

// AcceptKafkaEnvelope receives the envelope already decoded and routed by the
// Kafka intake. Scientific payload validation remains execution-specific.
func (m Master) AcceptKafkaEnvelope(ctx context.Context, execution, topic string, p int32, offset int64, decoded domain.DecodedEnvelope) (retErr error) {
	received := time.Now()
	e := decoded.Envelope()
	accepted := false
	defer func() {
		if e.ExecutionID == execution {
			m.observed(ctx, e, received, accepted, retErr)
		}
	}()
	if err := decoded.Header(); err != nil {
		return err
	}
	store, ok := m.Store.(positionedStore)
	if !ok {
		return fmt.Errorf("delivery progress store required")
	}
	if e.ExecutionID != execution {
		_, err := store.AcceptKafka(ctx, execution, topic, p, offset, nil)
		return err
	}
	if err := m.validateEnvelope(ctx, e); err != nil {
		return err
	}
	var err error
	accepted, err = store.AcceptKafka(ctx, execution, topic, p, offset, &e)
	return err
}
