package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxStore struct{ DB *pgxpool.Pool }

func (s OutboxStore) Advertise(ctx context.Context, o domain.Owner, url string) error {
	_, err := s.DB.Exec(ctx, `SELECT cad_v2.advertise_worker($1::uuid,$2,$3::uuid,$4,$5)`, o.ExecutionID, o.GroupID, o.OwnerID, o.Epoch, url)
	return err
}
func (s OutboxStore) Page(ctx context.Context, o domain.Owner, c domain.Cursor, limit int, pending bool) ([]domain.Delivery, error) {
	// The read is authorized at the command snapshot; late delivery is safe via
	// Master dedup. Ack/Save/Renew/Release retain exclusive SQL epoch fencing.
	rows, err := s.DB.Query(ctx, `SELECT checkpoint_revision,ordinal,envelope,created_at FROM cad_v2.read_outbox_page($1::uuid,$2,$3::uuid,$4,$5,$6,$7,$8)`, o.ExecutionID, o.GroupID, o.OwnerID, o.Epoch, c.Revision, c.Ordinal, limit, pending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []domain.Delivery{}
	size := 0
	for rows.Next() {
		var d domain.Delivery
		if err := rows.Scan(&d.Cursor.Revision, &d.Cursor.Ordinal, &d.Envelope, &d.CreatedAt); err != nil {
			return nil, err
		}
		if len(d.Envelope) > domain.MaxMessageBytes {
			return nil, fmt.Errorf("outbox message exceeds transport limit")
		}
		if size+len(d.Envelope) > domain.MaxMessageBytes && len(result) > 0 {
			break
		}
		size += len(d.Envelope)
		result = append(result, d)
	}
	return result, rows.Err()
}
func (s OutboxStore) Ack(ctx context.Context, o domain.Owner, id string) error {
	_, err := s.DB.Exec(ctx, `SELECT cad_v2.ack_delivery($1::uuid,$2,$3::uuid,$4,$5)`, o.ExecutionID, o.GroupID, o.OwnerID, o.Epoch, id)
	return err
}
func (s OutboxStore) AckCursor(ctx context.Context, o domain.Owner, c domain.Cursor) error {
	_, err := s.DB.Exec(ctx, `SELECT cad_v2.ack_poll_cursor($1::uuid,$2,$3::uuid,$4,$5,$6)`, o.ExecutionID, o.GroupID, o.OwnerID, o.Epoch, c.Revision, c.Ordinal)
	return err
}
func (s MasterStore) Discover(ctx context.Context, execution, groupID string) (domain.Endpoint, error) {
	var e domain.Endpoint
	err := s.DB.QueryRow(ctx, `SELECT w.endpoint,w.owner_id::text,w.epoch FROM cad_v2.worker_endpoints w JOIN cad_v2.partition_ownership o USING(execution_id,group_id) WHERE w.execution_id=$1::uuid AND w.group_id=$2 AND w.owner_id=o.owner_id AND w.epoch=o.epoch AND o.lease_until>clock_timestamp()`, execution, groupID).Scan(&e.URL, &e.OwnerID, &e.Epoch)
	return e, err
}
func (s MasterStore) PollCursor(ctx context.Context, execution, groupID string) (domain.Cursor, error) {
	var c domain.Cursor
	err := s.DB.QueryRow(ctx, `SELECT checkpoint_revision,ordinal FROM cad_v2.master_poll_cursors WHERE execution_id=$1::uuid AND group_id=$2`, execution, groupID).Scan(&c.Revision, &c.Ordinal)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, nil
	}
	return c, err
}
func (s MasterStore) acceptTx(ctx context.Context, tx pgx.Tx, e domain.Envelope) (bool, error) {
	if e.Kind == "tick_fragment" {
		return s.acceptFragmentTx(ctx, tx, e)
	}
	b, err := json.Marshal(e)
	if err != nil {
		return false, err
	}
	var accepted bool
	err = tx.QueryRow(ctx, `SELECT cad_v2.accept_result($1::uuid,$2::jsonb)`, e.ExecutionID, b).Scan(&accepted)
	return accepted, err
}
func (s MasterStore) AcceptAt(ctx context.Context, e domain.Envelope, c domain.Cursor) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	ok, err := s.acceptTx(ctx, tx, e)
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `SELECT cad_v2.advance_poll_cursor($1::uuid,$2,$3,$4)`, e.ExecutionID, e.GroupID, c.Revision, c.Ordinal); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return ok, nil
}
func (s MasterStore) BindResults(ctx context.Context, execution, cluster, topic, consumer string, starts map[string]int64) error {
	b, err := json.Marshal(starts)
	if err != nil {
		return err
	}
	_, err = s.DB.Exec(ctx, `SELECT cad_v2.bind_result_source($1::uuid,$2,$3,$4,$5::jsonb)`, execution, cluster, topic, consumer, b)
	return err
}
func (s MasterStore) ResultOffset(ctx context.Context, execution string, p int32) (int64, error) {
	var next int64
	err := s.DB.QueryRow(ctx, `SELECT next_offset FROM cad_v2.result_consumer_offsets WHERE execution_id=$1::uuid AND partition_id=$2`, execution, p).Scan(&next)
	return next, err
}
func (s MasterStore) AcceptKafka(ctx context.Context, execution, topic string, p int32, offset int64, e *domain.Envelope) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	accepted := false
	if e != nil {
		accepted, err = s.acceptTx(ctx, tx, *e)
		if err != nil {
			return false, err
		}
	}
	if _, err := tx.Exec(ctx, `SELECT cad_v2.advance_result_offset($1::uuid,$2,$3,$4,$5)`, execution, topic, p, offset, offset+1); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return accepted, nil
}
