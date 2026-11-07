package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MasterStore struct {
	DB     *pgxpool.Pool
	Stages *observability.Registry // optional, profiling trials only
}

func (s MasterStore) Identity(ctx context.Context, execution string) (group.RBI2Identity, error) {
	var id group.RBI2Identity
	var raw []byte
	if !domain.ValidExecution(execution) {
		return id, fmt.Errorf("execution UUID required")
	}
	err := s.DB.QueryRow(ctx, `SELECT r.identity_json FROM cad_v2.runs r JOIN cad_v2.executions x USING(run_id) WHERE execution_id=$1::uuid`, execution).Scan(&raw)
	if err != nil {
		return id, err
	}
	if err := json.Unmarshal(raw, &id); err != nil {
		return id, err
	}
	valid, err := group.NewRBI2Identity(id.ProfileSHA256, id.InputSHA256, id.Groups, id.Window, id.DeltaSeconds)
	if err != nil || valid.RunID != id.RunID || valid.ConfigID != id.ConfigID || valid.LegacyRunID != id.LegacyRunID || valid.Version != id.Version {
		return id, fmt.Errorf("invalid registered scientific identity")
	}
	return id, nil
}
func (s MasterStore) Accept(ctx context.Context, e domain.Envelope) (bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	accepted, err := s.acceptTx(ctx, tx, e)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return accepted, nil
}
func (s MasterStore) Events(ctx context.Context, execution, groupID, after string, limit int) ([]json.RawMessage, error) {
	if !domain.ValidExecution(execution) || limit < 1 || limit > 500 {
		return nil, fmt.Errorf("execution and limit 1..500 required")
	}
	rows, err := s.DB.Query(ctx, `SELECT payload FROM cad_v2.master_events WHERE execution_id=$1::uuid AND terminal AND ($2='' OR group_id=$2) AND event_id>$3 ORDER BY event_id LIMIT $4`, execution, groupID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []json.RawMessage{}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		result = append(result, json.RawMessage(raw))
	}
	return result, rows.Err()
}
