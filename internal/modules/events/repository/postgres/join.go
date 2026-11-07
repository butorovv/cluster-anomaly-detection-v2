package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/jackc/pgx/v5"
)

func (s MasterStore) acceptFragmentTx(ctx context.Context, tx pgx.Tx, e domain.Envelope) (bool, error) {
	defer observability.Stage(s.Stages, "join_total")()
	raw, err := json.Marshal(e)
	if err != nil {
		return false, err
	}
	var accepted bool
	if err := tx.QueryRow(ctx, `SELECT cad_v2.accept_tick_fragment($1::uuid,$2::jsonb)`, e.ExecutionID, raw).Scan(&accepted); err != nil {
		return false, err
	}
	if !accepted {
		return false, nil
	}
	var identityBytes []byte
	var fragmentCount int
	// accept_tick_fragment holds the existing canonical execution-row locks until
	// COMMIT. Count under those same locks; partial timestamps need no payloads.
	// Identity and completeness share one round trip. Durable SQL remains the sole
	// aggregation source, including after restart or an uncertain COMMIT outcome.
	if err := tx.QueryRow(ctx, `SELECT r.identity_json,
 (SELECT count(*) FROM cad_v2.master_tick_fragments f WHERE f.execution_id=x.execution_id AND f.event_time=$2)
 FROM cad_v2.runs r JOIN cad_v2.executions x USING(run_id) WHERE x.execution_id=$1::uuid`, e.ExecutionID, e.EventTime).Scan(&identityBytes, &fragmentCount); err != nil {
		return false, err
	}
	var id group.RBI2Identity
	if err := json.Unmarshal(identityBytes, &id); err != nil {
		return false, err
	}
	if fragmentCount < id.Groups {
		return true, nil
	}
	if fragmentCount != id.Groups {
		return false, fmt.Errorf("diagnostic join group cardinality mismatch")
	}
	rows, err := tx.Query(ctx, `SELECT envelope FROM cad_v2.master_tick_fragments WHERE execution_id=$1::uuid AND event_time=$2 ORDER BY group_id`, e.ExecutionID, e.EventTime)
	if err != nil {
		return false, err
	}
	envelopes := []domain.Envelope{}
	payloads := []domain.GroupResult{}
	decodeDone := observability.Stage(s.Stages, "join_fetch_and_json_decode")
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			rows.Close()
			return false, err
		}
		var envelope domain.Envelope
		if err := json.Unmarshal(b, &envelope); err != nil {
			rows.Close()
			return false, err
		}
		var payload domain.GroupResult
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			rows.Close()
			return false, err
		}
		envelopes = append(envelopes, envelope)
		payloads = append(payloads, payload)
	}
	rows.Close()
	decodeDone()
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(envelopes) != id.Groups {
		return false, fmt.Errorf("diagnostic join group cardinality mismatch")
	}
	rawRows := make([]group.Raw, id.Groups)
	for i, v := range envelopes {
		if v.GroupID != fmt.Sprintf("a%02d", i+1) || v.RunID != id.RunID || v.ConfigID != id.ConfigID {
			return false, fmt.Errorf("diagnostic join universe mismatch")
		}
		rawRows[i] = payloads[i].Raw
	}
	// The sole cross-group scientific operation. It annotates copies of Raw only;
	// every already accepted Temporal, Evidence ID and terminal event is preserved.
	annotateDone := observability.Stage(s.Stages, "join_annotate_outside")
	group.AnnotateOutside(rawRows)
	annotateDone()
	finalDone := observability.Stage(s.Stages, "join_final_envelopes_and_sql")
	for i, fragment := range envelopes {
		payload := payloads[i]
		payload.Raw = rawRows[i]
		if payload.Evidence != nil {
			payload.Evidence.Raw = rawRows[i]
		}
		bytes, err := json.Marshal(payload)
		if err != nil {
			return false, err
		}
		final := domain.Envelope{SchemaVersion: 1, ExecutionID: e.ExecutionID, RunID: id.RunID, ConfigID: id.ConfigID, GroupID: fragment.GroupID, MessageID: domain.MessageID(id.RunID, fragment.GroupID, "group_result", e.EventTime.UTC().Format("2006-01-02T15:04:05Z07:00"), 1), Kind: "group_result", Revision: 1, EventTime: e.EventTime, Terminal: true, Payload: bytes}
		if err := domain.Validate(final, id); err != nil {
			return false, err
		}
		if _, err := s.acceptTx(ctx, tx, final); err != nil {
			return false, err
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO cad_v2.master_tick_joins(execution_id,event_time) VALUES($1::uuid,$2)`, e.ExecutionID, e.EventTime); err != nil {
		return false, err
	}
	finalDone()
	return true, nil
}
