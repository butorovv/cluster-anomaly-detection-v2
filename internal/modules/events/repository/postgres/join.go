package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/jackc/pgx/v5"
)

func (s MasterStore) acceptFragmentTx(ctx context.Context, tx pgx.Tx, e domain.Envelope) (bool, error) {
	// V5.accept_result takes execution_partitions FOR UPDATE. A fragment INSERT
	// acquires a foreign-key KEY SHARE lock first. Two timestamp joins can otherwise
	// both hold KEY SHARE on the final group and deadlock while upgrading to UPDATE.
	// Lock the complete physical universe in canonical order BEFORE inserting any
	// fragment. V1-V5 stay unchanged; Worker ownership rows are not locked here.
	if _, err := tx.Exec(ctx, `SELECT group_id FROM cad_v2.execution_partitions WHERE execution_id=$1::uuid ORDER BY group_id FOR UPDATE`, e.ExecutionID); err != nil {
		return false, err
	}
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
	if err := tx.QueryRow(ctx, `SELECT r.identity_json FROM cad_v2.runs r JOIN cad_v2.executions x USING(run_id) WHERE x.execution_id=$1::uuid`, e.ExecutionID).Scan(&identityBytes); err != nil {
		return false, err
	}
	var id group.RBI2Identity
	if err := json.Unmarshal(identityBytes, &id); err != nil {
		return false, err
	}
	rows, err := tx.Query(ctx, `SELECT envelope FROM cad_v2.master_tick_fragments WHERE execution_id=$1::uuid AND event_time=$2 ORDER BY group_id`, e.ExecutionID, e.EventTime)
	if err != nil {
		return false, err
	}
	envelopes := []domain.Envelope{}
	payloads := []domain.GroupResult{}
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
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(envelopes) < id.Groups {
		return true, nil
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
	group.AnnotateOutside(rawRows)
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
	return true, nil
}
