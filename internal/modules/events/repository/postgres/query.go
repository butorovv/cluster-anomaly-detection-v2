package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"time"
)

func (s MasterStore) Results(ctx context.Context, execution string, after domain.ResultCursor, limit int) (domain.ResultPage, error) {
	page := domain.ResultPage{Items: []domain.GroupResult{}, Next: after}
	if !domain.ValidExecution(execution) || limit < 1 || limit > 128 {
		return page, fmt.Errorf("valid execution and limit 1..128 required")
	}
	rows, err := s.DB.Query(ctx, `SELECT tick,group_id,payload FROM cad_v2.master_results WHERE execution_id=$1::uuid AND (tick,group_id)>($2::timestamptz,$3) ORDER BY tick,group_id LIMIT $4`, execution, after.Tick, after.GroupID, limit)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	size := 0
	for rows.Next() {
		var tick time.Time
		var groupID string
		var b []byte
		if err := rows.Scan(&tick, &groupID, &b); err != nil {
			return page, err
		}
		if len(b) > domain.MaxMessageBytes {
			return page, fmt.Errorf("result too large")
		}
		if size+len(b) > domain.MaxMessageBytes && len(page.Items) > 0 {
			break
		}
		size += len(b)
		var result domain.GroupResult
		if err := json.Unmarshal(b, &result); err != nil {
			return page, err
		}
		page.Items = append(page.Items, result)
		page.Next = domain.ResultCursor{Tick: tick.UTC(), GroupID: groupID}
	}
	return page, rows.Err()
}
func (s MasterStore) Summary(ctx context.Context, execution string) (domain.Summary, error) {
	report := domain.Summary{ExecutionID: execution}
	id, err := s.Identity(ctx, execution)
	if err != nil {
		return report, err
	}
	report.RunID, report.ConfigID = id.RunID, id.ConfigID
	report.ExpectedTicks = int64(id.Window.End.Sub(id.Window.Start)/(10*time.Second)) * int64(id.Groups)
	// Aggregate already accepted immutable payloads; no voting/Temporal inference.
	err = s.DB.QueryRow(ctx, `SELECT t.n,t.positive,t.unknown,t.i2,e.n,e.new_events,e.rb,e.rb_parents FROM
 (SELECT count(*) n,count(*) FILTER(WHERE payload#>>'{temporal,vote}'='positive') positive,count(*) FILTER(WHERE payload#>>'{temporal,vote}'='unknown') unknown,count(*) FILTER(WHERE payload#>>'{temporal,local_i2,state}'='positive') i2 FROM cad_v2.master_results WHERE execution_id=$1::uuid) t
 CROSS JOIN (SELECT count(*) n,count(*) FILTER(WHERE (payload->>'new_disjoint_from_rb')::boolean) new_events,coalesce(sum(jsonb_array_length(payload->'rb_subspans')),0) rb,count(*) FILTER(WHERE NOT (payload->>'new_disjoint_from_rb')::boolean) rb_parents FROM cad_v2.master_events WHERE execution_id=$1::uuid AND terminal) e`, execution).Scan(&report.Ticks, &report.PositiveTicks, &report.UnknownTicks, &report.I2PositiveTicks, &report.Events, &report.NewEvents, &report.RBSubspans, &report.RBSupportedParents)
	report.Complete = report.Ticks == report.ExpectedTicks
	return report, err
}
