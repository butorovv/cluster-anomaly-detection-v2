package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	events "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var ErrLeased = errors.New("partition is still leased")

type Lease struct {
	Group, Owner string
	Epoch        int64
}
type Store struct {
	DB           DB
	Registration Registration
}

func loadCheckpoint(ctx context.Context, q rowQuery, execution, groupID string) (*core.Checkpoint, error) {
	var c core.Checkpoint
	var version int
	var codec string
	err := q.QueryRow(ctx, `SELECT checkpoint_revision,next_offset,last_tick,finalized,snapshot_schema_version,snapshot_codec,snapshot,snapshot_sha256 FROM cad_v2.group_checkpoints WHERE execution_id=$1::uuid AND group_id=$2`, execution, groupID).Scan(&c.Revision, &c.NextOffset, &c.LastTick, &c.Finalized, &version, &codec, &c.Snapshot, &c.SHA256)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if version != group.MathSnapshotSchemaVersion || codec != "json-utf8" {
		return nil, fmt.Errorf("unsupported checkpoint schema/codec")
	}
	if _, err := core.DecodeCheckpoint(c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *Store) Load(ctx context.Context, groupID string) (*core.Checkpoint, error) {
	return loadCheckpoint(ctx, s.DB, s.Registration.Binding.ExecutionID, groupID)
}

func (s *Store) Acquire(ctx context.Context, groupID, owner string, ttl time.Duration) (Lease, *core.Checkpoint, error) {
	lease := Lease{Group: groupID, Owner: owner}
	if ttl < time.Second || ttl > 5*time.Minute || !uuidValid(owner) {
		return lease, nil, fmt.Errorf("valid owner UUID and lease TTL required")
	}
	tx, err := s.DB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return lease, nil, err
	}
	defer tx.Rollback(context.Background())
	err = tx.QueryRow(ctx, `SELECT cad_v2.acquire_partition($1::uuid,$2,$3::uuid,$4::interval)`, s.Registration.Binding.ExecutionID, groupID, owner, fmt.Sprintf("%f seconds", ttl.Seconds())).Scan(&lease.Epoch)
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) && pgerr.Code == "55000" {
		return lease, nil, ErrLeased
	}
	if err != nil {
		return lease, nil, err
	}
	saved, err := loadCheckpoint(ctx, tx, s.Registration.Binding.ExecutionID, groupID)
	if err != nil {
		return lease, nil, err
	}
	if saved == nil {
		var count int
		if err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM cad_v2.outbox_messages WHERE execution_id=$1::uuid AND group_id=$2) + (SELECT count(*) FROM cad_v2.worker_tick_fragments WHERE execution_id=$1::uuid AND group_id=$2)`, s.Registration.Binding.ExecutionID, groupID).Scan(&count); err != nil {
			return lease, nil, err
		}
		if count != 0 {
			return lease, nil, fmt.Errorf("missing checkpoint with existing durable outputs")
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return lease, nil, err
	}
	return lease, saved, nil
}

func (s *Store) Renew(ctx context.Context, l Lease, ttl time.Duration) error {
	_, err := s.DB.Exec(ctx, `SELECT cad_v2.renew_partition($1::uuid,$2,$3::uuid,$4,$5::interval)`, s.Registration.Binding.ExecutionID, l.Group, l.Owner, l.Epoch, fmt.Sprintf("%f seconds", ttl.Seconds()))
	return err
}
func (s *Store) Release(ctx context.Context, l Lease) error {
	_, err := s.DB.Exec(ctx, `SELECT cad_v2.release_partition($1::uuid,$2,$3::uuid,$4)`, s.Registration.Binding.ExecutionID, l.Group, l.Owner, l.Epoch)
	return err
}

func (s *Store) Save(ctx context.Context, l Lease, transition core.Transition) (int64, error) {
	c := transition.Checkpoint
	snap, err := core.DecodeCheckpoint(c)
	if err != nil {
		return 0, err
	}
	if snap.Group != l.Group || snap.Binding != s.Registration.Binding || hashJSON(snap.Identity) != hashJSON(s.Registration.Identity) {
		return 0, fmt.Errorf("checkpoint identity mismatch")
	}
	outputs, err := core.TerminalEvents(s.Registration.Binding.ExecutionID, transition.Result.Events)
	if err != nil {
		return 0, err
	}
	payload, err := json.Marshal(outputs)
	if err != nil {
		return 0, err
	}
	fragments := []events.Envelope{}
	if s.Registration.Diagnostics {
		results := transition.Results
		if len(results) == 0 {
			results = []group.GroupProcessorResult{transition.Result}
		}
		for _, result := range results {
			fragment, err := events.TickFragment(s.Registration.Binding.ExecutionID, s.Registration.Identity, result)
			if err != nil {
				return 0, err
			}
			fragments = append(fragments, fragment)
		}
	}
	var revision int64
	for attempt := 0; ; attempt++ {
		var commitAttempted bool
		revision, commitAttempted, err = s.saveAttempt(ctx, l, c, payload, outputs, fragments)
		if err == nil {
			return revision, nil
		}
		if commitAttempted {
			break // ambiguous COMMIT is reconciled below, never blindly retried
		}
		var pgerr *pgconn.PgError
		if attempt == 2 || ctx.Err() != nil || !errors.As(err, &pgerr) ||
			(pgerr.Code != "55P03" && pgerr.Code != "57014" && pgerr.Code != "40001" && pgerr.Code != "40P01") {
			return 0, err
		}
		// The failed transaction has rolled back. Retry only this immutable
		// proposal, without re-running Step or acknowledging RAM/input progress.
		// Keep the caller's original deadline and re-check ownership in SQL.
		timer := time.NewTimer(time.Duration(attempt+1) * 50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
	// COMMIT errors may mean the server committed but the ACK was lost. Never
	// blindly retry the CAS/write; reread on an independent bounded connection.
	recovery, done := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer done()
	durable, readErr := s.Load(recovery, l.Group)
	if readErr != nil {
		return 0, fmt.Errorf("checkpoint commit outcome unresolved: %w", readErr)
	}
	if durable == nil || durable.Revision != c.Revision || durable.NextOffset != c.NextOffset || durable.SHA256 != c.SHA256 || !durable.LastTick.Equal(c.LastTick) || durable.Finalized != c.Finalized || !bytes.Equal(durable.Snapshot, c.Snapshot) {
		return 0, fmt.Errorf("checkpoint not confirmed after COMMIT error: %w", err)
	}
	for _, output := range outputs {
		var got []byte
		if readErr = s.DB.QueryRow(recovery, `SELECT envelope FROM cad_v2.outbox_messages WHERE execution_id=$1::uuid AND message_id=$2`, s.Registration.Binding.ExecutionID, output.MessageID).Scan(&got); readErr != nil {
			return 0, fmt.Errorf("checkpoint outbox reconciliation failed: %w", readErr)
		}
		var saved core.EventEnvelope
		if json.Unmarshal(got, &saved) != nil || hashJSON(saved) != hashJSON(output) {
			return 0, fmt.Errorf("checkpoint outbox reconciliation mismatch")
		}
	}
	for _, fragment := range fragments {
		bytes, err := json.Marshal(fragment)
		if err != nil {
			return 0, err
		}
		var exact bool
		if err := s.DB.QueryRow(recovery, `SELECT envelope=$3::jsonb FROM cad_v2.worker_tick_fragments WHERE execution_id=$1::uuid AND message_id=$2`, s.Registration.Binding.ExecutionID, fragment.MessageID, bytes).Scan(&exact); err != nil || !exact {
			return 0, fmt.Errorf("checkpoint fragment reconciliation failed: %v", err)
		}
	}
	return durable.Revision, nil
}

// saveAttempt reports whether COMMIT was sent. Only known SQL failures before
// that point may be retried; deferred rollback completes before returning.
func (s *Store) saveAttempt(ctx context.Context, l Lease, c core.Checkpoint, payload []byte, outputs []core.EventEnvelope, fragments []events.Envelope) (int64, bool, error) {
	tx, err := s.DB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback(context.Background())
	var revision int64
	err = tx.QueryRow(ctx, `SELECT cad_v2.commit_checkpoint($1::uuid,$2,$3::uuid,$4,$5,$6,$7,$8,'json-utf8',$9::bytea,$10::cad_v2.sha256_hex,$11,$12::jsonb)`, s.Registration.Binding.ExecutionID, l.Group, l.Owner, l.Epoch, c.Revision-1, c.NextOffset, c.LastTick, group.MathSnapshotSchemaVersion, c.Snapshot, c.SHA256, c.Finalized, payload).Scan(&revision)
	if err != nil {
		return 0, false, err
	}
	for i, fragment := range fragments {
		bytes, err := json.Marshal(fragment)
		if err != nil {
			return 0, false, err
		}
		if _, err := tx.Exec(ctx, `SELECT cad_v2.stage_tick_fragment($1::uuid,$2,$3::uuid,$4,$5,$6,$7::jsonb)`, s.Registration.Binding.ExecutionID, l.Group, l.Owner, l.Epoch, c.Revision, len(outputs)+i+1, bytes); err != nil {
			return 0, false, err
		}
	}
	err = tx.Commit(ctx)
	return revision, true, err
}

// Pending returns durable messages without acknowledgement or dispatch. They
// survive reassignment because neither lease ownership nor this read deletes rows.
func (s *Store) Pending(ctx context.Context, groupID string) ([]core.EventEnvelope, error) {
	rows, err := s.DB.Query(ctx, `SELECT envelope FROM cad_v2.outbox_messages WHERE execution_id=$1::uuid AND group_id=$2 AND delivered_at IS NULL ORDER BY checkpoint_revision,ordinal`, s.Registration.Binding.ExecutionID, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []core.EventEnvelope{}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		var e core.EventEnvelope
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}
