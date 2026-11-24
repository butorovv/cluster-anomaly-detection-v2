package postgres

import (
	"context"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Reader struct {
	pool  *pgxpool.Pool
	hosts map[string][domain.FeatureCount]int32
}

func NewReader(ctx context.Context, pool *pgxpool.Pool) (*Reader, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	hosts, err := loadSources(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &Reader{pool: pool, hosts: hosts}, nil
}

type ImportRange struct{ From, To time.Time }

func (r *Reader) Imported(ctx context.Context, sha, topologyHash string) ([]ImportRange, error) {
	rows, err := r.pool.Query(ctx, `SELECT i.window_start,i.window_end FROM cad_archive.imports i
 JOIN cad_archive.source s ON s.sha256=i.source_sha256
 WHERE s.sha256=$1 AND s.topology_sha256=$2 AND s.feature_schema='F18-v1' AND s.step_seconds=10`, sha, topologyHash)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ImportRange])
}

type Values [domain.FeatureCount]*float64

// Samples requires an allowlisted set of timestamps, never a free-form SQL range.
func (r *Reader) Samples(ctx context.Context, host string, ticks []time.Time) (map[int64]Values, error) {
	ids, ok := r.hosts[host]
	if !ok {
		return nil, ErrUnknownSource
	}
	values := make(map[int64]Values, len(ticks))
	if len(ticks) == 0 {
		return values, nil
	}
	index := make(map[int32]int, len(ids))
	for i, id := range ids {
		index[id] = i
	}
	rows, err := r.pool.Query(ctx, `SELECT ts,metric_source_id,value::double precision
 FROM project.telemetry WHERE metric_source_id=ANY($1::int[]) AND ts=ANY($2::timestamptz[])`, ids[:], ticks)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ts time.Time
		var id int32
		var measurement float64
		if err := rows.Scan(&ts, &id, &measurement); err != nil {
			return nil, err
		}
		sample := values[ts.Unix()]
		sample[index[id]] = &measurement
		values[ts.Unix()] = sample
	}
	return values, rows.Err()
}
