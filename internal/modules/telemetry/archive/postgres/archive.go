// Package postgres archives numeric F18 measurements in the unchanged project V1
// schema. It does not archive stream markers or consumer offsets. Callers may
// acknowledge input only after WriteBatch succeeds. A failed/ambiguous COMMIT can
// safely be retried with the same records.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrInvalidRecord = errors.New("invalid archive record")
	ErrBatchTooLarge = errors.New("archive batch too large")
	ErrConflict      = errors.New("archive integrity conflict")
	ErrSchema        = errors.New("incompatible archive schema or inventory")
	ErrUnknownSource = errors.New("unknown archive source")
	ErrTemporary     = errors.New("temporary archive database failure")
	ErrDatabase      = errors.New("archive database failure")
)

// DB is implemented by pgx.Conn and pgxpool.Pool. For concurrent use provide a
// concurrency-safe implementation (a single pgx.Conn is not concurrency-safe).
type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

// MaxBatchRecords bounds both transaction size and memory. Larger input must be
// split by the caller; each WriteBatch call is independently atomic.
const MaxBatchRecords = 4096
const statementRows = 1024

type Result struct {
	Records, Measurements, Inserted, Duplicates int
}

// Writer's dictionary is immutable after construction. Inventory administration
// must not reassign/delete sources while writers are running; recreate the writer
// after a controlled dictionary change. The adapter never updates measurements.
type Writer struct {
	db      DB
	limit   int
	sources map[string][domain.FeatureCount]int32
}

// New bootstraps/validates Summit inventory and loads source IDs once, within one
// transaction. batchLimit must be in 1..MaxBatchRecords. No telemetry is loaded.
func New(ctx context.Context, db DB, batchLimit int) (*Writer, error) {
	if db == nil || batchLimit < 1 || batchLimit > MaxBatchRecords {
		return nil, fmt.Errorf("%w: database and limit 1..%d required", ErrInvalidRecord, MaxBatchRecords)
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, classify(err)
	}
	defer rollback(tx)
	if err = bootstrap(ctx, tx); err != nil {
		return nil, classify(err)
	}
	sources, err := loadSources(ctx, tx)
	if err != nil {
		return nil, classify(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, classify(err)
	}
	return &Writer{db: db, limit: batchLimit, sources: sources}, nil
}

// SourceIDs returns a value copy in domain.FeatureNames order, irrespective of
// historical metric_source.source_key spelling.
func (w *Writer) SourceIDs(hostname string) ([domain.FeatureCount]int32, error) {
	ids, ok := w.sources[hostname]
	if !ok {
		return ids, fmt.Errorf("%w: %s", ErrUnknownSource, hostname)
	}
	return ids, nil
}

type measurementKey struct {
	source int32
	ts     time.Time
}
type measurement struct {
	measurementKey
	value string
}

// decimal uses the shortest decimal that round-trips to the original float64.
// PostgreSQL receives text -> numeric, never float8 -> numeric (which can round).
// Numeric equality deliberately identifies +0 and -0 as the same measurement.
func decimal(v float64) string {
	if v == 0 {
		return "0"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// WriteBatch validates the entire bounded input before SQL. Measurement results
// are returned only after COMMIT; an input with no available values needs no SQL.
// Duplicates includes repeats within the input batch.
// V1 timestamps have microsecond precision: finer timestamps are rejected rather
// than silently truncated. All finite values round-trip without extra rounding.
func (w *Writer) WriteBatch(ctx context.Context, records []domain.TelemetryRecord) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if len(records) > w.limit {
		return Result{}, ErrBatchTooLarge
	}
	result := Result{Records: len(records)}
	unique := make(map[measurementKey]string, len(records)*domain.FeatureCount)
	for i, r := range records {
		ids, err := w.SourceIDs(r.Hostname)
		if err != nil {
			return Result{}, err
		}
		if err = r.Validate(); err != nil {
			return Result{}, fmt.Errorf("%w: record %d: %v", ErrInvalidRecord, i, err)
		}
		if r.Timestamp.Nanosecond()%1000 != 0 || r.Timestamp.Year() < 1 || r.Timestamp.Year() > 9999 {
			return Result{}, fmt.Errorf("%w: timestamp must be a year 1..9999 microsecond instant", ErrInvalidRecord)
		}
		for f, value := range r.Features() {
			if r.AvailabilityMask&(1<<f) == 0 {
				continue
			}
			key := measurementKey{ids[f], r.Timestamp.UTC().Round(0)}
			v := decimal(value)
			if prior, ok := unique[key]; ok && prior != v {
				return Result{}, fmt.Errorf("%w: %s/%s", ErrConflict, r.Hostname, domain.FeatureNames[f])
			}
			unique[key] = v
			result.Measurements++
		}
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if len(unique) == 0 {
		return result, nil
	}
	rows := make([]measurement, 0, len(unique))
	for key, value := range unique {
		rows = append(rows, measurement{key, value})
	}
	// Consistent lock ordering also bounds deadlock risk for overlapping batches.
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].source != rows[j].source {
			return rows[i].source < rows[j].source
		}
		return rows[i].ts.Before(rows[j].ts)
	})
	tx, err := w.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Result{}, classify(err)
	}
	defer rollback(tx)
	for start := 0; start < len(rows); start += statementRows {
		end := min(start+statementRows, len(rows))
		ids := make([]int32, end-start)
		times := make([]time.Time, end-start)
		values := make([]string, end-start)
		for i, row := range rows[start:end] {
			ids[i], times[i], values[i] = row.source, row.ts, row.value
		}
		tag, e := tx.Exec(ctx, `INSERT INTO project.telemetry(metric_source_id,ts,value)
 SELECT id,ts,v::numeric FROM unnest($1::int[], $2::timestamptz[], $3::text[]) AS x(id,ts,v)
 ORDER BY id,ts ON CONFLICT (metric_source_id,ts) DO NOTHING`, ids, times, values)
		if e != nil {
			return Result{}, classify(e)
		}
		result.Inserted += int(tag.RowsAffected())
		// A separate READ COMMITTED statement sees a competing inserter after the
		// unique constraint wait. A same-statement CTE would use a stale snapshot.
		// SHARE locks prevent a concurrent UPDATE/DELETE until this transaction ends.
		var found, equal int
		e = tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE value = v::numeric) FROM (
 SELECT t.value,x.v FROM unnest($1::int[], $2::timestamptz[], $3::text[]) AS x(id,ts,v)
 JOIN project.telemetry t ON t.metric_source_id=x.id AND t.ts=x.ts FOR SHARE OF t) AS checked`, ids, times, values).Scan(&found, &equal)
		if e != nil {
			return Result{}, classify(e)
		}
		if found != len(ids) || equal != found {
			return Result{}, fmt.Errorf("%w: batch measurements differ from stored numeric values", ErrConflict)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return Result{}, classify(err)
	}
	result.Duplicates = result.Measurements - result.Inserted
	return result, nil
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	for _, sentinel := range []error{ErrSchema, ErrConflict, ErrUnknownSource, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, sentinel) {
			return err
		}
	}
	kind := ErrDatabase
	var p *pgconn.PgError
	if errors.As(err, &p) {
		switch {
		case p.Code == "23503":
			kind = ErrUnknownSource
		case p.Code == "40001" || p.Code == "40P01" || p.Code == "55P03" || p.Code == "57014" || len(p.Code) >= 2 && (p.Code[:2] == "08" || p.Code[:2] == "53") || p.Code == "57P01" || p.Code == "57P02" || p.Code == "57P03":
			kind = ErrTemporary
		case len(p.Code) >= 2 && (p.Code[:2] == "42" || p.Code[:2] == "23" || p.Code[:2] == "22"):
			kind = ErrSchema
		}
	} else {
		var networkError net.Error
		if errors.As(err, &networkError) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
			kind = ErrTemporary
		}
	}
	return fmt.Errorf("%w: %w", kind, err)
}
