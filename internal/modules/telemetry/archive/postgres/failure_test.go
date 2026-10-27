package postgres

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Simulate the ambiguous client outcome of a lost COMMIT acknowledgement. This
// does a real commit and only then loses the response; a fresh connection retries.
type lostAckDB struct{ conn *pgx.Conn }
type lostAckTx struct{ pgx.Tx }

func (d lostAckDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, e := d.conn.BeginTx(ctx, o)
	if e != nil {
		return nil, e
	}
	return lostAckTx{tx}, nil
}
func (tx lostAckTx) Commit(ctx context.Context) error {
	if e := tx.Tx.Commit(ctx); e != nil {
		return e
	}
	return io.ErrUnexpectedEOF
}

func TestLostCommitAcknowledgementCanBeReplayed(t *testing.T) {
	c, _ := archiveDB(t)
	ctx := context.Background()
	w, e := New(ctx, c, 1)
	if e != nil {
		t.Fatal(e)
	}
	r := recordAt(time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC), "a01n01", domain.FullAvailabilityMask, 0.1)
	w.db = lostAckDB{c}
	result, e := w.WriteBatch(ctx, []domain.TelemetryRecord{r})
	if e == nil || result != (Result{}) {
		t.Fatalf("ambiguous commit returned success: %+v %v", result, e)
	}
	fresh, e := pgx.ConnectConfig(ctx, c.Config().Copy())
	if e != nil {
		t.Fatal(e)
	}
	defer fresh.Close(ctx)
	w, e = New(ctx, fresh, 1)
	if e != nil {
		t.Fatal(e)
	}
	result, e = w.WriteBatch(ctx, []domain.TelemetryRecord{r})
	if e != nil || result.Inserted != 0 || result.Duplicates != 18 {
		t.Fatalf("retry after committed response loss: %+v %v", result, e)
	}
}

type cancelAfterInsert struct {
	cancel context.CancelFunc
	fired  bool
}

func (tr *cancelAfterInsert) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, queryTag{}, strings.HasPrefix(data.SQL, "INSERT INTO project.telemetry"))
}

type queryTag struct{}

func (tr *cancelAfterInsert) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if ctx.Value(queryTag{}) == true && data.Err == nil {
		tr.fired = true
		tr.cancel()
	}
}

func TestCancellationAfterInsertRollsBack(t *testing.T) {
	c, _ := archiveDB(t)
	ctx := context.Background()
	if e := Bootstrap(ctx, c); e != nil {
		t.Fatal(e)
	}
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	tr := &cancelAfterInsert{cancel: cancel}
	cfg := c.Config().Copy()
	cfg.Tracer = tr
	writerConn, e := pgx.ConnectConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer writerConn.Close(ctx)
	w, e := New(ctx, writerConn, 60)
	if e != nil {
		t.Fatal(e)
	}
	var batch []domain.TelemetryRecord
	for i := 0; i < 60; i++ {
		batch = append(batch, recordAt(time.Date(2022, 1, 18, 8, i, 0, 0, time.UTC), "a01n01", domain.FullAvailabilityMask, 1))
	}
	result, e := w.WriteBatch(writeCtx, batch)
	if e == nil || result != (Result{}) || !tr.fired {
		t.Fatalf("cancelled write: fired=%t result=%+v error=%v", tr.fired, result, e)
	}
	var count int
	if e = c.QueryRow(ctx, `SELECT count(*) FROM project.telemetry`).Scan(&count); e != nil || count != 0 {
		t.Fatalf("partial commit after cancellation: %d %v", count, e)
	}
}

func TestDatabaseErrorClassification(t *testing.T) {
	for _, tt := range []struct {
		code string
		want error
	}{
		{"23503", ErrUnknownSource}, {"42P01", ErrSchema}, {"23514", ErrSchema},
		{"40001", ErrTemporary}, {"40P01", ErrTemporary}, {"55P03", ErrTemporary},
		{"08006", ErrTemporary}, {"53300", ErrTemporary}, {"57P01", ErrTemporary}, {"XX000", ErrDatabase},
	} {
		t.Run(tt.code, func(t *testing.T) {
			original := &pgconn.PgError{Code: tt.code}
			err := classify(original)
			if !errors.Is(err, tt.want) || !errors.Is(err, original) {
				t.Fatal(err)
			}
		})
	}
	if !errors.Is(classify(context.Canceled), context.Canceled) {
		t.Fatal("lost cancellation identity")
	}
}
