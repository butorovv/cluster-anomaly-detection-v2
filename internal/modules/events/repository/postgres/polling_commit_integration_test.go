package postgres_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Hold the real server's successful COMMIT reply before delivering it to pgx.
// The database is already durable when committed closes; only the ACK is held.
func pollingCommitGate(t *testing.T, source *pgxpool.Pool) (*pgxpool.Pool, *atomic.Bool, <-chan struct{}, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	armed := &atomic.Bool{}
	committed, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var workers sync.WaitGroup
	var connections sync.Map
	acceptDone := make(chan struct{})
	t.Cleanup(func() {
		unblock()
		_ = listener.Close()
		<-acceptDone
		connections.Range(func(k, _ any) bool { _ = k.(net.Conn).Close(); return true })
		workers.Wait()
	})
	cfg := source.Config()
	target := net.JoinHostPort(cfg.ConnConfig.Host, fmt.Sprint(cfg.ConnConfig.Port))
	go func() {
		defer close(acceptDone)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer client.Close()
				connections.Store(client, true)
				defer connections.Delete(client)
				server, err := net.DialTimeout("tcp", target, 2*time.Second)
				if err != nil {
					return
				}
				defer server.Close()
				connections.Store(server, true)
				defer connections.Delete(server)
				copied := make(chan struct{})
				go func() { _, _ = io.Copy(server, client); _ = server.Close(); close(copied) }()
				defer func() { _ = client.Close(); _ = server.Close(); <-copied }()
				for {
					header := make([]byte, 5)
					if _, err := io.ReadFull(server, header); err != nil {
						return
					}
					size := int(binary.BigEndian.Uint32(header[1:]))
					if size < 4 || size > 32<<20 {
						return
					}
					body := make([]byte, size-4)
					if _, err := io.ReadFull(server, body); err != nil {
						return
					}
					if header[0] == 'C' && string(body) == "COMMIT\x00" && armed.CompareAndSwap(true, false) {
						close(committed)
						<-release
					}
					if _, err := client.Write(append(header, body...)); err != nil {
						return
					}
				}
			}()
		}
	}()
	cfg.ConnConfig.Host = "127.0.0.1"
	cfg.ConnConfig.Port = uint16(listener.Addr().(*net.TCPAddr).Port)
	cfg.ConnConfig.TLSConfig = nil
	cfg.ConnConfig.Fallbacks = nil
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool, armed, committed, unblock
}

func TestPollingCommitACKSurvivesPageCancellation(t *testing.T) {
	for _, decodedAPI := range []bool{false, true} {
		t.Run(fmt.Sprintf("decoded=%v", decodedAPI), func(t *testing.T) {
			r := newTimestampRig(t, 4, 0, 1)
			r.prime(t, 0, 3)
			pool, armed, committed, release := pollingCommitGate(t, r.master)
			defer release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := pool.Ping(ctx); err != nil {
				t.Fatal(err)
			}
			last := r.fragments[0][3]
			cursor := domain.Cursor{Revision: 1, Ordinal: 1}
			var logs bytes.Buffer
			master := usecase.Master{Store: repo.MasterStore{DB: pool}, Metrics: observability.New(), Measure: true,
				Logger: slog.New(slog.NewJSONHandler(&logs, nil))}
			wire, _ := json.Marshal(last)
			decoded, err := domain.DecodeEnvelope(wire)
			if err != nil {
				t.Fatal(err)
			}
			armed.Store(true)
			type outcome struct {
				receipt usecase.Receipt
				err     error
			}
			done := make(chan outcome, 1)
			go func() {
				if decodedAPI {
					receipt, err := master.AcceptAtEnvelope(ctx, decoded, cursor)
					done <- outcome{receipt, err}
				} else {
					receipt, err := master.AcceptAt(ctx, wire, cursor)
					done <- outcome{receipt, err}
				}
			}()
			select {
			case <-committed:
			case got := <-done:
				t.Fatal("did not reach real COMMIT", got.err)
			case <-time.After(5 * time.Second):
				t.Fatal("server did not COMMIT")
			}
			cancel() // Expire the page while its successful COMMIT ACK is in flight.
			r.counts(t, 0, 4, 4, 1)
			if got, err := r.store().PollCursor(context.Background(), last.ExecutionID, last.GroupID); err != nil || got != cursor {
				t.Fatal("server COMMIT did not persist cursor", got, err)
			}
			select {
			case got := <-done:
				t.Fatalf("page cancellation lost an already durable COMMIT confirmation: %v; measurement=%q", got.err, logs.String())
			case <-time.After(100 * time.Millisecond): // ACK remains deliberately withheld.
			}
			releasedAt := time.Now()
			release()
			select {
			case got := <-done:
				if got.err != nil || !got.receipt.Accepted {
					t.Fatal("successful original ACK lost", got.receipt, got.err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("COMMIT completion did not finish")
			}
			var observation struct {
				MessageID string    `json:"message_id"`
				Committed time.Time `json:"committed_at"`
				Accepted  bool      `json:"accepted"`
			}
			if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &observation); err != nil || !observation.Accepted || observation.MessageID != last.MessageID || observation.Committed.Before(releasedAt) {
				t.Fatal("original ACK was not measured at confirmation", observation, err)
			}
			original := append([]byte(nil), logs.Bytes()...)
			master.Store = r.store()
			if receipt, err := master.AcceptAtEnvelope(context.Background(), decoded, cursor); err != nil || receipt.Accepted {
				t.Fatal("retry was not deduplicated", receipt, err)
			}
			r.counts(t, 0, 4, 4, 1)
			if !bytes.HasPrefix(logs.Bytes(), original) {
				t.Fatal("retry replaced original committed_at")
			}
		})
	}
}

type cancelPollingBeforeCommit struct{ cancel context.CancelFunc }
type pollingCursorKey struct{}

func (c cancelPollingBeforeCommit) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, pollingCursorKey{}, strings.HasPrefix(data.SQL, "SELECT cad_v2.advance_poll_cursor("))
}
func (c cancelPollingBeforeCommit) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if cursor, _ := ctx.Value(pollingCursorKey{}).(bool); cursor && data.Err == nil {
		c.cancel()
	}
}

func TestPollingCancellationBeforeCommitRollsBackResultAndCursor(t *testing.T) {
	r := newTimestampRig(t, 4, 0, 1)
	r.prime(t, 0, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := r.master.Config()
	cfg.ConnConfig.Tracer = cancelPollingBeforeCommit{cancel: cancel}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	last := r.fragments[0][3]
	cursor := domain.Cursor{Revision: 1, Ordinal: 1}
	var logs bytes.Buffer
	master := usecase.Master{Store: repo.MasterStore{DB: pool}, Metrics: observability.New(), Measure: true, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}
	wire, _ := json.Marshal(last)
	if _, err := master.AcceptAt(ctx, wire, cursor); err == nil {
		t.Fatal("canceled pre-COMMIT transaction accepted")
	}
	r.counts(t, 0, 3, 0, 0)
	if got, err := r.store().PollCursor(context.Background(), last.ExecutionID, last.GroupID); err != nil || got != (domain.Cursor{}) {
		t.Fatal("rollback advanced cursor", got, err)
	}
	if strings.Contains(logs.String(), "result measurement") {
		t.Fatal("rollback fabricated confirmation", logs.String())
	}
	master.Store = r.store()
	if receipt, err := master.AcceptAt(context.Background(), wire, cursor); err != nil || !receipt.Accepted {
		t.Fatal(receipt, err)
	}
	if receipt, err := master.AcceptAt(context.Background(), wire, cursor); err != nil || receipt.Accepted {
		t.Fatal("duplicate retry", receipt, err)
	}
	r.counts(t, 0, 4, 4, 1)
}

func TestPollingMissingCommitACKDoesNotInventMeasurement(t *testing.T) {
	r := newTimestampRig(t, 4, 0, 1)
	r.prime(t, 0, 3)
	pool, armed, dropped := lostCommitPool(t, r.master)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	last := r.fragments[0][3]
	cursor := domain.Cursor{Revision: 1, Ordinal: 1}
	var logs bytes.Buffer
	master := usecase.Master{Store: repo.MasterStore{DB: pool}, Metrics: observability.New(), Measure: true,
		Logger: slog.New(slog.NewJSONHandler(&logs, nil))}
	wire, _ := json.Marshal(last)
	decoded, err := domain.DecodeEnvelope(wire)
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	if _, err := master.AcceptAtEnvelope(ctx, decoded, cursor); err == nil || !dropped.Load() {
		t.Fatal("a genuinely missing COMMIT ACK was reported as success", err)
	}
	r.counts(t, 0, 4, 4, 1)
	if got, err := r.store().PollCursor(ctx, last.ExecutionID, last.GroupID); err != nil || got != cursor {
		t.Fatal("lost ACK did not leave durable cursor", got, err)
	}
	if logs.Len() != 0 {
		t.Fatal("cursor existence fabricated committed_at", logs.String())
	}
	master.Store = r.store()
	if receipt, err := master.AcceptAtEnvelope(ctx, decoded, cursor); err != nil || receipt.Accepted {
		t.Fatal("lost ACK retry was not deduplicated", receipt, err)
	}
	var duplicate struct {
		Accepted bool `json:"accepted"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &duplicate); err != nil || duplicate.Accepted {
		t.Fatal("retry claimed the original acceptance", duplicate, err)
	}
	r.counts(t, 0, 4, 4, 1)
}

func TestPollingCommitACKWaitIsBounded(t *testing.T) {
	r := newTimestampRig(t, 4, 0, 1)
	r.prime(t, 0, 3)
	pool, armed, committed, release := pollingCommitGate(t, r.master)
	defer release()
	ctx := context.Background()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	last := r.fragments[0][3]
	var logs bytes.Buffer
	master := usecase.Master{Store: repo.MasterStore{DB: pool}, Metrics: observability.New(), Measure: true, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}
	wire, _ := json.Marshal(last)
	armed.Store(true)
	done := make(chan error, 1)
	go func() { _, err := master.AcceptAt(ctx, wire, domain.Cursor{Revision: 1, Ordinal: 1}); done <- err }()
	select {
	case <-committed:
	case err := <-done:
		t.Fatal("never reached COMMIT", err)
	case <-time.After(5 * time.Second):
		t.Fatal("server never committed")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("withheld ACK fabricated a confirmation")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("detached COMMIT wait is unbounded")
	}
	r.counts(t, 0, 4, 4, 1)
	if logs.Len() != 0 {
		t.Fatal("timeout fabricated committed_at", logs.String())
	}
}
