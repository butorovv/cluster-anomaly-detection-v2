package postgres_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func finalFor(t *testing.T, r *timestampRig, tick, g int) domain.Envelope {
	t.Helper()
	rows := make([]group.Raw, len(r.fragments[tick]))
	payloads := make([]domain.GroupResult, len(rows))
	for i, e := range r.fragments[tick] {
		if err := json.Unmarshal(e.Payload, &payloads[i]); err != nil {
			t.Fatal(err)
		}
		rows[i] = payloads[i].Raw
	}
	group.AnnotateOutside(rows)
	p := payloads[g]
	p.Raw = rows[g]
	if p.Evidence != nil {
		p.Evidence.Raw = rows[g]
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	e := r.fragments[tick][g]
	e.SchemaVersion = 1
	e.Kind = "group_result"
	e.Terminal = true
	e.Payload = b
	e.MessageID = domain.MessageID(e.RunID, e.GroupID, e.Kind, e.EventTime.UTC().Format(time.RFC3339), 1)
	return e
}

func TestTimestampSpellingsShareFragmentAndFinalGuard(t *testing.T) {
	for _, kind := range []string{"tick_fragment", "group_result"} {
		t.Run(kind, func(t *testing.T) {
			r := newTimestampRig(t, 4, 0, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			guard, err := r.master.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				t.Fatal(err)
			}
			defer guard.Rollback(context.Background())
			var pid uint32
			if err := guard.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
				t.Fatal(err)
			}
			b, _ := json.Marshal(r.fragments[0][0])
			var accepted bool
			if err := guard.QueryRow(ctx, `SELECT cad_v2.accept_tick_fragment($1::uuid,$2::jsonb)`, r.registration.Binding.ExecutionID, b).Scan(&accepted); err != nil || !accepted {
				t.Fatal(accepted, err)
			}
			e := r.fragments[0][1]
			function := "cad_v2.accept_tick_fragment"
			if kind == "group_result" {
				e = finalFor(t, r, 0, 1)
				function = "cad_v2.accept_result"
			}
			b, _ = json.Marshal(e)
			result := make(chan error, 1)
			go func() {
				tx, err := r.master.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
				if err != nil {
					result <- err
					return
				}
				defer tx.Rollback(context.Background())
				if _, err = tx.Exec(ctx, `SET LOCAL TimeZone='Asia/Tokyo'`); err == nil {
					err = tx.QueryRow(ctx, "SELECT "+function+"($1::uuid,jsonb_set($2::jsonb,'{event_time}',to_jsonb($3::text)))", e.ExecutionID, b, e.EventTime.UTC().Format("2006-01-02T15:04:05.000000Z")).Scan(&accepted)
				}
				if err == nil {
					err = tx.Commit(ctx)
				}
				result <- err
			}()
			waitBlockedBy(t, ctx, r.admin, pid)
			if err := guard.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			waitResult(t, ctx, result)
		})
	}
}

func TestEventRevisionsProgressDuringDiagnosticFinalJoin(t *testing.T) {
	r := newTimestampRig(t, 4, 0, 3)
	if len(r.events) == 0 {
		t.Fatal("fixture has no terminal events")
	}
	r.prime(t, 0, 3)
	r.prime(t, 2, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b := &finalInsertBarrier{ready: make(chan struct{}), release: make(chan struct{})}
	traced := barrierPool(t, r.master, b)
	var release sync.Once
	defer release.Do(func() { close(b.release) })
	held := make(chan error, 1)
	go func() { _, err := (repo.MasterStore{DB: traced}).Accept(ctx, r.fragments[0][3]); held <- err }()
	select {
	case <-b.ready:
	case err := <-held:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	done := make(chan error, 4)
	for _, revision := range []int64{3, 1, 2} {
		e := r.events[0]
		e.Revision = revision
		e.Terminal = revision == 3
		e.MessageID = domain.MessageID(e.RunID, e.GroupID, e.Kind, e.EventID, revision)
		if !e.Terminal {
			var payload map[string]json.RawMessage
			if err := json.Unmarshal(e.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			delete(payload, "closed")
			e.Payload, _ = json.Marshal(payload)
		}
		go func(e domain.Envelope) { _, err := r.store().Accept(ctx, e); done <- err }(e)
	}
	go func() { _, err := r.store().Accept(ctx, r.fragments[2][3]); done <- err }()
	for i := 0; i < 4; i++ {
		waitResult(t, ctx, done)
	}
	var revision int64
	var terminal bool
	if err := r.admin.QueryRow(ctx, `SELECT revision,terminal FROM cad_v2.master_events WHERE execution_id=$1::uuid AND event_id=$2`, r.events[0].ExecutionID, r.events[0].EventID).Scan(&revision, &terminal); err != nil || revision != 3 || !terminal {
		t.Fatal(revision, terminal, err)
	}
	r.counts(t, 0, 3, 0, 0)
	r.counts(t, 2, 4, 4, 1)
	release.Do(func() { close(b.release) })
	waitResult(t, ctx, held)
}

func TestFragmentPollingCursorRollsBackWithFinalFailure(t *testing.T) {
	r := newTimestampRig(t, 4, 0, 1)
	r.prime(t, 0, 3)
	ctx := context.Background()
	_, err := r.admin.Exec(ctx, `CREATE FUNCTION public.p15_fail_final() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.group_id='a02' THEN RAISE EXCEPTION 'p15 final failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER p15_fail_final BEFORE INSERT ON cad_v2.master_results FOR EACH ROW EXECUTE FUNCTION public.p15_fail_final()`)
	if err != nil {
		t.Fatal(err)
	}
	last := r.fragments[0][3]
	cursor := domain.Cursor{Revision: 1, Ordinal: 1}
	if _, err := r.store().AcceptAt(ctx, last, cursor); err == nil {
		t.Fatal("failed final accepted")
	}
	r.counts(t, 0, 3, 0, 0)
	if got, err := r.store().PollCursor(ctx, last.ExecutionID, last.GroupID); err != nil || got != (domain.Cursor{}) {
		t.Fatal("failed join moved polling cursor", got, err)
	}
	if _, err := r.admin.Exec(ctx, `DROP TRIGGER p15_fail_final ON cad_v2.master_results; DROP FUNCTION public.p15_fail_final()`); err != nil {
		t.Fatal(err)
	}
	if ok, err := r.store().AcceptAt(ctx, last, cursor); err != nil || !ok {
		t.Fatal(ok, err)
	}
	// Lost delivery receipt, a fresh pool, and an older cursor cannot regress progress.
	restarted, err := pgxpool.NewWithConfig(ctx, r.master.Config())
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	store := repo.MasterStore{DB: restarted}
	if ok, err := store.AcceptAt(ctx, last, domain.Cursor{Revision: 1, Ordinal: 2}); err != nil || ok {
		t.Fatal("lost receipt retry", ok, err)
	}
	if _, err := store.AcceptAt(ctx, last, cursor); err != nil {
		t.Fatal(err)
	}
	if got, err := store.PollCursor(ctx, last.ExecutionID, last.GroupID); err != nil || got != (domain.Cursor{Revision: 1, Ordinal: 2}) {
		t.Fatal("cursor regression", got, err)
	}
	r.counts(t, 0, 4, 4, 1)
	for _, alternate := range []bool{false, true} {
		conflict := last
		if alternate {
			conflict.MessageID = strings.Repeat("f", 64)
		} else {
			var p map[string]json.RawMessage
			_ = json.Unmarshal(conflict.Payload, &p)
			p["conflict"] = json.RawMessage("true")
			conflict.Payload, _ = json.Marshal(p)
		}
		_, err := store.Accept(ctx, conflict)
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "23505" {
			t.Fatal("conflicting retry not rejected", alternate, err)
		}
	}
}

func TestMasterBackendCrashRollsBackFinalAndKafkaOffset(t *testing.T) {
	r := newTimestampRig(t, 4, 0, 1)
	r.prime(t, 0, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.store().BindResults(ctx, r.registration.Binding.ExecutionID, "p15", "p15-results", "p15-master", r.registration.StartOffsets); err != nil {
		t.Fatal(err)
	}
	b := &finalInsertBarrier{ready: make(chan struct{}), release: make(chan struct{})}
	traced := barrierPool(t, r.master, b)
	var release sync.Once
	defer release.Do(func() { close(b.release) })
	done := make(chan error, 1)
	last := r.fragments[0][3]
	go func() {
		_, err := (repo.MasterStore{DB: traced}).AcceptKafka(ctx, last.ExecutionID, "p15-results", 3, 0, &last)
		done <- err
	}()
	select {
	case <-b.ready:
	case err := <-done:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var killed bool
	if err := r.admin.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, b.pid).Scan(&killed); err != nil || !killed {
		t.Fatal(killed, err)
	}
	release.Do(func() { close(b.release) })
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("crashed transaction reported success")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	r.counts(t, 0, 3, 0, 0)
	if offset, err := r.store().ResultOffset(ctx, last.ExecutionID, 3); err != nil || offset != 0 {
		t.Fatal(offset, err)
	}
	if ok, err := r.store().AcceptKafka(ctx, last.ExecutionID, "p15-results", 3, 0, &last); err != nil || !ok {
		t.Fatal(ok, err)
	}
	r.counts(t, 0, 4, 4, 1)
}

// A test-only PostgreSQL wire proxy drops the server's successful COMMIT reply.
// The server really commits; the client receives a network error, not a mocked Tx.
func lostCommitPool(t *testing.T, source *pgxpool.Pool) (*pgxpool.Pool, *atomic.Bool, *atomic.Bool) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	armed, dropped := &atomic.Bool{}, &atomic.Bool{}
	var workers sync.WaitGroup
	var connections sync.Map
	acceptDone := make(chan struct{})
	t.Cleanup(func() {
		_ = listener.Close()
		<-acceptDone
		connections.Range(func(k, v any) bool { _ = k.(net.Conn).Close(); return true })
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
						dropped.Store(true)
						return
					}
					if _, err := client.Write(append(header, body...)); err != nil {
						return
					}
				}
			}()
		}
	}()
	addr := listener.Addr().(*net.TCPAddr)
	cfg.ConnConfig.Host = "127.0.0.1"
	cfg.ConnConfig.Port = uint16(addr.Port)
	cfg.ConnConfig.TLSConfig = nil
	cfg.ConnConfig.Fallbacks = nil
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool, armed, dropped
}

func TestMasterLostCommitACKPreservesFinalAndProgress(t *testing.T) {
	for _, mode := range []string{"push", "polling", "kafka"} {
		t.Run(mode, func(t *testing.T) {
			r := newTimestampRig(t, 4, 0, 1)
			r.prime(t, 0, 3)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			last := r.fragments[0][3]
			if mode == "kafka" {
				if err := r.store().BindResults(ctx, last.ExecutionID, "p15", "p15-results", "p15-master", r.registration.StartOffsets); err != nil {
					t.Fatal(err)
				}
			}
			pool, armed, dropped := lostCommitPool(t, r.master)
			if err := pool.Ping(ctx); err != nil {
				t.Fatal(err)
			}
			armed.Store(true)
			call := func(s repo.MasterStore) (bool, error) {
				switch mode {
				case "polling":
					return s.AcceptAt(ctx, last, domain.Cursor{Revision: 1, Ordinal: 1})
				case "kafka":
					return s.AcceptKafka(ctx, last.ExecutionID, "p15-results", 3, 0, &last)
				default:
					return s.Accept(ctx, last)
				}
			}
			if _, err := call(repo.MasterStore{DB: pool}); err == nil || !dropped.Load() {
				t.Fatal("COMMIT reply was not lost", err, dropped.Load())
			}
			r.counts(t, 0, 4, 4, 1)
			if mode == "kafka" {
				if offset, err := r.store().ResultOffset(ctx, last.ExecutionID, 3); err != nil || offset != 1 {
					t.Fatal(offset, err)
				}
				// A restarted consumer loads SQL offset=1. An actual duplicate at the
				// next broker offset is idempotent and advances that position atomically.
				if ok, err := r.store().AcceptKafka(ctx, last.ExecutionID, "p15-results", 3, 1, &last); err != nil || ok {
					t.Fatal(ok, err)
				}
			} else {
				if ok, err := call(r.store()); err != nil || ok {
					t.Fatal("lost ACK retry", ok, err)
				}
			}
			if mode == "polling" {
				if got, err := r.store().PollCursor(ctx, last.ExecutionID, last.GroupID); err != nil || got != (domain.Cursor{Revision: 1, Ordinal: 1}) {
					t.Fatal(got, err)
				}
			}
			r.counts(t, 0, 4, 4, 1)
		})
	}
}
