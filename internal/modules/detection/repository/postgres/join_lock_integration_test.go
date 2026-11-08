package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	events "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	master "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type finalQueryKey struct{}

// This client-side barrier pauses after a real final INSERT, with its transaction
// still open. It adds no SQL lock and does not emulate relation-extension I/O.
type finalJoinBarrier struct {
	once           sync.Once
	ready, release chan struct{}
}

func (b *finalJoinBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, finalQueryKey{}, strings.HasPrefix(d.SQL, "SELECT cad_v2.accept_result("))
}

func (b *finalJoinBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	if final, _ := ctx.Value(finalQueryKey{}).(bool); final && d.Err == nil {
		b.once.Do(func() {
			close(b.ready)
			select {
			case <-b.release:
			case <-ctx.Done():
			}
		})
	}
}

func TestWorkerCheckpointDuringFinalJoin(t *testing.T) {
	for _, updateGuard := range []bool{true, false} {
		t.Run(fmt.Sprintf("update_guard_%v", updateGuard), func(t *testing.T) {
			pool, store, lease, _, candidate, f := diagnosticCandidate(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if updateGuard {
				// Install the real V7 function as a test-only negative control.
				// V9 contains no fragment identity lock to mechanically replace.
				v7, err := db.Files.ReadFile("migrations/V7__diagnostic_join.sql")
				if err != nil {
					t.Fatal(err)
				}
				definition := string(v7)
				definition = definition[strings.Index(definition, "CREATE FUNCTION cad_v2.accept_tick_fragment("):]
				definition = definition[:strings.Index(definition, "END $$;")+len("END $$;")]
				definition = strings.Replace(definition, "CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)
				if _, err := pool.Exec(ctx, definition); err != nil {
					t.Fatal(err)
				}
			}
			workerRuntime, masterRuntime := joinRuntimePools(t, pool)
			store.DB = workerRuntime
			service := usecase.Master{Store: master.MasterStore{DB: masterRuntime}}
			var last []byte
			for g := 1; g <= f.Options.Groups; g++ {
				result := candidate.Result
				if g > 1 {
					options := f.Options
					options.GroupID = fmt.Sprintf("a%02d", g)
					processor, err := group.NewGroupProcessor(f.Bundle, options)
					if err != nil {
						t.Fatal(err)
					}
					records := map[string]telemetry.TelemetryRecord{}
					for h := 1; h <= 18; h++ {
						r := workerfixture.Record(options.GroupID, h, options.Window.Start, 2)
						records[r.Hostname] = r
					}
					result, err = processor.Step(telemetry.GroupTick{Binding: options.Binding, GroupID: options.GroupID, Timestamp: options.Window.Start, Records: records, ValidRecords: 18})
					if err != nil {
						t.Fatal(err)
					}
				}
				e, err := events.TickFragment(store.Registration.Binding.ExecutionID, store.Registration.Identity, result)
				if err != nil {
					t.Fatal(err)
				}
				last, err = json.Marshal(e)
				if err != nil {
					t.Fatal(err)
				}
				if g < f.Options.Groups {
					if _, err := service.Accept(ctx, last); err != nil {
						t.Fatal(err)
					}
				}
			}
			barrier := &finalJoinBarrier{ready: make(chan struct{}), release: make(chan struct{})}
			cfg := masterRuntime.Config()
			cfg.ConnConfig.Tracer = barrier
			traced, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer traced.Close()
			// Always release before pool cleanup, including a failed assertion.
			var release sync.Once
			defer release.Do(func() { close(barrier.release) })
			completed := make(chan error, 1)
			go func() {
				_, err := (usecase.Master{Store: master.MasterStore{DB: traced}}).Accept(ctx, last)
				completed <- err
			}()
			select {
			case <-barrier.ready:
			case err := <-completed:
				t.Fatalf("join never reached final barrier: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var finals int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_results`).Scan(&finals); err != nil || finals != 0 {
				t.Fatal("uncommitted final visible", finals, err)
			}
			workerCfg := workerRuntime.Config()
			workerSetup := workerCfg.AfterConnect
			workerCfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
				if err := workerSetup(ctx, c); err != nil {
					return err
				}
				_, err := c.Exec(ctx, `SET lock_timeout='250ms'`)
				return err
			}
			worker, err := pgxpool.NewWithConfig(ctx, workerCfg)
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			concurrent := &checkpoints.Store{DB: worker, Registration: store.Registration}
			revision, saveErr := concurrent.Save(ctx, lease, *candidate)
			if updateGuard {
				var pgerr *pgconn.PgError
				if !errors.As(saveErr, &pgerr) || pgerr.Code != "55P03" {
					t.Fatalf("FOR UPDATE did not block actual checkpoint/fragment FK write: %v", saveErr)
				}
				var cp, outbox, fragments int
				if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM cad_v2.group_checkpoints),(SELECT count(*) FROM cad_v2.outbox_messages),(SELECT count(*) FROM cad_v2.worker_tick_fragments)`).Scan(&cp, &outbox, &fragments); err != nil || cp+outbox+fragments != 0 {
					t.Fatal("blocked checkpoint leaked durable state", cp, outbox, fragments, err)
				}
			} else if saveErr != nil || revision != 1 {
				t.Fatal("V9 blocked real Worker FK write during final join", revision, saveErr)
			}
			release.Do(func() { close(barrier.release) })
			if err := <-completed; err != nil {
				t.Fatal(err)
			}
			if updateGuard {
				if revision, err := concurrent.Save(ctx, lease, *candidate); err != nil || revision != 1 {
					t.Fatal("checkpoint retry after rollback failed", revision, err)
				}
			}
			saved, err := store.Load(ctx, lease.Group)
			if err != nil || saved == nil || saved.NextOffset != candidate.Checkpoint.NextOffset || saved.SHA256 != candidate.Checkpoint.SHA256 {
				t.Fatal("durable offset/snapshot changed", saved, err)
			}
			var exact bool
			fragment, err := events.TickFragment(store.Registration.Binding.ExecutionID, store.Registration.Identity, candidate.Result)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(fragment)
			if err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx, `SELECT envelope=$1::jsonb FROM cad_v2.worker_tick_fragments`, raw).Scan(&exact); err != nil || !exact {
				t.Fatal("Worker fragment scientific identity changed", exact, err)
			}
			var before, after string
			snapshotSQL := `SELECT jsonb_agg(to_jsonb(r) ORDER BY group_id)::text FROM cad_v2.master_results r`
			if err := pool.QueryRow(ctx, snapshotSQL).Scan(&before); err != nil {
				t.Fatal(err)
			}
			// Treat the successful Master COMMIT response as lost. Reconnect and
			// redeliver the last fragment; no RAM history is available to this pool.
			traced.Close()
			restarted, err := pgxpool.NewWithConfig(ctx, masterRuntime.Config())
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			receipt, err := (usecase.Master{Store: master.MasterStore{DB: restarted}}).Accept(ctx, last)
			if err != nil || receipt.Accepted {
				t.Fatal("lost-ACK retry after restart", receipt, err)
			}
			if err := restarted.QueryRow(ctx, snapshotSQL).Scan(&after); err != nil || before != after {
				t.Fatal("retry changed final payloads or MessageIDs", err)
			}
			var joins int
			if err := restarted.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_results`).Scan(&finals); err != nil || finals != f.Options.Groups {
				t.Fatal(finals, err)
			}
			if err := restarted.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_tick_joins`).Scan(&joins); err != nil || joins != 1 {
				t.Fatal(joins, err)
			}
		})
	}
}

func joinRuntimePools(t *testing.T, admin *pgxpool.Pool) (*pgxpool.Pool, *pgxpool.Pool) {
	t.Helper()
	w, m := fmt.Sprintf("p15jw_%d", time.Now().UnixNano()), fmt.Sprintf("p15jm_%d", time.Now().UnixNano())
	for _, role := range []string{w, m} {
		if _, err := admin.Exec(context.Background(), "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" NOLOGIN"); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, role := range []string{w, m} {
			q := pgx.Identifier{role}.Sanitize()
			if _, err := admin.Exec(context.Background(), "DROP OWNED BY "+q+";DROP ROLE "+q); err != nil {
				t.Error(err)
			}
		}
	})
	c, err := admin.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	err = db.GrantRuntime(context.Background(), c.Conn(), w, m)
	c.Release()
	if err != nil {
		t.Fatal(err)
	}
	makePool := func(role string) *pgxpool.Pool {
		cfg := admin.Config()
		cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
			_, err := c.Exec(ctx, "SET ROLE "+pgx.Identifier{role}.Sanitize())
			return err
		}
		p, err := pgxpool.NewWithConfig(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}
	return makePool(w), makePool(m)
}
