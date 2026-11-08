package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5/pgxpool"
)

func repoStore(pool *pgxpool.Pool) repo.MasterStore { return repo.MasterStore{DB: pool} }

func installV8Control(t *testing.T, r *timestampRig) {
	t.Helper()
	b, err := db.Files.ReadFile("migrations/V8__diagnostic_join_fk_lock.sql")
	if err != nil {
		t.Fatal(err)
	}
	// Test-only function overlay on a disposable DB. Tables/constraints are the
	// same in V8/V9; no migration bytes or ledger entries are rewritten.
	if _, err := r.admin.Exec(context.Background(), string(b)); err != nil {
		t.Fatal(err)
	}
}

func TestV8ControlBlocksIndependentTimestamps(t *testing.T) {
	r := newTimestampRig(t, 4, 0, 3)
	installV8Control(t, r)
	r.prime(t, 0, 3)
	r.prime(t, 2, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b := &finalInsertBarrier{ready: make(chan struct{}), release: make(chan struct{})}
	traced := barrierPool(t, r.master, b)
	var release sync.Once
	defer release.Do(func() { close(b.release) })
	held := make(chan error, 1)
	go func() { _, err := (repoStore(traced)).Accept(ctx, r.fragments[0][3]); held <- err }()
	select {
	case <-b.ready:
	case err := <-held:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitCtx, stop := context.WithCancel(ctx)
	defer stop()
	blocked := make(chan error, 2)
	go func() { _, err := r.store().Accept(waitCtx, r.fragments[1][0]); blocked <- err }()
	go func() { _, err := r.store().Accept(waitCtx, r.fragments[2][3]); blocked <- err }()
	waitBlockedBy(t, ctx, r.admin, b.pid)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var n int
		// A queued tuple lock can make the second waiter report the first
		// waiter as its blocker rather than T1. Both must actually be waiting.
		if err := r.admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND cardinality(pg_blocking_pids(pid))>0`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n >= 2 {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("V8 negative control did not block both independent transactions")
		}
	}
	r.counts(t, 1, 0, 0, 0)
	r.counts(t, 2, 3, 0, 0)
	stop()
	for i := 0; i < 2; i++ {
		select {
		case err := <-blocked:
			if err == nil {
				t.Fatal("V8 independent transaction escaped held guard")
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	release.Do(func() { close(b.release) })
	waitResult(t, ctx, held)
}

type sqlQuantiles struct {
	N             int `json:"n"`
	P50, P95, P99 float64
}

func quantiles(v []float64) sqlQuantiles {
	sort.Float64s(v)
	q := func(p float64) float64 {
		i := int(float64(len(v)) * p)
		if float64(i) < float64(len(v))*p {
			i++
		}
		return v[max(0, i-1)] * 1000
	}
	return sqlQuantiles{len(v), q(.5), q(.95), q(.99)}
}

// Opt-in, small synthetic SQL-only experiment. Both variants run identical Go,
// fixed scientific fixture and runtime roles, with V8/V9 SQL function bodies.
func TestTimestampSQLMeasurements(t *testing.T) {
	if os.Getenv("P15_SQL_MEASURE") != "1" {
		t.Skip("P15_SQL_MEASURE=1 for bounded V8/V9 SQL measurements")
	}
	const ticks, warm, groups = 28, 4, 16
	frozen := workerfixture.New(t, ticks, 0)
	var expectedDigest string
	for _, concurrency := range []int{1, 4} {
		for repeat := 0; repeat < 3; repeat++ {
			order := []int{8, 9}
			if repeat%2 == 1 {
				order = []int{9, 8}
			}
			for _, version := range order {
				t.Run(fmt.Sprintf("c%d-r%d-v%d", concurrency, repeat+1, version), func(t *testing.T) {
					r := newTimestampRig(t, groups, 0, ticks, &frozen)
					if version == 8 {
						installV8Control(t, r)
					}
					cfg := r.master.Config()
					cfg.MaxConns = 4
					pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer pool.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					connections := make([]*pgxpool.Conn, 4)
					for i := range connections {
						connections[i], err = pool.Acquire(ctx)
						if err != nil {
							t.Fatal(err)
						}
					}
					for _, c := range connections {
						c.Release()
					}
					store := repoStore(pool)
					for tick := 0; tick < warm; tick++ {
						for _, e := range r.fragments[tick] {
							if ok, err := store.Accept(ctx, e); err != nil || !ok {
								t.Fatal(ok, err)
							}
						}
					}
					var mu sync.Mutex
					fragments, finals := []float64{}, []float64{}
					var lockSamples, activitySamples int64
					observe, stop := context.WithCancel(ctx)
					observed := make(chan struct{})
					go func() {
						defer close(observed)
						ticker := time.NewTicker(5 * time.Millisecond)
						defer ticker.Stop()
						for {
							select {
							case <-observe.Done():
								return
							case <-ticker.C:
								var n int64
								if err := r.admin.QueryRow(observe, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'`).Scan(&n); err == nil {
									lockSamples += n
									activitySamples++
								}
							}
						}
					}()
					before := pool.Stat()
					start := time.Now()
					failures := make(chan error, ticks-warm)
					for base := warm; base < ticks; base += concurrency {
						var wg sync.WaitGroup
						for tick := base; tick < min(base+concurrency, ticks); tick++ {
							wg.Add(1)
							go func(tick int) {
								defer wg.Done()
								for g, e := range r.fragments[tick] {
									at := time.Now()
									ok, err := store.Accept(ctx, e)
									latency := time.Since(at).Seconds()
									if err != nil || !ok {
										failures <- fmt.Errorf("accept %v: %w", ok, err)
										return
									}
									mu.Lock()
									fragments = append(fragments, latency)
									if g == groups-1 {
										finals = append(finals, latency)
									}
									mu.Unlock()
								}
							}(tick)
						}
						wg.Wait()
					}
					elapsed := time.Since(start).Seconds()
					after := pool.Stat()
					stop()
					<-observed
					close(failures)
					for err := range failures {
						t.Fatal(err)
					}
					for tick := 0; tick < ticks; tick++ {
						r.counts(t, tick, groups, groups, 1)
					}
					var digest string
					if err := r.admin.QueryRow(ctx, `SELECT encode(sha256(convert_to(jsonb_agg(jsonb_build_array(group_id,tick,message_id,payload) ORDER BY tick,group_id)::text,'UTF8')),'hex') FROM cad_v2.master_results`).Scan(&digest); err != nil {
						t.Fatal(err)
					}
					if expectedDigest == "" {
						expectedDigest = digest
					} else if digest != expectedDigest {
						t.Fatal("V8/V9 final payload/MessageID digest mismatch", digest, expectedDigest)
					}
					data := map[string]any{"sql_version": version, "concurrency": concurrency, "repeat": repeat + 1, "groups": groups, "timestamps": ticks - warm, "fixture_run_id": r.registration.Identity.RunID, "elapsed_seconds": elapsed, "fragment_tx_per_second": float64((ticks-warm)*groups) / elapsed, "final_joins_per_second": float64(ticks-warm) / elapsed, "fragment_ms": quantiles(fragments), "last_fragment_ms": quantiles(finals), "pool_empty_wait_ms": float64(after.EmptyAcquireWaitTime()-before.EmptyAcquireWaitTime()) / float64(time.Millisecond), "pool_acquire_ms": float64(after.AcquireDuration()-before.AcquireDuration()) / float64(time.Millisecond), "lock_wait_backend_samples": lockSamples, "lock_sampling_rounds": activitySamples, "lock_timeout_errors": 0}
					data["final_payload_messageid_digest"] = digest
					b, err := json.Marshal(data)
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("p15_sql_measurement=%s", b)
				})
			}
		}
	}
}
