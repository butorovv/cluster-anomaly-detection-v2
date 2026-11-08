package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"testing"
	"time"
)

// Regression for the historical FK-upgrade deadlock. An external UPDATE lock
// deliberately blocks final FK checks; V9 never upgrades those FK KEY SHAREs.
func TestConcurrentTimestampJoinLockOrder(t *testing.T) {
	pool := postgrestest.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := workerfixture.New(t, 4, 0)
	f.Options.Groups = 4
	first, err := group.NewGroupProcessor(f.Bundle, f.Options)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := first.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r := checkpoints.Registration{Identity: snapshot.Identity, Binding: f.Options.Binding, ClusterID: "join-lock", TopicID: "join-lock", ConsumerGroup: "join-lock", OutputTransport: "http_push", Diagnostics: true, StartOffsets: map[string]int64{}}
	for p := 0; p < 16; p++ {
		r.StartOffsets[fmt.Sprint(p)] = 0
	}
	if err := checkpoints.Register(ctx, pool, r); err != nil {
		t.Fatal(err)
	}
	workerRole, masterRole := fmt.Sprintf("p10w_%d", time.Now().UnixNano()), fmt.Sprintf("p10m_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, "CREATE ROLE "+pgx.Identifier{workerRole}.Sanitize()+" NOLOGIN; CREATE ROLE "+pgx.Identifier{masterRole}.Sanitize()+" NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	connection, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = db.GrantRuntime(ctx, connection.Conn(), workerRole, masterRole)
	connection.Release()
	if err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Exec(ctx, "SET ROLE "+pgx.Identifier{masterRole}.Sanitize())
		return err
	}
	restricted, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		restricted.Close()
		for _, role := range []string{workerRole, masterRole} {
			quoted := pgx.Identifier{role}.Sanitize()
			if _, err := pool.Exec(context.Background(), "DROP OWNED BY "+quoted+";DROP ROLE "+quoted); err != nil {
				t.Error(err)
			}
		}
	})
	if _, err := restricted.Exec(ctx, `SELECT 1 FROM cad_v2.execution_partitions FOR UPDATE`); err == nil {
		t.Fatal("Master unexpectedly has direct UPDATE privileges")
	}
	service := usecase.Master{Store: repo.MasterStore{DB: restricted}}
	finals := [][]byte{}
	for g := 1; g <= 4; g++ {
		options := f.Options
		options.GroupID = fmt.Sprintf("a%02d", g)
		p, err := group.NewGroupProcessor(f.Bundle, options)
		if err != nil {
			t.Fatal(err)
		}
		for tick := 0; tick < 4; tick++ {
			missing := []string{}
			for h := 1; h <= 18; h++ {
				missing = append(missing, fmt.Sprintf("%sn%02d", options.GroupID, h))
			}
			result, err := p.Step(telemetry.GroupTick{Binding: options.Binding, GroupID: options.GroupID, Timestamp: options.Window.Start.Add(time.Duration(tick) * telemetry.GridStep), Records: map[string]telemetry.TelemetryRecord{}, Missing: missing})
			if err != nil {
				t.Fatal(err)
			}
			fragment, err := domain.TickFragment(options.Binding.ExecutionID, snapshot.Identity, result)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(fragment)
			if err != nil {
				t.Fatal(err)
			}
			if g < 4 {
				if _, err := service.Accept(ctx, raw); err != nil {
					t.Fatal(err)
				}
			} else {
				finals = append(finals, raw)
			}
		}
	}
	guard, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Rollback(context.Background())
	if _, err := guard.Exec(ctx, `SELECT 1 FROM cad_v2.execution_partitions WHERE execution_id=$1::uuid AND group_id='a01' FOR UPDATE`, r.Binding.ExecutionID); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, len(finals))
	for _, raw := range finals {
		go func(raw []byte) { _, err := service.Accept(ctx, raw); results <- err }(raw)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var blocked int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid<>pg_backend_pid()`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked >= len(finals) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("concurrent join transactions did not reach lock barrier", blocked)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := guard.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for range finals {
		if err := <-results; err != nil {
			t.Errorf("concurrent timestamp join failed: %v", err)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_results`).Scan(&count); err != nil || count != 16 {
		t.Fatal("lost concurrent final diagnostics", count, err)
	}
}

// The barrier is AFTER the first real final INSERT, before its COMMIT. T2 intake
// and T3 finalization must COMMIT while that transaction is still held open.
func TestIndependentTimestampsCommitBeforeHeldFinalJoin(t *testing.T) {
	for _, groups := range []int{4, 8, 16} {
		for _, delta := range []int{0, 10, 30, 60} {
			t.Run(fmt.Sprintf("g%d-d%d", groups, delta), func(t *testing.T) {
				r := newTimestampRig(t, groups, delta, 3)
				r.prime(t, 0, groups-1)
				r.prime(t, 2, groups-1)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				barrier := &finalInsertBarrier{ready: make(chan struct{}), release: make(chan struct{})}
				traced := barrierPool(t, r.master, barrier)
				var release sync.Once
				defer release.Do(func() { close(barrier.release) })
				t1 := make(chan error, 1)
				go func() { _, err := (repo.MasterStore{DB: traced}).Accept(ctx, r.fragments[0][groups-1]); t1 <- err }()
				select {
				case <-barrier.ready:
				case err := <-t1:
					t.Fatal("T1 did not reach final INSERT", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				t2, t3 := make(chan error, 1), make(chan error, 1)
				go func() { _, err := r.store().Accept(ctx, r.fragments[1][0]); t2 <- err }()
				go func() { _, err := r.store().Accept(ctx, r.fragments[2][groups-1]); t3 <- err }()
				waitResult(t, ctx, t2)
				waitResult(t, ctx, t3)
				r.counts(t, 0, groups-1, 0, 0)
				r.counts(t, 1, 1, 0, 0)
				r.counts(t, 2, groups, groups, 1)
				select {
				case err := <-t1:
					t.Fatal("T1 escaped barrier", err)
				default:
				}
				release.Do(func() { close(barrier.release) })
				waitResult(t, ctx, t1)
				r.counts(t, 0, groups, groups, 1)
			})
		}
	}
}

func TestConcurrentFinalArrivalsObserveCommittedPredecessor(t *testing.T) {
	for _, groups := range []int{4, 8, 16} {
		t.Run(fmt.Sprint(groups), func(t *testing.T) {
			r := newTimestampRig(t, groups, 30, 1)
			r.prime(t, 0, groups-2)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			guard, err := r.master.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				t.Fatal(err)
			}
			defer guard.Rollback(context.Background())
			// The penultimate arrival is real and uncommitted while the last waits.
			var pid uint32
			if err := guard.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
				t.Fatal(err)
			}
			b, _ := json.Marshal(r.fragments[0][groups-2])
			var accepted bool
			if err := guard.QueryRow(ctx, `SELECT cad_v2.accept_tick_fragment($1::uuid,$2::jsonb)`, r.registration.Binding.ExecutionID, b).Scan(&accepted); err != nil || !accepted {
				t.Fatal(accepted, err)
			}
			last := make(chan error, 1)
			go func() { _, err := r.store().Accept(ctx, r.fragments[0][groups-1]); last <- err }()
			waitBlockedBy(t, ctx, r.admin, pid)
			if err := guard.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			waitResult(t, ctx, last)
			r.counts(t, 0, groups, groups, 1)
			// Concurrent duplicate final arrivals must not add another final set.
			duplicates := make(chan error, 2)
			for i := 0; i < 2; i++ {
				go func() {
					ok, err := r.store().Accept(ctx, r.fragments[0][groups-1])
					if ok && err == nil {
						err = fmt.Errorf("duplicate accepted")
					}
					duplicates <- err
				}()
			}
			waitResult(t, ctx, duplicates)
			waitResult(t, ctx, duplicates)
			r.counts(t, 0, groups, groups, 1)
		})
	}
}
