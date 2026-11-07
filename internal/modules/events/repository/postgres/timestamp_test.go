package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type timestampRig struct {
	admin, master, worker *pgxpool.Pool
	registration          checkpoints.Registration
	fragments             [][]domain.Envelope // tick, then group; one shared scientific fixture
	events                []domain.Envelope
	fixture               workerfixture.Fixture
}

func newTimestampRig(t *testing.T, groups, delta, ticks int, frozen ...*workerfixture.Fixture) *timestampRig {
	t.Helper()
	admin := postgrestest.New(t)
	var f workerfixture.Fixture
	if len(frozen) > 0 {
		f = *frozen[0]
	} else {
		f = workerfixture.New(t, ticks, delta)
	}
	f.Options.Groups = groups
	r := &timestampRig{admin: admin, fragments: make([][]domain.Envelope, ticks), fixture: f}
	for tick := range r.fragments {
		r.fragments[tick] = make([]domain.Envelope, groups)
	}
	for g := 0; g < groups; g++ {
		opts := f.Options
		opts.GroupID = fmt.Sprintf("a%02d", g+1)
		processor, err := group.NewGroupProcessor(f.Bundle, opts)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := processor.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if g == 0 {
			r.registration = checkpoints.Registration{Identity: snapshot.Identity, Binding: opts.Binding, ClusterID: "p15", TopicID: "p15-input", ConsumerGroup: "p15", OutputTransport: "polling", Diagnostics: true, StartOffsets: map[string]int64{}}
			for p := 0; p < 16; p++ {
				r.registration.StartOffsets[fmt.Sprint(p)] = 0
			}
			if err := checkpoints.Register(context.Background(), admin, r.registration); err != nil {
				t.Fatal(err)
			}
		}
		for tick := 0; tick < ticks; tick++ {
			at := opts.Window.Start.Add(time.Duration(tick) * telemetry.GridStep)
			count := []int{18, 17, 0}[tick%3]
			records := map[string]telemetry.TelemetryRecord{}
			missing := []string{}
			for h := 1; h <= 18; h++ {
				record := workerfixture.Record(opts.GroupID, h, at, 2)
				if h <= count {
					records[record.Hostname] = record
				} else {
					missing = append(missing, record.Hostname)
				}
			}
			result, err := processor.Step(telemetry.GroupTick{Binding: opts.Binding, GroupID: opts.GroupID, Timestamp: at, Records: records, Missing: missing, ValidRecords: count})
			if err != nil {
				t.Fatal(err)
			}
			outputs, err := core.TerminalEvents(opts.Binding.ExecutionID, result.Events)
			if err != nil {
				t.Fatal(err)
			}
			for _, output := range outputs {
				b, err := json.Marshal(output)
				if err != nil {
					t.Fatal(err)
				}
				e, err := domain.Decode(b)
				if err != nil {
					t.Fatal(err)
				}
				r.events = append(r.events, e)
			}
			r.fragments[tick][g], err = domain.TickFragment(opts.Binding.ExecutionID, snapshot.Identity, result)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	workerRole, masterRole := fmt.Sprintf("p15w_%d", time.Now().UnixNano()), fmt.Sprintf("p15m_%d", time.Now().UnixNano())
	for _, role := range []string{workerRole, masterRole} {
		if _, err := admin.Exec(context.Background(), "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" NOLOGIN"); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, role := range []string{workerRole, masterRole} {
			quoted := pgx.Identifier{role}.Sanitize()
			if _, err := admin.Exec(context.Background(), "DROP OWNED BY "+quoted+"; DROP ROLE "+quoted); err != nil {
				t.Error(err)
			}
		}
	})
	c, err := admin.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	err = db.GrantRuntime(context.Background(), c.Conn(), workerRole, masterRole)
	c.Release()
	if err != nil {
		t.Fatal(err)
	}
	rolePool := func(role string) *pgxpool.Pool {
		cfg := admin.Config()
		cfg.MaxConns = 6
		cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
			_, err := c.Exec(ctx, "SET ROLE "+pgx.Identifier{role}.Sanitize()+"; SET statement_timeout='8000'; SET lock_timeout='5000'; SET default_transaction_isolation='repeatable read'")
			return err
		}
		pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	r.master, r.worker = rolePool(masterRole), rolePool(workerRole)
	var canUpdate, canLock bool
	if err := r.master.QueryRow(context.Background(), `SELECT has_table_privilege(current_user,'cad_v2.execution_partitions','UPDATE'),has_function_privilege(current_user,'cad_v2.lock_master_tick(uuid,timestamptz)','EXECUTE')`).Scan(&canUpdate, &canLock); err != nil || canUpdate || canLock {
		t.Fatal("runtime Master gained identity UPDATE/private helper privileges", canUpdate, canLock, err)
	}
	return r
}

func (r *timestampRig) store() repo.MasterStore { return repo.MasterStore{DB: r.master} }
func (r *timestampRig) prime(t *testing.T, tick, count int) {
	t.Helper()
	for _, e := range r.fragments[tick][:count] {
		if accepted, err := r.store().Accept(context.Background(), e); err != nil || !accepted {
			t.Fatal("prime", accepted, err)
		}
	}
}
func (r *timestampRig) counts(t *testing.T, tick, fragments, finals, joins int) {
	t.Helper()
	var f, z, j int
	err := r.admin.QueryRow(context.Background(), `SELECT
 (SELECT count(*) FROM cad_v2.master_tick_fragments WHERE execution_id=$1::uuid AND event_time=$2),
 (SELECT count(*) FROM cad_v2.master_results WHERE execution_id=$1::uuid AND tick=$2),
 (SELECT count(*) FROM cad_v2.master_tick_joins WHERE execution_id=$1::uuid AND event_time=$2)`, r.registration.Binding.ExecutionID, r.fragments[tick][0].EventTime).Scan(&f, &z, &j)
	if err != nil || f != fragments || z != finals || j != joins {
		t.Fatalf("tick%d counts=%d/%d/%d want=%d/%d/%d err=%v", tick, f, z, j, fragments, finals, joins, err)
	}
}

type finalInsertKey struct{}
type finalInsertBarrier struct {
	once           sync.Once
	ready, release chan struct{}
	pid            uint32 // published by closing ready
}

func (b *finalInsertBarrier) TraceQueryStart(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, finalInsertKey{}, strings.HasPrefix(d.SQL, "SELECT cad_v2.accept_result("))
}
func (b *finalInsertBarrier) TraceQueryEnd(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryEndData) {
	if final, _ := ctx.Value(finalInsertKey{}).(bool); final && d.Err == nil {
		b.once.Do(func() {
			b.pid = c.PgConn().PID()
			close(b.ready)
			select {
			case <-b.release:
			case <-ctx.Done():
			}
		})
	}
}
func barrierPool(t *testing.T, pool *pgxpool.Pool, b *finalInsertBarrier) *pgxpool.Pool {
	t.Helper()
	cfg := pool.Config()
	cfg.ConnConfig.Tracer = b
	out, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(out.Close)
	return out
}
func waitResult(t *testing.T, ctx context.Context, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
func waitBlockedBy(t *testing.T, ctx context.Context, pool *pgxpool.Pool, blocker uint32) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND $1::integer=ANY(pg_blocking_pids(pid))`, blocker).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("competitor never reached actual lock wait", ctx.Err())
		}
	}
}
