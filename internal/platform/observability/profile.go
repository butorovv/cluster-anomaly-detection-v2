package observability

import (
	"context"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Profiling is explicitly opt-in and separate from final throughput trials.
// Only loopback clients may inspect profiles, even on a public metrics listener.
func ProfilingEnabled() bool { return os.Getenv("CAD_PPROF") == "1" }

var profileOnce sync.Once

func profileHandler(next http.Handler) http.Handler {
	if !ProfilingEnabled() {
		return next
	}
	profileOnce.Do(func() {
		runtime.SetMutexProfileFraction(10)
		runtime.SetBlockProfileRate(1000000)
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	return http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if len(q.URL.Path) >= len("/debug/pprof/") && q.URL.Path[:len("/debug/pprof/")] == "/debug/pprof/" {
			host, _, err := net.SplitHostPort(q.RemoteAddr)
			if err != nil || !net.ParseIP(host).IsLoopback() {
				http.Error(w, "loopback profiling only", http.StatusForbidden)
				return
			}
			mux.ServeHTTP(w, q)
			return
		}
		next.ServeHTTP(w, q)
	})
}

// QueryTracer records wire round trips, including BEGIN/COMMIT, without values
// or SQL arguments. QueryRows ends only after rows have been consumed/closed.
// Sums overlap application stages; they must not be added to those stages.
type QueryTracer struct {
	Metrics     *Registry
	SummaryOnly bool // collect scoped calls and pool wait without per-statement histograms
}
type queryClock struct {
	start time.Time
	sql   string
}
type queryClockKey struct{}
type acquireClockKey struct{}
type intakeAcquireKey struct{}
type querySummaryKey struct{}

// QuerySummary is scoped to one synchronously used pgx transaction path.
// Calls count pgx query operations, including failed ones, not protocol packets.
type QuerySummary struct {
	Calls    int64
	Duration time.Duration
}

func WithQuerySummary(ctx context.Context, summary *QuerySummary) context.Context {
	return context.WithValue(ctx, querySummaryKey{}, summary)
}

func WithIntakeAcquireTiming(ctx context.Context) context.Context {
	return context.WithValue(ctx, intakeAcquireKey{}, true)
}

func (t QueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if t.SummaryOnly && ctx.Value(querySummaryKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, queryClockKey{}, queryClock{time.Now(), d.SQL})
}
func (t QueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	if c, ok := ctx.Value(queryClockKey{}).(queryClock); ok {
		elapsed := time.Since(c.start)
		if summary, ok := ctx.Value(querySummaryKey{}).(*QuerySummary); ok {
			summary.Calls++
			summary.Duration += elapsed
		}
		if !t.SummaryOnly {
			t.Metrics.Observe("sql/"+c.sql, elapsed)
			if d.Err != nil {
				t.Metrics.Add("sql_errors", 1)
			}
		}
	}
}
func (t QueryTracer) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	if t.SummaryOnly && ctx.Value(intakeAcquireKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, acquireClockKey{}, time.Now())
}
func (t QueryTracer) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, d pgxpool.TraceAcquireEndData) {
	if start, ok := ctx.Value(acquireClockKey{}).(time.Time); ok {
		elapsed := time.Since(start)
		if !t.SummaryOnly {
			t.Metrics.Observe("sql_pool_acquire", elapsed)
		}
		if ctx.Value(intakeAcquireKey{}) != nil {
			t.Metrics.Observe("master_pg_pool_acquire_duration", elapsed)
			if d.Err != nil {
				t.Metrics.Add("master_pg_pool_acquire_errors", 1)
			}
		}
	}
}

// Stage is a no-op when the optional registry is nil.
func Stage(r *Registry, name string) func() {
	if r == nil {
		return func() {}
	}
	start := time.Now()
	return func() { r.Observe(name, time.Since(start)) }
}

// Keep the query tracer independent of any logger, credentials or payloads.
var _ pgx.QueryTracer = QueryTracer{}
var _ pgxpool.AcquireTracer = QueryTracer{}
