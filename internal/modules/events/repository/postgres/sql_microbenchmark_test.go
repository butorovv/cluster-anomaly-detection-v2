package postgres

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Opt-in local SQL measurements, executed against the four existing snapshots
// via Go's test overlay. Clean runs use unmodified production files and no tracer.
// Diagnostic overlays replace only the three direct join json.Unmarshal calls
// with this counting delegate; they are never used for clean latency/throughput.
var microDecodeCount atomic.Int64
var microDecodeNanos atomic.Int64

func sqlMicroDecode(b []byte, v any) error {
	at := time.Now()
	err := json.Unmarshal(b, v)
	microDecodeNanos.Add(time.Since(at).Nanoseconds())
	microDecodeCount.Add(1)
	return err
}

type microKey struct{}
type microQuery struct {
	sql string
	at  time.Time
}
type microTrace struct {
	mu                                   sync.Mutex
	queries, payloadQueries, payloadRows int64
	commit, begin, sqlJoin               []float64
	body                                 []float64
	bodyStart                            time.Time
	ready, release                       chan struct{}
	once                                 sync.Once
}

func (s *microTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if strings.ToLower(d.SQL) == "commit" {
		s.mu.Lock()
		if !s.bodyStart.IsZero() {
			s.body = append(s.body, time.Since(s.bodyStart).Seconds())
			s.bodyStart = time.Time{}
		}
		s.mu.Unlock()
	}
	return context.WithValue(ctx, microKey{}, microQuery{d.SQL, time.Now()})
}
func (s *microTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	q, _ := ctx.Value(microKey{}).(microQuery)
	elapsed := time.Since(q.at).Seconds()
	s.mu.Lock()
	s.queries++
	if strings.HasPrefix(q.sql, "SELECT envelope FROM cad_v2.master_tick_fragments") {
		s.payloadQueries++
		s.payloadRows += d.CommandTag.RowsAffected()
	}
	switch strings.ToLower(q.sql) {
	case "commit":
		s.commit = append(s.commit, elapsed)
	case "begin":
		s.begin = append(s.begin, elapsed)
		s.bodyStart = time.Now()
	}
	if strings.HasPrefix(q.sql, "SELECT cad_v2.accept_tick_fragment") {
		s.sqlJoin = append(s.sqlJoin, elapsed)
	}
	s.mu.Unlock()
	if s.ready != nil && d.Err == nil && strings.HasPrefix(q.sql, "SELECT cad_v2.accept_result(") {
		s.once.Do(func() {
			close(s.ready)
			select {
			case <-s.release:
			case <-ctx.Done():
			}
		})
	}
}
func microPool(t *testing.T, pool *pgxpool.Pool, tracer *microTrace, timeout string) *pgxpool.Pool {
	t.Helper()
	cfg := pool.Config()
	cfg.MaxConns = 4
	if tracer != nil {
		cfg.ConnConfig.Tracer = tracer
	}
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = timeout
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	if err := p.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	return p
}

type microFixture struct {
	pool         *pgxpool.Pool
	f            workerfixture.Fixture
	r            checkpoints.Registration
	envelopes    [][]domain.Envelope
	inputHash    string
	expectedHash string
}

type microFixtureTB struct {
	testing.TB
	root string
}

func (t microFixtureTB) TempDir() string { return t.root }

func newMicroFixture(t *testing.T, ticks int) microFixture {
	t.Helper()
	ctx := context.Background()
	pool := postgrestest.New(t)
	// Source.Path participates in the existing fixture's frozen provenance.
	// A fixed test-only location keeps factorial input bytes and IDs identical.
	var fixtureTB testing.TB = t
	if root := os.Getenv("P14_SQL_MICRO_FIXTURE_ROOT"); root != "" {
		if !filepath.IsAbs(root) {
			t.Fatal("fixture root must be absolute")
		}
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatal(err)
		}
		fixtureTB = microFixtureTB{TB: t, root: root}
	}
	f := workerfixture.New(fixtureTB, ticks, 0)
	first, err := group.NewGroupProcessor(f.Bundle, f.Options)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := first.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r := checkpoints.Registration{Identity: snapshot.Identity, Binding: f.Options.Binding, ClusterID: "sql-micro", TopicID: "sql-micro", ConsumerGroup: "sql-micro", OutputTransport: "polling", Diagnostics: true, StartOffsets: map[string]int64{}}
	for i := 0; i < 16; i++ {
		r.StartOffsets[fmt.Sprint(i)] = 0
	}
	if err := checkpoints.Register(ctx, pool, r); err != nil {
		t.Fatal(err)
	}
	items := make([][]domain.Envelope, ticks)
	for i := range items {
		items[i] = make([]domain.Envelope, 16)
	}
	hash := sha256.New()
	for g := 0; g < 16; g++ {
		opts := f.Options
		opts.GroupID = fmt.Sprintf("a%02d", g+1)
		p, err := group.NewGroupProcessor(f.Bundle, opts)
		if err != nil {
			t.Fatal(err)
		}
		for tick := 0; tick < ticks; tick++ {
			at := opts.Window.Start.Add(time.Duration(tick) * telemetry.GridStep)
			records := map[string]telemetry.TelemetryRecord{}
			for host := 1; host <= 18; host++ {
				record := workerfixture.Record(opts.GroupID, host, at, 2)
				records[record.Hostname] = record
			}
			result, err := p.Step(telemetry.GroupTick{Binding: opts.Binding, GroupID: opts.GroupID, Timestamp: at, Records: records, ValidRecords: 18})
			if err != nil {
				t.Fatal(err)
			}
			e, err := domain.TickFragment(opts.Binding.ExecutionID, r.Identity, result)
			if err != nil {
				t.Fatal(err)
			}
			items[tick][g] = e
			b, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			hash.Write(b)
		}
	}
	// The last two prepared ticks serve only the stale-ownership probe.
	return microFixture{pool: pool, f: f, r: r, envelopes: items, inputHash: hex.EncodeToString(hash.Sum(nil)), expectedHash: microExpectedPrefix(t, items[:ticks-2], r.Identity.RunID)}
}

func microExpectedPrefix(t *testing.T, items [][]domain.Envelope, runID string) string {
	t.Helper()
	h := sha256.New()
	for _, tick := range items {
		payloads := make([]domain.GroupResult, len(tick))
		raw := make([]group.Raw, len(tick))
		for i, e := range tick {
			if err := json.Unmarshal(e.Payload, &payloads[i]); err != nil {
				t.Fatal(err)
			}
			raw[i] = payloads[i].Raw
		}
		group.AnnotateOutside(raw)
		for i, e := range tick {
			payloads[i].Raw = raw[i]
			if payloads[i].Evidence != nil {
				payloads[i].Evidence.Raw = raw[i]
			}
			b, err := json.Marshal(payloads[i])
			if err != nil {
				t.Fatal(err)
			}
			h.Write([]byte(domain.MessageID(runID, e.GroupID, "group_result", e.EventTime.UTC().Format(time.RFC3339), 1)))
			h.Write(b)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}
func realMicroFixture(t *testing.T, ticks int) microFixture {
	t.Helper()
	root := filepath.Join(os.Getenv("V1_GOLDEN_ROOT"), "g16-d0")
	b, err := os.ReadFile(filepath.Join(root, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var summary group.RBI2RunSummary
	if err := json.Unmarshal(b, &summary); err != nil {
		t.Fatal(err)
	}
	id := summary.Identity
	pool := postgrestest.New(t)
	r := checkpoints.Registration{Identity: id, Binding: telemetry.InputBinding{ExecutionID: "00000000-0000-4000-8000-000000000071", ProfileSHA256: id.ProfileSHA256, InputSHA256: id.InputSHA256["data/prepared/summit-D1-pilot/D1.parquet"]}, ClusterID: "sql-micro", TopicID: "sql-micro", ConsumerGroup: "sql-micro", OutputTransport: "polling", Diagnostics: true, StartOffsets: map[string]int64{}}
	for i := 0; i < 16; i++ {
		r.StartOffsets[fmt.Sprint(i)] = 0
	}
	if err := checkpoints.Register(context.Background(), pool, r); err != nil {
		t.Fatal(err)
	}
	payloads := make([][]domain.GroupResult, ticks)
	for i := range payloads {
		payloads[i] = make([]domain.GroupResult, 16)
	}
	for _, name := range []string{"raw", "temporal", "evidence"} {
		f, err := os.Open(filepath.Join(root, name+".ndjson"))
		if err != nil {
			t.Fatal(err)
		}
		s := bufio.NewScanner(f)
		s.Buffer(make([]byte, 65536), 1<<20)
		for s.Scan() {
			var header struct {
				Group     string    `json:"group"`
				Timestamp time.Time `json:"timestamp"`
			}
			if err := json.Unmarshal(s.Bytes(), &header); err != nil {
				t.Fatal(err)
			}
			if name == "evidence" {
				var nested struct {
					Raw struct {
						Group     string    `json:"group"`
						Timestamp time.Time `json:"timestamp"`
					} `json:"raw"`
				}
				if err := json.Unmarshal(s.Bytes(), &nested); err != nil {
					t.Fatal(err)
				}
				header.Group, header.Timestamp = nested.Raw.Group, nested.Raw.Timestamp
			}
			tick := int(header.Timestamp.Sub(id.Window.Start) / telemetry.GridStep)
			if tick < 0 || tick >= ticks {
				continue
			}
			var g int
			if _, err := fmt.Sscanf(header.Group, "a%d", &g); err != nil || g < 1 || g > 16 {
				t.Fatal(header, err)
			}
			p := &payloads[tick][g-1]
			switch name {
			case "raw":
				err = json.Unmarshal(s.Bytes(), &p.Raw)
			case "temporal":
				err = json.Unmarshal(s.Bytes(), &p.Temporal)
			case "evidence":
				p.Evidence = &group.RBI2Evidence{}
				err = json.Unmarshal(s.Bytes(), p.Evidence)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Err(); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	items := make([][]domain.Envelope, ticks)
	input := sha256.New()
	expected := sha256.New()
	for tick := range payloads {
		items[tick] = make([]domain.Envelope, 16)
		for g, p := range payloads[tick] {
			finalID := domain.MessageID(id.RunID, p.Raw.Group, "group_result", p.Raw.Timestamp.UTC().Format(time.RFC3339), 1)
			b, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			expected.Write([]byte(finalID))
			expected.Write(b)
			p.Raw.IF.OutsideGroups = nil
			p.Raw.IF.OutsideS = nil
			p.Raw.IF.E = nil
			p.Raw.IF.OutsideAvailable = 0
			p.Raw.IF.OutsideAnomalous = 0
			if p.Evidence != nil {
				copy := *p.Evidence
				copy.Raw = p.Raw
				p.Evidence = &copy
			}
			e, err := domain.TickFragment(r.Binding.ExecutionID, id, group.GroupProcessorResult{Raw: p.Raw, Temporal: p.Temporal, Evidence: p.Evidence})
			if err != nil {
				t.Fatal(err)
			}
			items[tick][g] = e
			b, err = json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			input.Write(b)
		}
	}
	return microFixture{pool: pool, r: r, envelopes: items, inputHash: hex.EncodeToString(input.Sum(nil)), expectedHash: hex.EncodeToString(expected.Sum(nil))}
}
func microSave(t *testing.T, data any) {
	t.Helper()
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("P14_SQL_MICRO_OUTPUT"), append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
func microSettings(t *testing.T, p *pgxpool.Pool) map[string]any {
	t.Helper()
	ctx := context.Background()
	out := map[string]any{}
	for _, key := range []string{"server_version", "shared_buffers", "max_connections", "fsync", "synchronous_commit", "default_transaction_isolation", "wal_sync_method", "full_page_writes", "track_io_timing", "shared_preload_libraries"} {
		var v string
		if err := p.QueryRow(ctx, "SHOW "+key).Scan(&v); err != nil {
			t.Fatal(err)
		}
		out[key] = v
	}
	var b []byte
	if err := p.QueryRow(ctx, `SELECT jsonb_agg(to_jsonb(m) ORDER BY version) FROM public.schema_migrations m`).Scan(&b); err != nil {
		t.Fatal(err)
	}
	var migrations any
	if err := json.Unmarshal(b, &migrations); err != nil {
		t.Fatal(err)
	}
	out["migrations"] = migrations
	out["client_max_connections"] = 4
	out["gomaxprocs"] = runtime.GOMAXPROCS(0)
	out["client_lock_timeout_ms"] = 2000
	out["statement_timeout_ms"] = 5000
	return out
}
func microDigest(t *testing.T, f microFixture, expected int) string {
	t.Helper()
	var count, joins int
	ctx := context.Background()
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_results`).Scan(&count); err != nil || count != expected*16 {
		t.Fatal("final cardinality", count, err)
	}
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_tick_joins`).Scan(&joins); err != nil || joins != expected {
		t.Fatal("join cardinality", joins, err)
	}
	rows, err := f.pool.Query(ctx, `SELECT message_id,payload FROM cad_v2.master_results ORDER BY tick,group_id`)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	defer rows.Close()
	for rows.Next() {
		var id string
		var payload []byte
		if err := rows.Scan(&id, &payload); err != nil {
			t.Fatal(err)
		}
		var typed domain.GroupResult
		if err := json.Unmarshal(payload, &typed); err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(typed)
		if err != nil {
			t.Fatal(err)
		}
		h.Write([]byte(id))
		h.Write(b)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if f.expectedHash != "" && got != f.expectedHash {
		t.Fatal("selected V1 payload/MessageID mismatch", got, f.expectedHash)
	}
	return got
}

func TestSQLMicroMeasurements(t *testing.T) {
	mode := os.Getenv("P14_SQL_MICRO_MODE")
	if mode == "" {
		t.Skip("opt-in local SQL measurement")
	}
	n, _ := strconv.Atoi(os.Getenv("P14_SQL_MICRO_N"))
	if n < 4 || n > 256 {
		t.Fatal("require 4..256 timestamps")
	}
	if mode == "locks" {
		microLocks(t, n)
		return
	}
	if mode != "clean" && mode != "diagnostic" {
		t.Fatal("invalid mode")
	}
	f := realMicroFixture(t, n+8)
	ctx := context.Background()
	var tracer *microTrace
	if mode == "diagnostic" {
		tracer = &microTrace{}
	}
	p := microPool(t, f.pool, tracer, "2000")
	store := MasterStore{DB: p}
	for i := 0; i < 8; i++ {
		for _, e := range f.envelopes[i] {
			if ok, err := store.Accept(ctx, e); err != nil || !ok {
				t.Fatal(ok, err)
			}
		}
	}
	if tracer != nil {
		tracer.queries = 0
		tracer.payloadQueries = 0
		tracer.payloadRows = 0
		tracer.begin = nil
		tracer.commit = nil
		tracer.sqlJoin = nil
		tracer.body = nil
	}
	microDecodeCount.Store(0)
	microDecodeNanos.Store(0)
	fragments := make([]float64, 0, n*16)
	final := make([]float64, 0, n)
	timestamps := make([]float64, 0, n)
	metrics := observability.New()
	cpuBefore := metrics.Snapshot().Process.CPUSeconds
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	failures := []string{}
	for i := 8; i < n+8; i++ {
		at := time.Now()
		for g, e := range f.envelopes[i] {
			s := time.Now()
			ok, err := store.Accept(ctx, e)
			elapsed := time.Since(s).Seconds()
			fragments = append(fragments, elapsed)
			if g == 15 {
				final = append(final, elapsed)
			}
			if err != nil || !ok {
				failures = append(failures, fmt.Sprint(ok, err))
			}
		}
		timestamps = append(timestamps, time.Since(at).Seconds())
	}
	elapsed := time.Since(start).Seconds()
	runtime.ReadMemStats(&after)
	cpuAfter := metrics.Snapshot().Process.CPUSeconds
	var cpu any
	if cpuBefore != nil && cpuAfter != nil {
		cpu = *cpuAfter - *cpuBefore
	}
	data := map[string]any{"mode": mode, "source_commit": os.Getenv("P14_SQL_MICRO_SOURCE"), "timestamps": n, "groups": 16, "warm_timestamps": 8, "input_hash": f.inputHash, "fragment_seconds": fragments, "final_fragment_tx_seconds": final, "timestamp_seconds": timestamps, "elapsed_seconds": elapsed, "successful_transactions_per_second": float64(n*16-len(failures)) / elapsed, "allocations": after.Mallocs - before.Mallocs, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "go_process_cpu_seconds": cpu, "failures": failures, "settings": microSettings(t, f.pool), "direct_join_json_unmarshal_calls": microDecodeCount.Load()}
	if len(failures) == 0 {
		data["final_scientific_and_messageid_digest"] = microDigest(t, f, n+8)
	}
	if tracer != nil {
		data["direct_join_json_unmarshal_seconds"] = float64(microDecodeNanos.Load()) / 1e9
		data["queries_round_trips"] = tracer.queries
		data["payload_selects"] = tracer.payloadQueries
		data["payload_rows_read"] = tracer.payloadRows
		data["begin_seconds"] = tracer.begin
		data["commit_seconds"] = tracer.commit
		data["accept_fragment_sql_seconds"] = tracer.sqlJoin
		data["transaction_body_seconds"] = tracer.body
	}
	microSave(t, data)
	if len(failures) > 0 {
		t.Fatal(failures)
	}
}

func microTransition(t *testing.T, p *core.Partition, f workerfixture.Fixture, tick int) *core.Transition {
	t.Helper()
	at := f.Options.Window.Start.Add(time.Duration(tick) * telemetry.GridStep)
	offset := int64(tick * 19)
	for h := 1; h <= 18; h++ {
		r := workerfixture.Record("a01", h, at, 2)
		_, err := p.Accept(offset+int64(h-1), telemetry.InputMessage{Version: telemetry.InputVersion, Type: telemetry.RecordMessage, Binding: f.Options.Binding, GroupID: "a01", Timestamp: at, Record: &r})
		if err != nil {
			t.Fatal(err)
		}
	}
	c := telemetry.TimestampComplete{GroupID: "a01", Timestamp: at, Records: 18, ValidRecords: 18}
	tr, err := p.Accept(offset+18, telemetry.InputMessage{Version: telemetry.InputVersion, Type: telemetry.CompleteMessage, Binding: f.Options.Binding, GroupID: "a01", Timestamp: at, Completion: &c})
	if err != nil || tr == nil {
		t.Fatal(tr, err)
	}
	return tr
}
func microLocks(t *testing.T, n int) {
	f := newMicroFixture(t, 2*n+2)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	p := microPool(t, f.pool, nil, "2000")
	worker := &checkpoints.Store{DB: p, Registration: f.r}
	lease, _, err := worker.Acquire(ctx, "a01", "00000000-0000-4000-8000-000000000099", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := core.NewPartition(f.f.Bundle, f.f.Options, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	store := MasterStore{DB: p}
	samples := []map[string]any{}
	expectedTimeouts := 0
	unexpected := []string{}
	for i := 0; i < n; i++ {
		current := microTransition(t, partition, f.f, 2*i)
		rev, err := worker.Save(ctx, lease, *current)
		if err != nil {
			t.Fatal(err)
		}
		if err := partition.Ack(rev); err != nil {
			t.Fatal(err)
		}
		next := microTransition(t, partition, f.f, 2*i+1)
		for _, e := range f.envelopes[2*i][:15] {
			if _, err := store.Accept(ctx, e); err != nil {
				t.Fatal(err)
			}
		}
		wt := &microTrace{}
		timeout := "2000"
		if i == n-1 {
			timeout = "60"
		}
		wp := microPool(t, f.pool, wt, timeout)
		pidConn, err := wp.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pid := int32(pidConn.Conn().PgConn().PID())
		pidConn.Release()
		barrier := &microTrace{ready: make(chan struct{}), release: make(chan struct{})}
		mp := microPool(t, f.pool, barrier, "2000")
		done := make(chan error, 1)
		last := f.envelopes[2*i][15]
		go func() { _, err := (MasterStore{DB: mp}).Accept(ctx, last); done <- err }()
		select {
		case <-barrier.ready:
		case err := <-done:
			t.Fatal("barrier", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		// Same real final-join hold in every variant. No fabricated SQL/advisory lock.
		holdStart := time.Now()
		go func() { time.Sleep(100 * time.Millisecond); close(barrier.release) }()
		workerDone := make(chan struct {
			revision int64
			err      error
			elapsed  float64
		}, 1)
		go func() {
			start := time.Now()
			r, e := (&checkpoints.Store{DB: wp, Registration: f.r}).Save(ctx, lease, *next)
			workerDone <- struct {
				revision int64
				err      error
				elapsed  float64
			}{r, e, time.Since(start).Seconds()}
		}()
		observed := false
		var lower, upper float64
		var result struct {
			revision int64
			err      error
			elapsed  float64
		}
		lastWait := time.Time{}
		finished := false
		for !finished {
			select {
			case result = <-workerDone:
				finished = true
			default:
				var wait bool
				var seconds float64
				err := f.pool.QueryRow(ctx, `SELECT coalesce(a.wait_event_type='Lock',false),coalesce((SELECT extract(epoch FROM clock_timestamp()-min(waitstart))::float8 FROM pg_locks WHERE pid=$1 AND NOT granted),0) FROM pg_stat_activity a WHERE a.pid=$1`, pid).Scan(&wait, &seconds)
				if err != nil {
					t.Fatal(err)
				}
				if wait {
					observed = true
					lower = max(lower, seconds)
					lastWait = time.Now()
				}
				time.Sleep(2 * time.Millisecond)
			}
		}
		if observed {
			upper = lower + time.Since(lastWait).Seconds()
		}
		var pgerr *pgconn.PgError
		timedOut := errors.As(result.err, &pgerr) && pgerr.Code == "55P03"
		if timedOut {
			expectedTimeouts++
			saved, err := worker.Load(ctx, "a01")
			if err != nil || saved.Revision != current.Checkpoint.Revision {
				t.Fatal("failed checkpoint leaked", err)
			}
		} else if result.err != nil {
			unexpected = append(unexpected, result.err.Error())
		}
		if err := <-done; err != nil {
			unexpected = append(unexpected, err.Error())
		}
		retried := false
		if timedOut {
			rev, err = worker.Save(ctx, lease, *next)
			retried = true
			if err != nil {
				unexpected = append(unexpected, err.Error())
			}
		} else {
			rev = result.revision
		}
		if err := partition.Ack(rev); err != nil {
			t.Fatal(err)
		}
		if accepted, err := store.Accept(ctx, last); err != nil || accepted {
			t.Fatal("duplicate", accepted, err)
		}
		saved, err := worker.Load(ctx, "a01")
		if err != nil || saved.NextOffset != next.Checkpoint.NextOffset || saved.SHA256 != next.Checkpoint.SHA256 {
			t.Fatal("offset/snapshot", err)
		}
		samples = append(samples, map[string]any{"worker_tx_seconds": result.elapsed, "lock_observed": observed, "lock_wait_lower_seconds": lower, "lock_wait_upper_seconds": upper, "timeout_probe": i == n-1, "lock_timeout": timedOut, "rollback_retry": retried, "worker_commit_seconds": wt.commit, "controlled_iteration_seconds": time.Since(holdStart).Seconds()})
		wp.Close()
		mp.Close()
	}
	// Real competing final joins, four transactions in flight, without a barrier.
	for i := 0; i < n; i++ {
		for _, e := range f.envelopes[2*i+1][:15] {
			if _, err := store.Accept(ctx, e); err != nil {
				t.Fatal(err)
			}
		}
	}
	concurrent := make([]float64, n)
	concurrentStart := time.Now()
	for first := 0; first < n; first += 4 {
		var wg sync.WaitGroup
		for j := first; j < min(first+4, n); j++ {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				at := time.Now()
				_, err := store.Accept(ctx, f.envelopes[2*j+1][15])
				concurrent[j] = time.Since(at).Seconds()
				if err != nil {
					t.Error(err)
				}
			}(j)
		}
		wg.Wait()
	}
	concurrentElapsed := time.Since(concurrentStart).Seconds()
	stale := lease
	stale.Epoch--
	staleCandidate := microTransition(t, partition, f.f, 2*n)
	_, staleErr := worker.Save(ctx, stale, *staleCandidate)
	var stalePG *pgconn.PgError
	if !errors.As(staleErr, &stalePG) || stalePG.Code != "55000" {
		t.Fatal("stale ownership accepted", staleErr)
	}
	saved, err := worker.Load(ctx, "a01")
	if err != nil || saved.Revision != int64(2*n) {
		t.Fatal("stale write changed revision", saved, err)
	}
	digest := microDigest(t, f, 2*n)
	microSave(t, map[string]any{"mode": "locks", "input_hash": f.inputHash, "samples": samples, "expected_timeout_count": expectedTimeouts, "unexpected_errors": unexpected, "settings": microSettings(t, p), "source_commit": os.Getenv("P14_SQL_MICRO_SOURCE"), "competing_final_join_seconds": concurrent, "competing_final_transactions_per_second": float64(n) / concurrentElapsed, "stale_ownership_rejected": true, "final_scientific_and_messageid_digest": digest, "notes": "100ms actual final-join hold; last sample has explicit 60ms lock_timeout probe; sampled pg_locks waitstart lower/upper bounds, not DataFileExtend simulation"})
	if len(unexpected) > 0 {
		t.Fatal(unexpected)
	}
}
