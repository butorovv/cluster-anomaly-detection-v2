package worker

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/resulttransport"
	ev "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/inputcodec"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	tel "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type p16Child struct {
	*workerProcess
	done chan struct{}
	err  error
}

func p16Start(t *testing.T, binary, logs, name string, worker bool, args []string, env ...string) *p16Child {
	t.Helper()
	f, err := os.Create(filepath.Join(logs, name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	c := exec.Command(binary, args...)
	c.Env = append(os.Environ(), env...)
	c.Stdout, c.Stderr = f, f
	if err := c.Start(); err != nil {
		f.Close()
		t.Fatal(err)
	}
	p := &p16Child{workerProcess: &workerProcess{cmd: c, log: f, logPath: f.Name()}, done: make(chan struct{})}
	go func() {
		p.err = c.Wait()
		f.Close()
		close(p.done)
	}()
	t.Cleanup(func() {
		select {
		case <-p.done:
			t.Errorf("%s exited before requested shutdown: %v", name, p.err)
			return
		default:
		}
		signalErr := c.Process.Signal(syscall.SIGTERM)
		if signalErr != nil {
			t.Errorf("%s SIGTERM: %v", name, signalErr)
			_ = c.Process.Kill()
		}
		select {
		case <-p.done:
			if p.err != nil && c.ProcessState.ExitCode() == 0 {
				t.Errorf("%s wait: %v", name, p.err)
				return
			}
			log, err := os.Open(p.logPath)
			if err == nil {
				defer log.Close()
				err = p16CheckShutdown(worker, signalErr == nil, c.ProcessState.ExitCode(), log)
			}
			if err != nil {
				t.Errorf("%s shutdown: %v (wait: %v)", name, err, p.err)
			}
		case <-time.After(18 * time.Second):
			_ = c.Process.Kill()
			<-p.done
			t.Errorf("%s shutdown timed out", name)
		}
	})
	return p
}

func p16DSN(dsn, name string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		panic(err)
	}
	q := u.Query()
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	return u.String()
}

func p16Messages(r *workerRig, id string, tick int) []tel.InputMessage {
	messages := r.messages(id, tick%6)
	at := r.fixture.Options.Window.Start.Add(time.Duration(tick) * tel.GridStep)
	for i := range messages {
		m := &messages[i]
		m.Timestamp = at
		if m.Record != nil {
			m.Record.Timestamp = at
		}
		if m.Completion != nil {
			m.Completion.Timestamp = at
		}
	}
	return messages
}

func p16Inputs(t *testing.T, r *workerRig, ticks int) ([][]*kgo.Record, []int64, map[string]ev.GroupResult) {
	t.Helper()
	const groups = 16
	o := r.fixture.Options
	window := tel.InputWindow{Start: o.Window.Start, End: o.Window.End}
	ids := make([]string, groups)
	journals := make([]*group.RBI2Journal, groups)
	for g := range groups {
		ids[g] = fmt.Sprintf("a%02d", g+1)
		hosts := make([]string, 18)
		for h := range hosts {
			hosts[h] = fmt.Sprintf("%sn%02d", ids[g], h+1)
		}
		j, err := group.NewRBI2Journal(r.store.Registration.Identity, ids[g], hosts)
		if err != nil {
			t.Fatal(err)
		}
		journals[g] = j
	}
	var batches [][]*kgo.Record
	expected := make(map[string]ev.GroupResult, ticks*groups)
	counts := make([]int64, groups)
	for tick := range ticks {
		if tick%6 == 0 {
			batches = append(batches, nil)
		}
		raw := make([]group.Raw, groups)
		for g, id := range ids {
			messages := p16Messages(r, id, tick)
			for _, m := range messages {
				b, err := inputcodec.Encode(m, window)
				if err != nil {
					t.Fatal(err)
				}
				batches[len(batches)-1] = append(batches[len(batches)-1], &kgo.Record{Topic: kafkainput.Topic, Partition: int32(g), Key: []byte(id), Value: b})
				counts[g]++
			}
			row, err := r.fixture.Bundle.EvaluateGroup(id, group.Tick{Timestamp: messages[0].Timestamp, Records: mapRecord(messages)}, nil)
			if err != nil {
				t.Fatal(err)
			}
			raw[g] = row
		}
		group.AnnotateOutside(raw)
		for g, row := range raw {
			temporal, evidence, _, err := journals[g].Step(row)
			if err != nil {
				t.Fatal(err)
			}
			expected[row.Group+"/"+row.Timestamp.UTC().Format(time.RFC3339Nano)] = ev.GroupResult{Raw: row, Temporal: temporal, Evidence: evidence}
		}
	}
	return batches, counts, expected
}

func p16Write(t *testing.T, name string, value any) {
	t.Helper()
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func p16CPUPath() string {
	id := os.Getenv("P16_KAFKA_PID")
	if _, err := strconv.Atoi(id); err != nil {
		return ""
	}
	b, err := os.ReadFile(filepath.Join("/proc", id, "cgroup"))
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		if path, ok := strings.CutPrefix(line, "0::/"); ok && path != "" {
			return filepath.Join("/sys/fs/cgroup", path, "cpu.stat")
		}
	}
	return ""
}

func p16CPU(path string) map[string]int64 {
	if path == "" {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	values := make(map[string]int64)
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			if v, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
				values[fields[0]] = v
			}
		}
	}
	return values
}

type p16Sample struct {
	At                                time.Time
	Joins, Finals, Finalized, Pending int
	DurableNextOffset                 int64
	Waits                             map[string]int
	KafkaCPU                          map[string]int64
	Master                            observability.Data
	WAL                               json.RawMessage
}

func p16ReadProgress(ctx context.Context, r *workerRig, s *p16Sample) error {
	return r.pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM cad_v2.master_tick_joins),(SELECT count(*) FROM cad_v2.master_results),(SELECT count(*) FROM cad_v2.group_checkpoints WHERE finalized),(SELECT count(*) FROM cad_v2.worker_tick_fragments WHERE delivered_at IS NULL)+(SELECT count(*) FROM cad_v2.outbox_messages WHERE delivered_at IS NULL),(SELECT coalesce(sum(next_offset),0)::bigint FROM cad_v2.result_consumer_offsets)").Scan(&s.Joins, &s.Finals, &s.Finalized, &s.Pending, &s.DurableNextOffset)
}

func p16SampleDetails(ctx context.Context, t *testing.T, r *workerRig, s *p16Sample, masterURL, cpuPath string) {
	t.Helper()
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	data, err := readProcessMetrics(bounded, masterURL)
	if err != nil {
		t.Fatal(err)
	}
	s.Master = data
	s.KafkaCPU = p16CPU(cpuPath)
	rows, err := r.pool.Query(bounded, "SELECT coalesce(nullif(application_name,''),'fixture'),coalesce(wait_event_type,'CPU'),coalesce(wait_event,'active'),count(*) FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND pid<>pg_backend_pid() GROUP BY 1,2,3")
	if err != nil {
		t.Fatal(err)
	}
	s.Waits = map[string]int{}
	for rows.Next() {
		var app, kind, event string
		var n int
		if err := rows.Scan(&app, &kind, &event, &n); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		s.Waits[app+"/"+kind+"/"+event] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.pool.QueryRow(bounded, "SELECT to_jsonb(w) FROM pg_stat_wal w").Scan(&s.WAL); err != nil {
		t.Fatal(err)
	}
}

type p16Report struct {
	Status, Execution, Run, Digest, EventsDigest                          string
	Workers, Ticks, Joins, Finals, ParentEvents, TraceFragments           int
	SourceStart, ReplayComplete, WorkerOutputObserved, FinalJoinConfirmed time.Time
	ElapsedSeconds, GroupTicksPerSecond                                   float64
	RelativeInput, RelativeResults                                        []int64
	Finalized, Pending, Duplicates                                        int
}

func p16Verify(t *testing.T, ctx context.Context, r *workerRig, expected map[string]ev.GroupResult, counts []int64, starts kadm.ListedOffsets, report *p16Report) {
	t.Helper()
	rows, err := r.pool.Query(ctx, "SELECT group_id,tick,message_id,payload FROM cad_v2.master_results ORDER BY tick,group_id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	h := sha256.New()
	seen := map[string]bool{}
	for rows.Next() {
		var g, id string
		var at time.Time
		var b []byte
		if err := rows.Scan(&g, &at, &id, &b); err != nil {
			t.Fatal(err)
		}
		key := g + "/" + at.UTC().Format(time.RFC3339Nano)
		want, ok := expected[key]
		var got ev.GroupResult
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		if !ok || seen[key] || !reflect.DeepEqual(want, got) || id != ev.MessageID(report.Run, g, "group_result", at.UTC().Format(time.RFC3339), 1) {
			t.Fatalf("final result or MessageID mismatch for %s", key)
		}
		seen[key] = true
		b, err = json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(h, "%s\n%s\n", id, b)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(expected) {
		t.Fatalf("got %d final results, want %d", len(seen), len(expected))
	}
	report.Digest = hex.EncodeToString(h.Sum(nil))
	if err := r.pool.QueryRow(ctx, "SELECT count(*),encode(sha256(convert_to(coalesce(jsonb_agg(jsonb_build_array(group_id,event_id,payload) ORDER BY group_id,event_id)::text,'[]'),'UTF8')),'hex') FROM cad_v2.master_events").Scan(&report.ParentEvents, &report.EventsDigest); err != nil {
		t.Fatal(err)
	}
	ends, err := kadm.NewClient(r.producer).ListEndOffsets(ctx, resulttransport.ResultsTopic, kafkainput.Topic)
	if err != nil {
		t.Fatal(err)
	}
	for p := range 16 {
		id := fmt.Sprintf("a%02d", p+1)
		var in, out int64
		if err := r.pool.QueryRow(ctx, "SELECT next_offset FROM cad_v2.group_checkpoints WHERE group_id=$1 AND finalized", id).Scan(&in); err != nil {
			t.Fatal(err)
		}
		if err := r.pool.QueryRow(ctx, "SELECT next_offset FROM cad_v2.result_consumer_offsets WHERE partition_id=$1", p).Scan(&out); err != nil {
			t.Fatal(err)
		}
		inEnd, inOK := ends.Lookup(kafkainput.Topic, int32(p))
		outEnd, outOK := ends.Lookup(resulttransport.ResultsTopic, int32(p))
		outStart, startOK := starts.Lookup(resulttransport.ResultsTopic, int32(p))
		inRel := in - r.store.Registration.StartOffsets[strconv.Itoa(p)]
		if !inOK || !outOK || !startOK || inEnd.Err != nil || outEnd.Err != nil || outStart.Err != nil ||
			inRel != counts[p] || in != inEnd.Offset || out != outEnd.Offset || out-outStart.Offset < int64(report.Ticks) {
			t.Fatalf("partition %d offsets mismatch: input SQL %d, broker %v, output SQL %d, broker %v", p, in, inEnd, out, outEnd)
		}
		report.RelativeInput = append(report.RelativeInput, inRel)
		report.RelativeResults = append(report.RelativeResults, out-outStart.Offset)
	}
}

func p16FinalTrace(t *testing.T, path string, ticks int, report *p16Report) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fragments := map[string]map[string]bool{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		var line map[string]json.RawMessage
		if json.Unmarshal(scanner.Bytes(), &line) != nil || string(line["msg"]) != "\"result measurement\"" || string(line["kind"]) != "\"tick_fragment\"" || string(line["accepted"]) != "true" {
			continue
		}
		var at time.Time
		if json.Unmarshal(line["committed_at"], &at) != nil || at.IsZero() {
			t.Fatal("no commit timestamp")
		}
		tick, g := string(line["tick"]), string(line["group"])
		if fragments[tick] == nil {
			fragments[tick] = map[string]bool{}
		}
		if fragments[tick][g] {
			t.Fatal("duplicate accepted fragment timing")
		}
		fragments[tick][g] = true
		report.TraceFragments++
		if at.After(report.FinalJoinConfirmed) {
			report.FinalJoinConfirmed = at
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(fragments) != ticks || report.TraceFragments != ticks*16 {
		t.Fatalf("fragment timing coverage %d/%d", report.TraceFragments, ticks*16)
	}
	for tick, groups := range fragments {
		if len(groups) != 16 {
			t.Fatalf("incomplete join timing %s", tick)
		}
	}
	report.ElapsedSeconds = report.FinalJoinConfirmed.Sub(report.SourceStart).Seconds()
	if report.ElapsedSeconds <= 0 {
		t.Fatal("invalid throughput interval")
	}
	report.GroupTicksPerSecond = float64(report.TraceFragments) / report.ElapsedSeconds
}

func p16Trial(t *testing.T, root string, f *workerfixture.Fixture, workers, ticks int) p16Report {
	t.Helper()
	report := p16Report{Status: "FAIL", Workers: workers, Ticks: ticks}
	// Register before the fixture so the report includes child and SQL cleanup.
	t.Cleanup(func() {
		if !t.Failed() {
			report.Status = "PASS"
		}
		p16Write(t, filepath.Join(root, "report.json"), report)
	})
	t.Setenv("P07_WORKER_INTEGRATION", "1")
	r := workerIntegrationWithCadence(t, 0, "kafka", f, 1, true)
	report.Execution = r.fixture.Options.Binding.ExecutionID
	report.Run = r.store.Registration.Identity.RunID
	ctx, cancel := context.WithTimeout(context.Background(), 165*time.Second)
	defer cancel()
	o := r.fixture.Options
	consumer := "p16-short-" + r.group
	r.bindResults(t, consumer)
	starts, err := kadm.NewClient(r.producer).ListEndOffsets(ctx, resulttransport.ResultsTopic)
	if err != nil {
		t.Fatal(err)
	}
	batches, counts, expected := p16Inputs(t, r, ticks)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	masterURL := "http://" + address
	seeds := strings.Join(r.broker.Seeds, ",")
	bin := filepath.Join(os.Getenv("P16_ROOT"), "bin")
	master := p16Start(t, filepath.Join(bin, "master"), root, "master", false,
		[]string{"--listen", address, "--mode", "kafka", "--execution-id", report.Execution, "--results-group", consumer,
			"--kafka-brokers", seeds, "--kafka-intake-concurrency", "4", "--kafka-poll-records", "16", "--measure=true"},
		"MASTER_DATABASE_URL="+p16DSN(r.dsn, "p16-master"), "CAD_MASTER_DIAGNOSTICS=1", "CAD_PPROF=0", "GOMAXPROCS=2")
	children := []*p16Child{master}
	var workerIDs []string
	for n := range workers {
		name := fmt.Sprintf("worker-%d", n+1)
		owner, err := NewOwnerID()
		if err != nil {
			t.Fatal(err)
		}
		workerIDs = append(workerIDs, owner)
		child := p16Start(t, filepath.Join(bin, "worker"), root, name, true,
			[]string{"--assets", r.fixture.Root, "--profile-sha256", o.Binding.ProfileSHA256, "--input-sha256", o.Binding.InputSHA256,
				"--execution-id", report.Execution, "--consumer-group", r.group, "--owner-id", owner, "--kafka-brokers", seeds,
				"--start", o.Window.Start.Format(time.RFC3339), "--end", o.Window.End.Format(time.RFC3339), "--groups", "16",
				"--lease-ttl", "15s", "--output-transport", "kafka", "--checkpoint-interval-ticks", "1", "--kafka-async=true"},
			"WORKER_DATABASE_URL="+p16DSN(r.dsn, "p16-"+name), "GOMAXPROCS=1", "CAD_PPROF=0")
		children = append(children, child)
	}
	checkChildren := func() {
		for _, p := range children {
			select {
			case <-p.done:
				t.Fatalf("process exited early (%s): %v", p.logPath, p.err)
			default:
			}
		}
	}
	client := &http.Client{Timeout: time.Second}
	isReady := func(endpoint string) bool {
		response, err := client.Get(endpoint + "/readyz")
		if err != nil {
			return false
		}
		response.Body.Close()
		return response.StatusCode == 200
	}
	waitWorker(t, 45*time.Second, func() bool {
		checkChildren()
		if !isReady(masterURL) {
			return false
		}
		for _, p := range children[1:] {
			if !p.discover() || !isReady(p.endpoint) {
				return false
			}
		}
		var groups, owners int
		err := r.pool.QueryRow(ctx, "SELECT count(*),count(DISTINCT owner_id) FROM cad_v2.partition_ownership WHERE lease_until>clock_timestamp() AND owner_id=ANY($1::uuid[])", workerIDs).Scan(&groups, &owners)
		return err == nil && groups == 16 && owners == workers
	})
	samples, err := os.Create(filepath.Join(root, "samples.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer samples.Close()
	enc := json.NewEncoder(samples)
	cpuPath := p16CPUPath()
	var first p16Sample
	first.At = time.Now()
	if err := p16ReadProgress(ctx, r, &first); err != nil {
		t.Fatal(err)
	}
	p16SampleDetails(ctx, t, r, &first, masterURL, cpuPath)
	if err := enc.Encode(first); err != nil {
		t.Fatal(err)
	}
	report.SourceStart = time.Now()
	type replayResult struct {
		at  time.Time
		err error
	}
	produced := make(chan replayResult, 1)
	go func() {
		for _, batch := range batches {
			if err := r.producer.ProduceSync(ctx, batch...).FirstErr(); err != nil {
				produced <- replayResult{err: err}
				return
			}
		}
		produced <- replayResult{at: time.Now()}
	}()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	lastSample := first.At
	var s p16Sample
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("short window timed out: %+v", s)
		case p := <-produced:
			if p.err != nil {
				t.Fatal(p.err)
			}
			report.ReplayComplete = p.at
			produced = nil
		case <-ticker.C:
			checkChildren()
			s = p16Sample{At: time.Now()}
			if err := p16ReadProgress(ctx, r, &s); err != nil {
				t.Fatal(err)
			}
			if report.WorkerOutputObserved.IsZero() && s.Finalized == 16 && s.Pending == 0 {
				report.WorkerOutputObserved = s.At
			}
			ready := s.Joins == ticks && s.Finals == ticks*16 && s.Finalized == 16 && s.Pending == 0 && !report.ReplayComplete.IsZero()
			if ready || s.At.Sub(lastSample) >= time.Second {
				p16SampleDetails(ctx, t, r, &s, masterURL, cpuPath)
				if err := enc.Encode(s); err != nil {
					t.Fatal(err)
				}
				lastSample = s.At
			}
			if ready {
				goto complete
			}
		}
	}
complete:
	// Auxiliary Kafka offset is never used as an acceptance oracle.
	waitWorker(t, 10*time.Second, func() bool {
		checkChildren()
		ends, err := kadm.NewClient(r.producer).ListEndOffsets(ctx, resulttransport.ResultsTopic)
		if err != nil {
			return false
		}
		var expectedNext int64
		for p := range 16 {
			end, ok := ends.Lookup(resulttransport.ResultsTopic, int32(p))
			if !ok || end.Err != nil {
				return false
			}
			expectedNext += end.Offset
		}
		var next int64
		return r.pool.QueryRow(ctx, "SELECT sum(next_offset)::bigint FROM cad_v2.result_consumer_offsets").Scan(&next) == nil && next == expectedNext
	})
	last := p16Sample{At: time.Now()}
	if err := p16ReadProgress(ctx, r, &last); err != nil {
		t.Fatal(err)
	}
	p16SampleDetails(ctx, t, r, &last, masterURL, cpuPath)
	if err := enc.Encode(last); err != nil {
		t.Fatal(err)
	}
	if last.Master.Labels["master_kafka_diagnostics"] != "1" || last.Master.Gauges["results_kafka_max_poll_records"] != 16 || last.Master.Gauges["results_kafka_max_partition_handlers"] != 4 {
		t.Fatal("diagnostics or fixed Kafka settings missing", last.Master.Labels, last.Master.Gauges)
	}
	report.Joins, report.Finals, report.Finalized, report.Pending = last.Joins, last.Finals, last.Finalized, last.Pending
	report.Duplicates = int(last.Master.Counters["master_duplicates"])
	if report.Duplicates != 0 || last.Master.Counters["master_rejected"] != 0 {
		t.Fatal("duplicate or rejected envelopes", last.Master.Counters)
	}
	p16Verify(t, ctx, r, expected, counts, starts, &report)
	p16FinalTrace(t, master.logPath, ticks, &report)
	return report
}

func TestP16ShortKafkaSeries(t *testing.T) {
	root := os.Getenv("P16_SERIES_DIR")
	if root == "" {
		t.Skip("explicit isolated P16 series directory required")
	}
	if os.Getenv("P07_TEST_KAFKA_ISOLATED") != "1" || os.Getenv("P16_PG_OK") != "1" || os.Getenv("P16_KAFKA_OK") != "1" {
		t.Fatal("isolated fixture not ready")
	}
	if os.Getenv("P16_ROOT") == "" {
		t.Fatal("P16_ROOT must contain bin/master and bin/worker built from the test revision")
	}
	for _, binary := range []string{"master", "worker"} {
		path := filepath.Join(os.Getenv("P16_ROOT"), "bin", binary)
		if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
			t.Fatalf("missing executable test binary %s", path)
		}
	}
	const ticks = 288
	fixture := workerfixture.New(t, ticks, 0)
	var control *p16Report
	for n, workers := range []int{1, 4, 1, 4, 1, 4} {
		name := fmt.Sprintf("%02d-w%d", n+1, workers)
		t.Run(name, func(t *testing.T) {
			runDir := filepath.Join(root, name)
			if err := os.Mkdir(runDir, 0700); err != nil {
				t.Fatal(err)
			}
			report := p16Trial(t, runDir, &fixture, workers, ticks)
			if control == nil {
				control = &report
			} else if control.Digest != report.Digest || control.EventsDigest != report.EventsDigest || control.ParentEvents != report.ParentEvents ||
				!reflect.DeepEqual(control.RelativeInput, report.RelativeInput) || !reflect.DeepEqual(control.RelativeResults, report.RelativeResults) {
				t.Fatal("runs differ in payload, event digest, or relative partition offsets")
			}
			t.Logf("replicate %s: %d joins, %.3f group-ticks/s digest=%s", name, report.Joins, report.GroupTicksPerSecond, report.Digest)
		})
	}
}
