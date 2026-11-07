package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	masterapp "github.com/butorovv/cluster-anomaly-detection-v2/internal/app/master"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/resulttransport"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/twmb/franz-go/pkg/kadm"
)

type workerProcess struct {
	cmd             *exec.Cmd
	logPath         string
	log             *os.File
	owner, endpoint string
	once            sync.Once
}

func (p *workerProcess) stop() {
	p.once.Do(func() { _ = p.cmd.Process.Kill(); _ = p.cmd.Wait(); _ = p.log.Close() })
}
func concurrencyArtifacts(t *testing.T, name string) string {
	t.Helper()
	root := os.Getenv("P15_ARTIFACT_DIR")
	if root == "" {
		return t.TempDir()
	}
	path := filepath.Join(root, name)
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	return path
}
func buildWorkerProcess(t *testing.T, directory string) string {
	t.Helper()
	binary := filepath.Join(directory, "worker.exe")
	if output, err := exec.Command("go", "build", "-o", binary, "../../../cmd/worker").CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v %s", err, output)
	}
	return binary
}
func startWorkerProcess(t *testing.T, rig *workerRig, binary, directory string, index int, mode, masterURL string) *workerProcess {
	t.Helper()
	opts := rig.fixture.Options
	cmd := exec.Command(binary, "--assets", rig.fixture.Root, "--profile-sha256", opts.Binding.ProfileSHA256, "--input-sha256", opts.Binding.InputSHA256, "--execution-id", opts.Binding.ExecutionID, "--consumer-group", rig.group, "--kafka-brokers", strings.Join(rig.broker.Seeds, ","), "--groups", fmt.Sprint(opts.Groups), "--start", opts.Window.Start.Format(time.RFC3339), "--end", opts.Window.End.Format(time.RFC3339), "--delta", fmt.Sprint(opts.DeltaSeconds), "--lease-ttl", "15s", "--output-transport", mode, "--master-url", masterURL, "--measure=true")
	cmd.Env = append(os.Environ(), "WORKER_DATABASE_URL="+rig.dsn, "GOMAXPROCS=2", "GOMEMLIMIT=256MiB", "CAD_PPROF=1")
	path := filepath.Join(directory, fmt.Sprintf("worker-%d.log", index))
	log, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	p := &workerProcess{cmd: cmd, logPath: path, log: log}
	t.Cleanup(p.stop)
	return p
}
func (p *workerProcess) discover() bool {
	b, err := os.ReadFile(p.logPath)
	if err != nil {
		return false
	}
	scanner := bufio.NewScanner(bytes.NewReader(b))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		var entry map[string]json.RawMessage
		if json.Unmarshal(scanner.Bytes(), &entry) != nil {
			continue
		}
		var msg string
		_ = json.Unmarshal(entry["msg"], &msg)
		if msg == "Worker endpoint started" {
			_ = json.Unmarshal(entry["owner"], &p.owner)
			_ = json.Unmarshal(entry["endpoint"], &p.endpoint)
			return p.owner != "" && p.endpoint != ""
		}
	}
	return false
}
func readProcessMetrics(ctx context.Context, endpoint string) (observability.Data, error) {
	var data observability.Data
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/metrics", nil)
	if err != nil {
		return data, err
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Do(request)
	if err != nil {
		return data, err
	}
	defer response.Body.Close()
	err = json.NewDecoder(response.Body).Decode(&data)
	return data, err
}
func workerProcessesReady(t *testing.T, rig *workerRig, processes []*workerProcess) map[string][]string {
	t.Helper()
	owners := map[string][]string{}
	waitWorker(t, 45*time.Second, func() bool {
		for _, p := range processes {
			if !p.discover() {
				return false
			}
			response, err := (&http.Client{Timeout: time.Second}).Get(p.endpoint + "/readyz")
			if err != nil {
				return false
			}
			_ = response.Body.Close()
			if response.StatusCode != 200 {
				return false
			}
		}
		rows, err := rig.pool.Query(context.Background(), `SELECT owner_id::text,group_id FROM cad_v2.partition_ownership WHERE lease_until>clock_timestamp() ORDER BY group_id`)
		if err != nil {
			return false
		}
		defer rows.Close()
		owners = map[string][]string{}
		total := 0
		for rows.Next() {
			var owner, g string
			if err := rows.Scan(&owner, &g); err != nil {
				return false
			}
			owners[owner] = append(owners[owner], g)
			total++
		}
		if total != 16 || len(owners) != 4 {
			return false
		}
		for _, p := range processes {
			if len(owners[p.owner]) == 0 {
				return false
			}
		}
		return rows.Err() == nil
	})
	return owners
}

func TestFourWorkerProcessesProgressWithSlowGroup(t *testing.T) {
	if !workerTestsEnabled(t) {
		return
	}
	frozen := workerfixture.New(t, 6, 30)
	frozen.Options.Groups = 16
	rig := workerIntegrationFixture(t, 30, "kafka", &frozen, true)
	directory := concurrencyArtifacts(t, "four-workers-"+rig.fixture.Options.Binding.ExecutionID)
	binary := buildWorkerProcess(t, directory)
	consumer := "p15-four-" + rig.group
	rig.bindResults(t, consumer)
	store := restrictedMaster(t, rig)
	metrics := observability.New()
	service := usecase.Master{Store: store, Metrics: metrics}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- masterapp.RunKafka(ctx, rig.broker.Seeds, consumer, rig.fixture.Options.Binding.ExecutionID, true, store, service)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Master did not join")
		}
	})
	processes := make([]*workerProcess, 4)
	for i := range processes {
		processes[i] = startWorkerProcess(t, rig, binary, directory, i, "kafka", "")
	}
	owners := workerProcessesReady(t, rig, processes)
	for tick := 0; tick < 5; tick++ {
		for g := 1; g <= 15; g++ {
			for _, m := range rig.messages(fmt.Sprintf("a%02d", g), tick) {
				rig.send(t, m)
			}
		}
	}
	// a16 has received no marker. Each other group must reach tick4/offset,
	// and the three processes without a16 must publish their durable fragments.
	waitWorker(t, 25*time.Second, func() bool {
		var checkpoints, fragments int
		err := rig.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM cad_v2.group_checkpoints WHERE group_id<>'a16' AND checkpoint_revision=5),(SELECT count(*) FROM cad_v2.master_tick_fragments WHERE group_id<>'a16')`).Scan(&checkpoints, &fragments)
		return err == nil && checkpoints == 15 && fragments == 75
	})
	var finals, joins int
	if err := rig.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM cad_v2.master_results),(SELECT count(*) FROM cad_v2.master_tick_joins)`).Scan(&finals, &joins); err != nil || finals != 0 || joins != 0 {
		t.Fatal("incomplete timestamp finalized", finals, joins, err)
	}
	beforeRelease := map[string]observability.Data{}
	for _, p := range processes {
		data, err := readProcessMetrics(context.Background(), p.endpoint)
		if err != nil {
			t.Fatal(err)
		}
		beforeRelease[p.owner] = data
		slowOwner := false
		for _, g := range owners[p.owner] {
			slowOwner = slowOwner || g == "a16"
		}
		if !slowOwner && (data.Counters["processed_ticks"] < int64(5*len(owners[p.owner])) || data.Counters["checkpoint_count"] < int64(5*len(owners[p.owner])) || data.Counters["output_successful_sends"] < int64(5*len(owners[p.owner]))) {
			t.Fatal("independent OS Worker did not compute/checkpoint/deliver", p.cmd.Process.Pid, p.owner, data.Counters)
		}
	}
	proof := map[string]any{"owners": owners, "workers_before_release": beforeRelease, "durable_fast_group_checkpoints": 15, "master_fragments_before_release": 75, "finals_before_release": finals, "joins_before_release": joins}
	write := func(name string, v any) {
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, name), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("before-release.json", proof)
	for tick := 0; tick < 5; tick++ {
		for _, m := range rig.messages("a16", tick) {
			rig.send(t, m)
		}
	}
	rig.sendTick(t, 5)
	rig.waitRevision(t, 6)
	waitWorker(t, 25*time.Second, func() bool {
		var n int
		err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_results`).Scan(&n)
		return err == nil && n == 96
	})
	expected := diagnosticOracle(t, rig)
	rows, err := rig.pool.Query(context.Background(), `SELECT group_id,tick,payload FROM cad_v2.master_results ORDER BY tick,group_id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var g string
		var at time.Time
		var b []byte
		if err := rows.Scan(&g, &at, &b); err != nil {
			t.Fatal(err)
		}
		var got domain.GroupResult
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, expected[g+"/"+at.UTC().Format(time.RFC3339Nano)]) {
			t.Fatal("scientific result changed", g, at)
		}
	}
	rows.Close()
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	assertMaster(t, rig, store)
	var digestBefore, digestAfter string
	digestSQL := `SELECT encode(sha256(convert_to(jsonb_agg(to_jsonb(r) ORDER BY tick,group_id)::text,'UTF8')),'hex') FROM cad_v2.master_results r`
	if err := rig.pool.QueryRow(context.Background(), digestSQL).Scan(&digestBefore); err != nil {
		t.Fatal(err)
	}
	rows, err = rig.pool.Query(context.Background(), `SELECT envelope FROM cad_v2.master_tick_fragments WHERE event_time=$1 ORDER BY group_id`, rig.fixture.Options.Window.Start)
	if err != nil {
		t.Fatal(err)
	}
	duplicateBytes := [][]byte{}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			t.Fatal(err)
		}
		duplicateBytes = append(duplicateBytes, b)
	}
	rows.Close()
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	sender, err := resulttransport.NewKafka(context.Background(), rig.broker.Seeds, false)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	for _, b := range duplicateBytes {
		e, err := domain.Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		if err := sender.Send(context.Background(), b, e.GroupID); err != nil {
			t.Fatal(err)
		}
	}
	ends, err := kadm.NewClient(rig.producer).ListEndOffsets(context.Background(), resulttransport.ResultsTopic)
	if err != nil {
		t.Fatal(err)
	}
	waitWorker(t, 15*time.Second, func() bool {
		for p := int32(0); p < 16; p++ {
			offset, err := store.ResultOffset(context.Background(), rig.fixture.Options.Binding.ExecutionID, p)
			if err != nil || offset < ends[resulttransport.ResultsTopic][p].Offset {
				return false
			}
		}
		return true
	})
	if err := rig.pool.QueryRow(context.Background(), digestSQL).Scan(&digestAfter); err != nil || digestBefore != digestAfter {
		t.Fatal("duplicate changed scientific results", err)
	}
	if metrics.Snapshot().Counters["master_duplicates"] < 16 {
		t.Fatal("duplicate replay not observed")
	}
	write("final-proof.json", map[string]any{"results": 96, "expected_exact": true, "result_digest": digestAfter, "master_metrics": metrics.Snapshot()})
	t.Logf("four_os_worker_processes=true fast_fragments_before_release=75 incomplete_finals=0 final_results=96 scientific_parity=true proof=%s", directory)
}
