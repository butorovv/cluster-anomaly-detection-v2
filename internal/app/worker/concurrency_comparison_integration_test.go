package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
)

// Opt-in diagnostic comparison, not a system acceptance benchmark. Each trial
// has its own database/execution, four OS Workers, identical frozen inputs/V9,
// and profiling enabled on both builds. Raw snapshots and logs stay external.
func TestP15ComparableProcessTrials(t *testing.T) {
	if os.Getenv("P15_COMPARISON") != "1" {
		t.Skip("opt-in P15_COMPARISON=1 and P15_BASELINE_SOURCE required")
	}
	baseline := os.Getenv("P15_BASELINE_SOURCE")
	if baseline == "" || os.Getenv("P15_ARTIFACT_DIR") == "" {
		t.Fatal("baseline source and external artifact directory required")
	}
	frozen := workerfixture.New(t, 6, 30)
	root := concurrencyArtifacts(t, "comparison-"+time.Now().UTC().Format("20060102T150405"))
	binaries := map[string]map[string]string{}
	for _, version := range []string{"p15-2", "p15-3"} {
		binaries[version] = map[string]string{}
		for _, app := range []string{"master", "worker"} {
			binary := filepath.Join(root, version+"-"+app+".exe")
			command := exec.Command("go", "build", "-o", binary, "./cmd/"+app)
			command.Dir = filepath.Join("..", "..", "..")
			if version == "p15-2" {
				command.Dir = baseline
			}
			if b, err := command.CombinedOutput(); err != nil {
				t.Fatalf("build %s/%s: %v %s", version, app, err, b)
			}
			binaries[version][app] = binary
		}
	}
	for _, mode := range []string{"kafka", "polling", "http_push"} {
		for repeat := 0; repeat < 3; repeat++ {
			versions := []string{"p15-2", "p15-3"}
			if repeat%2 == 1 {
				versions[0], versions[1] = versions[1], versions[0]
			}
			for _, version := range versions {
				t.Run(fmt.Sprintf("%s/%d/%s", mode, repeat+1, version), func(t *testing.T) {
					rig := workerIntegrationFixture(t, 30, mode, &frozen, true)
					directory := filepath.Join(root, fmt.Sprintf("%s-%d-%s", mode, repeat+1, version))
					if err := os.MkdirAll(directory, 0700); err != nil {
						t.Fatal(err)
					}
					listener, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					address := listener.Addr().String()
					_ = listener.Close()
					endpoint := "http://" + address
					args := []string{"--listen", address, "--mode", mode, "--execution-id", rig.fixture.Options.Binding.ExecutionID, "--kafka-brokers", rig.broker.Seeds[0], "--create-results-topic", "--poll-interval", "20ms", "--measure=true"}
					if version == "p15-3" {
						// Match baseline PollRecords(128); all other candidate limits
						// remain defaults. Production default is a smaller wave of 16.
						args = append(args, "--kafka-poll-records", "128")
					}
					master := exec.Command(binaries[version]["master"], args...)
					master.Env = append(os.Environ(), "MASTER_DATABASE_URL="+rig.dsn, "GOMAXPROCS=2", "GOMEMLIMIT=256MiB", "CAD_PPROF=1")
					log, err := os.Create(filepath.Join(directory, "master.log"))
					if err != nil {
						t.Fatal(err)
					}
					master.Stdout, master.Stderr = log, log
					if err := master.Start(); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = master.Process.Kill(); _ = master.Wait(); _ = log.Close() })
					waitWorker(t, 15*time.Second, func() bool { _, err := readProcessMetrics(context.Background(), endpoint); return err == nil })
					processes := make([]*workerProcess, 4)
					for i := range processes {
						processes[i] = startWorkerProcess(t, rig, binaries[version]["worker"], directory, i, mode, endpoint)
					}
					owners := workerProcessesReady(t, rig, processes)
					file, err := os.Create(filepath.Join(directory, "samples.ndjson"))
					if err != nil {
						t.Fatal(err)
					}
					encoder := json.NewEncoder(file)
					sample := func() {
						metrics := map[string]observability.Data{}
						for _, url := range append([]string{endpoint}, processes[0].endpoint, processes[1].endpoint, processes[2].endpoint, processes[3].endpoint) {
							if d, err := readProcessMetrics(context.Background(), url); err == nil {
								metrics[url] = d
							}
						}
						var lockWaiters int
						_ = rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'`).Scan(&lockWaiters)
						_ = encoder.Encode(map[string]any{"at": time.Now().UTC(), "metrics": metrics, "sql_lock_waiters": lockWaiters})
					}
					sample()
					ctx, stop := context.WithCancel(context.Background())
					sampled := make(chan struct{})
					go func() {
						defer close(sampled)
						ticker := time.NewTicker(100 * time.Millisecond)
						defer ticker.Stop()
						for {
							select {
							case <-ctx.Done():
								return
							case <-ticker.C:
								sample()
							}
						}
					}()
					t.Cleanup(func() { stop(); <-sampled; _ = file.Close() })
					started := time.Now()
					for tick := 0; tick < 6; tick++ {
						rig.sendTick(t, tick)
					}
					rig.waitRevision(t, 6)
					waitWorker(t, 30*time.Second, func() bool {
						var results int
						return rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_results`).Scan(&results) == nil && results == 96
					})
					elapsed := time.Since(started)
					stop()
					<-sampled
					// Joining the sampler makes this final snapshot deterministic.
					sample()
					var fragments, joins int
					_ = rig.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM cad_v2.master_tick_fragments),(SELECT count(*) FROM cad_v2.master_tick_joins)`).Scan(&fragments, &joins)
					proof, _ := json.MarshalIndent(map[string]any{"version": version, "mode": mode, "repeat": repeat + 1, "execution": rig.fixture.Options.Binding.ExecutionID, "run_id": rig.store.Registration.Identity.RunID, "config_id": rig.store.Registration.Identity.ConfigID, "owners": owners, "seconds": elapsed.Seconds(), "fragments": fragments, "joins": joins, "finals": 96, "fragment_per_second": float64(fragments) / elapsed.Seconds(), "joins_per_second": float64(joins) / elapsed.Seconds(), "profiling_enabled": true, "input_ticks": 6, "groups": 16, "workers": 4}, "", "  ")
					if err := os.WriteFile(filepath.Join(directory, "summary.json"), proof, 0600); err != nil {
						t.Fatal(err)
					}
					t.Logf("elapsed=%s fragments=%d joins=%d artifacts=%s", elapsed, fragments, joins, directory)
				})
			}
		}
	}
}
