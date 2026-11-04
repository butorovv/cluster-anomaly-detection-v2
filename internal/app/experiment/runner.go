package experiment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	workerapp "github.com/butorovv/cluster-anomaly-detection-v2/internal/app/worker"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/resulttransport"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	eventstore "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type RunSpec struct {
	Name        string  `json:"name"`
	Workers     int     `json:"workers"`
	Groups      int     `json:"groups"`
	Delta       int     `json:"delta"`
	Mode        string  `json:"mode"`
	Cadence     int     `json:"cadence"`
	PollMS      int     `json:"poll_ms"`
	ReplaySpeed float64 `json:"replay_speed"`
	Fault       string  `json:"fault,omitempty"`
}
type Config struct {
	Measure        bool      `json:"measure"`
	SchemaVersion  int       `json:"schema_version"`
	Assets         string    `json:"assets"`
	ProfileSHA     string    `json:"profile_sha256"`
	InputSHA       string    `json:"input_sha256"`
	Brokers        string    `json:"brokers"`
	GoldenRoot     string    `json:"golden_root"`
	GoldenManifest string    `json:"golden_manifest"`
	Output         string    `json:"output"`
	TimeoutSeconds int       `json:"timeout_seconds"`
	Runs           []RunSpec `json:"runs"`
}
type Result struct {
	SchemaVersion  int               `json:"schema_version"`
	Status         string            `json:"status"`
	Spec           RunSpec           `json:"configuration"`
	ExecutionID    string            `json:"execution_id"`
	RunID          string            `json:"run_id"`
	ConfigID       string            `json:"config_id"`
	Started        time.Time         `json:"started_at"`
	Finished       time.Time         `json:"finished_at"`
	ElapsedSeconds float64           `json:"elapsed_seconds"`
	ReplaySeconds  float64           `json:"replay_seconds"`
	Comparison     Comparison        `json:"comparison"`
	Assignment     map[string]string `json:"initial_assignment"`
	NextOffsets    map[string]int64  `json:"next_offsets"`
	BinarySHA256   map[string]string `json:"binary_sha256"`
	Environment    map[string]any    `json:"environment"`
	LogDirectory   string            `json:"log_directory,omitempty"`
	Fault          map[string]any    `json:"fault,omitempty"`
	Measurements   *Measurements     `json:"measurements,omitempty"`
	Error          string            `json:"error,omitempty"`
}

func saveJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}
func RunCLI(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("experiment", flag.ContinueOnError)
	flags.SetOutput(stderr)
	resume := flags.Bool("resume", false, "reuse saved PASS runs with identical configuration; retry unfinished/failed runs")
	analyze := flags.String("analyze-existing", "", "recompute measurements from saved logs/samples without replay")
	path := flags.String("config", "", "JSON experiment configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *analyze != "" {
		return analyzeExisting(*analyze, stdout)
	}
	if *path == "" || flags.NArg() != 0 {
		return fmt.Errorf("--config required")
	}
	b, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	var cfg Config
	if err := domain.StrictJSON(b, &cfg); err != nil {
		return err
	}
	cfg.Assets = os.ExpandEnv(cfg.Assets)
	cfg.Brokers = os.ExpandEnv(cfg.Brokers)
	cfg.GoldenRoot = os.ExpandEnv(cfg.GoldenRoot)
	cfg.GoldenManifest = os.ExpandEnv(cfg.GoldenManifest)
	cfg.Output = os.ExpandEnv(cfg.Output)
	if cfg.SchemaVersion != 1 || len(cfg.Runs) == 0 || cfg.Assets == "" || cfg.Brokers == "" || cfg.GoldenRoot == "" || cfg.GoldenManifest == "" || cfg.Output == "" || os.Getenv("SYSTEM_TEST_DATABASE_URL") == "" || os.Getenv("SYSTEM_KAFKA_ISOLATED") != "1" {
		return fmt.Errorf("explicit configuration, SYSTEM_TEST_DATABASE_URL and SYSTEM_KAFKA_ISOLATED=1 required")
	}
	if cfg.TimeoutSeconds < 1 {
		cfg.TimeoutSeconds = 900
	}
	if err := os.Mkdir(cfg.Output, 0700); err != nil && !(*resume && os.IsExist(err)) {
		return fmt.Errorf("new artifact output directory required: %w", err)
	}
	binaries, hashes, err := build(ctx, cfg.Output)
	if err != nil {
		return err
	}
	results := []Result{}
	for _, spec := range cfg.Runs {
		if *resume {
			b, err := os.ReadFile(filepath.Join(cfg.Output, spec.Name+".json"))
			if err == nil {
				var previous Result
				if json.Unmarshal(b, &previous) != nil {
					return fmt.Errorf("invalid saved result %s", spec.Name)
				}
				a, _ := json.Marshal(previous.Spec)
				b, _ := json.Marshal(spec)
				if string(a) != string(b) {
					return fmt.Errorf("saved configuration differs for %s", spec.Name)
				}
				if previous.Status != "PASS" {
					preserved := filepath.Join(cfg.Output, spec.Name+".failed-"+previous.Finished.UTC().Format("20060102T150405.000000000")+".json")
					if _, err := os.Stat(preserved); os.IsNotExist(err) {
						if err := saveJSON(preserved, previous); err != nil {
							return err
						}
					}
				}
				if previous.Status == "PASS" && previous.Comparison.Status == "PASS" {
					if cfg.Measure && (previous.Measurements == nil || previous.Measurements.Status != "PASS") {
						return fmt.Errorf("saved PASS lacks requested measurements: use a new output directory for %s", spec.Name)
					}
					results = append(results, previous)
					fmt.Fprintf(stdout, "SKIP saved PASS %s\n", spec.Name)
					continue
				}
			}
			if err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		fmt.Fprintf(stdout, "RUN %s workers=%d groups=%d delta=%d mode=%s cadence=%d\n", spec.Name, spec.Workers, spec.Groups, spec.Delta, spec.Mode, spec.Cadence)
		bounded, cancel := context.WithTimeout(ctx, time.Duration(cfg.TimeoutSeconds)*time.Second)
		result, err := runOne(bounded, cfg, spec, binaries, hashes)
		cancel()
		if err != nil {
			result.Status = "FAIL"
			result.Error = err.Error()
		}
		results = append(results, result)
		if err := saveJSON(filepath.Join(cfg.Output, spec.Name+".json"), result); err != nil {
			return err
		}
		if err := saveJSON(filepath.Join(cfg.Output, "matrix.json"), results); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s %s elapsed=%.3fs raw=%d positive=%d events=%d\n", result.Status, spec.Name, result.ElapsedSeconds, result.Comparison.Raw, result.Comparison.Positive, result.Comparison.Events)
		if err != nil {
			return err
		}
	}
	return nil
}
func build(ctx context.Context, out string) (map[string]string, map[string]string, error) {
	binaries, hashes := map[string]string{}, map[string]string{}
	suffix := ""
	if runtime.GOOS == "windows" {
		suffix = ".exe"
	}
	binDir := filepath.Join(out, "bin")
	if err := os.MkdirAll(binDir, 0700); err != nil {
		return nil, nil, err
	}
	for _, name := range []string{"worker", "master", "replayer"} {
		binary, err := filepath.Abs(filepath.Join(binDir, name+suffix))
		if err != nil {
			return nil, nil, err
		}
		cmd := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-o", binary, "./cmd/"+name)
		if b, err := cmd.CombinedOutput(); err != nil {
			return nil, nil, fmt.Errorf("build %s: %w %s", name, err, b)
		}
		b, err := os.ReadFile(binary)
		if err != nil {
			return nil, nil, err
		}
		hashes[name] = domain.Hash(b)
		binaries[name] = binary
	}
	return binaries, hashes, nil
}

type process struct {
	cmd             *exec.Cmd
	log             *os.File
	done            chan struct{}
	err             error
	owner, endpoint string
}

func startProcess(ctx context.Context, binary, logPath string, args []string, env []string) (*process, error) {
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = f
	cmd.Stderr = f
	p := &process{cmd: cmd, log: f, done: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return nil, err
	}
	go func() { p.err = cmd.Wait(); _ = f.Close(); close(p.done) }()
	return p, nil
}
func (p *process) stop() {
	if p == nil {
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Kill()
	<-p.done
}
func (p *process) failure() error {
	select {
	case <-p.done:
		if p.err != nil {
			return fmt.Errorf("process pid=%d: %w", p.cmd.Process.Pid, p.err)
		}
		return fmt.Errorf("process exited before completion pid=%d", p.cmd.Process.Pid)
	default:
		return nil
	}
}
func freeEndpoint() (string, string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", "", err
	}
	address := l.Addr().String()
	_ = l.Close()
	return address, "http://" + address, nil
}
func wait(ctx context.Context, condition func() (bool, error), processes func() []*process) error {
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := condition()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		for _, p := range processes() {
			if err := p.failure(); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func runOne(ctx context.Context, cfg Config, spec RunSpec, binaries, hashes map[string]string) (report Result, retErr error) {
	report = Result{SchemaVersion: 1, Status: "FAIL", Spec: spec, Started: time.Now().UTC(), Assignment: map[string]string{}, NextOffsets: map[string]int64{}, BinarySHA256: hashes, Environment: map[string]any{"go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "logical_cpus": runtime.NumCPU(), "host_cpu_identifier": os.Getenv("PROCESSOR_IDENTIFIER"), "worker_gomaxprocs": 1, "result_compression": "none (HTTP and Kafka); unchanged input Snappy", "master_gomaxprocs": 2, "reference_source_commit": "c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc", "latency_clock": "same-host processing wall/monotonic; no historical event_time subtraction"}}
	start := time.Now()
	defer func() { report.Finished = time.Now().UTC(); report.ElapsedSeconds = time.Since(start).Seconds() }()
	if spec.Name == "" || strings.ContainsAny(spec.Name, "/\\:") || spec.Workers < 1 || spec.Workers > 16 || (spec.Groups != 4 && spec.Groups != 8 && spec.Groups != 16) || (spec.Delta != 0 && spec.Delta != 10 && spec.Delta != 30 && spec.Delta != 60) || (spec.Cadence != 1 && spec.Cadence != 3 && spec.Cadence != 6) || (spec.Mode != "polling" && spec.Mode != "http_push" && spec.Mode != "kafka") {
		return report, fmt.Errorf("invalid run configuration")
	}
	reference := filepath.Join(cfg.GoldenRoot, fmt.Sprintf("g%d-d%d", spec.Groups, spec.Delta))
	if err := verifyReference(cfg.GoldenManifest, reference, spec.Groups, spec.Delta); err != nil {
		return report, err
	}
	execution, err := workerapp.NewOwnerID()
	if err != nil {
		return report, err
	}
	report.ExecutionID = execution
	logDir := filepath.Join(cfg.Output, spec.Name)
	if _, err := os.Stat(logDir); err == nil {
		logDir += ".attempt-" + execution
	}
	report.LogDirectory = logDir
	if err := os.Mkdir(logDir, 0700); err != nil {
		return report, err
	}
	admin, err := pgx.Connect(ctx, os.Getenv("SYSTEM_TEST_DATABASE_URL"))
	if err != nil {
		return report, fmt.Errorf("maintenance PostgreSQL unavailable")
	}
	defer admin.Close(context.Background())
	dbName := "system_run_" + strings.ReplaceAll(execution, "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
		return report, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)"); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()
	u, err := url.Parse(os.Getenv("SYSTEM_TEST_DATABASE_URL"))
	if err != nil || !strings.HasPrefix(u.Scheme, "postgres") {
		return report, fmt.Errorf("PostgreSQL URL required")
	}
	u.Path = "/" + dbName
	dsn := u.String()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return report, err
	}
	defer pool.Close()
	connection, err := pool.Acquire(ctx)
	if err != nil {
		return report, err
	}
	_, err = db.Apply(ctx, connection.Conn(), db.Files)
	connection.Release()
	if err != nil {
		return report, err
	}
	var maxConnections, buffers, fsync, synchronous string
	if err := pool.QueryRow(ctx, "SHOW max_connections").Scan(&maxConnections); err != nil {
		return report, err
	}
	_ = pool.QueryRow(ctx, "SHOW shared_buffers").Scan(&buffers)
	_ = pool.QueryRow(ctx, "SHOW fsync").Scan(&fsync)
	_ = pool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&synchronous)
	report.Environment["postgres_max_connections"] = maxConnections
	report.Environment["postgres_shared_buffers"] = buffers
	report.Environment["postgres_fsync"] = fsync
	report.Environment["postgres_synchronous_commit"] = synchronous
	seeds := strings.Split(cfg.Brokers, ",")
	client, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return report, err
	}
	defer client.Close()
	kafkaAdmin := kadm.NewClient(client)
	policy := "delete"
	created, err := kafkaAdmin.CreateTopics(ctx, 16, 1, map[string]*string{"cleanup.policy": &policy}, kafkainput.Topic)
	if err != nil {
		return report, err
	}
	if err := created[kafkainput.Topic].Err; err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return report, err
	}
	broker := kafkainput.Broker{Seeds: seeds}
	cluster, topic, err := kafkainput.DescribeSource(ctx, broker)
	if err != nil {
		return report, err
	}
	report.Environment["kafka_cluster_id"] = cluster
	report.Environment["input_topic_id"] = topic
	ends, err := kafkaAdmin.ListEndOffsets(ctx, kafkainput.Topic)
	if err != nil {
		return report, err
	}
	offsets := map[string]int64{}
	for p := int32(0); p < 16; p++ {
		v, ok := ends[kafkainput.Topic][p]
		if !ok || v.Err != nil {
			return report, fmt.Errorf("input end offsets unavailable")
		}
		offsets[strconv.Itoa(int(p))] = v.Offset
	}
	bundle, err := group.LoadBundle(group.SummitRBI2Files(cfg.Assets, cfg.ProfileSHA))
	if err != nil {
		return report, err
	}
	window := analysis.NIRWindows()[1]
	binding := telemetry.InputBinding{ExecutionID: execution, ProfileSHA256: cfg.ProfileSHA, InputSHA256: cfg.InputSHA}
	options := group.GroupProcessorOptions{GroupID: "a01", Groups: spec.Groups, Window: window, DeltaSeconds: spec.Delta, Binding: binding}
	processor, err := group.NewGroupProcessor(bundle, options)
	if err != nil {
		return report, err
	}
	snap, err := processor.Snapshot()
	if err != nil {
		return report, err
	}
	report.RunID, report.ConfigID = snap.Identity.RunID, snap.Identity.ConfigID
	registration := checkpoints.Registration{Identity: snap.Identity, Binding: binding, ClusterID: cluster, TopicID: topic, ConsumerGroup: "system-" + execution, StartOffsets: offsets, OutputTransport: spec.Mode, Diagnostics: true, CheckpointIntervalTicks: spec.Cadence}
	if err := checkpoints.Register(ctx, pool, registration); err != nil {
		return report, err
	}
	// Register an explicit output start before any Worker may publish. This avoids
	// charging earlier independent executions to a fresh Kafka benchmark, while
	// restart still restores the same durable output cursor and topic incarnation.
	if spec.Mode == "kafka" {
		sender, err := resulttransport.NewKafka(ctx, seeds, true)
		if err != nil {
			return report, err
		}
		pin := sender.Identity
		_ = sender.Close()
		ends, err := kafkaAdmin.ListEndOffsets(ctx, resulttransport.ResultsTopic)
		if err != nil {
			return report, err
		}
		starts := map[string]int64{}
		for p := int32(0); p < 16; p++ {
			entry, ok := ends[resulttransport.ResultsTopic][p]
			if !ok || entry.Err != nil {
				return report, fmt.Errorf("results initial end unavailable")
			}
			starts[strconv.Itoa(int(p))] = entry.Offset
		}
		if err := (eventstore.MasterStore{DB: pool}).BindResults(ctx, execution, pin.ClusterID, pin.TopicID, "cad-results-"+execution, starts); err != nil {
			return report, err
		}
	}
	masterAddress, masterURL, err := freeEndpoint()
	if err != nil {
		return report, err
	}
	sampler := newSampler()
	if cfg.Measure {
		defer sampler.Stop()
	}
	masterArgs := []string{"--listen", masterAddress, "--mode", spec.Mode, "--execution-id", execution, "--kafka-brokers", cfg.Brokers, "--create-results-topic"}
	if spec.PollMS < 1 {
		spec.PollMS = 100
	}
	masterArgs = append(masterArgs, "--poll-interval", fmt.Sprintf("%dms", spec.PollMS))
	if cfg.Measure {
		masterArgs = append(masterArgs, "--measure=true")
	}
	master, err := startProcess(ctx, binaries["master"], filepath.Join(logDir, "master.log"), masterArgs, []string{"MASTER_DATABASE_URL=" + dsn, "GOMAXPROCS=2"})
	if err != nil {
		return report, err
	}
	defer func() { master.stop() }()
	if cfg.Measure {
		sampler.Add("master", masterURL)
	}
	workers := []*process{}
	defer func() {
		for _, p := range workers {
			p.stop()
		}
	}()
	startWorker := func(index int) (*process, error) {
		address, endpoint, err := freeEndpoint()
		if err != nil {
			return nil, err
		}
		owner, err := workerapp.NewOwnerID()
		if err != nil {
			return nil, err
		}
		args := []string{"--assets", cfg.Assets, "--profile-sha256", cfg.ProfileSHA, "--input-sha256", cfg.InputSHA, "--execution-id", execution, "--owner-id", owner, "--consumer-group", registration.ConsumerGroup, "--kafka-brokers", cfg.Brokers, "--groups", strconv.Itoa(spec.Groups), "--delta", strconv.Itoa(spec.Delta), "--lease-ttl", "15s", "--diagnostics=true", "--checkpoint-interval-ticks", strconv.Itoa(spec.Cadence), "--output-transport", spec.Mode, "--master-url", masterURL, "--listen", address, "--advertise-url", endpoint, "--create-results-topic"}
		if cfg.Measure {
			args = append(args, "--measure=true")
		}
		p, err := startProcess(ctx, binaries["worker"], filepath.Join(logDir, fmt.Sprintf("worker-%02d.log", index)), args, []string{"WORKER_DATABASE_URL=" + dsn, "GOMAXPROCS=1"})
		if err == nil {
			p.owner, p.endpoint = owner, endpoint
			if cfg.Measure {
				sampler.Add(owner, endpoint)
			}
		}
		return p, err
	}
	for i := 0; i < spec.Workers; i++ {
		p, err := startWorker(i)
		if err != nil {
			return report, err
		}
		workers = append(workers, p)
	}
	active := func() []*process { return append(append([]*process{}, workers...), master) }
	if err := wait(ctx, func() (bool, error) {
		rows, err := pool.Query(ctx, `SELECT group_id,owner_id::text FROM cad_v2.partition_ownership WHERE owner_id IS NOT NULL AND lease_until>clock_timestamp()`)
		if err != nil {
			return false, err
		}
		assignment := map[string]string{}
		owners := map[string]bool{}
		for rows.Next() {
			var g, o string
			if err := rows.Scan(&g, &o); err != nil {
				rows.Close()
				return false, err
			}
			assignment[g] = o
			owners[o] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return false, err
		}
		ok := len(assignment) == spec.Groups && len(owners) == min(spec.Workers, spec.Groups)
		if ok {
			report.Assignment = assignment
		}
		return ok, nil
	}, active); err != nil {
		return report, err
	}
	replayArgs := []string{"--input", filepath.Join(cfg.Assets, "data/prepared/summit-D1-pilot/D1.parquet"), "--speed", strconv.FormatFloat(spec.ReplaySpeed, 'g', -1, 64), "--start", window.Start.Format(time.RFC3339), "--end", window.End.Format(time.RFC3339), "--completion-mode", "full-grid", "--wire-format", "v1", "--execution-id", execution, "--input-sha256", cfg.InputSHA, "--profile-sha256", cfg.ProfileSHA, "--output", "kafka", "--kafka-brokers", cfg.Brokers}
	if cfg.Measure {
		sampler.Start(ctx)
	}
	replayStart := time.Now()
	replayer, err := startProcess(ctx, binaries["replayer"], filepath.Join(logDir, "replayer.log"), replayArgs, []string{"GOMAXPROCS=1"})
	if err != nil {
		return report, err
	}
	defer replayer.stop()
	if spec.Fault != "" {
		faultStart := time.Now()
		switch spec.Fault {
		case "kill_worker":
			var killedOwner string
			var killedAt time.Time
			if err := wait(ctx, func() (bool, error) {
				g, owner, eventID, epoch, offset, tx, err := lockOpenEvent(ctx, pool)
				if errors.Is(err, pgx.ErrNoRows) {
					return false, nil
				}
				if err != nil {
					return false, err
				}
				var victim *process
				index := -1
				for i, p := range workers {
					if p.owner == owner {
						victim = p
						index = i
						break
					}
				}
				if victim == nil {
					_ = tx.Rollback(ctx)
					return false, fmt.Errorf("open event owner process absent")
				}
				killedAt = time.Now()
				victim.stop()
				_ = tx.Rollback(context.Background())
				replacement, err := startWorker(len(workers))
				if err != nil {
					return false, err
				}
				workers[index] = replacement
				killedOwner = owner
				report.Fault = map[string]any{"kind": "kill_worker", "group": g, "event_id": eventID, "old_owner": owner, "old_epoch": epoch, "durable_next_offset_at_kill": offset, "pid": victim.cmd.Process.Pid, "killed_at": killedAt.UTC()}
				return true, nil
			}, active); err != nil {
				return report, err
			}
			g := report.Fault["group"].(string)
			oldOffset := report.Fault["durable_next_offset_at_kill"].(int64)
			if err := wait(ctx, func() (bool, error) {
				var current string
				var epoch, next int64
				err := pool.QueryRow(ctx, `SELECT coalesce(o.owner_id::text,''),o.epoch,c.next_offset FROM cad_v2.partition_ownership o JOIN cad_v2.group_checkpoints c USING(execution_id,group_id) WHERE o.group_id=$1`, g).Scan(&current, &epoch, &next)
				if err != nil {
					return false, err
				}
				if current != "" && current != killedOwner && next > oldOffset {
					report.Fault["new_owner"] = current
					report.Fault["new_epoch"] = epoch
					report.Fault["time_to_resumed_checkpoint_seconds"] = time.Since(killedAt).Seconds()
					return true, nil
				}
				return false, nil
			}, active); err != nil {
				return report, err
			}
		case "rebalance":
			if err := wait(ctx, func() (bool, error) {
				var ready bool
				err := pool.QueryRow(ctx, `SELECT coalesce(bool_or(checkpoint_revision>=3 AND NOT finalized),false) FROM cad_v2.group_checkpoints`).Scan(&ready)
				return ready, err
			}, active); err != nil {
				return report, err
			}
			extra, err := startWorker(len(workers))
			if err != nil {
				return report, err
			}
			workers = append(workers, extra)
			if err := wait(ctx, func() (bool, error) {
				var owners int
				err := pool.QueryRow(ctx, `SELECT count(DISTINCT owner_id) FROM cad_v2.partition_ownership WHERE lease_until>clock_timestamp()`).Scan(&owners)
				return owners == min(len(workers), spec.Groups), err
			}, active); err != nil {
				return report, err
			}
			report.Fault = map[string]any{"kind": "rebalance", "workers_before": spec.Workers, "workers_after": len(workers), "seconds": time.Since(faultStart).Seconds()}
		case "restart_master":
			if err := wait(ctx, func() (bool, error) {
				var joined int
				err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_tick_joins`).Scan(&joined)
				return joined > 0 && joined < 2880, err
			}, active); err != nil {
				return report, err
			}
			master.stop()
			replacement, err := startProcess(ctx, binaries["master"], filepath.Join(logDir, "master-restart.log"), masterArgs, []string{"MASTER_DATABASE_URL=" + dsn, "GOMAXPROCS=2"})
			if err != nil {
				return report, err
			}
			master = replacement
			report.Fault = map[string]any{"kind": "restart_master", "restarted_at": time.Now().UTC()}
		default:
			return report, fmt.Errorf("unsupported fault configuration")
		}
	}
	select {
	case <-ctx.Done():
		return report, ctx.Err()
	case <-replayer.done:
		if replayer.err != nil {
			return report, replayer.err
		}
	}
	report.ReplaySeconds = time.Since(replayStart).Seconds()
	if err := wait(ctx, func() (bool, error) {
		var finalized, results int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.group_checkpoints WHERE finalized`).Scan(&finalized); err != nil {
			return false, err
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_results`).Scan(&results); err != nil {
			return false, err
		}
		return finalized == spec.Groups && results == 2880*spec.Groups, nil
	}, active); err != nil {
		return report, err
	}
	if cfg.Measure {
		sampler.Stop()
		if err := saveJSON(filepath.Join(logDir, "metrics-samples.json"), sampler.Samples()); err != nil {
			return report, err
		}
	}
	store := checkpoints.Store{DB: pool, Registration: registration}
	finalEnds, err := kafkaAdmin.ListEndOffsets(ctx, kafkainput.Topic)
	if err != nil {
		return report, err
	}
	for g := 1; g <= spec.Groups; g++ {
		groupID := fmt.Sprintf("a%02d", g)
		saved, err := store.Load(ctx, groupID)
		if err != nil || saved == nil {
			return report, fmt.Errorf("final checkpoint missing: %v", err)
		}
		opts := options
		opts.GroupID = groupID
		p, err := core.NewPartition(bundle, opts, offsets[strconv.Itoa(g-1)], saved)
		if err != nil || !p.Finished() {
			return report, fmt.Errorf("final restore invalid: %v", err)
		}
		if saved.NextOffset != finalEnds[kafkainput.Topic][int32(g-1)].Offset {
			return report, fmt.Errorf("final authoritative cursor mismatch %s", groupID)
		}
		report.NextOffsets[groupID] = saved.NextOffset
	}
	comparison, err := Compare(ctx, pool, execution, reference)
	report.Comparison = comparison
	if err != nil {
		return report, err
	}
	if spec.Fault == "kill_worker" {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cad_v2.master_events WHERE event_id=$1)`, report.Fault["event_id"]).Scan(&exists); err != nil || !exists {
			return report, fmt.Errorf("captured open EventID missing after recovery: %v", err)
		}
		report.Fault["event_id_preserved"] = true
	}
	if cfg.Measure {
		sampler.Stop()
		if err := saveJSON(filepath.Join(logDir, "metrics-samples.json"), sampler.Samples()); err != nil {
			return report, err
		}
		measured, err := measureLogs(logDir, report.RunID, spec.Groups, sampler)
		report.Measurements = &measured
		if err != nil {
			return report, fmt.Errorf("measurement: %w", err)
		}
	}
	report.Status = "PASS"
	return report, nil
}
func verifyReference(manifestPath, directory string, groups, delta int) error {
	b, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	var runs []struct {
		Groups int    `json:"groups"`
		Delta  int    `json:"delta"`
		Commit string `json:"source_commit"`
		Files  []struct {
			Path string `json:"path"`
			SHA  string `json:"sha256"`
		} `json:"files"`
	}
	if err := json.Unmarshal(b, &runs); err != nil {
		return err
	}
	for _, run := range runs {
		if run.Groups != groups || run.Delta != delta {
			continue
		}
		if run.Commit != "c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc" {
			return fmt.Errorf("untrusted reference source pin")
		}
		seen := map[string]bool{}
		for _, file := range run.Files {
			path := filepath.Join(directory, filepath.Base(file.Path))
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			h := sha256.New()
			_, err = io.Copy(h, f)
			_ = f.Close()
			if err != nil {
				return err
			}
			if hex.EncodeToString(h.Sum(nil)) != file.SHA {
				return fmt.Errorf("independent reference SHA mismatch %s", path)
			}
			seen[filepath.Base(path)] = true
		}
		for _, name := range []string{"raw.ndjson", "temporal.ndjson", "evidence.ndjson", "events.ndjson", "summary.json"} {
			if !seen[name] {
				return fmt.Errorf("reference manifest lacks %s", name)
			}
		}
		return nil
	}
	return fmt.Errorf("independent reference missing groups=%d delta=%d", groups, delta)
}

// Holding only the existing SQL owner row stabilizes the durable open snapshot
// while the process is killed. No synthetic event or alternative fencing is used.
func lockOpenEvent(ctx context.Context, pool *pgxpool.Pool) (string, string, string, int64, int64, pgx.Tx, error) {
	var g string
	err := pool.QueryRow(ctx, `SELECT group_id FROM cad_v2.group_checkpoints WHERE NOT finalized AND convert_from(snapshot,'UTF8')::jsonb#>'{journal,open}'<>'null'::jsonb LIMIT 1`).Scan(&g)
	if err != nil {
		return "", "", "", 0, 0, nil, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", "", "", 0, 0, nil, err
	}
	var owner string
	var epoch, offset int64
	if err := tx.QueryRow(ctx, `SELECT owner_id::text,epoch FROM cad_v2.partition_ownership WHERE group_id=$1 AND lease_until>clock_timestamp() FOR UPDATE`, g).Scan(&owner, &epoch); err != nil {
		_ = tx.Rollback(ctx)
		return "", "", "", 0, 0, nil, err
	}
	var bytes []byte
	if err := tx.QueryRow(ctx, `SELECT snapshot,next_offset FROM cad_v2.group_checkpoints WHERE group_id=$1`, g).Scan(&bytes, &offset); err != nil {
		_ = tx.Rollback(ctx)
		return "", "", "", 0, 0, nil, err
	}
	var snap group.GroupProcessorSnapshot
	if err := json.Unmarshal(bytes, &snap); err != nil {
		_ = tx.Rollback(ctx)
		return "", "", "", 0, 0, nil, err
	}
	if snap.Journal.Open == nil {
		_ = tx.Rollback(ctx)
		return "", "", "", 0, 0, nil, pgx.ErrNoRows
	}
	return g, owner, snap.Journal.Open.ID, epoch, offset, tx, nil
}
