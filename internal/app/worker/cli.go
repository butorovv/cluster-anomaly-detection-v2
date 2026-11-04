package worker

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RunCLI never registers/adopts state implicitly. --register-execution is an
// explicit administrative mode; ordinary instances require existing immutable metadata.
func RunCLI(ctx context.Context, args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	assets := flags.String("assets", "", "immutable Summit assets root")
	profile := flags.String("profile-sha256", "", "trusted frozen profile SHA256")
	source := flags.String("input-sha256", "", "prepared input SHA256 binding")
	execution := flags.String("execution-id", "", "registered execution UUID")
	ownerID := flags.String("owner-id", "", "unique process owner UUID (random by default)")
	consumer := flags.String("consumer-group", "", "shared Kafka consumer group")
	brokers := flags.String("kafka-brokers", "localhost:9092", "comma-separated Kafka seeds")
	start := flags.String("start", analysis.NIRWindows()[1].Start.Format(time.RFC3339), "inclusive UTC run start")
	end := flags.String("end", analysis.NIRWindows()[1].End.Format(time.RFC3339), "exclusive UTC run end")
	diagnostics := flags.Bool("diagnostics", true, "durable local fragments for complete outside join; immutable execution contract")
	groups := flags.Int("groups", 16, "scientific group universe 4/8/16")
	delta := flags.Int("delta", 0, "frozen TTL: 0, 10, 30, 60")
	lease := flags.Duration("lease-ttl", repo.DefaultLease, "SQL lease TTL (3..15s)")
	register := flags.Bool("register-execution", false, "admin-only explicit run/execution registration, then exit")
	offsets := flags.String("start-offsets", "", "registration-only JSON object with explicit offsets 0..15")
	output := flags.String("output-transport", "polling", "reserved SQL experiment metadata; no dispatcher is started")
	listen := flags.String("listen", "127.0.0.1:0", "Worker HTTP listen address")
	advertise := flags.String("advertise-url", "", "reachable Worker URL; inferred only for loopback")
	masterURL := flags.String("master-url", "http://127.0.0.1:8080", "Master URL for http_push")
	createResults := flags.Bool("create-results-topic", false, "explicitly initialize/validate group.results")
	level := flags.String("log-level", "info", "info or debug (offsets only, no telemetry payload)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *assets == "" || *profile == "" || *source == "" || *execution == "" || *consumer == "" {
		return fmt.Errorf("assets/profile/input/execution/consumer-group required; no positional arguments")
	}
	if *lease < 3*time.Second || *lease > 15*time.Second {
		return fmt.Errorf("lease TTL must be 3..15 seconds")
	}
	var logLevel slog.Level
	if *level == "debug" {
		logLevel = slog.LevelDebug
	} else if *level != "info" {
		return fmt.Errorf("log-level must be info or debug")
	}
	log := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: logLevel}))
	w := analysis.Window{}
	var err error
	w.Start, err = time.Parse(time.RFC3339Nano, *start)
	if err != nil {
		return fmt.Errorf("invalid run start")
	}
	w.End, err = time.Parse(time.RFC3339Nano, *end)
	if err != nil {
		return fmt.Errorf("invalid run end")
	}
	if err = (domain.InputWindow{Start: w.Start, End: w.End}).Validate(); err != nil {
		return err
	}
	w.Start, w.End = w.Start.UTC(), w.End.UTC()
	bundle, err := group.LoadBundle(group.SummitRBI2Files(*assets, *profile))
	if err != nil {
		return err
	}
	opts := group.GroupProcessorOptions{GroupID: "a01", Groups: *groups, Window: w, DeltaSeconds: *delta, Binding: domain.InputBinding{ExecutionID: *execution, InputSHA256: *source, ProfileSHA256: *profile}}
	p, err := group.NewGroupProcessor(bundle, opts)
	if err != nil {
		return err
	}
	snap, err := p.Snapshot()
	if err != nil {
		return err
	}
	broker := kafkainput.Broker{Seeds: strings.Split(*brokers, ",")}
	bounded, cancel := context.WithTimeout(ctx, 20*time.Second)
	cluster, topic, err := kafkainput.DescribeSource(bounded, broker)
	cancel()
	if err != nil {
		return err
	}
	dsn := os.Getenv("WORKER_DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("WORKER_DATABASE_URL required (not logged)")
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("invalid WORKER_DATABASE_URL")
	}
	poolConfig.MaxConns = 8
	poolConfig.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	poolConfig.ConnConfig.RuntimeParams["lock_timeout"] = "2000"
	poolConfig.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "10000"
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("PostgreSQL pool unavailable")
	}
	defer pool.Close()
	r := repo.Registration{Identity: snap.Identity, Binding: opts.Binding, ClusterID: cluster, TopicID: topic, ConsumerGroup: *consumer, OutputTransport: *output, Diagnostics: *diagnostics}
	if *register {
		if err := json.Unmarshal([]byte(*offsets), &r.StartOffsets); err != nil {
			return fmt.Errorf("16 explicit registration start offsets required")
		}
		if err := repo.Register(ctx, pool, r); err != nil {
			return err
		}
		log.InfoContext(ctx, "execution registered", "execution", *execution, "run_id", snap.Identity.RunID, "topic_id", topic, "consumer_group", *consumer)
		return nil
	}
	if *offsets != "" {
		return fmt.Errorf("start-offsets only allowed with explicit registration")
	}
	r.StartOffsets, err = repo.ReadStartOffsets(ctx, pool, *execution)
	if err != nil {
		return err
	}
	return RunWithOutput(ctx, Config{Bundle: bundle, Options: opts, Broker: broker, ConsumerGroup: *consumer, OwnerID: *ownerID, LeaseTTL: *lease, Logger: log}, &repo.Store{DB: pool, Registration: r}, pool, OutputOptions{Listen: *listen, Advertise: *advertise, MasterURL: *masterURL, CreateResultsTopic: *createResults})
}
