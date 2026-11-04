package master

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/httpinput"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

func RunCLI(ctx context.Context, args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("master", flag.ContinueOnError)
	flags.SetOutput(stderr)
	listen := flags.String("listen", "127.0.0.1:8080", "HTTP listen address")
	mode := flags.String("mode", "http_push", "polling | http_push | kafka")
	execution := flags.String("execution-id", "", "registered execution (required for polling/kafka)")
	interval := flags.Duration("poll-interval", 100*time.Millisecond, "Worker HTTP polling period")
	brokers := flags.String("kafka-brokers", "localhost:9092", "results Kafka seeds")
	resultsGroup := flags.String("results-group", "", "Kafka results Consumer Group; default execution-scoped")
	createResults := flags.Bool("create-results-topic", false, "explicitly initialize group.results")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *listen == "" {
		return fmt.Errorf("listen address required; no positional arguments")
	}
	dsn := os.Getenv("MASTER_DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("MASTER_DATABASE_URL required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("invalid MASTER_DATABASE_URL")
	}
	cfg.MaxConns = 16
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "2000"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("PostgreSQL pool unavailable")
	}
	defer pool.Close()
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = pool.Ping(bounded)
	cancel()
	if err != nil {
		return fmt.Errorf("PostgreSQL unavailable")
	}
	if *mode != "polling" && *mode != "http_push" && *mode != "kafka" {
		return fmt.Errorf("invalid output mode")
	}
	if *mode != "http_push" && *execution == "" {
		return fmt.Errorf("execution-id required for polling/kafka")
	}
	if *interval < time.Millisecond || *interval > time.Minute {
		return fmt.Errorf("poll-interval must be 1ms..1m")
	}
	ctx, cancelRuntime := context.WithCancel(ctx)
	defer cancelRuntime()
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	service := usecase.Master{Store: repo.MasterStore{DB: pool}}
	background := make(chan error, 1)
	if *mode == "http_push" {
		background <- nil
	} else {
		var registeredMode string
		if err := pool.QueryRow(ctx, `SELECT output_transport FROM cad_v2.executions WHERE execution_id=$1::uuid`, *execution).Scan(&registeredMode); err != nil || registeredMode != *mode {
			return fmt.Errorf("registered transport mismatch")
		}
		id, err := service.Store.Identity(ctx, *execution)
		if err != nil {
			return err
		}
		go func() {
			var err error
			if *mode == "polling" {
				err = (usecase.Poller{ExecutionID: *execution, Groups: id.Groups, Interval: *interval, Store: repo.MasterStore{DB: pool}, Master: service, Logger: log}).Run(ctx)
			} else {
				group := *resultsGroup
				if group == "" {
					group = "cad-results-" + *execution
				}
				err = RunKafka(ctx, strings.Split(*brokers, ","), group, *execution, *createResults, repo.MasterStore{DB: pool}, service)
			}
			background <- err
			if err != nil {
				cancelRuntime()
			}
		}()
	}
	server := &http.Server{Addr: *listen, Handler: httpinput.Handler(service, log), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	log.InfoContext(ctx, "Master started", "listen", *listen)
	var runErr error
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = err
		}
	case <-ctx.Done():
	}
	cancelRuntime()
	bounded, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	shutdownErr := server.Shutdown(bounded)
	if shutdownErr != nil {
		_ = server.Close()
	}
	backgroundErr := <-background
	if errors.Is(backgroundErr, context.Canceled) {
		backgroundErr = nil
	}
	return errors.Join(runErr, backgroundErr, shutdownErr)
}
