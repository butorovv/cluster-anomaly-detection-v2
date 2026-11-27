package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/app/telemetryarchive"
	archive "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/archive/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func run(ctx context.Context, args []string) error {
	if len(args) > 0 && args[0] == "serve" {
		return serve(ctx, args[1:])
	}
	if len(args) > 0 && (args[0] == "consume" || args[0] == "init-stream" || args[0] == "stream-progress") {
		return streamCommand(ctx, args[0], args[1:])
	}
	if len(args) == 0 || args[0] != "import-summit" {
		return fmt.Errorf("usage: telemetry-archive import-summit --input <prepared Summit Parquet>")
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	input := flags.String("input", "", "path to the registered Summit Parquet")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *input == "" || flags.NArg() != 0 || os.Getenv("ARCHIVE_DATABASE_URL") == "" {
		return fmt.Errorf("--input and ARCHIVE_DATABASE_URL are required")
	}
	conn, err := pgx.Connect(ctx, os.Getenv("ARCHIVE_DATABASE_URL"))
	if err != nil {
		return fmt.Errorf("archive database unavailable")
	}
	defer conn.Close(context.Background())
	manifest, err := telemetryarchive.ImportSummit(ctx, conn, *input)
	if err != nil {
		return fmt.Errorf("Summit import: %w", err)
	}
	return json.NewEncoder(os.Stdout).Encode(manifest)
}

func streamCommand(ctx context.Context, operation string, args []string) error {
	flags := flag.NewFlagSet(operation, flag.ContinueOnError)
	id := flags.String("execution", "", "execution UUID")
	source := flags.String("source", telemetryarchive.SummitSHA, "source SHA256")
	profile := flags.String("profile", "", "profile SHA256")
	from := flags.String("from", "", "inclusive UTC start")
	to := flags.String("to", "", "exclusive UTC end")
	brokers := flags.String("brokers", "127.0.0.1:29186", "Kafka bootstrap servers")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || os.Getenv("ARCHIVE_DATABASE_URL") == "" {
		return fmt.Errorf("invalid stream arguments or missing archive database configuration")
	}
	pool, err := pgxpool.New(ctx, os.Getenv("ARCHIVE_DATABASE_URL"))
	if err != nil {
		return fmt.Errorf("archive database configuration unavailable")
	}
	defer pool.Close()
	if operation == "init-stream" {
		if err := telemetryarchive.InitializeSource(ctx, pool, *source); err != nil {
			return err
		}
		_, err := archive.New(ctx, pool, 18)
		return err
	}
	end, err := time.Parse(time.RFC3339, *to)
	if err != nil {
		return err
	}
	if operation == "stream-progress" {
		var partitions, ticks int64
		if err := pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE next_tick=$2)
 FROM cad_archive.positions WHERE execution_id=$1::uuid`, *id, end).Scan(&partitions, &ticks); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"partitions": partitions, "finished_partitions": ticks, "complete": ticks == 16})
	}
	start, err := time.Parse(time.RFC3339, *from)
	if err != nil {
		return err
	}
	config := telemetryarchive.StreamConfig{Binding: domain.InputBinding{ExecutionID: *id, InputSHA256: *source, ProfileSHA256: *profile},
		Window: domain.InputWindow{Start: start, End: end}, Broker: kafkainput.Broker{Seeds: strings.Split(*brokers, ",")}}
	for {
		err := telemetryarchive.Consume(ctx, pool, config)
		if !retryableArchiveError(err) || ctx.Err() != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "Temporary archive failure; restoring PostgreSQL positions.")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func retryableArchiveError(err error) bool {
	if errors.Is(err, archive.ErrTemporary) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) ||
		pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
		return true
	}
	var networkErr *net.OpError
	if errors.As(err, &networkErr) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (strings.HasPrefix(pgErr.Code, "08") || pgErr.Code == "57P01" ||
		pgErr.Code == "40001" || pgErr.Code == "40P01")
}

func serve(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	listen := flags.String("listen", "127.0.0.1:18087", "loopback REST address")
	review := flags.String("review-root", "", "trusted catalog of verified baseline snapshots")
	runs := flags.String("runs-root", "", "trusted catalog of managed launches")
	if err := flags.Parse(args); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(*listen)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() ||
		*review == "" || *runs == "" || flags.NArg() != 0 || os.Getenv("ARCHIVE_READ_URL") == "" {
		return fmt.Errorf("serve requires a loopback listener, both catalog roots and ARCHIVE_READ_URL")
	}
	config, err := pgxpool.ParseConfig(os.Getenv("ARCHIVE_READ_URL"))
	if err != nil {
		return fmt.Errorf("invalid archive read configuration")
	}
	config.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("archive database unavailable")
	}
	defer pool.Close()
	reader, err := archive.NewReader(ctx, pool)
	if err != nil {
		return fmt.Errorf("cannot read archive inventory")
	}
	var stream telemetryarchive.StreamReader
	if endpoint := os.Getenv("ARCHIVE_STREAM_READ_URL"); endpoint != "" {
		config, err := pgxpool.ParseConfig(endpoint)
		if err != nil {
			return fmt.Errorf("invalid stream archive configuration")
		}
		config.MaxConns = 4
		streamPool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			return fmt.Errorf("stream archive database unavailable")
		}
		defer streamPool.Close()
		stream, err = archive.NewReader(ctx, streamPool)
		if err != nil {
			return fmt.Errorf("cannot read stream archive inventory")
		}
	}
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	catalog := &telemetryarchive.Catalog{RunsRoot: *runs, ReviewRoot: *review, Client: client}
	catalog.Prime(ctx, slog.Default())
	server := &http.Server{Addr: *listen, Handler: telemetryarchive.Handler(catalog, reader, stream),
		ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 8 * time.Second, MaxHeaderBytes: 8192}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
