package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/app/telemetryarchive"
	archive "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/archive/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func run(ctx context.Context, args []string) error {
	if len(args) > 0 && args[0] == "serve" {
		return serve(ctx, args[1:])
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
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	catalog := &telemetryarchive.Catalog{RunsRoot: *runs, ReviewRoot: *review, Client: client}
	server := &http.Server{Addr: *listen, Handler: telemetryarchive.Handler(catalog, reader),
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
