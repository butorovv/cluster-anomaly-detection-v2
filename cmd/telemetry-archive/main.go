package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/app/telemetryarchive"
	"github.com/jackc/pgx/v5"
)

func run(ctx context.Context, args []string) error {
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

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
