package main

import (
	"context"
	"errors"
	"flag"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/app/master"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := master.RunCLI(ctx, os.Args[1:], os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("Master stopped", "error", err)
		os.Exit(1)
	}
}
