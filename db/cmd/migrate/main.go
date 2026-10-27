package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	"github.com/jackc/pgx/v5"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(ctx context.Context) error {
	if len(os.Args) != 1 {
		return fmt.Errorf("usage: DATABASE_URL=... go run ./db/cmd/migrate (up only)")
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return fmt.Errorf("DATABASE_URL required")
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL failed")
	}
	defer conn.Close(context.Background())
	n, err := db.Apply(ctx, conn, db.Files)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "applied %d migration(s)\n", n)
	return nil
}
