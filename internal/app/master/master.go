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
	"time"
)

func RunCLI(ctx context.Context, args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("master", flag.ContinueOnError)
	flags.SetOutput(stderr)
	listen := flags.String("listen", "127.0.0.1:8080", "HTTP listen address")
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
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	service := usecase.Master{Store: repo.MasterStore{DB: pool}}
	server := &http.Server{Addr: *listen, Handler: httpinput.Handler(service, log), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	log.InfoContext(ctx, "Master started", "listen", *listen)
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		bounded, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(bounded); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}
