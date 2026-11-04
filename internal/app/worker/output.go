package worker

import (
	"context"
	"errors"
	"fmt"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/httpoutbox"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/resulttransport"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/jackc/pgx/v5/pgxpool"
	"net"
	"net/http"
	"net/url"
	"time"
)

type OutputOptions struct {
	Listen, Advertise, MasterURL string
	CreateResultsTopic           bool
}

func RunWithOutput(ctx context.Context, cfg Config, store *checkpoints.Store, pool *pgxpool.Pool, output OutputOptions) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	listener, err := net.Listen("tcp", output.Listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	advertised := output.Advertise
	if advertised == "" {
		host, port, _ := net.SplitHostPort(listener.Addr().String())
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("explicit advertise-url required for non-loopback listener")
		}
		advertised = "http://" + net.JoinHostPort(host, port)
	}
	u, err := url.Parse(advertised)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("invalid advertise-url")
	}
	dispatcher := &usecase.Dispatcher{ExecutionID: cfg.Options.Binding.ExecutionID, Mode: store.Registration.OutputTransport, Endpoint: advertised, Store: repo.OutboxStore{DB: pool}, Logger: cfg.Logger}
	switch dispatcher.Mode {
	case "polling":
	case "http_push":
		sender, err := resulttransport.NewHTTP(output.MasterURL, 3*time.Second)
		if err != nil {
			return err
		}
		dispatcher.Sender = sender
	case "kafka":
		sender, err := resulttransport.NewKafka(ctx, cfg.Broker.Seeds, output.CreateResultsTopic)
		if err != nil {
			return err
		}
		dispatcher.Sender = sender
	default:
		return fmt.Errorf("invalid registered output transport")
	}
	cfg.Output = dispatcher
	runner, err := New(cfg, store)
	if err != nil {
		if dispatcher.Sender != nil {
			_ = dispatcher.Sender.Close()
		}
		return err
	}
	server := &http.Server{Handler: httpoutbox.Handler(dispatcher), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	serving := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if !errors.Is(err, http.ErrServerClosed) {
			cancel()
		}
		serving <- err
	}()
	if cfg.Logger != nil {
		cfg.Logger.InfoContext(ctx, "Worker endpoint started", "execution", cfg.Options.Binding.ExecutionID, "owner", runner.cfg.OwnerID, "mode", dispatcher.Mode, "endpoint", advertised)
	}
	runErr := runner.Run(ctx)
	bounded, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	shutdownErr := server.Shutdown(bounded)
	if shutdownErr != nil {
		_ = server.Close()
	}
	serveErr := <-serving
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(runErr, serveErr, shutdownErr)
}
