package replayer

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/inputcodec"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/ndjson"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/usecase"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/parquet"
)

func Run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := Replayer(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		if errors.Is(err, context.Canceled) {
			return 130
		}
		fmt.Fprintln(os.Stderr, "replayer failed:", err)
		return 1
	}
	return 0
}

// selectedReader keeps sample-specific topology separate from generic F18 validation.
type selectedReader struct{ source usecase.RecordReader }

func (r selectedReader) Next(ctx context.Context) (domain.TelemetryRecord, error) {
	v, err := r.source.Next(ctx)
	if err != nil {
		return v, err
	}
	if err := topology.ValidateSelectedNode(v.Hostname, v.GroupID, v.NodeIndex); err != nil {
		return domain.TelemetryRecord{}, err
	}
	return v, nil
}

func Replayer(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) (result error) {
	cfg, err := load(args, getenv, stderr)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := parquet.OpenParquet(cfg.input)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			result = errors.Join(result, raw.Close())
		}
	}()
	log := slog.New(slog.NewTextHandler(stderr, nil))
	log.InfoContext(ctx, "dataset loaded", "input", cfg.input, "rows", raw.Rows())
	var reader usecase.RecordReader = selectedReader{source: raw}
	if !cfg.start.IsZero() {
		reader = &usecase.WindowReader{Source: reader, Start: cfg.start, End: cfg.end}
	}
	legacy := ndjson.NewJSONSender(stdout)
	var sender usecase.TelemetrySender = legacy
	var completer usecase.TimestampCompleter = legacy
	if cfg.wireFormat == "v1" {
		versioned := &inputcodec.Sender{Writer: stdout, Binding: cfg.binding, Window: domain.InputWindow{Start: cfg.start, End: cfg.end}}
		sender, completer = versioned, versioned
	}
	var producer *kafkainput.Producer
	if cfg.output == "kafka" {
		setup, cancel := context.WithTimeout(ctx, cfg.deliveryTimeout)
		producer, err = kafkainput.NewProducer(setup, cfg.kafkaConfig())
		cancel()
		if err != nil {
			return err
		}
		defer func() { result = errors.Join(result, producer.Close()) }()
		sender, completer = producer, producer
	}
	var stats usecase.Stats
	if cfg.completionMode == "full-grid" {
		stats, err = usecase.ReplayFullGrid(ctx, reader, sender, completer, usecase.RealClock{}, cfg.speed, cfg.start, cfg.end)
	} else {
		stats, err = usecase.Replay(ctx, reader, sender, completer, usecase.RealClock{}, cfg.speed)
	}
	if err != nil {
		if producer != nil {
			log.ErrorContext(ctx, "replay failed", "error", err, "rows_queued", stats.Rows, "confirmed_group_ticks", stats.Completions)
		} else {
			log.ErrorContext(ctx, "replay failed", "error", err, "rows_sent", stats.Rows)
		}
		return err
	}
	closed = true
	if err := raw.Close(); err != nil {
		return err
	}
	if producer != nil {
		if err := producer.Close(); err != nil {
			return err
		}
	}
	log.InfoContext(ctx, "replay finished", "rows", stats.Rows, "timestamps", stats.Timestamps,
		"groups", stats.Groups, "hosts", stats.Hosts, "invalid", stats.Invalid, "timestamp_completions", stats.Completions)
	return nil
}
