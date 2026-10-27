package replayer

import (
	"flag"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

type config struct {
	input           string
	speed           float64
	start, end      time.Time
	completionMode  string
	wireFormat      string
	binding         domain.InputBinding
	output          string
	brokers         string
	deliveryTimeout time.Duration
}

func load(args []string, getenv func(string) string, stderr io.Writer) (config, error) {
	var cfg config
	env := func(key, fallback string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return fallback
	}
	flags := flag.NewFlagSet("replayer", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.input, "input", env("REPLAYER_INPUT", ""), "prepared F18-v1 Parquet file (required)")
	speed := flags.String("speed", env("REPLAYER_SPEED", "1"), "pacing multiplier (0 = no intentional delay)")
	flags.StringVar(&cfg.output, "output", "stdout", "output adapter: stdout (NDJSON) or kafka")
	flags.StringVar(&cfg.brokers, "kafka-brokers", "localhost:9092", "comma-separated Kafka bootstrap servers")
	flags.DurationVar(&cfg.deliveryTimeout, "kafka-delivery-timeout", 30*time.Second, "Kafka acknowledgement deadline (>=1s)")
	completionMode := flags.String("completion-mode", "observed", "completion mode: observed or full-grid")
	flags.StringVar(&cfg.wireFormat, "wire-format", "legacy", "wire format: legacy or v1 (explicit window and bindings required)")
	flags.StringVar(&cfg.binding.ExecutionID, "execution-id", "", "input execution scope for v1 wire format")
	flags.StringVar(&cfg.binding.InputSHA256, "input-sha256", "", "trusted prepared source SHA256 binding for v1")
	flags.StringVar(&cfg.binding.ProfileSHA256, "profile-sha256", "", "trusted frozen profile SHA256 binding for v1")
	start := flags.String("start", "", "optional inclusive RFC3339 event-time boundary (requires --end)")
	end := flags.String("end", "", "optional exclusive RFC3339 event-time boundary (requires --start)")
	if err := flags.Parse(args); err != nil {
		return cfg, err
	}
	if len(flags.Args()) != 0 {
		return cfg, fmt.Errorf("unexpected positional arguments")
	}
	var err error
	cfg.speed, err = strconv.ParseFloat(*speed, 64)
	if err != nil || cfg.speed < 0 || math.IsNaN(cfg.speed) || math.IsInf(cfg.speed, 0) {
		return cfg, fmt.Errorf("speed must be finite and non-negative")
	}
	if cfg.input == "" {
		return cfg, fmt.Errorf("--input or REPLAYER_INPUT is required")
	}
	if cfg.output != "stdout" && cfg.output != "kafka" {
		return cfg, fmt.Errorf("--output must be stdout or kafka")
	}
	if cfg.output == "kafka" {
		if cfg.wireFormat != "v1" {
			return cfg, fmt.Errorf("Kafka requires --wire-format v1")
		}
		if cfg.deliveryTimeout < time.Second {
			return cfg, fmt.Errorf("Kafka delivery timeout must be >=1s")
		}
		for _, seed := range strings.Split(cfg.brokers, ",") {
			if strings.TrimSpace(seed) == "" {
				return cfg, fmt.Errorf("Kafka bootstrap servers must not be empty")
			}
		}
	}
	if *completionMode != "observed" && *completionMode != "full-grid" {
		return cfg, fmt.Errorf("--completion-mode must be observed or full-grid")
	}
	cfg.completionMode = *completionMode
	if *start != "" || *end != "" {
		cfg.start, err = time.Parse(time.RFC3339Nano, *start)
		if err != nil {
			return cfg, fmt.Errorf("--start: %w", err)
		}
		cfg.end, err = time.Parse(time.RFC3339Nano, *end)
		if err != nil {
			return cfg, fmt.Errorf("--end: %w", err)
		}
		cfg.start, cfg.end = cfg.start.UTC(), cfg.end.UTC()
		if cfg.start.IsZero() || !cfg.end.After(cfg.start) {
			return cfg, fmt.Errorf("invalid half-open time window")
		}
	}
	if cfg.completionMode == "full-grid" && (cfg.start.IsZero() || cfg.end.IsZero()) {
		return cfg, fmt.Errorf("--completion-mode full-grid requires explicit --start and --end")
	}
	if cfg.wireFormat != "legacy" && cfg.wireFormat != "v1" {
		return cfg, fmt.Errorf("--wire-format must be legacy or v1")
	}
	if cfg.completionMode == "full-grid" || cfg.wireFormat == "v1" {
		if err := (domain.InputWindow{Start: cfg.start, End: cfg.end}).Validate(); err != nil {
			return cfg, err
		}
	}
	if cfg.wireFormat == "v1" {
		if err := cfg.binding.Validate(); err != nil {
			return cfg, err
		}
	} else if cfg.binding != (domain.InputBinding{}) {
		return cfg, fmt.Errorf("input bindings require --wire-format v1")
	}
	return cfg, nil
}

func (cfg config) kafkaConfig() kafkainput.ProducerConfig {
	seeds := strings.Split(cfg.brokers, ",")
	for i := range seeds {
		seeds[i] = strings.TrimSpace(seeds[i])
	}
	return kafkainput.ProducerConfig{Broker: kafkainput.Broker{Seeds: seeds}, Binding: cfg.binding,
		Window: domain.InputWindow{Start: cfg.start, End: cfg.end}, DeliveryTimeout: cfg.deliveryTimeout}
}
