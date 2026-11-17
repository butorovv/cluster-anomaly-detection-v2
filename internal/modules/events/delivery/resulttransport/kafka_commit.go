package resulttransport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// commitKafkaProgress publishes a diagnostic broker offset after the durable
// SQL wave. A temporary coordinator failure may leave this offset behind.
func commitKafkaProgress(ctx context.Context, commit func(context.Context, ...*kgo.Record) error, lasts []*kgo.Record, metrics *observability.Registry) error {
	if len(lasts) == 0 {
		return nil
	}
	started := time.Now()
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := commit(bounded, lasts...)
	if err == nil {
		metrics.Observe("results_kafka_aux_commit", time.Since(started))
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kerr.RequestTimedOut) ||
		errors.Is(err, kerr.CoordinatorLoadInProgress) || errors.Is(err, kerr.CoordinatorNotAvailable) ||
		errors.Is(err, kerr.NotCoordinator) || errors.Is(err, kerr.RebalanceInProgress) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		metrics.Add("results_kafka_aux_commit_deferred", 1)
		slog.WarnContext(ctx, "Kafka auxiliary offset commit deferred; SQL progress retained", "error", err)
		return nil
	}
	return fmt.Errorf("commit durable output batch: %w", err)
}
