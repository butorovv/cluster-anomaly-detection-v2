package master

import (
	"context"
	"fmt"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/resulttransport"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/twmb/franz-go/pkg/kadm"
)

const (
	resultsBacklogValid = "results_kafka_backlog_sample_valid"
	resultsRateValid    = "results_kafka_durable_rate_valid"
)

type durableSample struct {
	at   time.Time
	next map[int32]int64
}

func startKafkaDiagnostics(ctx context.Context, admin *kadm.Client, pin resulttransport.SourceIdentity, store postgres.MasterStore, execution string) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		var previous durableSample
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				bounded, stop := context.WithTimeout(ctx, time.Second)
				sample, ends, err := readKafkaBacklog(bounded, admin, pin, store, execution)
				stop()
				if ctx.Err() != nil {
					return
				}
				previous = publishKafkaBacklog(store.Diagnostics, previous, sample, ends, err)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func readKafkaBacklog(ctx context.Context, admin *kadm.Client, pin resulttransport.SourceIdentity, store postgres.MasterStore, execution string) (durableSample, kadm.ListedOffsets, error) {
	// Read durable positions first so a concurrent append/commit cannot produce
	// an artificially negative lag. The two systems aren't sampled atomically.
	next, err := store.ResultOffsets(ctx, execution)
	sample := durableSample{at: time.Now(), next: next}
	if err != nil {
		return sample, nil, err
	}
	ends, err := admin.ListEndOffsets(ctx, resulttransport.ResultsTopic)
	if err != nil {
		return sample, nil, err
	}
	metadata, err := admin.Metadata(ctx, resulttransport.ResultsTopic)
	if err != nil {
		return sample, nil, err
	}
	topic, ok := metadata.Topics[resulttransport.ResultsTopic]
	if !ok || topic.Err != nil || metadata.Cluster != pin.ClusterID || topic.ID.String() != pin.TopicID {
		return sample, nil, fmt.Errorf("results topic identity unavailable for backlog sample")
	}
	return sample, ends, nil
}

func publishKafkaBacklog(metrics *observability.Registry, previous, current durableSample, ends kadm.ListedOffsets, sampleErr error) durableSample {
	gauges := map[string]float64{resultsBacklogValid: 0, resultsRateValid: 0}
	var backlog, confirmed int64
	window := current.at.Sub(previous.at)
	hasRate := !previous.at.IsZero() && window > 0
	for p := int32(0); p < 16 && sampleErr == nil; p++ {
		next, hasPosition := current.next[p]
		end, hasEnd := ends.Lookup(resulttransport.ResultsTopic, p)
		if !hasPosition || !hasEnd || end.Err != nil || next < 0 || end.Offset < next {
			sampleErr = fmt.Errorf("invalid results positions for partition %d", p)
			break
		}
		lag := end.Offset - next
		gauges[fmt.Sprintf("results_kafka_backlog_records_p%02d", p)] = float64(lag)
		backlog += lag
		if prior, ok := previous.next[p]; ok && prior <= next {
			confirmed += next - prior
		} else {
			hasRate = false
		}
	}
	if sampleErr != nil {
		metrics.SetGauges(map[string]float64{resultsBacklogValid: 0, resultsRateValid: 0})
		metrics.Add("results_kafka_backlog_sample_errors", 1)
		return durableSample{}
	}
	gauges[resultsBacklogValid] = 1
	gauges["results_kafka_backlog_records"] = float64(backlog)
	gauges["results_kafka_backlog_sample_unix_seconds"] = float64(current.at.UnixNano()) / 1e9
	if hasRate {
		gauges[resultsRateValid] = 1
		gauges["results_kafka_durable_confirmations_per_second"] = float64(confirmed) / window.Seconds()
	}
	metrics.SetGauges(gauges)
	return current
}
