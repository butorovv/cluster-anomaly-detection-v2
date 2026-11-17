package resulttransport

import (
	"context"
	"errors"
	"testing"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaAuxCommitOnlyDefersTemporaryCoordinatorErrors(t *testing.T) {
	for _, test := range []struct {
		name        string
		err         error
		deferCommit bool
	}{
		{"success", nil, false},
		{"coordinator moved", kerr.NotCoordinator, true},
		{"coordinator startup", kerr.CoordinatorLoadInProgress, true},
		{"coordinator down", kerr.CoordinatorNotAvailable, true},
		{"rebalance", kerr.RebalanceInProgress, true},
		{"request timeout", kerr.RequestTimedOut, true},
		{"deadline", context.DeadlineExceeded, true},
		{"ownership", kerr.IllegalGeneration, false},
		{"fenced", kerr.FencedInstanceID, false},
		{"identity", kerr.UnknownTopicID, false},
		{"authorization", kerr.GroupAuthorizationFailed, false},
		{"configuration", kerr.InvalidGroupID, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			metrics := observability.New()
			r := &kgo.Record{Partition: 2, Offset: 8}
			err := commitKafkaProgress(context.Background(), func(ctx context.Context, records ...*kgo.Record) error {
				if _, ok := ctx.Deadline(); !ok || len(records) != 1 || records[0] != r {
					t.Fatal("auxiliary commit lost its deadline or durable prefix")
				}
				return test.err
			}, []*kgo.Record{r}, metrics)
			deferred := metrics.Snapshot().Counters["results_kafka_aux_commit_deferred"]
			if test.deferCommit {
				if err != nil || deferred != 1 {
					t.Fatalf("want visible deferral, got %v (%d)", err, deferred)
				}
			} else if !errors.Is(err, test.err) || deferred != 0 {
				t.Fatalf("lost commit outcome: %v (%d)", err, deferred)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := commitKafkaProgress(ctx, func(context.Context, ...*kgo.Record) error {
		return kerr.NotCoordinator
	}, []*kgo.Record{{}}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown was treated as coordinator recovery: %v", err)
	}
	if err := commitKafkaProgress(ctx, func(context.Context, ...*kgo.Record) error {
		return kerr.GroupAuthorizationFailed
	}, []*kgo.Record{{}}, nil); !errors.Is(err, kerr.GroupAuthorizationFailed) {
		t.Fatalf("shutdown hid authorization failure: %v", err)
	}
}
