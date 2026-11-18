package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/resulttransport"
	"github.com/twmb/franz-go/pkg/kadm"
)

func TestWorkerRestartUsesRegisteredKafkaOutput(t *testing.T) {
	for _, mode := range []struct {
		name  string
		async bool
	}{{"sync", false}, {"async", true}} {
		t.Run(mode.name, func(t *testing.T) {
			rig := workerIntegrationFixture(t, 0, "kafka", nil, true)
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			execution := rig.fixture.Options.Binding.ExecutionID
			run := func(ctx context.Context) error {
				return RunWithOutput(ctx, Config{
					Bundle: rig.fixture.Bundle, Options: rig.fixture.Options, Broker: rig.broker,
					ConsumerGroup: rig.group, LeaseTTL: 3 * time.Second, Logger: discardLog(),
				}, rig.store, rig.pool, OutputOptions{Listen: "127.0.0.1:0", KafkaAsync: mode.async})
			}
			if err := run(ctx); err == nil || !strings.Contains(err.Error(), "load registered Kafka output source") {
				t.Fatalf("execution registration without output binding: %v", err)
			}
			rig.bindResults(t, "restart-"+execution)
			_, stop, stopped := rig.start(t)
			rig.sendTick(t, 0)
			rig.waitRevision(t, 1)
			stopWorker(t, stop, stopped)

			pending := func() int {
				t.Helper()
				var count int
				err := rig.pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.worker_tick_fragments
					WHERE execution_id=$1 AND delivered_at IS NULL`, execution).Scan(&count)
				if err != nil {
					t.Fatal(err)
				}
				return count
			}
			before := pending()
			if before == 0 {
				t.Fatal("no pending fragments for restart")
			}
			end := func() int64 {
				t.Helper()
				offsets, err := kadm.NewClient(rig.producer).ListEndOffsets(ctx, resulttransport.ResultsTopic)
				if err != nil {
					t.Fatal(err)
				}
				var sum int64
				for _, offset := range offsets[resulttransport.ResultsTopic] {
					if offset.Err != nil {
						t.Fatal(offset.Err)
					}
					sum += offset.Offset
				}
				return sum
			}
			initialEnd := end()
			for _, column := range []string{"topic_id", "cluster_id"} {
				// Simulate a durable pin from a different incarnation without touching the broker.
				if _, err := rig.pool.Exec(ctx, "UPDATE cad_v2.result_topic_bindings SET "+column+"='prior-'||"+column+" WHERE execution_id=$1", execution); err != nil {
					t.Fatal(err)
				}
				if err := run(ctx); err == nil || !strings.Contains(err.Error(), "differs from registered execution") {
					t.Fatalf("new Worker accepted mismatched %s: %v", column, err)
				}
				if pending() != before || end() != initialEnd {
					t.Fatalf("new Worker produced or acknowledged with mismatched %s", column)
				}
				if _, err := rig.pool.Exec(ctx, "UPDATE cad_v2.result_topic_bindings SET "+column+"=substring("+column+" from 7) WHERE execution_id=$1", execution); err != nil {
					t.Fatal(err)
				}
			}
			runtime, stopRuntime := context.WithCancel(ctx)
			defer stopRuntime()
			done := make(chan error, 1)
			go func() { done <- run(runtime) }()
			waitWorker(t, 15*time.Second, func() bool { return pending() == 0 })
			stopRuntime()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if end() <= initialEnd {
				t.Fatal("restarted Worker did not publish pending output")
			}
		})
	}
}
