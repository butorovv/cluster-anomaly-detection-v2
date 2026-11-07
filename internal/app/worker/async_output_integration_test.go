package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	masterapp "github.com/butorovv/cluster-anomaly-detection-v2/internal/app/master"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/resulttransport"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
)

type pausedDeliveryAck struct {
	usecase.Outbox
	once      sync.Once
	entered   chan string
	blockedID string
}

func (s *pausedDeliveryAck) Ack(ctx context.Context, o domain.Owner, id string) error {
	s.once.Do(func() { s.blockedID = id; s.entered <- id })
	if id == s.blockedID {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.Outbox.Ack(ctx, o, id)
}

func TestWorkerAsyncRestartAfterKafkaACKBeforeSQLACK(t *testing.T) {
	rig := workerIntegrationFixture(t, 30, "kafka", nil, true)
	consumer := "b2-master-" + rig.group
	rig.bindResults(t, consumer)
	store := restrictedMaster(t, rig)
	metrics := observability.New()
	service := usecase.Master{Store: store, Metrics: metrics}
	mctx, mstop := context.WithCancel(context.Background())
	masterDone := make(chan error, 1)
	go func() {
		masterDone <- masterapp.RunKafka(mctx, rig.broker.Seeds, consumer, rig.fixture.Options.Binding.ExecutionID, true, store, service)
	}()
	t.Cleanup(func() {
		mstop()
		select {
		case err := <-masterDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Master cleanup timed out")
		}
	})
	paused := &pausedDeliveryAck{Outbox: repo.OutboxStore{DB: rig.pool}, entered: make(chan string, 1)}
	newOutput := func(outbox usecase.Outbox) *usecase.Dispatcher {
		s, err := resulttransport.NewKafkaAsync(context.Background(), rig.broker.Seeds, true, resulttransport.KafkaAsyncOptions{MaxRecords: 4, MaxBytes: domain.MaxMessageBytes + 256})
		if err != nil {
			t.Fatal(err)
		}
		return &usecase.Dispatcher{ExecutionID: rig.fixture.Options.Binding.ExecutionID, Mode: "kafka", Store: outbox, AsyncSender: s, AsyncConfig: usecase.AsyncConfig{DrainTimeout: 100 * time.Millisecond}, Logger: discardLog()}
	}
	_, stop, stopped := rig.startOutput(t, newOutput(paused))
	rig.sendTick(t, 0)
	rig.waitRevision(t, 1)
	var id string
	select {
	case id = <-paused.entered:
	case <-time.After(20 * time.Second):
		t.Fatal("Kafka ACK / SQL gap not reached")
	}
	// Keep k's delivery SQL ACK blocked across retries while the SAME Worker
	// computes and checkpoints k+1. This is actual Kafka/PG progress, not a
	// goroutine-count assertion or a timing assumption about a 3s request.
	rig.sendTick(t, 1)
	rig.waitRevision(t, 2)
	var stillPending bool
	if err := rig.pool.QueryRow(context.Background(), `SELECT delivered_at IS NULL FROM cad_v2.worker_tick_fragments WHERE message_id=$1`, id).Scan(&stillPending); err != nil || !stillPending {
		t.Fatal("k delivery ACK completed before k+1 checkpoint", stillPending, err)
	}
	stopWorker(t, stop, stopped)
	var pending bool
	err := rig.pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM cad_v2.worker_tick_fragments WHERE message_id=$1 AND delivered_at IS NULL)`, id).Scan(&pending)
	if err != nil || !pending {
		t.Fatal("unacknowledged durable fragment lost", pending, err)
	}
	_, stop2, stopped2 := rig.startOutput(t, newOutput(repo.OutboxStore{DB: rig.pool}))
	defer stopWorker(t, stop2, stopped2)
	for tick := 2; tick < 6; tick++ {
		rig.sendTick(t, tick)
		rig.waitRevision(t, int64(tick+1))
	}
	want := rig.expectedEvents(t)
	waitWorker(t, 30*time.Second, func() bool {
		var count int
		err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_events`).Scan(&count)
		return err == nil && count == len(want)
	})
	assertMaster(t, rig, store)
	waitWorker(t, 15*time.Second, func() bool {
		var count int
		err := rig.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM cad_v2.outbox_messages WHERE delivered_at IS NULL)+(SELECT count(*) FROM cad_v2.worker_tick_fragments WHERE delivered_at IS NULL)`).Scan(&count)
		return err == nil && count == 0
	})
	if metrics.Snapshot().Counters["master_duplicates"] < 1 {
		t.Fatal("expected duplicate publish after producer/worker restart")
	}
	var ids int
	// tick_fragment identity is deduplicated by V7 in this table. The final
	// group_result in master_inbox has its own kind-specific MessageID.
	if err = rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_tick_fragments WHERE message_id=$1`, id).Scan(&ids); err != nil || ids != 1 {
		t.Fatal("MessageID dedup not preserved", ids, err)
	}
}
