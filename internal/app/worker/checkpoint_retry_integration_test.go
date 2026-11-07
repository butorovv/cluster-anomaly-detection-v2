package worker

import (
	"context"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestWorkerContinuesSamePartitionAfterRetriedStageTimeout(t *testing.T) {
	pool := postgrestest.New(t)
	f := workerfixture.New(t, 6, 30)
	execution, err := NewOwnerID()
	if err != nil {
		t.Fatal(err)
	}
	f.Options.Binding.ExecutionID = execution
	processor, err := group.NewGroupProcessor(f.Bundle, f.Options)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := processor.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	registration := repo.Registration{Identity: snapshot.Identity, Binding: f.Options.Binding, ClusterID: "retry-test", TopicID: "AAAAAAAAAAAAAAAAAAAAAA==", ConsumerGroup: "retry-" + execution, OutputTransport: "polling", Diagnostics: true, CheckpointIntervalTicks: 1, StartOffsets: map[string]int64{}}
	for p := 0; p < 16; p++ {
		registration.StartOffsets[strconv.Itoa(p)] = 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := repo.Register(ctx, pool, registration); err != nil {
		t.Fatal(err)
	}
	adminSQL := `CREATE SEQUENCE retry_worker_calls;
CREATE FUNCTION retry_worker_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF nextval('public.retry_worker_calls') <= 1 THEN PERFORM pg_sleep(0.2); END IF; RETURN NEW; END $$;
CREATE TRIGGER retry_worker_delay BEFORE INSERT ON cad_v2.worker_tick_fragments FOR EACH ROW EXECUTE FUNCTION retry_worker_delay()`
	if _, err := pool.Exec(ctx, adminSQL); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `SET statement_timeout='35ms'`)
		return err
	}
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	store := &repo.Store{DB: runtime, Registration: registration}
	runner, err := New(Config{Bundle: f.Bundle, Options: f.Options, Broker: kafkainput.Broker{Seeds: []string{"not-used:9092"}}, ConsumerGroup: registration.ConsumerGroup, CheckpointIntervalTicks: 1, LeaseTTL: 15 * time.Second, Logger: slog.New(slog.NewTextHandler(discardWriter{}, nil))}, store)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runner.close(); err != nil {
			t.Error(err)
		}
	}()
	id := kafkainput.Identity{Topic: kafkainput.Topic, TopicID: registration.TopicID, Partition: 0, GroupID: "a01", Binding: f.Options.Binding, Window: domain.InputWindow{Start: f.Options.Window.Start, End: f.Options.Window.End}}
	if err := runner.assignedHook(ctx, []kafkainput.Identity{id}); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.load(ctx, id); err != nil {
		t.Fatal(err)
	}
	rig := workerRig{fixture: f}
	offset := int64(0)
	for tick := 0; tick < 2; tick++ {
		for _, message := range rig.messages("a01", tick) {
			record := &kgo.Record{Topic: kafkainput.Topic, Partition: 0, Offset: offset, Timestamp: time.Now()}
			if err := runner.handle(ctx, record, message); err != nil {
				t.Fatalf("tick %d offset %d: %v", tick, offset, err)
			}
			offset++
		}
	}
	durable, err := store.Load(ctx, "a01")
	if err != nil || durable == nil || durable.Revision != 2 || durable.NextOffset != offset {
		t.Fatalf("same worker did not advance after retry: checkpoint=%#v offset=%d err=%v", durable, offset, err)
	}
	var fragments int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.worker_tick_fragments`).Scan(&fragments); err != nil || fragments != 2 {
		t.Fatalf("fragments=%d want 2 err=%v", fragments, err)
	}
	var outbox int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.outbox_messages`).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if outbox != 0 {
		t.Fatalf("unexpected terminal events=%d", outbox)
	}
	if runner.parts[0].core == nil {
		t.Fatal("worker partition unexpectedly absent after retry")
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
