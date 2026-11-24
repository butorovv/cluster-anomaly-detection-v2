package telemetryarchive

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	archive "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/archive/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The operator supplies an isolated archive test database. Its contents are
// retained for inspection, and the test uses a new execution for each run.
func TestStreamArchiveTransaction(t *testing.T) {
	endpoint := os.Getenv("ARCHIVE_INTEGRATION_TEST_URL")
	if endpoint == "" {
		t.Skip("requires an isolated PostgreSQL archive test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := InitializeSource(ctx, pool, SummitSHA); err != nil {
		t.Fatal(err)
	}
	writer, err := archive.New(ctx, pool, 18)
	if err != nil {
		t.Fatal(err)
	}
	cfg := StreamConfig{Binding: domain.InputBinding{
		ExecutionID: fmt.Sprintf("cccccccc-aaaa-4bbb-8eee-%012x", time.Now().UnixNano()&((1<<48)-1)),
		InputSHA256: SummitSHA, ProfileSHA256: summitProfile},
		Window: domain.InputWindow{Start: summitWindow.Start, End: summitWindow.Start.Add(20 * time.Second)}}
	store := streamStore{pool: pool, writer: writer, config: cfg, cluster: "isolated-test-cluster", topic: "isolated-test-topic"}
	if err := store.register(ctx); err != nil {
		t.Fatal(err)
	}
	id := kafkainput.Identity{TopicID: store.topic, Partition: 0, GroupID: "a01", Binding: cfg.Binding, Window: cfg.Window}
	old, err := store.acquire(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	active, err := store.acquire(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	tick := &domain.GroupTick{Timestamp: cfg.Window.Start, Records: map[string]domain.TelemetryRecord{}}
	if _, err := store.save(ctx, 0, *old, 0, tick); err == nil {
		t.Fatal("stale owner advanced archive")
	}
	aborted, stop := context.WithCancel(ctx)
	stop()
	if _, err := store.save(aborted, 0, *active, 0, tick); err == nil {
		t.Fatal("canceled transaction committed")
	}
	bad := domain.TelemetryRecord{Timestamp: tick.Timestamp, Hostname: "a01n01", GroupID: "a01", NodeIndex: 1}
	tick.Records[bad.Hostname] = bad
	if _, err := store.save(ctx, 0, *active, 1, tick); err == nil {
		t.Fatal("bad telemetry committed")
	}
	fresh, err := store.acquire(ctx, id)
	if err != nil || fresh.offset != 0 || !fresh.next.Equal(tick.Timestamp) {
		t.Fatalf("rollback moved offset: %+v, %v", fresh, err)
	}
	tick.Records = map[string]domain.TelemetryRecord{}
	if _, err := store.save(ctx, 0, *fresh, 0, tick); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.acquire(ctx, id)
	if err != nil || recovered.offset != 1 || !recovered.next.Equal(tick.Timestamp.Add(10*time.Second)) {
		t.Fatalf("recovery missed committed marker: %+v, %v", recovered, err)
	}
	reader, err := archive.NewReader(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	masks, err := reader.StreamMasks(ctx, cfg.Binding.ExecutionID, SummitSHA, summitProfile, 0, 1, tick.Timestamp, recovered.next)
	if err != nil || masks[tick.Timestamp.Unix()] != -1 {
		t.Fatalf("empty tick mask = %v, %v", masks, err)
	}
}
