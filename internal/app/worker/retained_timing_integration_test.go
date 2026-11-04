package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Exercise the actual retained-assignment loader against SQL: an uncommitted
// complete tick and partial successor must both be replayed, including timings.
func TestRetainedRestoreDiscardsUnconfirmedTiming(t *testing.T) {
	pool := postgrestest.New(t)
	f := workerfixture.New(t, 6, 30)
	execution, err := NewOwnerID()
	if err != nil {
		t.Fatal(err)
	}
	f.Options.Binding.ExecutionID = execution
	control, err := group.NewGroupProcessor(f.Bundle, f.Options)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := control.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	registration := repo.Registration{Identity: initial.Identity, Binding: f.Options.Binding, ClusterID: "isolated-retained", TopicID: "AAAAAAAAAAAAAAAAAAAAAA==", ConsumerGroup: "retained-" + execution, StartOffsets: map[string]int64{}, OutputTransport: "polling", CheckpointIntervalTicks: 3, Diagnostics: true}
	for p := 0; p < 16; p++ {
		registration.StartOffsets[strconv.Itoa(p)] = 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := repo.Register(ctx, pool, registration); err != nil {
		t.Fatal(err)
	}
	store := &repo.Store{DB: pool, Registration: registration}
	var log bytes.Buffer
	runner, err := New(Config{Bundle: f.Bundle, Options: f.Options, Broker: kafkainput.Broker{Seeds: []string{"not-used:9092"}}, ConsumerGroup: registration.ConsumerGroup, CheckpointIntervalTicks: 3, LeaseTTL: 15 * time.Second, Measure: true, Logger: slog.New(slog.NewJSONHandler(&log, nil))}, store)
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
	accept := func(offset int64, m domain.InputMessage) {
		t.Helper()
		if err := runner.handle(ctx, &kgo.Record{Topic: kafkainput.Topic, Partition: 0, Offset: offset, Timestamp: time.Now()}, m); err != nil {
			t.Fatal(err)
		}
	}
	for offset, m := range rig.messages("a01", 0) {
		accept(int64(offset), m)
	}
	accept(19, rig.messages("a01", 1)[0])
	position, err := runner.load(ctx, id)
	if err != nil || position.NextOffset != 0 {
		t.Fatal("retained restore did not rewind unconfirmed tail", position, err)
	}
	offset := int64(0)
	for tick := 0; tick < 3; tick++ {
		records := map[string]domain.TelemetryRecord{}
		messages := rig.messages("a01", tick)
		for _, m := range messages {
			accept(offset, m)
			offset++
			if m.Record != nil {
				records[m.Record.Hostname] = *m.Record
			}
		}
		missing := []string{}
		for node := 1; node <= 18; node++ {
			host := fmt.Sprintf("a01n%02d", node)
			if _, ok := records[host]; !ok {
				missing = append(missing, host)
			}
		}
		if _, err := control.Step(domain.GroupTick{Binding: f.Options.Binding, GroupID: "a01", Timestamp: messages[0].Timestamp, Records: records, Missing: missing, ValidRecords: messages[len(messages)-1].Completion.ValidRecords}); err != nil {
			t.Fatal(err)
		}
	}
	saved, err := store.Load(ctx, "a01")
	if err != nil || saved == nil || saved.NextOffset != offset {
		t.Fatal(saved, err)
	}
	want, err := control.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	wantBytes, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(saved.Snapshot, wantBytes) {
		t.Fatal("restore/replay changed mathematical snapshot")
	}
	if got := strings.Count(log.String(), `"msg":"tick durable"`); got != 3 {
		t.Fatalf("discarded RAM attempt leaked into durability measurements: got %d traces want 3", got)
	}
	var fragments int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.worker_tick_fragments`).Scan(&fragments); err != nil || fragments != 3 {
		t.Fatal("fragment loss/duplication", fragments, err)
	}
	t.Logf("retained restore: exact continuous snapshot, durable next_offset=%d, fragments=3, durability traces=3", saved.NextOffset)
}
