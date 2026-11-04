package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"reflect"
	"testing"
	"time"
)

func TestMasterIdempotencyRevisionOrderIsolation(t *testing.T) {
	pool := postgrestest.New(t)
	f := workerfixture.New(t, 6, 30)
	p, err := group.NewGroupProcessor(f.Bundle, f.Options)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	registration := checkpoints.Registration{Identity: snap.Identity, Binding: f.Options.Binding, ClusterID: "master-test", TopicID: "topic-1", ConsumerGroup: "master-test", OutputTransport: "http_push", StartOffsets: map[string]int64{}}
	for i := 0; i < 16; i++ {
		registration.StartOffsets[fmt.Sprint(i)] = 0
	}
	ctx := context.Background()
	if err := checkpoints.Register(ctx, pool, registration); err != nil {
		t.Fatal(err)
	}
	for tick := 0; tick < 6; tick++ {
		at := f.Options.Window.Start.Add(time.Duration(tick) * telemetry.GridStep)
		records := map[string]telemetry.TelemetryRecord{}
		for h := 1; h <= 18; h++ {
			r := workerfixture.Record("a01", h, at, 2)
			records[r.Hostname] = r
		}
		if _, err := p.Step(telemetry.GroupTick{Binding: f.Options.Binding, GroupID: "a01", Timestamp: at, Records: records, ValidRecords: 18}); err != nil {
			t.Fatal(err)
		}
	}
	events, err := p.Finish()
	if err != nil || len(events) != 1 {
		t.Fatal(events, err)
	}
	outputs, err := core.TerminalEvents(f.Options.Binding.ExecutionID, events)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(outputs[0])
	if err != nil {
		t.Fatal(err)
	}
	store := repo.MasterStore{DB: pool}
	master := usecase.Master{Store: store}
	receipt, err := master.Accept(ctx, raw)
	if err != nil || !receipt.Accepted {
		t.Fatal(receipt, err)
	}
	for i := 0; i < 4; i++ {
		r, err := master.Accept(ctx, raw)
		if err != nil || r.Accepted {
			t.Fatal("duplicate", r, err)
		}
	}
	var envelope domain.Envelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	conflicting := envelope
	var altered map[string]any
	_ = json.Unmarshal(conflicting.Payload, &altered)
	altered["positive_ticks"] = 99
	conflicting.Payload, _ = json.Marshal(altered)
	b, _ := json.Marshal(conflicting)
	if _, err := master.Accept(ctx, b); err == nil {
		t.Fatal("conflicting retry accepted")
	}
	alternate := envelope
	alternate.MessageID = "alternate"
	b, _ = json.Marshal(alternate)
	if _, err := master.Accept(ctx, b); err == nil {
		t.Fatal("alternate message id accepted for same logical revision")
	}
	// The original payload is retained exactly in scientific meaning, not rebuilt.
	got, err := store.Events(ctx, envelope.ExecutionID, "a01", "", 100)
	if err != nil || len(got) != 1 {
		t.Fatal(got, err)
	}
	var saved group.RBI2Event
	if err := json.Unmarshal(got[0], &saved); err != nil || !reflect.DeepEqual(saved, events[0]) {
		t.Fatal("payload rewritten", err)
	}
	other := registration
	other.Binding.ExecutionID = "00000000-0000-4000-8000-000000000082"
	other.ConsumerGroup = "master-other"
	if err := checkpoints.Register(ctx, pool, other); err != nil {
		t.Fatal(err)
	}
	isolated := envelope
	isolated.ExecutionID = other.Binding.ExecutionID
	b, _ = json.Marshal(isolated)
	if _, err := master.Accept(ctx, b); err != nil {
		t.Fatal("execution isolation", err)
	}
	// A separate execution receives terminal revision 3 before open revisions 2/1.
	ordered := registration
	ordered.Binding.ExecutionID = "00000000-0000-4000-8000-000000000083"
	ordered.ConsumerGroup = "master-ordered"
	if err := checkpoints.Register(ctx, pool, ordered); err != nil {
		t.Fatal(err)
	}
	for _, revision := range []int64{3, 1, 2, 2} {
		e := envelope
		e.ExecutionID = ordered.Binding.ExecutionID
		e.Revision = revision
		e.MessageID = domain.MessageID(e.RunID, e.GroupID, e.Kind, e.EventID, revision)
		e.Terminal = revision == 3
		if !e.Terminal {
			var payload map[string]json.RawMessage
			_ = json.Unmarshal(e.Payload, &payload)
			delete(payload, "closed")
			e.Payload, _ = json.Marshal(payload)
		}
		b, _ := json.Marshal(e)
		if _, err := master.Accept(ctx, b); err != nil {
			t.Fatal("allowed revision reorder", revision, err)
		}
	}
	var revision int64
	if err := pool.QueryRow(ctx, `SELECT revision FROM cad_v2.master_events WHERE execution_id=$1::uuid`, ordered.Binding.ExecutionID).Scan(&revision); err != nil || revision != 3 {
		t.Fatal("late revision rolled projection back", revision, err)
	}
	for _, mutate := range []func(*domain.Envelope){func(e *domain.Envelope) { e.RunID = "wrong" }, func(e *domain.Envelope) { e.ConfigID = "wrong" }, func(e *domain.Envelope) { e.EventID = "wrong" }, func(e *domain.Envelope) { e.GroupID = "a99" }, func(e *domain.Envelope) { e.SchemaVersion = 99 }} {
		bad := envelope
		mutate(&bad)
		b, _ := json.Marshal(bad)
		if _, err := master.Accept(ctx, b); err == nil {
			t.Fatal("invalid binding accepted")
		}
	}
}
