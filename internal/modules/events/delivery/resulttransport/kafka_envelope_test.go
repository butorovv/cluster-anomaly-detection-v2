package resulttransport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaDecodedEnvelopeValidationAndRouting(t *testing.T) {
	r, fixtureStore, execution := kafkaEnvelopeFixture(t)
	mutatePayload := func(wire []byte) []byte {
		e, err := domain.Decode(wire)
		if err != nil {
			t.Fatal(err)
		}
		e.Payload = json.RawMessage(`{"unexpected":true}`)
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for _, tt := range []struct {
		name      string
		foreign   bool
		edit      func(*kgo.Record)
		wantError bool
	}{
		{name: "own execution"},
		{name: "foreign execution", foreign: true},
		{name: "foreign payload remains opaque", foreign: true, edit: func(r *kgo.Record) { r.Value = mutatePayload(r.Value) }},
		{name: "own malformed payload", wantError: true, edit: func(r *kgo.Record) { r.Value = mutatePayload(r.Value) }},
		{name: "duplicate header", wantError: true, edit: func(r *kgo.Record) {
			r.Value = bytes.Replace(r.Value, []byte(`"schema_version":2`), []byte(`"schema_version":2,"schema_version":2`), 1)
		}},
		{name: "duplicate nested key on foreign execution", foreign: true, wantError: true, edit: func(r *kgo.Record) {
			r.Value = bytes.Replace(mutatePayload(r.Value), []byte(`{"unexpected":true}`), []byte(`{"unexpected":true,"unexpected":false}`), 1)
		}},
		{name: "unknown header", wantError: true, edit: func(r *kgo.Record) { r.Value = append([]byte(`{"unknown":1,`), r.Value[1:]...) }},
		{name: "trailing JSON", wantError: true, edit: func(r *kgo.Record) { r.Value = append(bytes.Clone(r.Value), []byte(` {}`)...) }},
		{name: "wrong key on foreign execution", foreign: true, wantError: true, edit: func(r *kgo.Record) { r.Key = []byte("a02") }},
		{name: "wrong partition on foreign execution", foreign: true, wantError: true, edit: func(r *kgo.Record) { r.Partition = 1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			record := *r
			record.Value = bytes.Clone(r.Value)
			if tt.edit != nil {
				tt.edit(&record)
			}
			store := &envelopeStore{id: fixtureStore.id}
			selected := execution
			if tt.foreign {
				selected = "00000000-0000-4000-8000-000000000099"
			}
			master := usecase.Master{Store: store, Metrics: observability.New()}
			progress := processPartition(context.Background(), partitionBatch{records: []*kgo.Record{&record}}, time.Now(),
				func(ctx context.Context, r *kgo.Record, decoded domain.DecodedEnvelope) error {
					return master.AcceptKafkaEnvelope(ctx, selected, "topic-id", r.Partition, r.Offset, decoded)
				}, observability.New())
			if (progress.err != nil) != tt.wantError {
				t.Fatalf("error=%v, wantError=%t", progress.err, tt.wantError)
			}
			if tt.wantError {
				if store.next != 0 || store.applied != 0 || progress.last != nil {
					t.Fatalf("invalid message changed progress: %+v", store)
				}
				return
			}
			wantApplied := 1
			if tt.foreign {
				wantApplied = 0
			}
			if store.next != 1 || store.applied != wantApplied || store.identities != wantApplied || progress.last != &record {
				t.Fatalf("unexpected application/position: %+v", store)
			}
		})
	}
}

func TestKafkaDecodedAndRawMasterReceiptsMatch(t *testing.T) {
	r, rawStore, execution := kafkaEnvelopeFixture(t)
	decodedStore := &envelopeStore{id: rawStore.id}
	decoded, err := domain.DecodeEnvelope(r.Value)
	if err != nil {
		t.Fatal(err)
	}
	rawMaster := usecase.Master{Store: rawStore, Metrics: observability.New()}
	decodedMaster := usecase.Master{Store: decodedStore, Metrics: observability.New()}
	if err := rawMaster.AcceptKafka(context.Background(), execution, "topic-id", 0, 0, r.Value); err != nil {
		t.Fatal(err)
	}
	if err := decodedMaster.AcceptKafkaEnvelope(context.Background(), execution, "topic-id", 0, 0, decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rawStore, decodedStore) || !reflect.DeepEqual(rawMaster.Metrics.Snapshot().Counters, decodedMaster.Metrics.Snapshot().Counters) {
		t.Fatal("raw and decoded API receipt/progress differ")
	}
	if err := decodedMaster.AcceptKafkaEnvelope(context.Background(), execution, "topic-id", 0, 1, domain.DecodedEnvelope{}); err == nil || decodedStore.next != 1 {
		t.Fatal("zero decoded envelope advanced progress")
	}
	invalid, err := domain.DecodeEnvelope(append([]byte(`{"extra":0,`), r.Value[1:]...))
	if err == nil {
		t.Fatal("unknown envelope field accepted")
	}
	if err := decodedMaster.AcceptKafkaEnvelope(context.Background(), execution, "topic-id", 0, 1, invalid); err == nil || decodedStore.next != 1 {
		t.Fatal("failed strict decode advanced progress")
	}
}

type envelopeStore struct {
	usecase.Store
	id         group.RBI2Identity
	identities int
	applied    int
	next       int64
}

func (s *envelopeStore) Identity(context.Context, string) (group.RBI2Identity, error) {
	s.identities++
	return s.id, nil
}

func (s *envelopeStore) AcceptAt(context.Context, domain.Envelope, domain.Cursor) (bool, error) {
	return false, fmt.Errorf("unexpected polling call")
}

func (s *envelopeStore) AcceptKafka(_ context.Context, _, _ string, _ int32, offset int64, e *domain.Envelope) (bool, error) {
	s.next = offset + 1
	if e != nil {
		s.applied++
	}
	return e != nil, nil
}

func kafkaEnvelopeFixture(t testing.TB) (*kgo.Record, *envelopeStore, string) {
	t.Helper()
	f := workerfixture.New(t, 1, 0)
	processor, err := group.NewGroupProcessor(f.Bundle, f.Options)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := processor.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	missing := make([]string, 18)
	for i := range missing {
		missing[i] = fmt.Sprintf("a01n%02d", i+1)
	}
	result, err := processor.Step(telemetry.GroupTick{Binding: f.Options.Binding, GroupID: "a01", Timestamp: f.Options.Window.Start, Records: map[string]telemetry.TelemetryRecord{}, Missing: missing})
	if err != nil {
		t.Fatal(err)
	}
	e, err := domain.TickFragment(f.Options.Binding.ExecutionID, snapshot.Identity, result)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: ResultsTopic, Partition: 0, Key: []byte("a01"), Value: wire}, &envelopeStore{id: snapshot.Identity}, e.ExecutionID
}

func BenchmarkKafkaEnvelopePath(b *testing.B) {
	for _, foreign := range []bool{false, true} {
		name := "OwnExecution"
		if foreign {
			name = "ForeignExecution"
		}
		b.Run(name, func(b *testing.B) {
			r, store, execution := kafkaEnvelopeFixture(b)
			if foreign {
				execution = "00000000-0000-4000-8000-000000000099"
			}
			master := usecase.Master{Store: store, Metrics: observability.New()}
			handle := func(ctx context.Context, record *kgo.Record, wire domain.DecodedEnvelope) error {
				return master.AcceptKafkaEnvelope(ctx, execution, "results-topic-id", record.Partition, record.Offset, wire)
			}
			metrics := observability.New()
			ctx := context.Background()
			part := partitionBatch{records: []*kgo.Record{r}}
			b.ReportAllocs()
			b.SetBytes(int64(len(r.Value)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				progress := processPartition(ctx, part, time.Now(), handle, metrics)
				if progress.err != nil {
					b.Fatal(progress.err)
				}
			}
		})
	}
}
