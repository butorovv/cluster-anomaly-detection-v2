package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
)

// pollDecodeStore models dedup plus cursor advance as one in-memory commit.
// It verifies the usecase contract; it cannot prove PostgreSQL transaction
// durability, isolation, or crash recovery.
type pollDecodeStore struct {
	Store
	id      group.RBI2Identity
	results map[string]domain.Envelope
	cursor  domain.Cursor
	order   []string
	cursors []domain.Cursor
	err     error
	discard bool
}

func (s *pollDecodeStore) Identity(context.Context, string) (group.RBI2Identity, error) {
	return s.id, nil
}
func (s *pollDecodeStore) AcceptAt(_ context.Context, e domain.Envelope, c domain.Cursor) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	if !c.After(s.cursor) {
		return false, errors.New("nonincreasing fake storage cursor")
	}
	if s.discard {
		return true, nil
	}
	accepted := false
	if _, exists := s.results[e.MessageID]; !exists {
		s.results[e.MessageID] = e
		s.order = append(s.order, e.MessageID)
		accepted = true
	}
	// Match SQL AcceptAt's atomic duplicate-safe result insert plus cursor advance.
	s.cursor = c
	s.cursors = append(s.cursors, c)
	return accepted, nil
}
func (*pollDecodeStore) AcceptKafka(context.Context, string, string, int32, int64, *domain.Envelope) (bool, error) {
	return false, nil
}

type pollDecodeDirectory struct {
	endpoint string
	cursor   domain.Cursor
}

func (d pollDecodeDirectory) Discover(context.Context, string, string) (domain.Endpoint, error) {
	return domain.Endpoint{URL: d.endpoint, OwnerID: "owner", Epoch: 3}, nil
}
func (d pollDecodeDirectory) PollCursor(context.Context, string, string) (domain.Cursor, error) {
	return d.cursor, nil
}

func pollingFixture(t testing.TB, count int) (domain.Page, group.RBI2Identity, string) {
	t.Helper()
	f := workerfixture.New(t, count, 0)
	p, err := group.NewGroupProcessor(f.Bundle, f.Options)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	items := make([]domain.Delivery, count)
	for i := range items {
		at := f.Options.Window.Start.Add(time.Duration(i) * telemetry.GridStep)
		missing := make([]string, 18)
		for j := range missing {
			missing[j] = fmt.Sprintf("a01n%02d", j+1)
		}
		result, err := p.Step(telemetry.GroupTick{Binding: f.Options.Binding, GroupID: "a01", Timestamp: at, Records: map[string]telemetry.TelemetryRecord{}, Missing: missing})
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
		items[i] = domain.Delivery{Cursor: domain.Cursor{Revision: int64(i + 1), Ordinal: i}, Envelope: wire}
	}
	return domain.Page{ExecutionID: f.Options.Binding.ExecutionID, GroupID: "a01", OwnerID: "owner", Epoch: 3, Items: items}, snapshot.Identity, f.Options.Binding.ExecutionID
}

func runPollFixture(t testing.TB, page domain.Page, id group.RBI2Identity, execution string, storageErr error) (*pollDecodeStore, error) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(page) }))
	defer server.Close()
	store := &pollDecodeStore{id: id, results: map[string]domain.Envelope{}, err: storageErr}
	p := Poller{ExecutionID: execution, Store: pollDecodeDirectory{endpoint: server.URL}, Master: Master{Store: store, Metrics: observability.New()}, Client: server.Client()}
	err := p.poll(context.Background(), "a01")
	return store, err
}

func TestPollStrictDecodeAcceptsOrderedPageAndRejectsInvalidInputs(t *testing.T) {
	page, id, execution := pollingFixture(t, 2)
	store, err := runPollFixture(t, page, id, execution, nil)
	if err != nil || len(store.results) != 2 || store.cursor != page.Items[1].Cursor {
		t.Fatalf("ordered page results=%d cursor=%v err=%v", len(store.results), store.cursor, err)
	}
	for _, item := range page.Items {
		e, _ := domain.Decode(item.Envelope)
		if _, ok := store.results[e.MessageID]; !ok {
			t.Fatalf("missing result %s", e.MessageID)
		}
	}

	cases := []struct {
		name       string
		edit       func(*domain.Page)
		storageErr error
		wantCursor domain.Cursor
	}{
		{"cross execution envelope", func(p *domain.Page) {
			e, _ := domain.Decode(p.Items[0].Envelope)
			e.ExecutionID = "00000000-0000-4000-8000-000000000099"
			p.Items[0].Envelope, _ = json.Marshal(e)
		}, nil, domain.Cursor{}},
		{"cross group envelope", func(p *domain.Page) {
			e, _ := domain.Decode(p.Items[0].Envelope)
			e.GroupID = "a02"
			p.Items[0].Envelope, _ = json.Marshal(e)
		}, nil, domain.Cursor{}},
		{"page execution mismatch", func(p *domain.Page) { p.ExecutionID = "00000000-0000-4000-8000-000000000099" }, nil, domain.Cursor{}},
		{"page group mismatch", func(p *domain.Page) { p.GroupID = "a02" }, nil, domain.Cursor{}},
		{"nonincreasing cursor", func(p *domain.Page) { p.Items[1].Cursor = p.Items[0].Cursor }, nil, domain.Cursor{Revision: 1}},
		{"owner mismatch", func(p *domain.Page) { p.OwnerID = "other" }, nil, domain.Cursor{}},
		{"epoch mismatch", func(p *domain.Page) { p.Epoch++ }, nil, domain.Cursor{}},
		{"invalid payload", func(p *domain.Page) {
			e, _ := domain.Decode(p.Items[0].Envelope)
			e.Payload = json.RawMessage(`{"wrong":true}`)
			p.Items[0].Envelope, _ = json.Marshal(e)
		}, nil, domain.Cursor{}},
		{"failed storage", func(*domain.Page) {}, errors.New("store failed"), domain.Cursor{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := page
			p.Items = append([]domain.Delivery(nil), page.Items...)
			tc.edit(&p)
			st, err := runPollFixture(t, p, id, execution, tc.storageErr)
			if err == nil {
				t.Fatal("expected failure")
			}
			if st.cursor != tc.wantCursor {
				t.Fatalf("cursor=%v want %v", st.cursor, tc.wantCursor)
			}
		})
	}

	for _, tc := range []struct {
		name string
		edit func([]byte) []byte
	}{
		{"unknown field", func(b []byte) []byte { return append([]byte(`{"unknown":1,`), b[1:]...) }},
		{"duplicate header", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"schema_version":2`), []byte(`"schema_version":2,"schema_version":2`), 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := page
			p.Items = append([]domain.Delivery(nil), page.Items...)
			p.Items[0].Envelope = tc.edit(p.Items[0].Envelope)
			st, err := runPollFixture(t, p, id, execution, nil)
			if err == nil || st.cursor != (domain.Cursor{}) {
				t.Fatalf("invalid strict envelope cursor=%v err=%v", st.cursor, err)
			}
		})
	}

	// Header is otherwise valid and payload is JSON, but the wire envelope is >4 MiB.
	oversize := page
	oversize.Items = append([]domain.Delivery(nil), page.Items...)
	base, _ := domain.Decode(oversize.Items[0].Envelope)
	base.Payload = json.RawMessage(append(append([]byte(`{"padding":"`), bytes.Repeat([]byte{'x'}, domain.MaxMessageBytes)...), []byte(`"}`)...))
	oversize.Items[0].Envelope, _ = json.Marshal(base)
	if len(oversize.Items[0].Envelope) <= domain.MaxMessageBytes || len(oversize.Items[0].Envelope) >= 2*domain.MaxMessageBytes {
		t.Fatalf("bad oversize fixture: %d bytes", len(oversize.Items[0].Envelope))
	}
	st, err := runPollFixture(t, oversize, id, execution, nil)
	if err == nil || st.cursor != (domain.Cursor{}) {
		t.Fatalf("oversize cursor=%v err=%v", st.cursor, err)
	}
}

func TestPollPageSequenceAndDuplicateMessageIDMatchByteAPI(t *testing.T) {
	page, id, execution := pollingFixture(t, 2)
	duplicate := page.Items[0]
	duplicate.Cursor = domain.Cursor{Revision: 3, Ordinal: 2}
	page.Items = append(page.Items, duplicate)
	newPath, err := runPollFixture(t, page, id, execution, nil)
	if err != nil {
		t.Fatal(err)
	}
	legacyPath := &pollDecodeStore{id: id, results: map[string]domain.Envelope{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(page) }))
	defer server.Close()
	p := Poller{ExecutionID: execution, Store: pollDecodeDirectory{endpoint: server.URL}, Master: Master{Store: legacyPath, Metrics: observability.New()}, Client: server.Client()}
	if err := legacyPoll(context.Background(), p, "a01"); err != nil {
		t.Fatal(err)
	}
	if len(newPath.results) != 2 || len(legacyPath.results) != 2 || newPath.cursor != legacyPath.cursor || newPath.cursor != duplicate.Cursor {
		t.Fatalf("new results/cursor=%d/%v legacy=%d/%v", len(newPath.results), newPath.cursor, len(legacyPath.results), legacyPath.cursor)
	}
	if !reflect.DeepEqual(newPath.order, legacyPath.order) || !reflect.DeepEqual(newPath.cursors, legacyPath.cursors) {
		t.Fatalf("sequence mismatch new=%v/%v legacy=%v/%v", newPath.order, newPath.cursors, legacyPath.order, legacyPath.cursors)
	}
	for id, e := range newPath.results {
		other, ok := legacyPath.results[id]
		if !ok || !bytes.Equal(e.Payload, other.Payload) {
			t.Fatalf("legacy result mismatch for %s", id)
		}
	}
	if _, ok := newPath.results[func() string { e, _ := domain.Decode(duplicate.Envelope); return e.MessageID }()]; !ok {
		t.Fatal("duplicate message missing")
	}

	first, _ := domain.Decode(page.Items[0].Envelope)
	byteStore := &pollDecodeStore{id: id, results: map[string]domain.Envelope{}}
	byteMaster := Master{Store: byteStore, Metrics: observability.New()}
	receipt, err := byteMaster.AcceptAt(context.Background(), page.Items[0].Envelope, page.Items[0].Cursor)
	if err != nil || !receipt.Accepted || byteStore.cursor != page.Items[0].Cursor || !bytes.Equal(byteStore.results[first.MessageID].Payload, first.Payload) {
		t.Fatalf("valid byte API receipt=%+v cursor=%v err=%v", receipt, byteStore.cursor, err)
	}
	if len(byteStore.results) != 1 || !reflect.DeepEqual(byteStore.results[first.MessageID], newPath.results[first.MessageID]) {
		t.Fatal("byte API result differs from decoded polling acceptance")
	}
	failStore := &pollDecodeStore{id: id, results: map[string]domain.Envelope{}, err: errors.New("failed transaction")}
	failReceipt, err := (Master{Store: failStore, Metrics: observability.New()}).AcceptAt(context.Background(), page.Items[0].Envelope, domain.Cursor{Revision: 9, Ordinal: 0})
	if err == nil || failReceipt.Accepted || failStore.cursor != (domain.Cursor{}) || len(failStore.results) != 0 {
		t.Fatalf("failed byte API receipt=%+v cursor=%v results=%d err=%v", failReceipt, failStore.cursor, len(failStore.results), err)
	}
}

func TestAcceptAtEnvelopeRejectsZeroValueAndByteAPIRemainsStrict(t *testing.T) {
	store := &pollDecodeStore{results: map[string]domain.Envelope{}}
	m := Master{Store: store, Metrics: observability.New()}
	if _, err := m.AcceptAtEnvelope(context.Background(), domain.DecodedEnvelope{}, domain.Cursor{Revision: 1}); err == nil {
		t.Fatal("zero decoded envelope accepted")
	}
	if _, err := m.AcceptAt(context.Background(), []byte(`{"unknown":1}`), domain.Cursor{Revision: 1}); err == nil {
		t.Fatal("byte API accepted unknown header")
	}
	if len(store.results) != 0 || store.cursor != (domain.Cursor{}) {
		t.Fatalf("invalid input mutated fake store: results=%d cursor=%v", len(store.results), store.cursor)
	}
}
