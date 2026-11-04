package resulttransport

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

func TestHTTPSenderCloseReleasesIdleConnection(t *testing.T) {
	closed := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"message_id": "receipt", "accepted": true})
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			select {
			case closed <- struct{}{}:
			default:
			}
		}
	}
	server.Start()
	defer server.Close()
	sender, err := NewHTTP(server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	e := domain.Envelope{SchemaVersion: 1, ExecutionID: "12345678-1234-1234-1234-123456789abc", RunID: strings.Repeat("a", 64), ConfigID: strings.Repeat("b", 64), GroupID: "a01", MessageID: "receipt", Kind: "rbi2_event", Revision: 1, EventTime: time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC), Payload: json.RawMessage(`{}`)}
	raw, _ := json.Marshal(e)
	if err := sender.Send(context.Background(), raw, "a01"); err != nil {
		t.Fatal(err)
	}
	if err := sender.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("idle result connection leaked after Close")
	}
}
