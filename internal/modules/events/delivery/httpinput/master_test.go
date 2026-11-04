package httpinput

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"

	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestEventsDistinguishesInvalidQueryFromUnavailableDatabase(t *testing.T) {
	// A closed pool deterministically fails without opening a database connection.
	pool, err := pgxpool.New(context.Background(), "postgres://unused@127.0.0.1:1/unused?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	handler := Handler(usecase.Master{Store: repo.MasterStore{DB: pool}}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	for _, tc := range []struct {
		query  string
		status int
	}{
		{"execution_id=12345678-1234-1234-1234-123456789abc", 503},
		{"execution_id=invalid", 400},
		{"execution_id=12345678-1234-1234-1234-123456789abc&limit=501", 400},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/v1/events?"+tc.query, nil))
		if response.Code != tc.status {
			t.Fatalf("query %s: got %d want %d", tc.query, response.Code, tc.status)
		}
	}
}
