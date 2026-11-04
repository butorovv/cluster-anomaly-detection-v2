package httpinput

import (
	"encoding/json"
	"errors"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/jackc/pgx/v5/pgconn"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

func Handler(master usecase.Master, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /v1/results", func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, domain.MaxMessageBytes))
		if err != nil {
			http.Error(w, "result exceeds limit", http.StatusRequestEntityTooLarge)
			return
		}
		receipt, err := master.Accept(r.Context(), b)
		if err != nil {
			status := http.StatusServiceUnavailable
			var p *pgconn.PgError
			if errors.As(err, &p) {
				if p.Code == "23505" || p.Code == "55000" {
					status = http.StatusConflict
				} else if p.Code == "22023" {
					status = http.StatusBadRequest
				}
			} else if strings.HasPrefix(err.Error(), "invalid ") {
				status = http.StatusBadRequest
			}
			log.WarnContext(r.Context(), "result rejected", "error", err)
			http.Error(w, http.StatusText(status), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(receipt)
	})
	mux.HandleFunc("GET /v1/events", func(w http.ResponseWriter, r *http.Request) {
		limit := 100
		if v := r.URL.Query().Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				http.Error(w, "invalid limit", 400)
				return
			}
			limit = n
		}
		events, err := master.Store.Events(r.Context(), r.URL.Query().Get("execution_id"), r.URL.Query().Get("group_id"), r.URL.Query().Get("after"), limit)
		if err != nil {
			http.Error(w, "event query failed", 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(events)
	})
	return mux
}
