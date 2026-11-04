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
	"time"
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
	mux.HandleFunc("GET /v1/results", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		execution := q.Get("execution_id")
		if !domain.ValidExecution(execution) {
			http.Error(w, "execution UUID required", 400)
			return
		}
		limit := 64
		if q.Get("limit") != "" {
			n, err := strconv.Atoi(q.Get("limit"))
			if err != nil || n < 1 || n > 128 {
				http.Error(w, "limit must be 1..128", 400)
				return
			}
			limit = n
		}
		cursor := domain.ResultCursor{GroupID: q.Get("after_group")}
		if q.Get("after_tick") != "" {
			at, err := time.Parse(time.RFC3339Nano, q.Get("after_tick"))
			if err != nil {
				http.Error(w, "invalid after_tick", 400)
				return
			}
			cursor.Tick = at.UTC()
		}
		page, err := master.Store.Results(r.Context(), execution, cursor, limit)
		if err != nil {
			http.Error(w, "results unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(page)
	})
	mux.HandleFunc("GET /v1/summary", func(w http.ResponseWriter, r *http.Request) {
		execution := r.URL.Query().Get("execution_id")
		if !domain.ValidExecution(execution) {
			http.Error(w, "execution UUID required", 400)
			return
		}
		summary, err := master.Store.Summary(r.Context(), execution)
		if err != nil {
			http.Error(w, "summary unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(summary)
	})
	return mux
}
