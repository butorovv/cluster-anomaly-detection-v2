package httpoutbox

import (
	"encoding/json"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"net/http"
	"strconv"
	"time"
)

func Handler(dispatcher *usecase.Dispatcher) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /v1/outbox", func(w http.ResponseWriter, r *http.Request) {
		dispatcher.Metrics.Add("poll_http_requests", 1)
		q := r.URL.Query()
		if dispatcher.Mode != "polling" || q.Get("execution_id") != dispatcher.ExecutionID {
			http.Error(w, "execution/mode mismatch", 400)
			return
		}
		owner, ok := dispatcher.Owner(q.Get("group_id"))
		if !ok {
			http.Error(w, "group not currently assigned", 409)
			return
		}
		revision, err := strconv.ParseInt(q.Get("revision"), 10, 64)
		if err != nil || revision < 0 {
			http.Error(w, "invalid revision", 400)
			return
		}
		ordinal, err := strconv.Atoi(q.Get("ordinal"))
		if err != nil || ordinal < 0 {
			http.Error(w, "invalid ordinal", 400)
			return
		}
		limit, err := strconv.Atoi(q.Get("limit"))
		if err != nil || limit < 1 || limit > 128 {
			http.Error(w, "invalid page size", 400)
			return
		}
		cursor := domain.Cursor{Revision: revision, Ordinal: ordinal}
		if err := dispatcher.Store.AckCursor(r.Context(), owner, cursor); err != nil {
			http.Error(w, "owner/progress unavailable", 409)
			return
		}
		items, err := dispatcher.Store.Page(r.Context(), owner, cursor, limit, false)
		if err != nil {
			http.Error(w, "owner/outbox unavailable", 409)
			return
		}
		sentAt := time.Now()
		for _, item := range items {
			dispatcher.Metrics.Add("output_attempts", 1)
			dispatcher.Metrics.Add("output_attempt_payload_bytes", int64(len(item.Envelope)))
			if dispatcher.Measure && dispatcher.Logger != nil {
				e, err := domain.Decode(item.Envelope)
				if err == nil {
					dispatcher.Logger.InfoContext(r.Context(), "output attempt", "execution", owner.ExecutionID, "group", owner.GroupID, "message_id", e.MessageID, "kind", e.Kind, "sent_at", sentAt, "bytes", len(item.Envelope))
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		// Avoid expanding an already bounded JSON envelope up to sixfold for
		// '<', '>' and '&'. The response is JSON, not embedded HTML.
		encoder := json.NewEncoder(w)
		encoder.SetEscapeHTML(false)
		_ = encoder.Encode(domain.Page{ExecutionID: owner.ExecutionID, GroupID: owner.GroupID, OwnerID: owner.OwnerID, Epoch: owner.Epoch, Items: items})
	})
	return mux
}
