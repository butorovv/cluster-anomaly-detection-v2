package telemetryarchive

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	events "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	archive "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/archive/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const publicationPath = "/v1/telemetry/publication"

var errUnknownRun = errors.New("unknown execution or group")

type Selection struct {
	Execution string
	Group     string
	Host      string
	From, To  time.Time
}

type Publication struct {
	Execution string      `json:"execution_id"`
	Source    string      `json:"source_sha256"`
	Profile   string      `json:"profile_sha256"`
	Start     time.Time   `json:"window_start"`
	End       time.Time   `json:"window_end"`
	Ticks     []time.Time `json:"ticks"`
}

func selection(r *http.Request, requireHost bool) (Selection, bool) {
	q := r.URL.Query()
	s := Selection{Execution: q.Get("execution_id"), Group: q.Get("group_id"), Host: q.Get("host_id")}
	s.From, _ = time.Parse(time.RFC3339, q.Get("from"))
	s.To, _ = time.Parse(time.RFC3339, q.Get("to"))
	_, groupError := topology.PartitionForGroup(s.Group)
	valid := events.ValidExecution(s.Execution) && groupError == nil &&
		domain.OnGrid(s.From) && domain.OnGrid(s.To) && s.From.Before(s.To) && s.To.Sub(s.From) <= 610*time.Second
	if requireHost {
		hostGroup, index, err := topology.ParseHostname(s.Host)
		valid = valid && err == nil && hostGroup == s.Group && topology.ValidateSelectedNode(s.Host, s.Group, index) == nil
	}
	return s, valid
}

func publishJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(value)
}

// PublicationHandler exposes a bounded index-only view of committed diagnostic
// ticks. The normal benchmark master installs no route unless configured.
func PublicationHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s, ok := selection(r, false)
		if !ok {
			http.Error(w, "invalid execution, group or interval", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		p := Publication{Execution: s.Execution, Ticks: []time.Time{}}
		err := pool.QueryRow(ctx, `SELECT r.input_sha256->>$3,r.profile_sha256::text,r.window_start,r.window_end
 FROM cad_v2.executions x JOIN cad_v2.runs r USING(run_id)
 JOIN cad_v2.execution_partitions ep USING(execution_id)
 WHERE x.execution_id=$1::uuid AND ep.group_id=$2`, s.Execution, s.Group, SummitAsset).
			Scan(&p.Source, &p.Profile, &p.Start, &p.End)
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "execution or group unavailable", 404)
			return
		}
		if err != nil {
			http.Error(w, "publication unavailable", 503)
			return
		}
		rows, err := pool.Query(ctx, `SELECT tick FROM cad_v2.master_results
 WHERE execution_id=$1::uuid AND group_id=$2 AND tick >= $3 AND tick < $4 ORDER BY tick`,
			s.Execution, s.Group, s.From, s.To)
		if err != nil {
			http.Error(w, "publication unavailable", 503)
			return
		}
		p.Ticks, err = pgx.CollectRows(rows, pgx.RowTo[time.Time])
		if err != nil {
			http.Error(w, "publication unavailable", 503)
			return
		}
		publishJSON(w, p)
	})
}

type Measurement struct {
	Time   time.Time       `json:"time"`
	Status string          `json:"status"`
	Values *archive.Values `json:"values,omitempty"`
}

type Channel struct {
	Name string `json:"name"`
	Unit string `json:"unit"`
}

type Response struct {
	Execution  string                       `json:"execution_id"`
	Source     string                       `json:"source_sha256"`
	Profile    string                       `json:"profile_sha256"`
	Provenance string                       `json:"provenance"`
	Channels   [domain.FeatureCount]Channel `json:"channels"`
	Items      []Measurement                `json:"items"`
}

type PublicationSource interface {
	Published(context.Context, Selection) (Publication, error)
}

type PhysicalReader interface {
	Imported(context.Context, string, string) ([]archive.ImportRange, error)
	Samples(context.Context, string, []time.Time) (map[int64]archive.Values, error)
}

func Handler(source PublicationSource, reader PhysicalReader) http.Handler {
	inflight := make(chan struct{}, 8)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /v1/telemetry", func(w http.ResponseWriter, r *http.Request) {
		s, ok := selection(r, true)
		if !ok {
			http.Error(w, "invalid execution, group, host or interval", 400)
			return
		}
		select {
		case inflight <- struct{}{}:
			defer func() { <-inflight }()
		default:
			http.Error(w, "temporarily busy", 503)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
		defer cancel()
		p, err := source.Published(ctx, s)
		if errors.Is(err, errUnknownRun) {
			http.Error(w, "unknown execution or group", 404)
			return
		}
		if err != nil {
			http.Error(w, "publication unavailable", 503)
			return
		}
		if p.Execution != s.Execution || p.Source == "" || p.Profile == "" || !p.Start.Before(p.End) {
			http.Error(w, "execution identity unavailable", 409)
			return
		}
		ranges, err := reader.Imported(ctx, p.Source, TopologySHA())
		if err != nil {
			http.Error(w, "archive unavailable", 503)
			return
		}
		result := Response{Execution: p.Execution, Source: p.Source, Profile: p.Profile, Provenance: "verified_import",
			Items: make([]Measurement, 0, 61)}
		for i, name := range domain.FeatureNames {
			unit := "W"
			if i >= 10 {
				unit = "°C"
			}
			result.Channels[i] = Channel{Name: name, Unit: unit}
		}
		published := make(map[int64]bool, len(p.Ticks))
		for _, tick := range p.Ticks {
			published[tick.Unix()] = true
		}
		var eligible []time.Time
		start, end := s.From, s.To
		if p.Start.After(start) {
			start = p.Start
		}
		if p.End.Before(end) {
			end = p.End
		}
		for tick := start; tick.Before(end); tick = tick.Add(10 * time.Second) {
			item := Measurement{Time: tick.UTC(), Status: "not_published"}
			if published[tick.Unix()] {
				item.Status = "pending_archive"
				for _, coverage := range ranges {
					if !tick.Before(coverage.From) && tick.Before(coverage.To) {
						eligible = append(eligible, tick)
						item.Status = "missing_observation"
						break
					}
				}
			}
			result.Items = append(result.Items, item)
		}
		values, err := reader.Samples(ctx, s.Host, eligible)
		if err != nil {
			http.Error(w, "archive unavailable", 503)
			return
		}
		for i := range result.Items {
			item := &result.Items[i]
			if value, ok := values[item.Time.Unix()]; ok && item.Status == "missing_observation" {
				item.Status = "available"
				item.Values = &value
			}
		}
		publishJSON(w, result)
	})
	return mux
}
