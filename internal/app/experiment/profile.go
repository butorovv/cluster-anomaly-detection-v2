package experiment

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Profile runs are explicitly separate from throughput trials. Collect three
// short CPU windows (the existing Worker HTTP server has a five-second timeout).
// Snapshots retain SQL round-trip/stage histograms and native Go profiles.
func collectProfiles(ctx context.Context, s *sampler, directory string) error {
	s.mu.Lock()
	endpoints := make(map[string]string, len(s.endpoints))
	for id, url := range s.endpoints {
		endpoints[id] = url
	}
	s.mu.Unlock()
	for window := 1; window <= 3; window++ {
		timer := time.NewTimer(30 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		var wg sync.WaitGroup
		failures := make(chan error, len(endpoints))
		for id, url := range endpoints {
			wg.Add(1)
			go func() {
				defer wg.Done()
				root := filepath.Join(directory, "profiles", id)
				if err := os.MkdirAll(root, 0700); err != nil {
					failures <- err
					return
				}
				for _, kind := range []string{"profile?seconds=3", "heap", "allocs", "goroutine", "mutex", "block", "metrics"} {
					path := "/debug/pprof/" + kind
					name := kind
					if kind == "profile?seconds=3" {
						name = "cpu"
					}
					if kind == "metrics" {
						path = "/metrics"
					}
					if err := downloadProfile(ctx, url+path, filepath.Join(root, fmt.Sprintf("%02d-%s", window, name))); err != nil {
						failures <- err
						return
					}
				}
			}()
		}
		wg.Wait()
		close(failures)
		for err := range failures {
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func downloadProfile(ctx context.Context, url, path string) error {
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	q, err := http.NewRequestWithContext(bounded, "GET", url, nil)
	if err != nil {
		return err
	}
	r, err := http.DefaultClient.Do(q)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("profile endpoint: %s", r.Status)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(f, r.Body)
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func saveSQLProfile(ctx context.Context, pool *pgxpool.Pool, path string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var data []byte
	err := pool.QueryRow(ctx, `SELECT coalesce(jsonb_agg(to_jsonb(s)),'[]'::jsonb) FROM (SELECT query,calls,total_exec_time,mean_exec_time,rows,shared_blks_hit,shared_blks_read,temp_blks_written,wal_records,wal_bytes FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC) s`).Scan(&data)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		return err
	}
	plans := map[string]json.RawMessage{}
	for name, query := range map[string]string{
		"existing_payloads":  `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT envelope FROM cad_v2.master_tick_fragments WHERE execution_id=(SELECT execution_id FROM cad_v2.executions LIMIT 1) AND event_time=(SELECT min(event_time) FROM cad_v2.master_tick_fragments) ORDER BY group_id`,
		"completeness_count": `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT count(*) FROM cad_v2.master_tick_fragments WHERE execution_id=(SELECT execution_id FROM cad_v2.executions LIMIT 1) AND event_time=(SELECT min(event_time) FROM cad_v2.master_tick_fragments)`,
		"database_stats":     `SELECT to_jsonb(s) FROM pg_stat_database s WHERE datname=current_database()`,
		"wal_stats":          `SELECT to_jsonb(s) FROM pg_stat_wal s`,
	} {
		var raw []byte
		if err := pool.QueryRow(ctx, query).Scan(&raw); err != nil {
			return err
		}
		plans[name] = json.RawMessage(raw)
	}
	return saveJSON(path+".plans.json", plans)
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
