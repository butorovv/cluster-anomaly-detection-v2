package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
)

type pollingDirectory struct {
	repo.MasterStore
	mu                   sync.Mutex
	endpoint             string
	epoch                map[string]int64
	discoveries, cursors map[string]int
}

func (d *pollingDirectory) Discover(_ context.Context, _ string, g string) (domain.Endpoint, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.discoveries[g]++
	return domain.Endpoint{URL: d.endpoint, OwnerID: "00000000-0000-4000-8000-000000000091", Epoch: d.epoch[g]}, nil
}
func (d *pollingDirectory) PollCursor(ctx context.Context, x, g string) (domain.Cursor, error) {
	d.mu.Lock()
	d.cursors[g]++
	d.mu.Unlock()
	return d.MasterStore.PollCursor(ctx, x, g)
}

func TestPollingSlowGroupAllowsDurableOtherGroupsAndRefreshesOwner(t *testing.T) {
	pool := postgrestest.New(t)
	fixture := workerfixture.New(t, 1, 0)
	fixture.Options.Groups = 4
	fragments := map[string]json.RawMessage{}
	var registration checkpoints.Registration
	for g := 1; g <= 4; g++ {
		opts := fixture.Options
		opts.GroupID = fmt.Sprintf("a%02d", g)
		processor, err := group.NewGroupProcessor(fixture.Bundle, opts)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := processor.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if g == 1 {
			registration = checkpoints.Registration{Identity: snapshot.Identity, Binding: opts.Binding, ClusterID: "poll-test", TopicID: "poll-input", ConsumerGroup: "poll", OutputTransport: "polling", Diagnostics: true, StartOffsets: map[string]int64{}}
			for p := 0; p < 16; p++ {
				registration.StartOffsets[fmt.Sprint(p)] = 0
			}
			if err := checkpoints.Register(context.Background(), pool, registration); err != nil {
				t.Fatal(err)
			}
		}
		missing := []string{}
		for h := 1; h <= 18; h++ {
			missing = append(missing, fmt.Sprintf("%sn%02d", opts.GroupID, h))
		}
		result, err := processor.Step(telemetry.GroupTick{Binding: opts.Binding, GroupID: opts.GroupID, Timestamp: opts.Window.Start, Records: map[string]telemetry.TelemetryRecord{}, Missing: missing})
		if err != nil {
			t.Fatal(err)
		}
		e, err := domain.TickFragment(opts.Binding.ExecutionID, snapshot.Identity, result)
		if err != nil {
			t.Fatal(err)
		}
		fragments[opts.GroupID], err = json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
	}
	store := repo.MasterStore{DB: pool}
	directory := &pollingDirectory{MasterStore: store, epoch: map[string]int64{}, discoveries: map[string]int{}, cursors: map[string]int{}}
	for g := 1; g <= 4; g++ {
		directory.epoch[fmt.Sprintf("a%02d", g)] = 1
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var mu sync.Mutex
	requests, inflight, peak := map[string]int{}, map[string]int{}, map[string]int{}
	active, maxActive := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g := r.URL.Query().Get("group_id")
		mu.Lock()
		requests[g]++
		attempt := requests[g]
		inflight[g]++
		active++
		peak[g] = max(peak[g], inflight[g])
		maxActive = max(maxActive, active)
		mu.Unlock()
		defer func() { mu.Lock(); inflight[g]--; active--; mu.Unlock() }()
		if g == "a01" {
			once.Do(func() { close(entered) })
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		if g == "a02" && attempt == 1 {
			w.WriteHeader(503)
			return
		}
		directory.mu.Lock()
		if g == "a03" && attempt == 1 {
			directory.epoch[g]++
		}
		epoch := directory.epoch[g]
		directory.mu.Unlock()
		items := []domain.Delivery{}
		if r.URL.Query().Get("revision") == "0" {
			items = append(items, domain.Delivery{Cursor: domain.Cursor{Revision: 1, Ordinal: 1}, Envelope: fragments[g]})
		}
		_ = json.NewEncoder(w).Encode(domain.Page{ExecutionID: registration.Binding.ExecutionID, GroupID: g, OwnerID: "00000000-0000-4000-8000-000000000091", Epoch: epoch, Items: items})
	}))
	defer server.Close()
	directory.endpoint = server.URL
	metrics := observability.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (Poller{ExecutionID: registration.Binding.ExecutionID, Groups: 4, Concurrency: 4, Interval: 5 * time.Millisecond, Store: directory, Master: Master{Store: store, Metrics: metrics}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}).Run(ctx)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("slow request not entered")
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		err := pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_poll_cursors WHERE checkpoint_revision=1 AND ordinal=1`).Scan(&count)
		mu.Lock()
		continued := requests["a02"] >= 3 && requests["a03"] >= 3 && requests["a04"] >= 2
		mu.Unlock()
		if err == nil && count == 3 && continued {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("other groups did not commit and keep polling before slow release", count, err)
		}
	}
	var finals int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_results`).Scan(&finals); err != nil || finals != 0 {
		t.Fatal("incomplete timestamp finalized", finals, err)
	}
	mu.Lock()
	for g, p := range peak {
		if p != 1 {
			t.Errorf("group %s inflight peak=%d", g, p)
		}
	}
	if maxActive > 4 {
		t.Errorf("HTTP concurrency=%d", maxActive)
	}
	mu.Unlock()
	releaseOnce.Do(func() { close(release) })
	for {
		if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_results`).Scan(&finals); err == nil && finals == 4 {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("join incomplete after release")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("poller did not join canceled HTTP operations")
	}
	mu.Lock()
	defer mu.Unlock()
	if active != 0 {
		t.Fatal("HTTP operation leaked", active)
	}
	directory.mu.Lock()
	defer directory.mu.Unlock()
	for g, n := range directory.discoveries {
		if n != directory.cursors[g] || n < 1 {
			t.Fatal("owner/cursor not refreshed", g, n, directory.cursors[g])
		}
	}
}
