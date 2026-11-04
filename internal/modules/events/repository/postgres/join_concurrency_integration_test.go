package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"testing"
	"time"
)

// Hold a01 until ALL completing timestamps are queued. The old lock order first
// took FK KEY SHARE on a04, then V5 upgraded all rows to UPDATE: a deterministic
// cross-timestamp upgrade cycle after releasing this guard. Acquire UPDATE in
// canonical order before FK INSERT so concurrent joins queue safely instead.
func TestConcurrentTimestampJoinLockOrder(t *testing.T) {
	pool := postgrestest.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := workerfixture.New(t, 4, 0)
	f.Options.Groups = 4
	first, err := group.NewGroupProcessor(f.Bundle, f.Options)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := first.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r := checkpoints.Registration{Identity: snapshot.Identity, Binding: f.Options.Binding, ClusterID: "join-lock", TopicID: "join-lock", ConsumerGroup: "join-lock", OutputTransport: "http_push", Diagnostics: true, StartOffsets: map[string]int64{}}
	for p := 0; p < 16; p++ {
		r.StartOffsets[fmt.Sprint(p)] = 0
	}
	if err := checkpoints.Register(ctx, pool, r); err != nil {
		t.Fatal(err)
	}
	service := usecase.Master{Store: repo.MasterStore{DB: pool}}
	finals := [][]byte{}
	for g := 1; g <= 4; g++ {
		options := f.Options
		options.GroupID = fmt.Sprintf("a%02d", g)
		p, err := group.NewGroupProcessor(f.Bundle, options)
		if err != nil {
			t.Fatal(err)
		}
		for tick := 0; tick < 4; tick++ {
			missing := []string{}
			for h := 1; h <= 18; h++ {
				missing = append(missing, fmt.Sprintf("%sn%02d", options.GroupID, h))
			}
			result, err := p.Step(telemetry.GroupTick{Binding: options.Binding, GroupID: options.GroupID, Timestamp: options.Window.Start.Add(time.Duration(tick) * telemetry.GridStep), Records: map[string]telemetry.TelemetryRecord{}, Missing: missing})
			if err != nil {
				t.Fatal(err)
			}
			fragment, err := domain.TickFragment(options.Binding.ExecutionID, snapshot.Identity, result)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(fragment)
			if err != nil {
				t.Fatal(err)
			}
			if g < 4 {
				if _, err := service.Accept(ctx, raw); err != nil {
					t.Fatal(err)
				}
			} else {
				finals = append(finals, raw)
			}
		}
	}
	guard, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Rollback(context.Background())
	if _, err := guard.Exec(ctx, `SELECT 1 FROM cad_v2.execution_partitions WHERE execution_id=$1::uuid AND group_id='a01' FOR UPDATE`, r.Binding.ExecutionID); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, len(finals))
	for _, raw := range finals {
		go func(raw []byte) { _, err := service.Accept(ctx, raw); results <- err }(raw)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var blocked int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid<>pg_backend_pid()`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked >= len(finals) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("concurrent join transactions did not reach lock barrier", blocked)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := guard.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for range finals {
		if err := <-results; err != nil {
			t.Errorf("concurrent timestamp join failed: %v", err)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_results`).Scan(&count); err != nil || count != 16 {
		t.Fatal("lost concurrent final diagnostics", count, err)
	}
}
