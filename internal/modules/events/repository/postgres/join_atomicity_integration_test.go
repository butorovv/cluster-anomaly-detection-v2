package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5"
)

func TestDiagnosticJoinRollsBackAndCanRetry(t *testing.T) {
	pool := postgrestest.New(t)
	ctx := context.Background()
	f := workerfixture.New(t, 1, 0)
	f.Options.Groups = 4
	p, err := group.NewGroupProcessor(f.Bundle, f.Options)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r := checkpoints.Registration{Identity: snapshot.Identity, Binding: f.Options.Binding, ClusterID: "p14-atomic", TopicID: "p14-atomic", ConsumerGroup: "p14-atomic", OutputTransport: "polling", Diagnostics: true, StartOffsets: map[string]int64{}}
	for partition := 0; partition < 16; partition++ {
		r.StartOffsets[fmt.Sprint(partition)] = 0
	}
	if err := checkpoints.Register(ctx, pool, r); err != nil {
		t.Fatal(err)
	}
	fragments := make([][]byte, 4)
	for i := range fragments {
		options := f.Options
		options.GroupID = fmt.Sprintf("a%02d", i+1)
		processor, err := group.NewGroupProcessor(f.Bundle, options)
		if err != nil {
			t.Fatal(err)
		}
		at := options.Window.Start
		missing := make([]string, 18)
		for host := 1; host <= 18; host++ {
			missing[host-1] = fmt.Sprintf("%sn%02d", options.GroupID, host)
		}
		result, err := processor.Step(telemetry.GroupTick{Binding: options.Binding, GroupID: options.GroupID, Timestamp: at, Records: map[string]telemetry.TelemetryRecord{}, Missing: missing})
		if err != nil {
			t.Fatal(err)
		}
		fragment, err := domain.TickFragment(options.Binding.ExecutionID, snapshot.Identity, result)
		if err != nil {
			t.Fatal(err)
		}
		fragments[i], err = json.Marshal(fragment)
		if err != nil {
			t.Fatal(err)
		}
	}
	sqlStore := repo.MasterStore{DB: pool}
	service := usecase.Master{Store: sqlStore}
	for _, raw := range fragments[:3] {
		if receipt, err := service.Accept(ctx, raw); err != nil || !receipt.Accepted {
			t.Fatal("partial fragment not accepted", receipt, err)
		}
	}
	// Exercise the SQL side of Kafka intake without a broker: the last fragment,
	// all finals, join marker and result offset must share the same transaction.
	if err := sqlStore.BindResults(ctx, r.Binding.ExecutionID, "p14-atomic", "p14-results", "p14-master", r.StartOffsets); err != nil {
		t.Fatal(err)
	}
	var last domain.Envelope
	if err := json.Unmarshal(fragments[3], &last); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE FUNCTION cad_v2.p14_fail_second_final() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.group_id='a02' THEN RAISE EXCEPTION 'injected final insert failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER p14_fail_second_final BEFORE INSERT ON cad_v2.master_results FOR EACH ROW EXECUTE FUNCTION cad_v2.p14_fail_second_final()`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlStore.AcceptKafka(ctx, r.Binding.ExecutionID, "p14-results", 0, 0, &last); err == nil {
		t.Fatal("last fragment unexpectedly survived final materialization failure")
	}
	if offset, err := sqlStore.ResultOffset(ctx, r.Binding.ExecutionID, 0); err != nil || offset != 0 {
		t.Fatal("failed join advanced durable result offset", offset, err)
	}
	var fragmentsCount, finals, joins int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_tick_fragments WHERE execution_id=$1::uuid`, r.Binding.ExecutionID).Scan(&fragmentsCount); err != nil || fragmentsCount != 3 {
		t.Fatal("failed join did not roll back last fragment", fragmentsCount, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_results WHERE execution_id=$1::uuid`, r.Binding.ExecutionID).Scan(&finals); err != nil || finals != 0 {
		t.Fatal("failed join left partial finals", finals, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_tick_joins WHERE execution_id=$1::uuid`, r.Binding.ExecutionID).Scan(&joins); err != nil || joins != 0 {
		t.Fatal("failed join left marker", joins, err)
	}
	if _, err := pool.Exec(ctx, `DROP TRIGGER p14_fail_second_final ON cad_v2.master_results; DROP FUNCTION cad_v2.p14_fail_second_final()`); err != nil {
		t.Fatal(err)
	}
	if accepted, err := sqlStore.AcceptKafka(ctx, r.Binding.ExecutionID, "p14-results", 0, 0, &last); err != nil || !accepted {
		t.Fatal("retry of rolled back fragment failed", accepted, err)
	}
	if offset, err := sqlStore.ResultOffset(ctx, r.Binding.ExecutionID, 0); err != nil || offset != 1 {
		t.Fatal("successful join did not commit result offset", offset, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_results WHERE execution_id=$1::uuid`, r.Binding.ExecutionID).Scan(&finals); err != nil || finals != 4 {
		t.Fatal("retry did not materialize all final groups", finals, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.master_tick_joins WHERE execution_id=$1::uuid`, r.Binding.ExecutionID).Scan(&joins); err != nil || joins != 1 {
		t.Fatal("retry did not write exactly one join marker", joins, err)
	}
	var idsBefore []string
	rows, err := pool.Query(ctx, `SELECT message_id FROM cad_v2.master_results WHERE execution_id=$1::uuid ORDER BY group_id`, r.Binding.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		idsBefore = append(idsBefore, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if accepted, err := sqlStore.AcceptKafka(ctx, r.Binding.ExecutionID, "p14-results", 0, 1, &last); err != nil || accepted {
		t.Fatal("duplicate last fragment was not idempotent", accepted, err)
	}
	if offset, err := sqlStore.ResultOffset(ctx, r.Binding.ExecutionID, 0); err != nil || offset != 2 {
		t.Fatal("duplicate delivery did not advance result offset exactly once", offset, err)
	}
	var idsAfter []string
	rows, err = pool.Query(ctx, `SELECT message_id FROM cad_v2.master_results WHERE execution_id=$1::uuid ORDER BY group_id`, r.Binding.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		idsAfter = append(idsAfter, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(idsBefore) != 4 || len(idsAfter) != 4 {
		t.Fatal("unexpected final message ID cardinality", idsBefore, idsAfter)
	}
	for i := range idsBefore {
		if idsBefore[i] != idsAfter[i] {
			t.Fatal("duplicate changed final message IDs", idsBefore, idsAfter)
		}
	}
}

func TestMasterIdentityLocksAllowKeyShare(t *testing.T) {
	for _, kind := range []string{"tick_fragment", "group_result"} {
		t.Run(kind, func(t *testing.T) {
			pool := postgrestest.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			f := workerfixture.New(t, 1, 0)
			f.Options.Groups = 4
			results := make([]group.GroupProcessorResult, 4)
			var id group.RBI2Identity
			for g := 1; g <= 4; g++ {
				options := f.Options
				options.GroupID = fmt.Sprintf("a%02d", g)
				processor, err := group.NewGroupProcessor(f.Bundle, options)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := processor.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				id = snapshot.Identity
				missing := make([]string, 18)
				for h := 1; h <= 18; h++ {
					missing[h-1] = fmt.Sprintf("%sn%02d", options.GroupID, h)
				}
				results[g-1], err = processor.Step(telemetry.GroupTick{Binding: options.Binding, GroupID: options.GroupID, Timestamp: options.Window.Start, Records: map[string]telemetry.TelemetryRecord{}, Missing: missing})
				if err != nil {
					t.Fatal(err)
				}
			}
			r := checkpoints.Registration{Identity: id, Binding: f.Options.Binding, ClusterID: "p14-lock", TopicID: "p14-lock", ConsumerGroup: "p14-lock", OutputTransport: "polling", Diagnostics: true, StartOffsets: map[string]int64{}}
			for partition := 0; partition < 16; partition++ {
				r.StartOffsets[fmt.Sprint(partition)] = 0
			}
			if err := checkpoints.Register(ctx, pool, r); err != nil {
				t.Fatal(err)
			}
			envelope, err := domain.TickFragment(r.Binding.ExecutionID, id, results[0])
			if err != nil {
				t.Fatal(err)
			}
			function := "cad_v2.accept_tick_fragment"
			if kind == "group_result" {
				raw := make([]group.Raw, 4)
				for i := range raw {
					raw[i] = results[i].Raw
				}
				group.AnnotateOutside(raw)
				payload, err := json.Marshal(domain.GroupResult{Raw: raw[0], Temporal: results[0].Temporal, Evidence: results[0].Evidence})
				if err != nil {
					t.Fatal(err)
				}
				envelope = domain.Envelope{SchemaVersion: 1, ExecutionID: r.Binding.ExecutionID, RunID: id.RunID, ConfigID: id.ConfigID, GroupID: "a01", MessageID: domain.MessageID(id.RunID, "a01", "group_result", f.Options.Window.Start.UTC().Format(time.RFC3339), 1), Kind: "group_result", Revision: 1, EventTime: f.Options.Window.Start, Terminal: true, Payload: payload}
				if err := domain.Validate(envelope, id); err != nil {
					t.Fatal(err)
				}
				function = "cad_v2.accept_result"
			}
			bytes, err := json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			locker, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer locker.Rollback(context.Background())
			var accepted bool
			if err := locker.QueryRow(ctx, "SELECT "+function+"($1::uuid,$2::jsonb)", envelope.ExecutionID, bytes).Scan(&accepted); err != nil || !accepted {
				t.Fatal("valid Master input rejected", accepted, err)
			}
			keyShareCtx, keyShareCancel := context.WithTimeout(ctx, 400*time.Millisecond)
			defer keyShareCancel()
			var selected int
			err = pool.QueryRow(keyShareCtx, `SELECT 1 FROM cad_v2.execution_partitions WHERE execution_id=$1::uuid AND group_id=$2 FOR KEY SHARE`, envelope.ExecutionID, envelope.GroupID).Scan(&selected)
			if err != nil || selected != 1 {
				t.Fatalf("Worker FK-style KEY SHARE blocked: value=%d err=%v", selected, err)
			}
			mutatorCtx, mutatorCancel := context.WithTimeout(ctx, 250*time.Millisecond)
			defer mutatorCancel()
			var duplicate bool
			err = pool.QueryRow(mutatorCtx, "SELECT "+function+"($1::uuid,$2::jsonb)", envelope.ExecutionID, bytes).Scan(&duplicate)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("competing same-timestamp intake did not wait: value=%v err=%v", duplicate, err)
			}
			if err := locker.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
				t.Fatal(err)
			}
		})
	}
}
