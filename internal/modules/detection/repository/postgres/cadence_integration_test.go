package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"testing"
	"time"
)

func TestCadenceAtomicBatchAndLostCommitACK(t *testing.T) {
	for _, cadence := range []int{1, 3, 6} {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("c%d-committed%v", cadence, after), func(t *testing.T) {
				ctx := context.Background()
				pool := postgrestest.New(t)
				f := workerfixture.New(t, 7, 30)
				p, err := core.NewPartitionWithCadence(f.Bundle, f.Options, 0, nil, cadence)
				if err != nil {
					t.Fatal(err)
				}
				model, err := group.NewGroupProcessor(f.Bundle, f.Options)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := model.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				r := postgres.Registration{Identity: snapshot.Identity, Binding: f.Options.Binding, ClusterID: "cadence", TopicID: "cadence", ConsumerGroup: "cadence", OutputTransport: "http_push", Diagnostics: true, CheckpointIntervalTicks: cadence, StartOffsets: map[string]int64{}}
				for i := 0; i < 16; i++ {
					r.StartOffsets[fmt.Sprint(i)] = 0
				}
				if err := postgres.Register(ctx, pool, r); err != nil {
					t.Fatal(err)
				}
				incompatible := r
				incompatible.CheckpointIntervalTicks = 3
				if cadence == 3 {
					incompatible.CheckpointIntervalTicks = 6
				}
				if err := postgres.ValidateRegistration(ctx, pool, incompatible); err == nil {
					t.Fatal("cadence adopted a different execution contract")
				}
				store := &postgres.Store{DB: pool, Registration: r}
				lease, _, err := store.Acquire(ctx, "a01", "00000000-0000-4000-8000-000000000077", 15*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				messages := []domain.InputMessage{}
				for tick := 0; tick < 7; tick++ {
					at := f.Options.Window.Start.Add(time.Duration(tick) * domain.GridStep)
					for h := 1; h <= 18; h++ {
						record := workerfixture.Record("a01", h, at, 2)
						messages = append(messages, domain.InputMessage{Version: domain.InputVersion, Type: domain.RecordMessage, Binding: f.Options.Binding, GroupID: "a01", Timestamp: at, Record: &record})
					}
					c := domain.TimestampComplete{GroupID: "a01", Timestamp: at, Records: 18, ValidRecords: 18}
					messages = append(messages, domain.InputMessage{Version: domain.InputVersion, Type: domain.CompleteMessage, Binding: f.Options.Binding, GroupID: "a01", Timestamp: at, Completion: &c})
				}
				faulted := false
				durableTicks := 0
				for i := 0; i < len(messages); i++ {
					tr, err := p.Accept(int64(i), messages[i])
					if err != nil {
						t.Fatal(err)
					}
					if tr == nil {
						continue
					}
					target := store
					if !faulted {
						target = &postgres.Store{DB: &commitFaultDB{DB: pool, after: after}, Registration: r}
					}
					revision, saveErr := target.Save(ctx, lease, *tr)
					if !faulted {
						faulted = true
						if !after {
							if saveErr == nil {
								t.Fatal("failed batch was confirmed")
							}
							p.Fail(saveErr)
							saved, err := store.Load(ctx, "a01")
							if err != nil || saved != nil {
								t.Fatal("unconfirmed batch durable", err)
							}
							var count int
							if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.worker_tick_fragments`).Scan(&count); err != nil || count != 0 {
								t.Fatal("RAM fragments escaped", count, err)
							}
							p, err = core.NewPartitionWithCadence(f.Bundle, f.Options, 0, nil, cadence)
							if err != nil {
								t.Fatal(err)
							}
							i = -1
							continue
						}
					}
					if saveErr != nil {
						t.Fatal(saveErr)
					}
					if err := p.Ack(revision); err != nil {
						t.Fatal(err)
					}
					durableTicks += len(tr.Results)
					var fragments int
					if err := pool.QueryRow(ctx, `SELECT count(*) FROM cad_v2.worker_tick_fragments`).Scan(&fragments); err != nil || fragments != durableTicks {
						t.Fatal("incomplete batch fragments", fragments, durableTicks, err)
					}
				}
				saved, err := store.Load(ctx, "a01")
				if err != nil || !saved.Finalized || saved.NextOffset != int64(len(messages)) || saved.Revision != int64((7+cadence-1)/cadence) {
					t.Fatal("final batch boundary", saved, err)
				}
				outputs, err := store.Pending(ctx, "a01")
				if err != nil || len(outputs) != 1 || !outputs[0].Terminal {
					t.Fatal("lost terminal Flush", len(outputs), err)
				}
				var decoded group.GroupProcessorSnapshot
				if err := json.Unmarshal(saved.Snapshot, &decoded); err != nil || decoded.Journal.Open != nil {
					t.Fatal("final Journal not closed", err)
				}
			})
		}
	}
}
