package usecase_test

import (
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"reflect"
	"testing"
)

// A crash loses complete-but-uncommitted ticks AND a prefetched partial tick.
// Only acknowledged results count; replay must reproduce every original field.
func TestCadenceBatchReplayAndFinalization(t *testing.T) {
	for _, delta := range []int{0, 10, 30, 60} {
		f := workerfixture.New(t, 14, delta)
		messages := []domain.InputMessage{}
		markers := []int{}
		for tick := 0; tick < 14; tick++ {
			count, base := 18, 2.0
			if tick == 7 || tick == 10 {
				base = 0
			}
			if tick == 8 {
				count = 17
			}
			if tick == 9 {
				count = 0
			}
			messages = append(messages, tickMessages(f, tick, count, base)...)
			markers = append(markers, len(messages)-1)
		}
		run := func(t *testing.T, cadence int, crash bool) ([]group.GroupProcessorResult, []group.RBI2Event, core.Checkpoint) {
			p, err := core.NewPartitionWithCadence(f.Bundle, f.Options, 0, nil, cadence)
			if err != nil {
				t.Fatal(err)
			}
			var saved core.Checkpoint
			results := []group.GroupProcessorResult{}
			events := []group.RBI2Event{}
			crashAt := markers[cadence] + 2 // two records beyond the unconfirmed complete tail
			crashed := false
			for i := 0; i < len(messages); i++ {
				tr, err := p.Accept(int64(i), messages[i])
				if err != nil {
					t.Fatal(err)
				}
				if tr != nil {
					wantCount := cadence
					if tr.Checkpoint.Finalized {
						wantCount = 14 - int(saved.LastTick.Sub(f.Options.Window.Start)/domain.GridStep) - 1
						if saved.Revision == 0 {
							wantCount = 14
						}
					}
					if len(tr.Results) != wantCount || tr.Checkpoint.NextOffset != int64(i+1) {
						t.Fatal("invalid batch/offset", len(tr.Results), wantCount, tr.Checkpoint.NextOffset, i)
					}
					results = append(results, tr.Results...)
					events = append(events, tr.Result.Events...)
					saved = tr.Checkpoint
					if err := p.Ack(saved.Revision); err != nil {
						t.Fatal(err)
					}
				}
				if crash && !crashed && i == crashAt {
					var snapshot group.GroupProcessorSnapshot
					if err := json.Unmarshal(saved.Snapshot, &snapshot); err != nil || snapshot.Journal.Open == nil {
						t.Fatal("mid-parent durable snapshot absent", err)
					}
					if saved.NextOffset > int64(i) {
						t.Fatal("partial offset became durable")
					}
					p, err = core.NewPartitionWithCadence(f.Bundle, f.Options, 0, &saved, cadence)
					if err != nil {
						t.Fatal(err)
					}
					i = int(saved.NextOffset) - 1
					crashed = true
				}
			}
			if !p.Finished() || !saved.Finalized {
				t.Fatal("short final batch not flushed")
			}
			if crash && !crashed {
				t.Fatal("crash not exercised")
			}
			return results, events, saved
		}
		wantTicks, wantEvents, wantCheckpoint := run(t, 1, false)
		for _, cadence := range []int{1, 3, 6} {
			for _, crash := range []bool{false, true} {
				t.Run(fmt.Sprintf("d%d-c%d-crash%v", delta, cadence, crash), func(t *testing.T) {
					ticks, events, saved := run(t, cadence, crash)
					if !reflect.DeepEqual(ticks, wantTicks) || !reflect.DeepEqual(events, wantEvents) || saved.SHA256 != wantCheckpoint.SHA256 || saved.NextOffset != wantCheckpoint.NextOffset {
						t.Fatal("cadence or recovery changed mathematical outputs")
					}
					if saved.Revision != int64((14+cadence-1)/cadence) {
						t.Fatal("wrong cadence revision", saved.Revision)
					}
				})
			}
		}
	}
}
