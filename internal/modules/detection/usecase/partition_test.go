package usecase_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
)

func tickMessages(f workerfixture.Fixture, index, count int, base float64) []domain.InputMessage {
	at := f.Options.Window.Start.Add(time.Duration(index) * domain.GridStep)
	rows := make([]domain.InputMessage, 0, count+1)
	for h := 1; h <= count; h++ {
		r := workerfixture.Record("a01", h, at, base)
		rows = append(rows, domain.InputMessage{Version: domain.InputVersion, Type: domain.RecordMessage, Binding: f.Options.Binding, GroupID: "a01", Timestamp: at, Record: &r})
	}
	c := domain.TimestampComplete{GroupID: "a01", Timestamp: at, Records: count, ValidRecords: count}
	return append(rows, domain.InputMessage{Version: domain.InputVersion, Type: domain.CompleteMessage, Binding: f.Options.Binding, GroupID: "a01", Timestamp: at, Completion: &c})
}

func TestPartitionCompletePartialEmptyAndParity(t *testing.T) {
	for _, delta := range []int{0, 10, 30, 60} {
		f := workerfixture.New(t, 4, delta)
		p, err := core.NewPartition(f.Bundle, f.Options, 7, nil)
		if err != nil {
			t.Fatal(err)
		}
		reference, err := group.NewGroupProcessor(f.Bundle, f.Options)
		if err != nil {
			t.Fatal(err)
		}
		offset := int64(7)
		for i, count := range []int{18, 17, 0, 18} {
			messages := tickMessages(f, i, count, 2)
			records := map[string]domain.TelemetryRecord{}
			missing := []string{}
			for h := 1; h <= 18; h++ {
				r := workerfixture.Record("a01", h, messages[0].Timestamp, 2)
				if h <= count {
					records[r.Hostname] = r
				} else {
					missing = append(missing, r.Hostname)
				}
			}
			want, err := reference.Step(domain.GroupTick{Binding: f.Options.Binding, GroupID: "a01", Timestamp: messages[0].Timestamp, Records: records, Missing: missing, ValidRecords: count})
			if err != nil {
				t.Fatal(err)
			}
			if i == 3 {
				tail, err := reference.Finish()
				if err != nil {
					t.Fatal(err)
				}
				want.Events = append(want.Events, tail...)
			}
			for k, m := range messages {
				transition, err := p.Accept(offset, m)
				offset++
				if err != nil {
					t.Fatal(err)
				}
				if k < len(messages)-1 {
					if transition != nil {
						t.Fatal("record advanced durable boundary")
					}
					continue
				}
				if transition == nil || transition.Checkpoint.NextOffset != offset || transition.Checkpoint.Revision != int64(i+1) || !reflect.DeepEqual(transition.Result, want) {
					t.Fatal("complete boundary differs from frozen reference", transition)
				}
				if i == 1 || i == 2 {
					if transition.Result.Temporal.Vote != group.Unknown {
						t.Fatal("missing became known")
					}
				}
				if err := p.Ack(transition.Checkpoint.Revision); err != nil {
					t.Fatal(err)
				}
			}
		}
		if !p.Finished() {
			t.Fatal("final marker did not finalize")
		}
	}
}

func TestPartitionPartialTailReplaysFromDurableBoundary(t *testing.T) {
	f := workerfixture.New(t, 2, 30)
	p, err := core.NewPartition(f.Bundle, f.Options, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	var saved core.Checkpoint
	offset := int64(0)
	for _, m := range tickMessages(f, 0, 18, 2) {
		tr, err := p.Accept(offset, m)
		offset++
		if err != nil {
			t.Fatal(err)
		}
		if tr != nil {
			saved = tr.Checkpoint
			if err := p.Ack(saved.Revision); err != nil {
				t.Fatal(err)
			}
		}
	}
	second := tickMessages(f, 1, 18, 2)
	for _, m := range second[:2] {
		if tr, err := p.Accept(offset, m); err != nil || tr != nil {
			t.Fatal(tr, err)
		}
		offset++
	}
	if saved.NextOffset != 19 {
		t.Fatal("partial offset leaked into durable state", saved.NextOffset)
	}
	restored, err := core.NewPartition(f.Bundle, f.Options, 0, &saved)
	if err != nil {
		t.Fatal(err)
	}
	offset = saved.NextOffset
	for _, m := range second {
		tr, err := restored.Accept(offset, m)
		offset++
		if err != nil {
			t.Fatal(err)
		}
		if tr != nil {
			if !tr.Checkpoint.Finalized || len(tr.Result.Events) != 1 {
				t.Fatal("lost open event on restore")
			}
			if err := restored.Ack(tr.Checkpoint.Revision); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !restored.Finished() {
		t.Fatal("restore did not finish")
	}
}

func TestPartitionFailStopAndSnapshotMetadata(t *testing.T) {
	f := workerfixture.New(t, 2, 0)
	p, _ := core.NewPartition(f.Bundle, f.Options, 0, nil)
	first := tickMessages(f, 0, 0, 0)[0]
	tr, err := p.Accept(0, first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Accept(1, tickMessages(f, 1, 0, 0)[0]); err == nil {
		t.Fatal("advanced before durable ACK")
	}
	if err = p.Ack(tr.Checkpoint.Revision); err == nil {
		t.Fatal("failed RAM resumed")
	}
	p, _ = core.NewPartition(f.Bundle, f.Options, 0, nil)
	if _, err = p.Accept(1, first); err == nil {
		t.Fatal("wrong offset accepted")
	}
	if _, err = p.Accept(0, first); err == nil {
		t.Fatal("invalid RAM reused")
	}
	p, _ = core.NewPartition(f.Bundle, f.Options, 0, nil)
	tr, err = p.Accept(0, first)
	if err != nil {
		t.Fatal(err)
	}
	bad := tr.Checkpoint
	bad.SHA256 = "wrong"
	if _, err = core.NewPartition(f.Bundle, f.Options, 0, &bad); err == nil {
		t.Fatal("invalid checksum restored")
	}
	bad = tr.Checkpoint
	var snap group.GroupProcessorSnapshot
	_ = json.Unmarshal(bad.Snapshot, &snap)
	snap.SchemaVersion++
	bad.Snapshot, _ = json.Marshal(snap)
	bad.SHA256 = core.SnapshotHash(bad.Snapshot)
	if _, err = core.NewPartition(f.Bundle, f.Options, 0, &bad); err == nil {
		t.Fatal("invalid math version restored")
	}
}
