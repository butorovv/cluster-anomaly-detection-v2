package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func cadenceMessages(f workerfixture.Fixture, g string, tick int) []domain.InputMessage {
	at := f.Options.Window.Start.Add(time.Duration(tick) * domain.GridStep)
	count := 18
	if tick == 10 {
		count = 0
	}
	if tick == 11 {
		count = 17
	}
	messages := []domain.InputMessage{}
	for h := 1; h <= count; h++ {
		record := workerfixture.Record(g, h, at, 2)
		messages = append(messages, domain.InputMessage{Version: domain.InputVersion, Type: domain.RecordMessage, Binding: f.Options.Binding, GroupID: g, Timestamp: at, Record: &record})
	}
	c := domain.TimestampComplete{GroupID: g, Timestamp: at, Records: count, ValidRecords: count}
	return append(messages, domain.InputMessage{Version: domain.InputVersion, Type: domain.CompleteMessage, Binding: f.Options.Binding, GroupID: g, Timestamp: at, Completion: &c})
}
func TestWorkerCadenceProcessCrashAndReplay(t *testing.T) {
	if os.Getenv("P07_WORKER_INTEGRATION") != "1" {
		t.Skip("real isolated services required")
	}
	binary := filepath.Join(t.TempDir(), "worker.exe")
	if output, err := exec.Command("go", "build", "-o", binary, "../../../cmd/worker").CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, output)
	}
	for _, cadence := range []int{1, 3, 6} {
		t.Run(fmt.Sprint(cadence), func(t *testing.T) {
			f := workerfixture.New(t, 13, 30)
			f.Options.Groups = 4
			rig := workerIntegrationWithCadence(t, 30, "polling", &f, cadence, true)
			f = rig.fixture
			logPath := filepath.Join(t.TempDir(), "worker.log")
			logFile, err := os.Create(logPath)
			if err != nil {
				t.Fatal(err)
			}
			defer logFile.Close()
			o := f.Options
			cmd := exec.Command(binary, "--assets", f.Root, "--profile-sha256", o.Binding.ProfileSHA256, "--input-sha256", o.Binding.InputSHA256, "--execution-id", o.Binding.ExecutionID, "--consumer-group", rig.group, "--kafka-brokers", strings.Join(rig.broker.Seeds, ","), "--groups", "4", "--start", o.Window.Start.Format(time.RFC3339), "--end", o.Window.End.Format(time.RFC3339), "--delta", "30", "--lease-ttl", "3s", "--checkpoint-interval-ticks", strconv.Itoa(cadence), "--log-level", "debug")
			cmd.Env = append(os.Environ(), "WORKER_DATABASE_URL="+rig.dsn)
			cmd.Stderr = logFile
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			killed := false
			t.Cleanup(func() {
				if !killed {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			})
			sendTick := func(tick int) {
				for g := 1; g <= 4; g++ {
					for _, m := range cadenceMessages(f, fmt.Sprintf("a%02d", g), tick) {
						rig.send(t, m)
					}
				}
			}
			for tick := 0; tick < cadence; tick++ {
				sendTick(tick)
			}
			rig.waitRevision(t, 1)
			saved, err := rig.store.Load(context.Background(), "a01")
			if err != nil {
				t.Fatal(err)
			}
			var snapshot group.GroupProcessorSnapshot
			if err := json.Unmarshal(saved.Snapshot, &snapshot); err != nil || snapshot.Journal.Open == nil {
				t.Fatal("open parent absent", err)
			}
			eventID := snapshot.Journal.Open.ID
			sendTick(cadence)
			partial := cadenceMessages(f, "a01", cadence+1)
			for _, m := range partial[:2] {
				rig.send(t, m)
			}
			start := rig.store.Registration.StartOffsets["0"]
			wanted := start + int64((cadence+1)*19+1)
			waitWorker(t, 20*time.Second, func() bool {
				b, _ := os.ReadFile(logPath)
				return strings.Contains(string(b), fmt.Sprintf(`"offset":%d,"complete":false`, wanted))
			})
			durable, err := rig.store.Load(context.Background(), "a01")
			if err != nil {
				t.Fatal(err)
			}
			wantOffset := start + int64(cadence*19)
			if cadence == 1 {
				wantOffset += 19
			}
			if durable.NextOffset != wantOffset {
				t.Fatal("unconfirmed tail offset durable", durable.NextOffset, wantOffset)
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			killed = true
			next, cancel, done := rig.start(t)
			defer stopWorker(t, cancel, done)
			waitWorker(t, 30*time.Second, func() bool { return countAssigned(next) == 16 })
			for _, m := range partial[2:] {
				rig.send(t, m)
			}
			for g := 2; g <= 4; g++ {
				for _, m := range cadenceMessages(f, fmt.Sprintf("a%02d", g), cadence+1) {
					rig.send(t, m)
				}
			}
			for tick := cadence + 2; tick < 13; tick++ {
				sendTick(tick)
			}
			rig.waitRevision(t, int64((13+cadence-1)/cadence))
			expected := map[string]group.RBI2Event{}
			actual := map[string]group.RBI2Event{}
			for g := 1; g <= 4; g++ {
				options := f.Options
				options.GroupID = fmt.Sprintf("a%02d", g)
				p, err := group.NewGroupProcessor(f.Bundle, options)
				if err != nil {
					t.Fatal(err)
				}
				for tick := 0; tick < 13; tick++ {
					messages := cadenceMessages(f, options.GroupID, tick)
					records := map[string]domain.TelemetryRecord{}
					missing := []string{}
					for _, m := range messages {
						if m.Record != nil {
							records[m.Record.Hostname] = *m.Record
						}
					}
					for h := 1; h <= 18; h++ {
						host := fmt.Sprintf("%sn%02d", options.GroupID, h)
						if _, ok := records[host]; !ok {
							missing = append(missing, host)
						}
					}
					result, err := p.Step(domain.GroupTick{Binding: options.Binding, GroupID: options.GroupID, Timestamp: messages[0].Timestamp, Records: records, Missing: missing, ValidRecords: len(records)})
					if err != nil {
						t.Fatal(err)
					}
					for _, event := range result.Events {
						expected[event.ID] = event
					}
				}
				tail, err := p.Finish()
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range tail {
					expected[event.ID] = event
				}
				pending, err := rig.store.Pending(context.Background(), options.GroupID)
				if err != nil {
					t.Fatal(err)
				}
				for _, e := range pending {
					if _, ok := actual[e.EventID]; ok {
						t.Fatal("duplicate event")
					}
					actual[e.EventID] = e.Payload
				}
				saved, err := rig.store.Load(context.Background(), options.GroupID)
				if err != nil || !saved.Finalized {
					t.Fatal("final checkpoint absent", err)
				}
			}
			if !reflect.DeepEqual(actual, expected) {
				t.Fatal("batch crash changed parent/VoteSegments/RBSubspans")
			}
			if _, ok := actual[eventID]; !ok {
				t.Fatal("open EventID lost after recovery")
			}
			var fragments int
			if err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.worker_tick_fragments`).Scan(&fragments); err != nil || fragments != 4*13 {
				t.Fatal("lost/duplicated durable fragments", fragments, err)
			}
		})
	}
}
