package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	eventusecase "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/inputcodec"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type workerRig struct {
	fixture  workerfixture.Fixture
	store    *repo.Store
	pool     *pgxpool.Pool
	broker   kafkainput.Broker
	producer *kgo.Client
	group    string
	dsn      string
}

func workerIntegration(t *testing.T, delta int) *workerRig {
	return workerIntegrationMode(t, delta, "polling")
}
func workerIntegrationMode(t *testing.T, delta int, mode string) *workerRig {
	return workerIntegrationFixture(t, delta, mode, nil)
}
func workerIntegrationFixture(t *testing.T, delta int, mode string, frozen *workerfixture.Fixture, diagnostics ...bool) *workerRig {
	t.Helper()
	if os.Getenv("P07_WORKER_INTEGRATION") != "1" {
		t.Skip("opt-in P07_WORKER_INTEGRATION=1 requires isolated real Kafka/PostgreSQL")
	}
	brokers, dsn := os.Getenv("P07_TEST_KAFKA_BROKERS"), os.Getenv("P07_TEST_DATABASE_URL")
	if brokers == "" || dsn == "" || os.Getenv("P07_TEST_KAFKA_ISOLATED") != "1" {
		t.Fatal("explicit isolated Kafka and PostgreSQL prerequisites required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	broker := kafkainput.Broker{Seeds: strings.Split(brokers, ",")}
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker.Seeds...), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	admin := kadm.NewClient(producer)
	results, err := admin.CreateTopics(ctx, 16, 1, map[string]*string{"cleanup.policy": strPointer("delete")}, kafkainput.Topic)
	if err != nil {
		t.Fatal(err)
	}
	if e := results[kafkainput.Topic].Err; e != nil && !strings.Contains(e.Error(), "TOPIC_ALREADY_EXISTS") {
		t.Fatal(e)
	}
	var cluster, topic string
	waitWorker(t, 20*time.Second, func() bool {
		var err error
		cluster, topic, err = kafkainput.DescribeSource(ctx, broker)
		return err == nil
	})
	ends, err := admin.ListEndOffsets(ctx, kafkainput.Topic)
	if err != nil {
		t.Fatal(err)
	}
	var f workerfixture.Fixture
	if frozen != nil {
		f = *frozen
	} else {
		f = workerfixture.New(t, 6, delta)
	}
	execution, err := NewOwnerID()
	if err != nil {
		t.Fatal(err)
	}
	f.Options.Binding.ExecutionID = execution
	p, err := group.NewGroupProcessor(f.Bundle, f.Options)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal("test PostgreSQL unavailable")
	}
	name := "p07_worker_" + strings.ReplaceAll(execution, "-", "")
	if _, err := connection.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	pool, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if _, e := connection.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); e != nil {
			t.Error(e)
		}
		_ = connection.Close(cleanup)
	})
	migration, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Apply(ctx, migration, db.Files); err != nil {
		t.Fatal(err)
	}
	_ = migration.Close(ctx)
	consumer := "p07-" + execution
	registration := repo.Registration{Identity: snap.Identity, Binding: f.Options.Binding, ClusterID: cluster, TopicID: topic, ConsumerGroup: consumer, OutputTransport: mode, Diagnostics: len(diagnostics) > 0 && diagnostics[0], StartOffsets: map[string]int64{}}
	for i := int32(0); i < 16; i++ {
		v := ends[kafkainput.Topic][i]
		if v.Err != nil {
			t.Fatal(v.Err)
		}
		registration.StartOffsets[strconv.Itoa(int(i))] = v.Offset
	}
	if err := repo.Register(ctx, pool, registration); err != nil {
		t.Fatal(err)
	}
	return &workerRig{fixture: f, store: &repo.Store{DB: pool, Registration: registration}, pool: pool, broker: broker, producer: producer, group: consumer, dsn: u.String()}
}
func strPointer(s string) *string { return &s }
func waitWorker(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("Worker integration condition timed out")
}

func (r *workerRig) start(t *testing.T) (*Runner, context.CancelFunc, <-chan error) {
	return r.startOutput(t, nil)
}
func (r *workerRig) startOutput(t *testing.T, output *eventusecase.Dispatcher) (*Runner, context.CancelFunc, <-chan error) {
	t.Helper()
	runner, err := New(Config{Bundle: r.fixture.Bundle, Options: r.fixture.Options, Broker: r.broker, ConsumerGroup: r.group, LeaseTTL: 3 * time.Second, Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)), Output: output}, r.store)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	exited := make(chan struct{})
	go func() { defer close(exited); done <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(15 * time.Second):
			t.Error("Worker cleanup timed out")
		}
	})
	return runner, cancel, done
}
func stopWorker(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	case <-time.After(15 * time.Second):
		t.Error("Worker did not stop")
	}
}
func countAssigned(r *Runner) int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.assigned) }
func (r *workerRig) waitRevision(t *testing.T, revision int64) {
	t.Helper()
	waitWorker(t, 40*time.Second, func() bool {
		var count int
		err := r.pool.QueryRow(context.Background(), "SELECT count(*) FROM cad_v2.group_checkpoints WHERE checkpoint_revision=$1", revision).Scan(&count)
		return err == nil && count == r.fixture.Options.Groups
	})
}

func (r *workerRig) messages(groupID string, tick int) []domain.InputMessage {
	counts := []int{18, 18, 17, 0, 18, 18}
	bases := []float64{2, 0, 2, 0, 2, 2}
	at := r.fixture.Options.Window.Start.Add(time.Duration(tick) * domain.GridStep)
	result := []domain.InputMessage{}
	for node := 1; node <= counts[tick]; node++ {
		record := workerfixture.Record(groupID, node, at, bases[tick])
		result = append(result, domain.InputMessage{Version: domain.InputVersion, Type: domain.RecordMessage, Binding: r.fixture.Options.Binding, GroupID: groupID, Timestamp: at, Record: &record})
	}
	c := domain.TimestampComplete{GroupID: groupID, Timestamp: at, Records: counts[tick], ValidRecords: counts[tick]}
	return append(result, domain.InputMessage{Version: domain.InputVersion, Type: domain.CompleteMessage, Binding: r.fixture.Options.Binding, GroupID: groupID, Timestamp: at, Completion: &c})
}
func (r *workerRig) send(t *testing.T, message domain.InputMessage) {
	t.Helper()
	window := domain.InputWindow{Start: r.fixture.Options.Window.Start, End: r.fixture.Options.Window.End}
	encoded, err := inputcodec.Encode(message, window)
	if err != nil {
		t.Fatal(err)
	}
	partition, _ := strconv.Atoi(message.GroupID[1:])
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	record := &kgo.Record{Topic: kafkainput.Topic, Partition: int32(partition - 1), Key: []byte(message.GroupID), Value: encoded}
	if err := r.producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		t.Fatal(err)
	}
}
func (r *workerRig) sendTick(t *testing.T, tick int) {
	for g := 1; g <= 16; g++ {
		for _, m := range r.messages(fmt.Sprintf("a%02d", g), tick) {
			r.send(t, m)
		}
	}
}

func (r *workerRig) expectedEvents(t *testing.T) map[string]group.RBI2Event {
	t.Helper()
	want := map[string]group.RBI2Event{}
	for g := 1; g <= r.fixture.Options.Groups; g++ {
		opts := r.fixture.Options
		opts.GroupID = fmt.Sprintf("a%02d", g)
		p, err := group.NewGroupProcessor(r.fixture.Bundle, opts)
		if err != nil {
			t.Fatal(err)
		}
		for tick := 0; tick < 6; tick++ {
			messages := r.messages(opts.GroupID, tick)
			records := map[string]domain.TelemetryRecord{}
			missing := []string{}
			for _, m := range messages {
				if m.Record != nil {
					records[m.Record.Hostname] = *m.Record
				}
			}
			for node := 1; node <= 18; node++ {
				host := fmt.Sprintf("%sn%02d", opts.GroupID, node)
				if _, ok := records[host]; !ok {
					missing = append(missing, host)
				}
			}
			result, err := p.Step(domain.GroupTick{Binding: opts.Binding, GroupID: opts.GroupID, Timestamp: messages[0].Timestamp, Records: records, Missing: missing, ValidRecords: messages[len(messages)-1].Completion.ValidRecords})
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range result.Events {
				want[event.ID] = event
			}
		}
		tail, err := p.Finish()
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range tail {
			want[event.ID] = event
		}
		final, err := p.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		durable, err := r.store.Load(context.Background(), opts.GroupID)
		if err != nil || durable == nil {
			t.Fatal("missing final checkpoint", err)
		}
		gotSnapshot, err := core.DecodeCheckpoint(*durable)
		if err != nil || !reflect.DeepEqual(gotSnapshot, final) {
			t.Fatalf("final mathematical state differs from continuous oracle for %s: %v", opts.GroupID, err)
		}
	}
	return want
}
func (r *workerRig) assertEvents(t *testing.T) {
	t.Helper()
	want := r.expectedEvents(t)
	got := map[string]group.RBI2Event{}
	for g := 1; g <= r.fixture.Options.Groups; g++ {
		outputs, err := r.store.Pending(context.Background(), fmt.Sprintf("a%02d", g))
		if err != nil {
			t.Fatal(err)
		}
		for _, output := range outputs {
			if _, ok := got[output.EventID]; ok {
				t.Fatal("duplicate terminal event")
			}
			got[output.EventID] = output.Payload
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("durable terminal events differ from continuous frozen oracle: got%d want%d", len(got), len(want))
	}
}

func TestWorkerKafkaContinuous(t *testing.T) {
	rig := workerIntegration(t, 30)
	_, cancel, done := rig.start(t)
	defer stopWorker(t, cancel, done)
	for tick := 0; tick < 6; tick++ {
		rig.sendTick(t, tick)
		rig.waitRevision(t, int64(tick+1))
	}
	rig.assertEvents(t)
}

func TestWorkerKafkaSingleAndRebalanceRecovery(t *testing.T) {
	for _, delta := range []int{0, 30} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			rig := workerIntegration(t, delta)
			a, cancelA, doneA := rig.start(t)
			defer func() {
				if cancelA != nil {
					stopWorker(t, cancelA, doneA)
				}
			}()
			waitWorker(t, 30*time.Second, func() bool { return countAssigned(a) == 16 })
			rig.sendTick(t, 0)
			rig.waitRevision(t, 1)
			rig.sendTick(t, 1)
			rig.waitRevision(t, 2)
			b, cancelB, doneB := rig.start(t)
			defer stopWorker(t, cancelB, doneB)
			waitWorker(t, 35*time.Second, func() bool { na, nb := countAssigned(a), countAssigned(b); return na > 0 && nb > 0 && na+nb == 16 })
			rig.sendTick(t, 2)
			rig.waitRevision(t, 3)
			stopWorker(t, cancelA, doneA)
			cancelA = nil
			waitWorker(t, 35*time.Second, func() bool { return countAssigned(b) == 16 })
			for tick := 3; tick < 6; tick++ {
				rig.sendTick(t, tick)
				rig.waitRevision(t, int64(tick+1))
			}
			rig.assertEvents(t)
		})
	}
}

func TestWorkerKafkaProcessCrashAndPartialReplay(t *testing.T) {
	rig := workerIntegration(t, 30)
	binary := filepath.Join(t.TempDir(), "worker.exe")
	build := exec.Command("go", "build", "-o", binary, "../../../cmd/worker")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Worker: %v %s", err, output)
	}
	logPath := filepath.Join(t.TempDir(), "worker.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	options := rig.fixture.Options
	cmd := exec.Command(binary, "--assets", rig.fixture.Root, "--profile-sha256", options.Binding.ProfileSHA256, "--input-sha256", options.Binding.InputSHA256, "--execution-id", options.Binding.ExecutionID, "--consumer-group", rig.group, "--kafka-brokers", strings.Join(rig.broker.Seeds, ","), "--start", options.Window.Start.Format(time.RFC3339), "--end", options.Window.End.Format(time.RFC3339), "--delta", "30", "--lease-ttl", "3s", "--log-level", "debug", "--diagnostics=false")
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
		_ = logFile.Close()
	})
	rig.sendTick(t, 0)
	rig.waitRevision(t, 1)
	rig.sendTick(t, 1)
	rig.waitRevision(t, 2)
	partial := rig.messages("a01", 2)
	for _, m := range partial[:2] {
		rig.send(t, m)
	}
	start := rig.store.Registration.StartOffsets["0"]
	wantedOffset := start + 2*19 + 1
	waitWorker(t, 20*time.Second, func() bool {
		raw, _ := os.ReadFile(logPath)
		return strings.Contains(string(raw), fmt.Sprintf(`"offset":%d,"complete":false`, wantedOffset))
	})
	durable, err := rig.store.Load(context.Background(), "a01")
	if err != nil {
		t.Fatal(err)
	}
	if durable.NextOffset != start+38 {
		t.Fatal("partial RAM offset became durable", durable.NextOffset)
	}
	var saved group.GroupProcessorSnapshot
	if err := json.Unmarshal(durable.Snapshot, &saved); err != nil || saved.Journal.Open == nil {
		t.Fatal("open event absent at crash", err)
	}
	expectedEventID := saved.Journal.Open.ID
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	killed = true
	_ = logFile.Close()
	next, cancel, done := rig.start(t)
	defer stopWorker(t, cancel, done)
	waitWorker(t, 40*time.Second, func() bool { return countAssigned(next) == 16 })
	// The two previously received messages remain in Kafka and must be replayed.
	for _, m := range partial[2:] {
		rig.send(t, m)
	}
	for g := 2; g <= 16; g++ {
		for _, m := range rig.messages(fmt.Sprintf("a%02d", g), 2) {
			rig.send(t, m)
		}
	}
	rig.waitRevision(t, 3)
	for tick := 3; tick < 6; tick++ {
		rig.sendTick(t, tick)
		rig.waitRevision(t, int64(tick+1))
	}
	rig.assertEvents(t)
	outputs, err := rig.store.Pending(context.Background(), "a01")
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, e := range outputs {
		ids = append(ids, e.EventID)
	}
	sort.Strings(ids)
	found := false
	for _, id := range ids {
		found = found || id == expectedEventID
	}
	if !found {
		t.Fatal("restart changed open parent EventID")
	}
}
