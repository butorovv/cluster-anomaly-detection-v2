package kafkainput

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/inputcodec"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/usecase"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var errDone = errors.New("expected test window complete")

type testAdmin struct {
	*kadm.Client
	client *kgo.Client
}

func integration(t *testing.T) (ConsumerConfig, *testAdmin) {
	t.Helper()
	if os.Getenv("P04_KAFKA") != "1" && os.Getenv("P04_KAFKA_STRICT") != "1" {
		t.Skip("opt-in: P04_KAFKA=1 (strict: P04_KAFKA_STRICT=1)")
	}
	seeds := os.Getenv("P04_KAFKA_BROKERS")
	if seeds == "" {
		seeds = "localhost:9092"
	}
	broker := Broker{Seeds: strings.Split(seeds, ","), Topic: fmt.Sprintf("p04-%d", time.Now().UnixNano())}
	cl, err := kgo.NewClient(append(broker.options(), kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	a := &testAdmin{kadm.NewClient(cl), cl}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := cl.Ping(ctx); err != nil {
		t.Fatalf("requested Kafka acceptance requires a real broker: %v", err)
	}
	created, err := a.CreateTopics(ctx, 16, 1, map[string]*string{"cleanup.policy": kmsgString("delete")}, broker.Topic)
	if err != nil || created[broker.Topic].Err != nil {
		t.Fatal("create isolated topic", err, created)
	}
	waitFor(t, ctx, func() bool { _, err := CheckTopic(ctx, cl, broker.Topic); return err == nil })
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := a.DeleteTopics(ctx, broker.Topic)
		if err != nil {
			t.Error(err)
		}
	})
	start := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	cfg := ConsumerConfig{Broker: broker, Binding: domain.InputBinding{ExecutionID: broker.Topic, InputSHA256: strings.Repeat("a", 64), ProfileSHA256: strings.Repeat("b", 64)}, Window: domain.InputWindow{Start: start, End: start.Add(100 * time.Second)}, Group: broker.Topic}
	cfg.Load = func(_ context.Context, id Identity) (Position, error) { return Position{id, 0}, nil }
	return cfg, a
}

func kmsgString(v string) *string { return &v }

func synth(cfg ConsumerConfig, g, h, ti int) domain.TelemetryRecord {
	r := domain.TelemetryRecord{Timestamp: cfg.Window.Start.Add(time.Duration(ti) * domain.GridStep), GroupID: fmt.Sprintf("a%02d", g), Hostname: fmt.Sprintf("a%02dn%02d", g, h), NodeIndex: h, Valid: true, AvailabilityMask: domain.FullAvailabilityMask}
	var f [18]float64
	for i := range f {
		f[i] = float64(g*1000+h*20+i) + float64(ti)/10
	}
	if ti == 1 && h == 2 {
		f[10] = math.NaN()
		r.Valid = false
		r.AvailabilityMask &^= 1 << 10
	}
	r.SetFeatures(f)
	return r
}

func producer(t *testing.T, ctx context.Context, cfg ConsumerConfig) *Producer {
	t.Helper()
	p, err := NewProducer(ctx, ProducerConfig{Broker: cfg.Broker, Binding: cfg.Binding, Window: cfg.Window})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func publishMarker(t *testing.T, ctx context.Context, a *testAdmin, cfg ConsumerConfig, p int32, ti int) *kgo.Record {
	t.Helper()
	c := domain.TimestampComplete{GroupID: fmt.Sprintf("a%02d", p+1), Timestamp: cfg.Window.Start.Add(time.Duration(ti) * domain.GridStep)}
	m := domain.InputMessage{Version: domain.InputVersion, Type: domain.CompleteMessage, Binding: cfg.Binding, GroupID: c.GroupID, Timestamp: c.Timestamp, Completion: &c}
	b, err := inputcodec.Encode(m, cfg.Window)
	if err != nil {
		t.Fatal(err)
	}
	r := &kgo.Record{Topic: cfg.topic(), Partition: p, Key: []byte(c.GroupID), Value: b}
	if err := a.client.ProduceSync(ctx, r).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestKafkaSyntheticTicks(t *testing.T) {
	cfg, a := integration(t)
	cfg.Window.End = cfg.Window.Start.Add(3 * domain.GridStep)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	p := producer(t, ctx, cfg)
	for ti, n := range []int{18, 17, 0} {
		for g := 1; g <= 16; g++ {
			valid := 0
			for h := 1; h <= n; h++ {
				r := synth(cfg, g, h, ti)
				if r.Valid {
					valid++
				}
				if err := p.Send(ctx, r); err != nil {
					t.Fatal(err)
				}
			}
			if err := p.Complete(ctx, domain.TimestampComplete{Timestamp: cfg.Window.Start.Add(time.Duration(ti) * domain.GridStep), GroupID: fmt.Sprintf("a%02d", g), Records: n, ValidRecords: valid}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	id, err := CheckTopic(ctx, a.client, cfg.topic())
	if err != nil {
		t.Fatal(err)
	}
	rows, markers, ticks := 0, 0, 0
	for partition := int32(0); partition < 16; partition++ {
		assembler, err := usecase.NewTickAssembler(fmt.Sprintf("a%02d", partition+1), cfg.Binding, cfg.Window)
		if err != nil {
			t.Fatal(err)
		}
		err = ConsumePartition(ctx, cfg, Position{cfg.identity(id, partition), 0}, func(_ context.Context, _ *kgo.Record, m domain.InputMessage) error {
			if m.Record != nil {
				rows++
			} else {
				markers++
			}
			tick, err := assembler.Accept(m)
			if err != nil {
				return err
			}
			if tick == nil {
				return nil
			}
			ti := int(tick.Timestamp.Sub(cfg.Window.Start) / domain.GridStep)
			n := []int{18, 17, 0}[ti]
			if len(tick.Records) != n || len(tick.Missing) != 18-n {
				return fmt.Errorf("tick counts: %+v", tick)
			}
			for h := 1; h <= n; h++ {
				want := synth(cfg, int(partition)+1, h, ti)
				if !reflect.DeepEqual(want, tick.Records[want.Hostname]) { // NaN-aware comparison below
					got := tick.Records[want.Hostname]
					wf, gf := want.Features(), got.Features()
					for i := range wf {
						if math.IsNaN(wf[i]) && math.IsNaN(gf[i]) {
							wf[i], gf[i] = 0, 0
						}
					}
					want.SetFeatures(wf)
					got.SetFeatures(gf)
					if want != got {
						return fmt.Errorf("record changed")
					}
				}
			}
			ticks++
			if ti == 2 {
				return errDone
			}
			return nil
		})
		if !errors.Is(err, errDone) {
			t.Fatal(err)
		}
		if err := assembler.Finish(); err != nil {
			t.Fatal(err)
		}
	}
	if rows != 560 || markers != 48 || ticks != 48 {
		t.Fatal(rows, markers, ticks)
	}
	t.Logf("16 physical partitions: records=%d markers=%d ticks=%d; full/partial/empty, exact F18/masks/order", rows, markers, ticks)
}

func TestKafkaInvalidInputAndOffsets(t *testing.T) {
	cfg, a := integration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	r := publishMarker(t, ctx, a, cfg, 0, 0)
	id, err := CheckTopic(ctx, a.client, cfg.topic())
	if err != nil {
		t.Fatal(err)
	}
	base := Position{cfg.identity(id, 0), r.Offset}
	for _, tc := range []struct {
		name   string
		mutate func(*kgo.Record)
	}{
		{"invalid_json", func(r *kgo.Record) { r.Value = []byte("{") }},
		{"version", func(r *kgo.Record) {
			r.Value = bytes.Replace(r.Value, []byte(domain.InputVersion), []byte("telemetry-input-v99"), 1)
		}},
		{"schema", func(r *kgo.Record) {
			r.Value = bytes.Replace(r.Value, []byte(`"records":0`), []byte(`"records":"x"`), 1)
		}},
		{"routing", func(r *kgo.Record) { r.Partition = 1 }},
		{"key", func(r *kgo.Record) { r.Key = []byte("a02") }},
		{"binding", func(r *kgo.Record) {
			r.Value = bytes.Replace(r.Value, []byte(cfg.Binding.InputSHA256), []byte(strings.Repeat("c", 64)), 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := &kgo.Record{Topic: cfg.topic(), Partition: 0, Key: []byte("a01"), Value: append([]byte(nil), r.Value...)}
			tc.mutate(bad)
			if err := a.client.ProduceSync(ctx, bad).FirstErr(); err != nil {
				t.Fatal(err)
			}
			pos := Position{cfg.identity(id, bad.Partition), bad.Offset}
			called := false
			err := ConsumePartition(ctx, cfg, pos, func(context.Context, *kgo.Record, domain.InputMessage) error { called = true; return errDone })
			if err == nil || errors.Is(err, errDone) || called {
				t.Fatal("invalid message was processed", err, called)
			}
			t.Log(err)
		})
	}
	for _, n := range []int64{-1, 999999} {
		pos := base
		pos.NextOffset = n
		err := ConsumePartition(ctx, cfg, pos, func(context.Context, *kgo.Record, domain.InputMessage) error { return errDone })
		if err == nil {
			t.Fatal("invalid offset accepted", n)
		}
	}
	bad := base
	bad.Identity.TopicID = "recreated-topic"
	if err := ConsumePartition(ctx, cfg, bad, func(context.Context, *kgo.Record, domain.InputMessage) error { return errDone }); err == nil {
		t.Fatal("identity accepted")
	}
	deleted, err := a.DeleteRecords(ctx, kadm.Offsets{cfg.topic(): {0: {Topic: cfg.topic(), Partition: 0, At: r.Offset + 1}}})
	if err != nil || deleted.Error() != nil {
		t.Fatal(err, deleted)
	}
	if err := ConsumePartition(ctx, cfg, base, func(context.Context, *kgo.Record, domain.InputMessage) error { return errDone }); err == nil || errors.Is(err, errDone) {
		t.Fatal("expired offset reset silently", err)
	}
}

// The fixture is intentionally small but external to consumers. State survives a
// failed handler/client, and every new assignment must load it again.
type offsetFixture struct {
	mu        sync.Mutex
	positions map[int32]Position
	loads     map[int32]int
}

func (f *offsetFixture) load(_ context.Context, id Identity) (Position, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads[id.Partition]++
	p, ok := f.positions[id.Partition]
	if !ok {
		return Position{}, fmt.Errorf("no durable offset")
	}
	return p, nil
}
func (f *offsetFixture) save(r *kgo.Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.positions[r.Partition]
	p.NextOffset = r.Offset + 1
	f.positions[r.Partition] = p
}

func waitFor(t *testing.T, ctx context.Context, check func() bool) {
	t.Helper()
	timer := time.NewTicker(20 * time.Millisecond)
	defer timer.Stop()
	for {
		if check() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("condition not reached", ctx.Err())
		case <-timer.C:
		}
	}
}

func TestKafkaGroupRebalanceAndRecovery(t *testing.T) {
	cfg, a := integration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	// Prefix creates real non-zero offsets. The retained payloads for recovery
	// are complete empty ticks, so no mathematical/assembler state is invented.
	for i := 0; i < 3; i++ {
		publishMarker(t, ctx, a, cfg, 0, 0)
	}
	first := map[int32]int64{}
	for p := int32(0); p < 16; p++ {
		for ti := 0; ti < 6; ti++ {
			r := publishMarker(t, ctx, a, cfg, p, ti)
			if ti == 0 {
				first[p] = r.Offset
			}
		}
	}
	id, err := CheckTopic(ctx, a.client, cfg.topic())
	if err != nil {
		t.Fatal(err)
	}
	f := &offsetFixture{positions: map[int32]Position{}, loads: map[int32]int{}}
	commits := kadm.Offsets{cfg.topic(): {}}
	for p := int32(0); p < 16; p++ {
		f.positions[p] = Position{cfg.identity(id, p), first[p] + 1}
		commits[cfg.topic()][p] = kadm.Offset{Topic: cfg.topic(), Partition: p, At: first[p] + 5, LeaderEpoch: -1}
	}
	committed, err := a.CommitOffsets(ctx, cfg.Group, commits)
	if err != nil || committed.Error() != nil {
		t.Fatal(err, committed)
	}
	cfg.Load = f.load
	crash := errors.New("simulated crash before durable acknowledgement")
	var seen []int64
	err = ConsumeGroup(ctx, cfg, func(_ context.Context, r *kgo.Record, _ domain.InputMessage) error {
		if r.Partition == 0 {
			seen = append(seen, r.Offset)
			return crash
		}
		return nil
	})
	if !errors.Is(err, crash) || !reflect.DeepEqual(seen, []int64{first[0] + 1}) {
		t.Fatal("external offset did not override Kafka commit", err, seen)
	}
	// Restart and prove all offsets after the durable boundary are delivered.
	seen = nil
	err = ConsumeGroup(ctx, cfg, func(_ context.Context, r *kgo.Record, _ domain.InputMessage) error {
		if r.Partition == 0 {
			seen = append(seen, r.Offset)
			if len(seen) == 3 {
				return errDone
			}
		}
		return nil
	})
	if !errors.Is(err, errDone) || !reflect.DeepEqual(seen, []int64{first[0] + 1, first[0] + 2, first[0] + 3}) {
		t.Fatal(err, seen)
	}
	t.Logf("recovery partition 0: Kafka commit=%d durable=%d; crash and restart read %v", first[0]+5, first[0]+1, seen)
	var mu sync.Mutex
	owners := map[int32]string{}
	counts := map[string]int{}
	var violations []string
	observe := func(name string) func(string, []int32) {
		return func(event string, ps []int32) {
			mu.Lock()
			defer mu.Unlock()
			for _, p := range ps {
				if event == "assigned" {
					if prior := owners[p]; prior != "" && prior != name {
						violations = append(violations, fmt.Sprintf("%d owned by %s and %s", p, prior, name))
					}
					owners[p] = name
				} else if owners[p] == name {
					delete(owners, p)
				}
			}
			counts = map[string]int{}
			for _, name := range owners {
				counts[name]++
			}
		}
	}
	start := func(name string) (context.CancelFunc, <-chan error) {
		local, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		c := cfg
		c.Observe = observe(name)
		go func() {
			done <- ConsumeGroup(local, c, func(_ context.Context, r *kgo.Record, _ domain.InputMessage) error {
				mu.Lock()
				if owners[r.Partition] != name {
					violations = append(violations, "processing without ownership")
				}
				mu.Unlock()
				f.save(r)
				return nil
			})
		}()
		return stop, done
	}
	stopA, doneA := start("A")
	defer stopA()
	waitFor(t, ctx, func() bool { mu.Lock(); defer mu.Unlock(); return counts["A"] == 16 })
	stopB, doneB := start("B")
	defer stopB()
	waitFor(t, ctx, func() bool { mu.Lock(); defer mu.Unlock(); return counts["A"] == 8 && counts["B"] == 8 })
	t.Log("assignment: A=16 -> A=8 B=8")
	stopA()
	<-doneA
	waitFor(t, ctx, func() bool { mu.Lock(); defer mu.Unlock(); return counts["B"] == 16 })
	t.Log("A stopped: B=16")
	stopA2, doneA2 := start("A2")
	defer stopA2()
	waitFor(t, ctx, func() bool { mu.Lock(); defer mu.Unlock(); return counts["A2"] == 8 && counts["B"] == 8 })
	// New data after reassignment proves assigned owners actually fetch/process.
	for p := int32(0); p < 16; p++ {
		publishMarker(t, ctx, a, cfg, p, 6)
	}
	waitFor(t, ctx, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		for p, pos := range f.positions {
			if pos.NextOffset != first[p]+7 {
				return false
			}
		}
		return true
	})
	stopA2()
	stopB()
	<-doneA2
	<-doneB
	mu.Lock()
	defer mu.Unlock()
	if len(violations) > 0 {
		t.Fatal(violations)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for p, n := range f.loads {
		if n < 3 {
			t.Fatal("offset not reloaded", p, n)
		}
	}
	t.Log("rejoin: A2=8 B=8; new records processed, no simultaneous partition owners; external positions reloaded")
}

func TestKafkaCancellationAndPartitionCount(t *testing.T) {
	cfg, a := integration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wrong := cfg
	wrong.Topic = cfg.Topic + "-wrong"
	created, err := a.CreateTopics(ctx, 1, 1, nil, wrong.Topic)
	if err != nil || created[wrong.Topic].Err != nil {
		t.Fatal(err, created)
	}
	defer a.DeleteTopics(ctx, wrong.Topic)
	if _, err := NewProducer(ctx, ProducerConfig{Broker: wrong.Broker, Binding: cfg.Binding, Window: cfg.Window}); err == nil {
		t.Fatal("wrong partition count accepted")
	}
	p := producer(t, ctx, cfg)
	stopped, stop := context.WithCancel(ctx)
	stop()
	if err := p.Send(stopped, synth(cfg, 1, 1, 0)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := p.Complete(ctx, domain.TimestampComplete{GroupID: "a01", Timestamp: cfg.Window.Start}); err == nil {
		t.Fatal("failed producer published marker")
	}
	if err := ConsumeGroup(stopped, cfg, func(context.Context, *kgo.Record, domain.InputMessage) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestKafkaGroupRejectsInvalidCheckpoint(t *testing.T) {
	cfg, _ := integration(t)
	for _, kind := range []string{"negative", "future", "identity", "missing"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			c := cfg
			c.Group = cfg.Group + "-" + kind
			c.Load = func(_ context.Context, id Identity) (Position, error) {
				p := Position{id, 0}
				switch kind {
				case "negative":
					p.NextOffset = -1
				case "future":
					p.NextOffset = 1
				case "identity":
					p.Identity.Binding.ExecutionID = "other-execution"
				case "missing":
					return Position{}, fmt.Errorf("durable checkpoint missing")
				}
				return p, nil
			}
			called := false
			err := ConsumeGroup(ctx, c, func(context.Context, *kgo.Record, domain.InputMessage) error { called = true; return errDone })
			if err == nil || called || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errDone) {
				t.Fatal("invalid checkpoint did not fail before fetch", err, called)
			}
			t.Log(err)
		})
	}
}

func TestKafkaAssemblerRejectsBrokenStream(t *testing.T) {
	cfg, a := integration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, err := CheckTopic(ctx, a.client, cfg.topic())
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"counts", "timestamp", "missing_marker"} {
		t.Run(kind, func(t *testing.T) {
			c := domain.TimestampComplete{Timestamp: cfg.Window.Start, GroupID: "a01", Records: 1, ValidRecords: 1}
			m := domain.InputMessage{Version: domain.InputVersion, Type: domain.CompleteMessage, Binding: cfg.Binding, GroupID: c.GroupID, Timestamp: c.Timestamp, Completion: &c}
			if kind == "timestamp" {
				m.Timestamp = m.Timestamp.Add(domain.GridStep)
				c.Timestamp = m.Timestamp
				c.Records, c.ValidRecords = 0, 0
			}
			if kind == "missing_marker" {
				r := synth(cfg, 1, 1, 0)
				m.Type = domain.RecordMessage
				m.Completion = nil
				m.Record = &r
			}
			payload, err := inputcodec.Encode(m, cfg.Window)
			if err != nil {
				t.Fatal(err)
			}
			r := &kgo.Record{Topic: cfg.topic(), Partition: 0, Key: []byte("a01"), Value: payload}
			if err := a.client.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
			assembler, err := usecase.NewTickAssembler("a01", cfg.Binding, cfg.Window)
			if err != nil {
				t.Fatal(err)
			}
			published := 0
			err = ConsumePartition(ctx, cfg, Position{cfg.identity(id, 0), r.Offset}, func(_ context.Context, _ *kgo.Record, m domain.InputMessage) error {
				tick, err := assembler.Accept(m)
				if tick != nil {
					published++
				}
				if err != nil {
					return err
				}
				return errDone
			})
			if published != 0 {
				t.Fatal("published unconfirmed tick")
			}
			if kind == "missing_marker" {
				if !errors.Is(err, errDone) || assembler.Finish() == nil {
					t.Fatal("missing marker accepted", err)
				}
			} else if err == nil || errors.Is(err, errDone) {
				t.Fatal("broken stream accepted", err)
			}
		})
	}
}

func TestKafkaBrokerOutage(t *testing.T) {
	if os.Getenv("P04_KAFKA_OUTAGE") != "1" {
		if os.Getenv("P04_KAFKA_STRICT") == "1" {
			t.Fatal("strict acceptance needs P04_KAFKA_OUTAGE=1 for the owned Compose broker restart test")
		}
		t.Skip("P04_KAFKA_OUTAGE=1 allows restarting only this repo's Compose broker")
	}
	cfg, a := integration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if strings.Join(cfg.Seeds, ",") != "localhost:9092" {
		t.Fatal("outage test restricted to local Compose broker")
	}
	compose := func(args ...string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "docker", append([]string{"compose"}, args...)...)
		cmd.Dir = "../../../../.."
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("compose %v: %v %s", args, err, out)
		}
	}
	p := producer(t, ctx, cfg)
	timeoutProducer := producer(t, ctx, cfg)
	consumerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	received := make(chan int64, 16)
	var offsetMu sync.Mutex
	next := map[int32]int64{}
	cfg.Load = func(_ context.Context, id Identity) (Position, error) {
		offsetMu.Lock()
		defer offsetMu.Unlock()
		return Position{id, next[id.Partition]}, nil
	}
	go func() {
		done <- ConsumeGroup(consumerCtx, cfg, func(_ context.Context, r *kgo.Record, _ domain.InputMessage) error {
			offsetMu.Lock()
			next[r.Partition] = r.Offset + 1
			offsetMu.Unlock()
			received <- r.Offset
			return nil
		})
	}()
	publishMarker(t, ctx, a, cfg, 0, 0)
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	compose("stop", "kafka")
	defer compose("start", "kafka")
	short, shortCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	err := timeoutProducer.Complete(short, domain.TimestampComplete{GroupID: "a16", Timestamp: cfg.Window.Start})
	shortCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unavailable broker must fail delivery deadline", err)
	}
	if err := timeoutProducer.Complete(ctx, domain.TimestampComplete{GroupID: "a16", Timestamp: cfg.Window.Start}); err == nil {
		t.Fatal("failed producer did not latch")
	}
	_ = timeoutProducer.Close()
	if err := p.Send(ctx, synth(cfg, 1, 1, 1)); err != nil {
		t.Fatal(err)
	}
	ack := make(chan error, 1)
	go func() {
		ack <- p.Complete(ctx, domain.TimestampComplete{GroupID: "a01", Timestamp: cfg.Window.Start.Add(domain.GridStep), Records: 1, ValidRecords: 1})
	}()
	select {
	case err := <-ack:
		t.Fatal("producer acknowledged while broker stopped", err)
	case <-time.After(2 * time.Second):
	}
	compose("start", "kafka")
	if err := <-ack; err != nil {
		t.Fatal("producer failed to reconnect", err)
	}
	// Both record and marker must arrive after reconnection.
	for i := 0; i < 2; i++ {
		select {
		case offset := <-received:
			if offset != int64(i+1) {
				t.Fatalf("after outage offset=%d want=%d", offset, i+1)
			}
		case err := <-done:
			t.Fatalf("consumer stopped during outage: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	t.Log("real broker stop/start: no acknowledgement during outage; producer and consumer reconnected, record+marker delivered")
}
