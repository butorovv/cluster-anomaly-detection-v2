package replayer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/kafkainput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/usecase"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	prepared "github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/parquet"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// tickHashes is a constant-memory, per-partition comparison of every assembled
// field, including exact finite float bits and NaN-aware missingness. Expected
// ticks come from a separate direct Parquet read, never from Kafka payloads.
type tickHashes struct {
	h             [16]hash.Hash
	rows, markers int64
	counts        [16]int64
}

func newTickHashes() *tickHashes {
	d := &tickHashes{}
	for i := range d.h {
		d.h[i] = sha256.New()
	}
	return d
}
func (d *tickHashes) add(t domain.GroupTick) error {
	p, err := topology.PartitionForGroup(t.GroupID)
	if err != nil {
		return err
	}
	h := d.h[p]
	write := func(v uint64) { var b [8]byte; binary.LittleEndian.PutUint64(b[:], v); _, _ = h.Write(b[:]) }
	text := func(v string) { write(uint64(len(v))); _, _ = io.WriteString(h, v) }
	text(t.Binding.ExecutionID)
	text(t.Binding.InputSHA256)
	text(t.Binding.ProfileSHA256)
	text(t.GroupID)
	write(uint64(t.Timestamp.UnixNano()))
	write(uint64(t.ValidRecords))
	hosts := make([]string, 0, len(t.Records))
	for host := range t.Records {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)
	write(uint64(len(hosts)))
	for _, host := range hosts {
		r := t.Records[host]
		text(r.Hostname)
		text(r.GroupID)
		write(uint64(r.Timestamp.UnixNano()))
		write(uint64(r.NodeIndex))
		write(uint64(r.AvailabilityMask))
		if r.Valid {
			write(1)
		} else {
			write(0)
		}
		for _, v := range r.Features() {
			if math.IsNaN(v) {
				write(0x7ff8000000000000)
			} else {
				write(math.Float64bits(v))
			}
		}
	}
	write(uint64(len(t.Missing)))
	for _, host := range t.Missing {
		text(host)
	}
	d.rows += int64(len(t.Records))
	d.markers++
	d.counts[p]++
	return nil
}

type directTicks struct {
	binding    domain.InputBinding
	assemblers map[string]*usecase.TickAssembler
	digest     *tickHashes
}

func (d *directTicks) Send(_ context.Context, r domain.TelemetryRecord) error {
	_, err := d.assemblers[r.GroupID].Accept(domain.InputMessage{Version: domain.InputVersion, Type: domain.RecordMessage, Binding: d.binding, GroupID: r.GroupID, Timestamp: r.Timestamp, Record: &r})
	return err
}
func (d *directTicks) Complete(_ context.Context, c domain.TimestampComplete) error {
	tick, err := d.assemblers[c.GroupID].Accept(domain.InputMessage{Version: domain.InputVersion, Type: domain.CompleteMessage, Binding: d.binding, GroupID: c.GroupID, Timestamp: c.Timestamp, Completion: &c})
	if err != nil {
		return err
	}
	if tick == nil {
		return fmt.Errorf("direct marker produced no tick")
	}
	return d.digest.add(*tick)
}
func makeAssemblers(t *testing.T, b domain.InputBinding, w domain.InputWindow) map[string]*usecase.TickAssembler {
	t.Helper()
	out := map[string]*usecase.TickAssembler{}
	for _, g := range topology.CanonicalGroups() {
		a, err := usecase.NewTickAssembler(g.Code, b, w)
		if err != nil {
			t.Fatal(err)
		}
		out[g.Code] = a
	}
	return out
}

func kafkaReplayParity(t *testing.T, path string, real bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	seeds := os.Getenv("P04_KAFKA_BROKERS")
	if seeds == "" {
		seeds = "localhost:9092"
	}
	broker := kafkainput.Broker{Seeds: strings.Split(seeds, ",")}
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker.Seeds...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	a := kadm.NewClient(cl)
	id, err := kafkainput.CheckTopic(ctx, cl, kafkainput.Topic)
	if err != nil {
		t.Fatal("prepare Compose telemetry.raw first", err)
	}
	ends, err := a.ListEndOffsets(ctx, kafkainput.Topic)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	window := domain.InputWindow{Start: start, End: start.Add(8 * time.Hour)}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	hasher := sha256.New()
	_, err = io.Copy(hasher, file)
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.InputBinding{ExecutionID: fmt.Sprintf("p04-parity-%d", time.Now().UnixNano()), InputSHA256: fmt.Sprintf("%x", hasher.Sum(nil)), ProfileSHA256: "a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36"}
	if real && binding.InputSHA256 != "cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7" {
		t.Fatal("original prepared SHA mismatch")
	}
	if !real {
		window.End = start.Add(30 * time.Second)
	}
	want := newTickHashes()
	expected := &directTicks{binding: binding, assemblers: makeAssemblers(t, binding, window), digest: want}
	r, err := prepared.OpenParquet(path)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := usecase.ReplayFullGrid(ctx, &usecase.WindowReader{Source: r, Start: window.Start, End: window.End}, expected, expected, usecase.RealClock{}, 0, window.Start, window.End)
	closeErr := r.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	for _, assembler := range expected.assemblers {
		if err := assembler.Finish(); err != nil {
			t.Fatal(err)
		}
	}
	got := newTickHashes()
	assemblers := makeAssemblers(t, binding, window)
	finished := errors.New("all 16 group windows completed")
	cfg := kafkainput.ConsumerConfig{Broker: broker, Binding: binding, Window: window, Group: binding.ExecutionID,
		Load: func(_ context.Context, identity kafkainput.Identity) (kafkainput.Position, error) {
			if identity.TopicID != id {
				return kafkainput.Position{}, fmt.Errorf("topic identity changed")
			}
			offset, ok := ends[kafkainput.Topic][identity.Partition]
			if !ok || offset.Err != nil {
				return kafkainput.Position{}, fmt.Errorf("initial offset missing: %v", offset.Err)
			}
			return kafkainput.Position{Identity: identity, NextOffset: offset.Offset}, nil
		},
		Observe: func(event string, ps []int32) { t.Logf("consumer %s partitions=%v", event, ps) },
	}
	done := make(chan error, 1)
	go func() {
		done <- kafkainput.ConsumeGroup(ctx, cfg, func(_ context.Context, _ *kgo.Record, m domain.InputMessage) error {
			tick, err := assemblers[m.GroupID].Accept(m)
			if err != nil {
				return err
			}
			if tick == nil {
				return nil
			}
			if err := got.add(*tick); err != nil {
				return err
			}
			if got.markers == want.markers {
				return finished
			}
			return nil
		})
	}()
	args := []string{"--input", path, "--speed", "0", "--output", "kafka", "--kafka-brokers", seeds, "--completion-mode", "full-grid", "--wire-format", "v1", "--start", window.Start.Format(time.RFC3339), "--end", window.End.Format(time.RFC3339), "--execution-id", binding.ExecutionID, "--input-sha256", binding.InputSHA256, "--profile-sha256", binding.ProfileSHA256}
	if err := Replayer(ctx, args, func(string) string { return "" }, io.Discard, io.Discard); err != nil {
		cancel()
		<-done
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, finished) {
		t.Fatal(err)
	}
	for _, assembler := range assemblers {
		if err := assembler.Finish(); err != nil {
			t.Fatal(err)
		}
	}
	if want.rows != got.rows || want.markers != got.markers || want.counts != got.counts {
		t.Fatal("counts differ", want.rows, got.rows, want.markers, got.markers)
	}
	for p := range want.h {
		if string(want.h[p].Sum(nil)) != string(got.h[p].Sum(nil)) {
			t.Fatal("Parquet/Kafka assembled tick digest differs", p)
		}
	}
	if real && (got.rows != 829440 || got.markers != 46080) {
		t.Fatal("real D2 counts", got.rows, got.markers)
	}
	if got.rows != stats.Rows || got.markers != stats.Completions {
		t.Fatal("direct replay stats differ")
	}
	t.Logf("real_D2=%t rows=%d markers=%d Kafka_messages=%d assembled=%d groups=16 partitions=16; all per-partition tick hashes match independent direct Parquet; bounded streaming", real, got.rows, got.markers, got.rows+got.markers, got.markers)
}

func TestKafkaReplayerSynthetic(t *testing.T) {
	if os.Getenv("P04_KAFKA") != "1" && os.Getenv("P04_KAFKA_STRICT") != "1" {
		t.Skip("opt-in Kafka")
	}
	path := filepath.Join(t.TempDir(), "synthetic.parquet")
	writeCLIFixture(t, path)
	kafkaReplayParity(t, path, false)
}
func TestKafkaRealD2(t *testing.T) {
	if os.Getenv("P04_REAL_ACCEPTANCE") != "1" {
		t.Skip("P04_REAL_ACCEPTANCE=1 requires Kafka and original P03_REAL_ROOT assets")
	}
	root := os.Getenv("P03_REAL_ROOT")
	if root == "" {
		t.Fatal("P03_REAL_ROOT required")
	}
	kafkaReplayParity(t, filepath.Join(root, "data/prepared/summit-D1-pilot/D1.parquet"), true)
}
