package experiment

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Distribution struct {
	Count int     `json:"count"`
	Min   float64 `json:"min_seconds"`
	Mean  float64 `json:"mean_seconds"`
	P50   float64 `json:"p50_seconds"`
	P95   float64 `json:"p95_seconds"`
	P99   float64 `json:"p99_seconds"`
	Max   float64 `json:"max_seconds"`
}

func distribution(values []float64) Distribution {
	if len(values) == 0 {
		return Distribution{}
	}
	v := append([]float64(nil), values...)
	sort.Float64s(v)
	sum := 0.0
	for _, x := range v {
		sum += x
	}
	percentile := func(p float64) float64 { return v[max(0, int(math.Ceil(p*float64(len(v))))-1)] }
	return Distribution{len(v), v[0], sum / float64(len(v)), percentile(.5), percentile(.95), percentile(.99), v[len(v)-1]}
}

type Measurements struct {
	KafkaTimings            map[string]observability.Histogram `json:"kafka_stage_histograms,omitempty"`
	PostgresWaitSamples     map[string]int                     `json:"postgres_active_wait_samples,omitempty"`
	PostgresWaitSampleCount int                                `json:"postgres_wait_sample_count"`
	KafkaLagStatus          string                             `json:"input_kafka_lag_status"`
	Status                  string                             `json:"status"`
	ResourceStatus          string                             `json:"resource_status"`
	Clock                   string                             `json:"clock"`
	SourceStart             time.Time                          `json:"source_start"`
	FinalJoin               time.Time                          `json:"final_join_confirmed"`
	Throughput              float64                            `json:"group_ticks_per_second"`
	Compute                 Distribution                       `json:"compute"`
	InputQueue              Distribution                       `json:"source_to_first_worker_receive"`
	DurabilityWait          Distribution                       `json:"computed_to_durable_confirmation"`
	Transport               Distribution                       `json:"send_to_master_receive"`
	MasterCommit            Distribution                       `json:"master_intake_to_commit_confirmation"`
	EndToEnd                Distribution                       `json:"source_to_final_join_confirmation"`
	WorkerCPUSeconds        float64                            `json:"worker_cpu_seconds"`
	MasterCPUSeconds        float64                            `json:"master_cpu_seconds"`
	WorkerPeakRSS           uint64                             `json:"worker_peak_aggregate_rss_bytes"`
	MasterPeakRSS           uint64                             `json:"master_peak_rss_bytes"`
	OutboxBacklogMax        float64                            `json:"outbox_backlog_max"`
	KafkaLagMax             float64                            `json:"input_kafka_lag_records_max"`
	Counters                map[string]int64                   `json:"counters"`
	Samples                 int                                `json:"resource_samples"`
	Network                 string                             `json:"network_measurement"`
}
type resourceSample struct {
	PostgresWaits        map[string]int `json:"postgres_active_waits,omitempty"`
	At                   time.Time      `json:"at"`
	WorkerCPU, MasterCPU float64
	WorkerRSS, MasterRSS uint64
	Backlog, Lag         float64
	Counters             map[string]int64 `json:"counters"`
}
type sampler struct {
	database  *pgxpool.Pool
	mu        sync.Mutex
	endpoints map[string]string
	last      map[string]observability.Data
	samples   []resourceSample
	cancel    context.CancelFunc
	done      chan struct{}
	stopOnce  sync.Once
	started   bool
}

func newSampler() *sampler {
	return &sampler{endpoints: map[string]string{}, last: map[string]observability.Data{}}
}
func (s *sampler) Add(id, url string) { s.mu.Lock(); defer s.mu.Unlock(); s.endpoints[id] = url }
func (s *sampler) Start(ctx context.Context) {
	s.mu.Lock()
	s.started = true
	bounded, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	s.mu.Unlock()
	go func() {
		defer close(s.done)
		s.capture(bounded)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-bounded.Done():
				return
			case <-ticker.C:
				s.capture(bounded)
			}
		}
	}()
}
func (s *sampler) Stop() {
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if !started {
		return
	}
	s.stopOnce.Do(func() {
		s.cancel()
		<-s.done
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		s.capture(ctx)
	})
}
func (s *sampler) capture(ctx context.Context) {
	s.mu.Lock()
	endpoints := map[string]string{}
	for id, url := range s.endpoints {
		endpoints[id] = url
	}
	s.mu.Unlock()
	current := map[string]observability.Data{}
	var waits map[string]int
	if s.database != nil {
		bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		rows, err := s.database.Query(bounded, `SELECT coalesce(wait_event_type,'CPU'),coalesce(wait_event,'ACTIVE'),count(*) FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND pid<>pg_backend_pid() GROUP BY 1,2`)
		if err == nil {
			waits = map[string]int{}
			for rows.Next() {
				var kind, event string
				var count int
				if rows.Scan(&kind, &event, &count) == nil {
					waits[kind+"/"+event] = count
				}
			}
			rows.Close()
			if rows.Err() != nil {
				waits = nil
			}
		}
		cancel()
	}
	for id, url := range endpoints {
		bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		req, err := http.NewRequestWithContext(bounded, "GET", url+"/metrics", nil)
		if err == nil {
			response, e := http.DefaultClient.Do(req)
			if e == nil {
				var data observability.Data
				if response.StatusCode == 200 && json.NewDecoder(http.MaxBytesReader(nil, response.Body, 1<<20)).Decode(&data) == nil {
					current[id] = data
				}
				_ = response.Body.Close()
			}
		}
		cancel()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, data := range current {
		s.last[id] = data
	}
	sample := resourceSample{At: time.Now().UTC(), Counters: map[string]int64{}, PostgresWaits: waits}
	for id, data := range s.last {
		for key, value := range data.Counters {
			sample.Counters[key] += value
		}
		if data.Process.CPUSeconds != nil {
			if id == "master" {
				sample.MasterCPU += *data.Process.CPUSeconds
			} else {
				sample.WorkerCPU += *data.Process.CPUSeconds
			}
		}
	}
	for id, data := range current {
		if data.Process.RSSBytes != nil {
			if id == "master" {
				sample.MasterRSS += *data.Process.RSSBytes
			} else {
				sample.WorkerRSS += *data.Process.RSSBytes
			}
		}
		sample.Backlog = max(sample.Backlog, data.Gauges["execution_outbox_backlog"])
		for key, value := range data.Gauges {
			if strings.HasPrefix(key, "kafka_lag_records_a") {
				sample.Lag += value
			}
		}
	}
	s.samples = append(s.samples, sample)
}
func (s *sampler) Samples() []resourceSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]resourceSample(nil), s.samples...)
}

type trace struct {
	Message    string    `json:"msg"`
	Execution  string    `json:"execution"`
	Group      string    `json:"group"`
	MessageID  string    `json:"message_id"`
	Kind       string    `json:"kind"`
	Tick       time.Time `json:"tick"`
	Received   time.Time `json:"received_at"`
	Computed   time.Time `json:"computed_at"`
	Confirmed  time.Time `json:"confirmed_at"`
	Committed  time.Time `json:"committed_at"`
	Sent       time.Time `json:"sent_at"`
	Source     time.Time `json:"source_first_at"`
	Compute    float64   `json:"compute_seconds"`
	Durability float64   `json:"durability_wait_seconds"`
	Accepted   bool      `json:"accepted"`
}

func measureLogs(directory, run string, groups int, s *sampler, expectedTicks ...int) (Measurements, error) {
	m := Measurements{Status: "FAIL", Clock: "same-host CreateTime Kafka timestamps + process wall clocks; compute durations monotonic; nearest-rank quantiles", Counters: map[string]int64{}, Network: "actual result-client HTTP/Kafka protocol stream read/write bytes and application payload counts; Kafka includes producer+consumer legs; TCP/IP framing and PostgreSQL/input/broker internal traffic NOT_MEASURED"}
	ticks := map[string]trace{}
	durable := map[string]trace{}
	attempts := map[string][]time.Time{}
	accepted := map[string]trace{}
	paths, err := filepath.Glob(filepath.Join(directory, "*.log"))
	if err != nil {
		return m, err
	}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return m, err
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		for scanner.Scan() {
			var item trace
			if json.Unmarshal(scanner.Bytes(), &item) != nil {
				continue
			}
			key := item.Group + "/" + item.Tick.UTC().Format(time.RFC3339Nano)
			switch item.Message {
			case "tick measurement":
				if previous, ok := ticks[key]; !ok || item.Computed.After(previous.Computed) {
					ticks[key] = item
				}
			case "tick durable":
				if previous, ok := durable[key]; !ok || item.Confirmed.After(previous.Confirmed) {
					durable[key] = item
				}
			case "output attempt":
				attempts[item.MessageID] = append(attempts[item.MessageID], item.Sent)
			case "result measurement":
				prior, ok := accepted[item.MessageID]
				if !ok || item.Committed.Before(prior.Committed) {
					accepted[item.MessageID] = item
				}
			}
		}
		readErr := scanner.Err()
		_ = file.Close()
		if readErr != nil {
			return m, readErr
		}
	}
	compute, queue, durability, transport, masterCommit, e2e := []float64{}, []float64{}, []float64{}, []float64{}, []float64{}, []float64{}
	joinTimes := map[time.Time]time.Time{}
	joinCounts := map[time.Time]int{}
	for id, item := range accepted {
		var sent time.Time
		for _, candidate := range attempts[id] {
			if !candidate.After(item.Received) && (sent.IsZero() || candidate.After(sent)) {
				sent = candidate
			}
		}
		if sent.IsZero() {
			return m, fmt.Errorf("missing send trace %s", id)
		}
		transport = append(transport, item.Received.Sub(sent).Seconds())
		masterCommit = append(masterCommit, item.Committed.Sub(item.Received).Seconds())
		if item.Kind == "tick_fragment" {
			at := item.Tick.UTC()
			joinTimes[at] = maxTime(joinTimes[at], item.Committed)
			joinCounts[at]++
		}
	}
	for key, item := range ticks {
		ack, ok := durable[key]
		if !ok {
			return m, fmt.Errorf("missing durable timing %s", key)
		}
		id := domain.MessageID(run, item.Group, "tick_fragment", item.Tick.UTC().Format(time.RFC3339), 1)
		if _, ok := accepted[id]; !ok {
			return m, fmt.Errorf("missing Master fragment timing %s", key)
		}
		at := item.Tick.UTC()
		if joinCounts[at] != groups {
			return m, fmt.Errorf("incomplete final join timing %s", at)
		}
		if item.Received.Sub(item.Source) > 24*time.Hour {
			return m, fmt.Errorf("source timestamp is not a fresh processing clock: %s", key)
		}
		if item.Source.IsZero() {
			return m, fmt.Errorf("missing real Kafka CreateTime %s", key)
		}
		compute = append(compute, item.Compute)
		queue = append(queue, item.Received.Sub(item.Source).Seconds())
		durability = append(durability, ack.Durability)
		e2e = append(e2e, joinTimes[at].Sub(item.Source).Seconds())
		if m.SourceStart.IsZero() || item.Source.Before(m.SourceStart) {
			m.SourceStart = item.Source
		}
		m.FinalJoin = maxTime(m.FinalJoin, joinTimes[at])
	}
	if len(ticks) == 0 {
		return m, fmt.Errorf("empty timing trace")
	}
	if len(expectedTicks) != 0 && len(ticks) != expectedTicks[0] {
		return m, fmt.Errorf("incomplete timing trace: got %d ticks want %d", len(ticks), expectedTicks[0])
	}
	for name, values := range map[string][]float64{"compute": compute, "queue": queue, "durability": durability, "transport": transport, "master_commit": masterCommit, "e2e": e2e} {
		for _, v := range values {
			if v < 0 || math.IsInf(v, 0) || math.IsNaN(v) {
				return m, fmt.Errorf("invalid/negative %s clock sample %g", name, v)
			}
		}
	}
	m.Compute, m.InputQueue, m.DurabilityWait, m.Transport, m.MasterCommit, m.EndToEnd = distribution(compute), distribution(queue), distribution(durability), distribution(transport), distribution(masterCommit), distribution(e2e)
	m.KafkaTimings = map[string]observability.Histogram{}
	for _, data := range s.last {
		for key, value := range data.Histograms {
			if !strings.HasPrefix(key, "results_kafka_") {
				continue
			}
			total := m.KafkaTimings[key]
			total.Count += value.Count
			total.Sum += value.Sum
			total.Max = max(total.Max, value.Max)
			if total.Buckets == nil {
				total.Buckets = make([]int64, len(value.Buckets))
			}
			for i, n := range value.Buckets {
				total.Buckets[i] += n
			}
			m.KafkaTimings[key] = total
		}
	}
	m.Throughput = float64(len(ticks)) / m.FinalJoin.Sub(m.SourceStart).Seconds()
	samples := s.Samples()
	m.KafkaLagStatus = "MEASURED_PARTITION_HIGH_WATERMARK_1HZ"
	if len(samples) == 0 {
		m.KafkaLagStatus = "NOT_MEASURED"
	}
	m.Samples = len(samples)
	m.ResourceStatus = "NOT_MEASURED"
	if len(s.last) > 0 {
		m.ResourceStatus = "PASS"
		for _, data := range s.last {
			if data.Process.CPUSeconds == nil || data.Process.RSSBytes == nil {
				m.ResourceStatus = "NOT_MEASURED"
			}
		}
	}
	for _, sample := range samples {
		if sample.PostgresWaits != nil {
			m.PostgresWaitSampleCount++
		}
		if sample.PostgresWaits != nil && m.PostgresWaitSamples == nil {
			m.PostgresWaitSamples = map[string]int{}
		}
		for key, count := range sample.PostgresWaits {
			m.PostgresWaitSamples[key] += count
		}
		m.WorkerCPUSeconds = max(m.WorkerCPUSeconds, sample.WorkerCPU)
		m.MasterCPUSeconds = max(m.MasterCPUSeconds, sample.MasterCPU)
		m.WorkerPeakRSS = max(m.WorkerPeakRSS, sample.WorkerRSS)
		m.MasterPeakRSS = max(m.MasterPeakRSS, sample.MasterRSS)
		m.OutboxBacklogMax = max(m.OutboxBacklogMax, sample.Backlog)
		m.KafkaLagMax = max(m.KafkaLagMax, sample.Lag)
		for key, value := range sample.Counters {
			m.Counters[key] = max(m.Counters[key], value)
		}
	}
	m.Status = "PASS"
	return m, nil
}
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func analyzeExisting(path string, out io.Writer) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var result Result
	if err := json.Unmarshal(raw, &result); err != nil {
		return err
	}
	if result.Comparison.Status != "PASS" {
		return fmt.Errorf("only runs with successful independent parity may be analyzed")
	}
	directory := result.LogDirectory
	if directory == "" {
		directory = filepath.Join(filepath.Dir(path), result.Spec.Name)
	}
	b, err := os.ReadFile(filepath.Join(directory, "metrics-samples.json"))
	if err != nil {
		return err
	}
	s := newSampler()
	if err := json.Unmarshal(b, &s.samples); err != nil {
		return err
	}
	measured, err := measureLogs(directory, result.RunID, result.Spec.Groups, s, result.Comparison.Raw)
	if err != nil {
		return err
	}
	analysis := struct {
		SourceSHA256 string       `json:"source_result_sha256"`
		Source       string       `json:"source_result"`
		Measurements Measurements `json:"measurements"`
	}{domain.Hash(raw), path, measured}
	output := path + ".analysis.json"
	if _, err := os.Stat(output); err == nil {
		return fmt.Errorf("analysis output already exists")
	}
	if err := saveJSON(output, analysis); err != nil {
		return err
	}
	fmt.Fprintln(out, output)
	return nil
}
