// Package observability records bounded operational measurements outside frozen payloads.
package observability

import (
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
	"time"
)

type Histogram struct {
	Count   int64   `json:"count"`
	Sum     float64 `json:"sum_seconds"`
	Max     float64 `json:"max_seconds"`
	Buckets []int64 `json:"buckets"`
}

var Bounds = []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 300}

type Registry struct {
	mu         sync.Mutex
	labels     map[string]string
	counters   map[string]int64
	gauges     map[string]float64
	histograms map[string]Histogram
}
type Process struct {
	CPUSeconds *float64 `json:"cpu_seconds"`
	RSSBytes   *uint64  `json:"rss_bytes"`
	HeapBytes  uint64   `json:"heap_bytes"`
	Goroutines int      `json:"goroutines"`
	PID        int      `json:"pid"`
}
type Data struct {
	Labels     map[string]string    `json:"labels"`
	At         time.Time            `json:"at"`
	Counters   map[string]int64     `json:"counters"`
	Gauges     map[string]float64   `json:"gauges"`
	Histograms map[string]Histogram `json:"histograms"`
	Bounds     []float64            `json:"histogram_upper_bounds_seconds"`
	Process    Process              `json:"process"`
}

func New() *Registry {
	return &Registry{labels: map[string]string{}, counters: map[string]int64{}, gauges: map[string]float64{}, histograms: map[string]Histogram{}}
}
func (r *Registry) Label(k, v string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.labels[k] = v
}
func (r *Registry) Add(k string, n int64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters[k] += n
}
func (r *Registry) Set(k string, v float64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[k] = v
}
func (r *Registry) Observe(k string, d time.Duration) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	h := r.histograms[k]
	h.Count++
	v := d.Seconds()
	h.Sum += v
	h.Max = max(h.Max, v)
	if h.Buckets == nil {
		h.Buckets = make([]int64, len(Bounds)+1)
	}
	index := len(Bounds)
	for i, b := range Bounds {
		if v <= b {
			index = i
			break
		}
	}
	h.Buckets[index]++
	r.histograms[k] = h
}
func (r *Registry) Snapshot() Data {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := Data{Labels: map[string]string{}, At: time.Now().UTC(), Counters: map[string]int64{}, Gauges: map[string]float64{}, Histograms: map[string]Histogram{}, Bounds: append([]float64(nil), Bounds...), Process: process()}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	d.Process.HeapBytes = m.HeapAlloc
	d.Process.Goroutines = runtime.NumGoroutine()
	for k, v := range r.labels {
		d.Labels[k] = v
	}
	for k, v := range r.counters {
		d.Counters[k] = v
	}
	for k, v := range r.gauges {
		d.Gauges[k] = v
	}
	for k, v := range r.histograms {
		v.Buckets = append([]int64(nil), v.Buckets...)
		d.Histograms[k] = v
	}
	return d
}
func (r *Registry) Handler(next http.Handler) http.Handler {
	return profileHandler(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.Method == "GET" && q.URL.Path == "/metrics" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(r.Snapshot())
			return
		}
		next.ServeHTTP(w, q)
	}))
}
