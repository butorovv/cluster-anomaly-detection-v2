package observability

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestMetricsConcurrentAndDetached(t *testing.T) {
	r := New()
	var w sync.WaitGroup
	for i := 0; i < 8; i++ {
		w.Add(1)
		go func() {
			defer w.Done()
			for j := 0; j < 100; j++ {
				r.Add("ticks", 1)
				r.Observe("compute", time.Millisecond)
			}
		}()
	}
	w.Wait()
	d := r.Snapshot()
	if d.Counters["ticks"] != 800 || d.Histograms["compute"].Count != 800 {
		t.Fatal("lost concurrent observations", d)
	}
	d.Counters["ticks"] = 0
	h := d.Histograms["compute"]
	h.Buckets[0] = 999
	if r.Snapshot().Counters["ticks"] != 800 || r.Snapshot().Histograms["compute"].Buckets[0] != 0 {
		t.Fatal("mutable exported metrics")
	}
	if runtime.GOOS == "linux" || runtime.GOOS == "windows" {
		if d.Process.CPUSeconds == nil || d.Process.RSSBytes == nil || *d.Process.RSSBytes == 0 {
			t.Fatal("native process metrics unavailable", d.Process)
		}
	}
}
