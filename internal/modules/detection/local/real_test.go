package local

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

// Explicit opt-in: real D1/D2 scoring is separate from fast synthetic tests.
func TestRealB1D2MatchesResearch(t *testing.T) {
	if os.Getenv("LOCAL_B1_REAL") != "1" {
		t.Skip("LOCAL_B1_REAL=1 required; real models/telemetry are read-only")
	}
	root := filepath.Join("..", "..")
	m, err := OpenModels(filepath.Join(root, "data/prepared/summit-D1-pilot/D1.parquet"), filepath.Join(root, "data/prepared/summit-D1-pilot/manifest.json"), filepath.Join(root, "results/statistical-20220118-8h-v1.json"), filepath.Join(root, "results/isolation-20220118-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if m.Binding.PreparedSHA256 != "cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7" ||
		m.Binding.IsolationSHA256 != "6aa1ca4f2f76b56418aafaa1ea25e6812a3b9fdcc0d309aeed44dc1db03f2da3" {
		t.Fatal("regression reference only applies to the audited source/forest")
	}
	profilePath := filepath.Join(root, "configs/local-b1.summit-20220118.json")
	p, err := LoadProfile(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "results/context-v1/thresholds.json"))
	if err != nil {
		t.Fatal(err)
	}
	var old struct {
		B1 map[string]float64 `json:"B1"`
	}
	if err = json.Unmarshal(b, &old); err != nil {
		t.Fatal(err)
	}
	if len(old.B1) != 288 {
		t.Fatal("research host count")
	}
	for h, theta := range old.B1 {
		if p.Thresholds[h] != theta || p.Observations[h] != 2880 {
			t.Fatal("threshold changed", h)
		}
	}
	// Compare each score/decision with the preserved research export, not just
	// the total count. D2 is used for regression only, never for calibration.
	metaBytes, err := os.ReadFile(filepath.Join(root, "results/context-v1/metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	var meta struct {
		Binary map[string]string `json:"binary_sha256"`
	}
	if err = json.Unmarshal(metaBytes, &meta); err != nil {
		t.Fatal(err)
	}
	exportPath := filepath.Join(root, "results/context-v1/D2.bin")
	if hash, err := fileDigest(exportPath); err != nil || hash != meta.Binary["D2.bin"] {
		t.Fatal("D2 export checksum mismatch", hash, err)
	}
	export, err := os.Open(exportPath)
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	reader := bufio.NewReaderSize(export, 1<<20)
	hostIndex := map[string]int{}
	for i, host := range p.Binding.Hosts {
		hostIndex[host] = i
	}
	checked := 0
	result, err := m.Run(context.Background(), profilePath, analysis.NIRWindows()[1], func(r LocalResult) error {
		var row [216]byte
		if _, err := io.ReadFull(reader, row[:]); err != nil {
			return err
		}
		score := math.Float64frombits(binary.LittleEndian.Uint64(row[160:168]))
		if r.Timestamp.UnixMicro() != int64(binary.LittleEndian.Uint64(row[:8])) ||
			hostIndex[r.Hostname] != int(binary.LittleEndian.Uint32(row[8:12])) ||
			r.AnomalyScore == nil || *r.AnomalyScore != score || r.IsAnomaly == nil ||
			*r.IsAnomaly != (score > old.B1[r.Hostname]) {
			return fmt.Errorf("D2 research result mismatch at row %d", checked)
		}
		checked++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Counts.States != 829440 || result.Counts.Available != 829440 || result.Counts.Anomalies != 14341 {
		t.Fatalf("B1 regression mismatch: %+v", result.Counts)
	}
	if _, err := reader.ReadByte(); err != io.EOF || checked != 829440 {
		t.Fatal("D2 export length mismatch", checked, err)
	}
	for _, c := range result.PerHost {
		if c.States != 2880 || c.Available != 2880 {
			t.Fatal("host coverage changed", c)
		}
	}
	// Independent D1-only rebuild: no training and no D2 threshold adjustment.
	fresh, audit, err := m.BuildProfile(context.Background())
	if err != nil || audit.Valid != 829440 {
		t.Fatal(audit, err)
	}
	for h, theta := range old.B1 {
		if fresh.Thresholds[h] != theta {
			t.Fatal("D1 rebuild differs from preserved B1", h)
		}
	}
	t.Logf("D1 exact thresholds: %d hosts; D2 available=%d B1 signals=%d", p.HostCount, result.Counts.Available, result.Counts.Anomalies)
}
