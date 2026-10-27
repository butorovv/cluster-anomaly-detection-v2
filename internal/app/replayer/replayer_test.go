package replayer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/parquet-go/parquet-go"
)

type fixtureRow struct {
	Timestamp        int64    `parquet:"timestamp,timestamp(microsecond:utc)"`
	Hostname         string   `parquet:"hostname"`
	GroupID          string   `parquet:"group_id"`
	NodeIndex        int32    `parquet:"node_index,int(32)"`
	PSU0Power        *float64 `parquet:"psu0_power,optional"`
	PSU1Power        *float64 `parquet:"psu1_power,optional"`
	CPU0Power        *float64 `parquet:"cpu0_power,optional"`
	CPU1Power        *float64 `parquet:"cpu1_power,optional"`
	GPU0Power        *float64 `parquet:"gpu0_power,optional"`
	GPU1Power        *float64 `parquet:"gpu1_power,optional"`
	GPU2Power        *float64 `parquet:"gpu2_power,optional"`
	GPU3Power        *float64 `parquet:"gpu3_power,optional"`
	GPU4Power        *float64 `parquet:"gpu4_power,optional"`
	GPU5Power        *float64 `parquet:"gpu5_power,optional"`
	CPU0Temp         *float64 `parquet:"cpu0_mean_temp,optional"`
	CPU1Temp         *float64 `parquet:"cpu1_mean_temp,optional"`
	GPU0Temp         *float64 `parquet:"gpu0_core_temp,optional"`
	GPU1Temp         *float64 `parquet:"gpu1_core_temp,optional"`
	GPU2Temp         *float64 `parquet:"gpu2_core_temp,optional"`
	GPU3Temp         *float64 `parquet:"gpu3_core_temp,optional"`
	GPU4Temp         *float64 `parquet:"gpu4_core_temp,optional"`
	GPU5Temp         *float64 `parquet:"gpu5_core_temp,optional"`
	Valid            bool     `parquet:"is_valid"`
	AvailabilityMask uint32   `parquet:"availability_mask"`
	CPU0ValidCores   int32    `parquet:"cpu0_valid_cores,int(32)"`
	CPU1ValidCores   int32    `parquet:"cpu1_valid_cores,int(32)"`
}

func writeCLIFixture(t *testing.T, path string) {
	t.Helper()
	base := fixtureRow{Timestamp: time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC).UnixMicro(), Hostname: "a01n01", GroupID: "a01", NodeIndex: 1, Valid: true, AvailabilityMask: domain.FullAvailabilityMask, CPU0ValidCores: 23, CPU1ValidCores: 23}
	fields := reflect.ValueOf(&base).Elem()
	for i := 0; i < 18; i++ {
		v := float64(i + 1)
		fields.Field(i + 4).Set(reflect.ValueOf(&v))
	}
	partial := base
	partial.Hostname = "a01n02"
	partial.NodeIndex = 2
	partial.CPU0Temp = nil
	partial.CPU0ValidCores = 0
	partial.Valid = false
	partial.AvailabilityMask &^= 1 << 10
	missing := base
	missing.Hostname = "a02n01"
	missing.GroupID = "a02"
	missing.Valid = false
	missing.AvailabilityMask = 0
	missing.CPU0ValidCores = 0
	missing.CPU1ValidCores = 0
	fields = reflect.ValueOf(&missing).Elem()
	for i := 0; i < 18; i++ {
		fields.Field(i + 4).Set(reflect.Zero(fields.Field(i + 4).Type()))
	}
	later := base
	later.Timestamp += 20_000_000
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[fixtureRow](f, parquet.KeyValueMetadata("schema_version", "F18-v1"))
	if _, err = w.Write([]fixtureRow{base, partial, missing, later}); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCLIEndToEnd(t *testing.T) {
	dir := t.TempDir()
	// Optional export supports a local CLI demonstration without committed data.
	if export := os.Getenv("REPLAYER_CONTRACT_DIR"); export != "" {
		dir = export
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "synthetic.parquet")
	writeCLIFixture(t, path)
	var out, stderr bytes.Buffer
	getenv := func(string) string { return "" }
	if err := Replayer(context.Background(), []string{"--input", path, "--speed", "0"}, getenv, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 7 {
		t.Fatal(out.String())
	}
	wantTypes := []string{"telemetry", "telemetry", "timestamp_complete", "telemetry", "timestamp_complete", "telemetry", "timestamp_complete"}
	wantHosts := map[int]string{0: "a01n01", 1: "a01n02", 3: "a02n01", 5: "a01n01"}
	for i, line := range lines {
		var got struct {
			Type         string
			Timestamp    time.Time
			Hostname     string
			Group        string `json:"group_id"`
			F18          []*float64
			Mask         uint32 `json:"availability_mask"`
			Valid        bool   `json:"is_valid"`
			Records      int
			ValidRecords int `json:"valid_records"`
		}
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatal(err)
		}
		if got.Type != wantTypes[i] {
			t.Fatalf("message %d: %s", i, line)
		}
		wantTime := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
		if i >= 5 {
			wantTime = wantTime.Add(20 * time.Second)
		}
		if !got.Timestamp.Equal(wantTime) {
			t.Fatal("event time changed", got.Timestamp)
		}
		if host, ok := wantHosts[i]; ok {
			if got.Hostname != host || len(got.F18) != 18 {
				t.Fatal(got)
			}
			wantMask := domain.FullAvailabilityMask
			if i == 1 {
				wantMask &^= 1 << 10
			}
			if i == 3 {
				wantMask = 0
			}
			if got.Mask != wantMask || got.Valid != (wantMask == domain.FullAvailabilityMask) {
				t.Fatal(got)
			}
			for k, value := range got.F18 {
				if wantMask&(1<<k) == 0 {
					if value != nil {
						t.Fatal("missing coordinate is not null")
					}
				} else if value == nil || *value != float64(k+1) {
					t.Fatal("feature order changed")
				}
			}
		} else {
			records, valid, group := 1, 1, "a01"
			if i == 2 {
				records, valid = 2, 1
			}
			if i == 4 {
				group, valid = "a02", 0
			}
			if got.Records != records || got.ValidRecords != valid || got.Group != group {
				t.Fatal(got)
			}
		}
	}
	for _, stat := range []string{"rows=4", "timestamps=2", "groups=2", "hosts=3", "invalid=2", "timestamp_completions=3"} {
		if !strings.Contains(stderr.String(), stat) {
			t.Fatal(stderr.String())
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "expected.ndjson"), out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	stderr.Reset()
	err := Replayer(context.Background(), []string{"--input", path, "--speed", "0", "--start", "2022-01-18T08:00:20Z", "--end", "2022-01-18T08:00:30Z"}, getenv, &out, &stderr)
	if err != nil || strings.Count(out.String(), "\n") != 2 {
		t.Fatal(err, out.String())
	}
}

func TestCLIHelpAndInvalidConfiguration(t *testing.T) {
	var out, stderr bytes.Buffer
	getenv := func(string) string { return "" }
	if err := Replayer(context.Background(), []string{"--help"}, getenv, &out, &stderr); !errors.Is(err, flag.ErrHelp) || out.Len() != 0 || !strings.Contains(stderr.String(), "--input") && !strings.Contains(stderr.String(), "-input") {
		t.Fatal(err, stderr.String())
	}
	for _, args := range [][]string{nil, {"--input", "x", "--speed", "NaN"}, {"--input", "x", "--speed", "-1"}, {"--input", "x", "--output", "log"}, {"--input", "x", "--start", "bad"}, {"--input", "x", "--end", "2022-01-18T08:00:00Z"}, {"--input", "x", "--start", "2022-01-18T08:00:00Z", "--end", "2022-01-18T08:00:00Z"}, {"--input", "x", "unexpected"}} {
		if _, err := load(args, getenv, io.Discard); err == nil {
			t.Fatal(args)
		}
	}
	cfg, err := load([]string{"--input", "explicit", "--speed", "0"}, func(string) string { return "malformed-env" }, io.Discard)
	if err != nil || cfg.input != "explicit" || cfg.speed != 0 {
		t.Fatal(cfg, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Replayer(ctx, []string{"--input", "x"}, getenv, io.Discard, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
