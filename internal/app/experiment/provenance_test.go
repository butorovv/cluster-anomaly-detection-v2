package experiment

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func configurationFixture(t *testing.T) (Config, string) {
	t.Helper()
	root := t.TempDir()
	input := filepath.Join(root, "data/prepared/summit-D1-pilot/D1.parquet")
	profile := filepath.Join(root, "configs/group-profile.summit-v1.json")
	manifest := filepath.Join(root, "golden.json")
	for _, path := range []string{input, profile, manifest} {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(path), 0600); err != nil {
			t.Fatal(err)
		}
	}
	inputSHA, err := fileSHA(input)
	if err != nil {
		t.Fatal(err)
	}
	profileSHA, err := fileSHA(profile)
	if err != nil {
		t.Fatal(err)
	}
	return Config{Assets: root, InputSHA: inputSHA, ProfileSHA: profileSHA, GoldenManifest: manifest, Brokers: "isolated:9092"}, input
}

func TestResumeBindsGlobalConfiguration(t *testing.T) {
	cfg, input := configurationFixture(t)
	baseline, err := configurationHash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Config){func(c *Config) { c.Measure = true }, func(c *Config) { c.Brokers = "other:9092" }} {
		other := cfg
		change(&other)
		hash, err := configurationHash(other)
		if err != nil || hash == baseline {
			t.Fatal("global change was not bound", err)
		}
		if err := reusable(Result{Provenance: baseline}, other, hash); err == nil {
			t.Fatal("stale PASS reused")
		}
	}
	if err := reusable(Result{}, cfg, baseline); err == nil {
		t.Fatal("legacy unverifiable PASS reused")
	}
	if err := os.WriteFile(cfg.GoldenManifest, []byte("new golden"), 0600); err != nil {
		t.Fatal(err)
	}
	changed, err := configurationHash(cfg)
	if err != nil || changed == baseline {
		t.Fatal("golden manifest change not bound", err)
	}
	if err := os.WriteFile(input, []byte("different dataset"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := configurationHash(cfg); err == nil {
		t.Fatal("actual input SHA not verified")
	}
}

func TestRunnerKafkaIntakeFingerprint(t *testing.T) {
	cfg, _ := configurationFixture(t)
	hashes := map[string]string{}
	for _, mode := range []string{"", "wave", "pipeline"} {
		cfg.KafkaIntakeMode = mode
		var err error
		hashes[mode], err = configurationHash(cfg)
		if err != nil {
			t.Fatal(err)
		}
	}
	if hashes[""] != hashes["wave"] || hashes["wave"] == hashes["pipeline"] {
		t.Fatalf("unexpected mode fingerprints: %v", hashes)
	}
	for _, pair := range [][2]string{{"wave", "pipeline"}, {"pipeline", "wave"}} {
		saved := Result{Status: "PASS", Spec: RunSpec{Mode: "kafka"}, KafkaIntakeMode: pair[0], Provenance: hashes[pair[0]]}
		cfg.KafkaIntakeMode = pair[1]
		if err := reusable(saved, cfg, hashes[pair[1]]); err == nil || !strings.Contains(err.Error(), "configuration differs") {
			t.Fatalf("resume did not reject %s under %s: %v", pair[0], pair[1], err)
		}
		saved.Provenance = hashes[pair[1]]
		if err := reusable(saved, cfg, hashes[pair[1]]); err == nil || err.Error() != "saved Kafka intake mode differs" {
			t.Fatalf("resume did not check the recorded mode: %v", err)
		}
	}
	cfg.KafkaIntakeMode = "unknown"
	if _, err := configurationHash(cfg); err == nil || !strings.Contains(err.Error(), "invalid Kafka intake mode") {
		t.Fatalf("invalid intake mode accepted: %v", err)
	}
}

func TestRunnerKafkaIntakeMasterArgsAndReport(t *testing.T) {
	for _, tt := range []struct{ name, transport, configured, effective string }{
		{"default", "kafka", "", "wave"},
		{"explicit wave", "kafka", "wave", "wave"},
		{"pipeline", "kafka", "pipeline", "pipeline"},
		{"polling", "polling", "pipeline", ""},
		{"http push", "http_push", "pipeline", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{KafkaIntakeMode: tt.configured}
			spec := RunSpec{Mode: tt.transport}
			args := masterArguments(cfg, spec, "127.0.0.1:8080", "test-execution")
			index := slices.Index(args, "--kafka-intake-mode")
			if tt.effective == "" && index != -1 || tt.effective != "" &&
				(index == -1 || index == len(args)-1 || args[index+1] != tt.effective) {
				t.Fatalf("unexpected Master args: %v", args)
			}
			// An incomplete spec returns before touching Kafka or PostgreSQL.
			report, err := runOne(context.Background(), cfg, spec, verifiedBuild{})
			if err == nil || report.KafkaIntakeMode != tt.effective {
				t.Fatalf("recorded mode %q, want %q (run error: %v)", report.KafkaIntakeMode, tt.effective, err)
			}
		})
	}
}

func TestRunnerKafkaPollRecordsFingerprint(t *testing.T) {
	cfg, _ := configurationFixture(t)
	cfg.KafkaIntakeMode = "pipeline"
	defaultHash, err := configurationHash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.KafkaPollRecords = 16
	explicitHash, err := configurationHash(cfg)
	if err != nil || explicitHash != defaultHash {
		t.Fatalf("omitted and explicit default differ: %v", err)
	}
	cfg.KafkaPollRecords = 32
	changedHash, err := configurationHash(cfg)
	if err != nil || changedHash == defaultHash {
		t.Fatalf("poll size change not fingerprinted: %v", err)
	}
	saved := Result{Status: "PASS", Spec: RunSpec{Mode: "kafka"}, KafkaIntakeMode: "pipeline", KafkaPollRecords: 16, Provenance: defaultHash}
	if err := reusable(saved, cfg, changedHash); err == nil || !strings.Contains(err.Error(), "configuration differs") {
		t.Fatalf("stale poll-size PASS reused: %v", err)
	}
	saved.Provenance = changedHash
	if err := reusable(saved, cfg, changedHash); err == nil || err.Error() != "saved Kafka poll records differs" {
		t.Fatalf("recorded poll size not checked: %v", err)
	}
	for _, invalid := range []int{-1, 129} {
		cfg.KafkaPollRecords = invalid
		if _, err := configurationHash(cfg); err == nil {
			t.Fatalf("invalid poll size %d accepted", invalid)
		}
	}
}

func TestRunnerKafkaPollRecordsMasterArgsAndReport(t *testing.T) {
	for _, tt := range []struct {
		name, transport       string
		configured, effective int
	}{
		{"default", "kafka", 0, 16},
		{"explicit default", "kafka", 16, 16},
		{"32", "kafka", 32, 32},
		{"polling unaffected", "polling", 32, 0},
		{"push unaffected", "http_push", 32, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var cfg Config
			if err := json.Unmarshal([]byte(fmt.Sprintf(`{"kafka_intake_mode":"pipeline","kafka_poll_records":%d}`, tt.configured)), &cfg); err != nil {
				t.Fatal(err)
			}
			spec := RunSpec{Mode: tt.transport}
			args := masterArguments(cfg, spec, "127.0.0.1:8080", "test-execution")
			index := slices.Index(args, "--kafka-poll-records")
			if tt.effective == 0 && index != -1 || tt.effective != 0 &&
				(index == -1 || index == len(args)-1 || args[index+1] != fmt.Sprint(tt.effective)) {
				t.Fatalf("unexpected Master poll-size args: %v", args)
			}
			if cfg.kafkaIntake().Concurrency != 4 || cfg.kafkaIntake().IntakeMode != "pipeline" {
				t.Fatal("unrelated Kafka defaults changed")
			}
			report, err := runOne(context.Background(), cfg, spec, verifiedBuild{})
			if err == nil || report.KafkaPollRecords != tt.effective {
				t.Fatalf("recorded size %d, want %d (run error: %v)", report.KafkaPollRecords, tt.effective, err)
			}
			data, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), `"kafka_poll_records"`) != (tt.effective != 0) {
				t.Fatalf("unexpected report field: %s", data)
			}
		})
	}
}
