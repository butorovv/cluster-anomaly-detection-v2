package experiment

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResumeBindsGlobalConfiguration(t *testing.T) {
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
	inputSHA, _ := fileSHA(input)
	profileSHA, _ := fileSHA(profile)
	cfg := Config{Assets: root, InputSHA: inputSHA, ProfileSHA: profileSHA, GoldenManifest: manifest, Brokers: "isolated:9092"}
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
	if err := os.WriteFile(manifest, []byte("new golden"), 0600); err != nil {
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
