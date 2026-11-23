package experiment

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

func TestRunnerBuildFromExportInsideForeignGit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	source, foreign, overlay := t.TempDir(), t.TempDir(), t.TempDir()
	writeBuildFixture(t, source, "go.mod", "module example.com/runner-fixture\n\ngo 1.23\n")
	for _, path := range []string{"experiments/runner", "cmd/master", "cmd/worker", "cmd/replayer"} {
		writeBuildFixture(t, source, path+"/main.go", "package main\nfunc main() {}\n")
	}
	git(t, source, "init", "-q")
	git(t, source, "add", ".")
	git(t, source, "commit", "-qm", "actual source")
	commit := git(t, source, "rev-parse", "HEAD")
	archive := filepath.Join(t.TempDir(), "export.tar")
	git(t, source, "archive", "--format=tar", "-o", archive, commit)
	archiveSHA, err := fileSHA(archive)
	if err != nil {
		t.Fatal(err)
	}

	git(t, foreign, "init", "-q")
	git(t, foreign, "commit", "--allow-empty", "-qm", "unrelated work")
	foreignCommit := git(t, foreign, "rev-parse", "HEAD")
	writeBuildFixture(t, foreign, "go.work", "invalid surrounding workspace\n")
	writeBuildFixture(t, overlay, "cmd/worker/main.go", "package main\nimport \"fmt\"\nfunc main(){ fmt.Println(\"overlay worker\") }\n")
	root := filepath.Join(foreign, "build")
	var stdout bytes.Buffer
	if err := RunCLI(ctx, []string{"--build-source", archive, "--source-sha256", archiveSHA, "--source-overlay", overlay, "--build-output", root}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	exported := filepath.Join(root, "source")
	if observed := git(t, exported, "rev-parse", "HEAD"); observed != foreignCommit || observed == commit {
		t.Fatalf("test did not build an export located inside a foreign Git tree: %s", observed)
	}
	path, pin := filepath.Join(root, buildManifestName), strings.TrimSpace(stdout.String())
	runner := binaryPath(root, "runner")
	b, err := loadBuild(path, pin, runner)
	if err != nil {
		t.Fatal(err)
	}
	if b.SourceCommit != commit || b.SourceCommit == foreignCommit || b.ArchiveSHA256 != archiveSHA || !validDigest(b.OverlaySHA256, 64) {
		t.Fatalf("wrong provenance: %+v", b.buildManifest)
	}
	info, err := buildinfo.ReadFile(b.paths["worker"])
	if err != nil {
		t.Fatal(err)
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			t.Fatalf("ambient VCS revision retained: %s", setting.Value)
		}
	}
	output, err := exec.Command(b.paths["worker"]).Output()
	if err != nil || strings.TrimSpace(string(output)) != "overlay worker" {
		t.Fatalf("built Worker did not include the verified overlay: %q %v", output, err)
	}
	// The live runner's report uses the verified identity even on early failure.
	report, err := runOne(ctx, Config{}, RunSpec{}, b)
	if err == nil || report.Environment["source_commit"] != commit || report.BuildManifestSHA != pin {
		t.Fatalf("runner did not bind report to manifest: %+v %v", report, err)
	}

	t.Run("missing or wrong manifest", func(t *testing.T) {
		for _, pin := range []string{"", strings.Repeat("0", 64)} {
			if _, err := loadBuild(path, pin, runner); err == nil {
				t.Fatal("unverified manifest accepted")
			}
		}
	})
	t.Run("foreign executable", func(t *testing.T) {
		if _, err := loadBuild(path, pin, b.paths["worker"]); err == nil {
			t.Fatal("substituted runner accepted")
		}
	})
	t.Run("tampered executable", func(t *testing.T) {
		assertBuildTamperRejected(t, b.paths["worker"], func() error { return b.verifyBinary("worker") })
	})
	t.Run("tampered overlay", func(t *testing.T) {
		assertBuildTamperRejected(t, filepath.Join(root, "overlay", "cmd", "worker", "main.go"), func() error {
			_, err := loadBuild(path, pin, runner)
			return err
		})
	})
	t.Run("tampered archive", func(t *testing.T) {
		assertBuildTamperRejected(t, filepath.Join(root, "source.tar"), func() error {
			_, err := loadBuild(path, pin, runner)
			return err
		})
	})
	t.Run("manifest claiming the surrounding commit", func(t *testing.T) {
		manifest := b.buildManifest
		manifest.SourceCommit = foreignCommit
		altered := filepath.Join(root, "altered.json")
		if err := saveJSON(altered, manifest); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(altered)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := loadBuild(altered, domain.Hash(data), runner); err == nil {
			t.Fatal("manifest relabelled the pinned archive")
		}
	})
	t.Run("legacy resume leaves artifacts intact", func(t *testing.T) {
		out := t.TempDir()
		const legacy = `[{"status":"PASS","environment":{"source_commit":"ambient"}}]`
		writeBuildFixture(t, out, "matrix.json", legacy)
		if err := checkResumeBuild(Config{Output: out}, b); err == nil {
			t.Fatal("legacy PASS accepted for resume")
		}
		got, err := os.ReadFile(filepath.Join(out, "matrix.json"))
		if err != nil || string(got) != legacy {
			t.Fatalf("legacy file changed: %q %v", got, err)
		}
	})
}

func writeBuildFixture(t *testing.T, root, name, value string) {
	t.Helper()
	file := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
}

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=Runner Test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false"}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

func assertBuildTamperRejected(t *testing.T, path string, verify func() error) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Error(err)
		}
	})
	if err := os.WriteFile(path, []byte("modified"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := verify(); err == nil {
		t.Fatalf("tampered %s accepted", filepath.Base(path))
	}
}
