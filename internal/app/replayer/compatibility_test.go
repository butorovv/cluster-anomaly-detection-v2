package replayer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const sourceCommit = "891e0ef71a0da0f9ad256f91d199fef38d1355a9"

// TestV1Compatibility executes unchanged V1 Go source on the same generated
// Parquet as the real V2 CLI. V1 is read only; builds use t.TempDir.
func TestV1Compatibility(t *testing.T) {
	source := os.Getenv("REPLAYER_V1_SOURCE")
	if source == "" {
		t.Skip("set REPLAYER_V1_SOURCE to the pinned clean V1 checkout")
	}
	source, err := filepath.Abs(source)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := func(dir, executable string, args ...string) ([]byte, []byte) {
		t.Helper()
		cmd := exec.CommandContext(ctx, executable, args...)
		cmd.Dir = dir
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("%s %v: %v\n%s", executable, args, err, stderr.String())
		}
		return stdout.Bytes(), stderr.Bytes()
	}
	commit, _ := command(source, "git", "rev-parse", "HEAD")
	if strings.TrimSpace(string(commit)) != sourceCommit {
		t.Fatalf("V1 HEAD must be %s", sourceCommit)
	}
	before, _ := command(source, "git", "status", "--porcelain=v1", "--untracked-files=all")
	if len(before) != 0 {
		t.Fatal("V1 source checkout must be clean")
	}
	t.Cleanup(func() {
		cmd := exec.Command("git", "-C", source, "status", "--porcelain=v1", "--untracked-files=all")
		after, err := cmd.Output()
		if err != nil || !bytes.Equal(before, after) {
			t.Errorf("V1 status changed: %s (%v)", after, err)
		}
	})
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	reference := filepath.Join(work, "reference")
	write := func(path string, data []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	read := func(path string) []byte {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	for _, path := range []string{
		"internal/modules/telemetry/domain/node.go", "internal/modules/telemetry/domain/record.go",
		"internal/replayer/reader.go", "internal/replayer/scheduler.go", "internal/replayer/sender.go", "internal/replayer/window.go",
	} {
		write(filepath.Join(reference, path), read(filepath.Join(source, path)))
	}
	// Reduce only the module graph, preserving V1 source bytes and dependency versions.
	mod := bytes.Replace(read(filepath.Join(root, "go.mod")), []byte("module github.com/butorovv/cluster-anomaly-detection-v2"), []byte("module cluster-anomaly-detection"), 1)
	write(filepath.Join(reference, "go.mod"), mod)
	write(filepath.Join(reference, "go.sum"), read(filepath.Join(root, "go.sum")))
	write(filepath.Join(reference, "cmd/contract/main.go"), []byte(v1ReplayMain))
	fixture := filepath.Join(work, "synthetic.parquet")
	writeCLIFixture(t, fixture)
	goName, binaryName := "go", "replayer"
	if runtime.GOOS == "windows" {
		goName += ".exe"
		binaryName += ".exe"
	}
	goBinary := filepath.Join(runtime.GOROOT(), "bin", goName)
	v1, metadata := command(reference, goBinary, "run", "-mod=readonly", "./cmd/contract", fixture)
	var stats struct {
		Rows, Timestamps, Completions, Invalid int64
		Groups, Hosts                          int
	}
	if err := json.Unmarshal(metadata, &stats); err != nil {
		t.Fatal(err, string(metadata))
	}
	if stats.Rows != 4 || stats.Timestamps != 2 || stats.Completions != 3 || stats.Invalid != 2 || stats.Groups != 2 || stats.Hosts != 3 {
		t.Fatal(stats)
	}
	binary := filepath.Join(work, binaryName)
	command(root, goBinary, "build", "-o", binary, "./cmd/replayer")
	v2, _ := command(root, binary, "--input", fixture, "--speed", "0")
	if !bytes.Equal(v1, v2) {
		t.Fatalf("V1/V2 NDJSON differs\nV1: %s\nV2: %s", v1, v2)
	}
	if bytes.Count(v2, []byte("\n")) != 7 {
		t.Fatal("unexpected message count")
	}
	t.Logf("V1 commit %s; identical NDJSON SHA256 %x; rows=4 invalid=2 completions=3 groups=2 hosts=3; speed=0 made no V1 Sleep calls", sourceCommit, sha256.Sum256(v2))
}

const v1ReplayMain = `package main
import (
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "os"
 "time"
 "cluster-anomaly-detection/internal/replayer"
)
type noWaitClock struct{}
func(noWaitClock)Now()time.Time{return time.Time{}}
func(noWaitClock)Sleep(context.Context,time.Duration)error{panic("speed=0 attempted to sleep")}
func run()(result error){
 r,err:=replayer.OpenParquet(os.Args[1]);if err!=nil{return err}
 defer func(){result=errors.Join(result,r.Close())}()
 sink:=replayer.NewJSONSender(os.Stdout)
 stats,err:=replayer.Replay(context.Background(),r,sink,sink,noWaitClock{},0)
 if err!=nil{return err}
 return json.NewEncoder(os.Stderr).Encode(stats)
}
func main(){if err:=run();err!=nil{fmt.Fprintln(os.Stderr,err);os.Exit(1)}}
`
