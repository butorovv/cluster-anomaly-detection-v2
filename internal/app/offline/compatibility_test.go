package offline

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
)

const v1Commit = "c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc"
const realProfileSHA = "a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36"

func TestV1OfflineCanonicalParity(t *testing.T) {
	if os.Getenv("OFFLINE_V1_SOURCE") == "" {
		t.Skip("OFFLINE_V1_SOURCE required: unchanged pinned V1 comparator")
	}
	root, sha, w := syntheticBundle(t)
	v1Parity(t, root, sha, w, false)
}
func TestRealD2AllDeltaCanonicalParity(t *testing.T) {
	root := os.Getenv("OFFLINE_REAL_ROOT")
	if root == "" || os.Getenv("OFFLINE_V1_SOURCE") == "" {
		t.Skip("REAL_D2_GOLDEN: NOT_RUN; OFFLINE_REAL_ROOT and OFFLINE_V1_SOURCE required")
	}
	v1Parity(t, root, realProfileSHA, analysis.NIRWindows()[1], true)
}

// Execute unchanged V1 files in a temporary module, never in its source checkout.
// Compare every output byte, including Unknown/missing rows, full raw/evidence,
// temporal witnesses, identity, closure reasons, VoteSegments and RBSubspans.
// This is stricter than V1's float comparator (1e-8 + 1e-8*abs(reference)).
func v1Parity(t *testing.T, assets, sha string, w analysis.Window, realD2 bool) {
	t.Helper()
	source, err := filepath.Abs(os.Getenv("OFFLINE_V1_SOURCE"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	command := func(dir, exe string, args ...string) []byte {
		t.Helper()
		cmd := exec.CommandContext(ctx, exe, args...)
		cmd.Dir = dir
		var out, stderr bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("%s %v: %v\n%s", exe, args, err, stderr.String())
		}
		return out.Bytes()
	}
	head := strings.TrimSpace(string(command(source, "git", "rev-parse", "HEAD")))
	if head != v1Commit {
		t.Fatalf("V1 must be pinned at %s, got %s", v1Commit, head)
	}
	before := command(source, "git", "status", "--porcelain=v1", "--untracked-files=all")
	if len(before) != 0 {
		t.Fatal("V1 must be clean")
	}
	defer func() {
		after := command(source, "git", "status", "--porcelain=v1", "--untracked-files=all")
		if !bytes.Equal(before, after) {
			t.Error("V1 modified")
		}
	}()
	work := t.TempDir()
	reference := filepath.Join(work, "reference")
	write := func(path string, b []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	read := func(path string) []byte {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for _, dir := range []string{"internal/group", "internal/local"} {
		entries, err := os.ReadDir(filepath.Join(source, dir))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") && !strings.HasSuffix(e.Name(), "_test.go") {
				name := filepath.Join(dir, e.Name())
				write(filepath.Join(reference, name), read(filepath.Join(source, name)))
			}
		}
	}
	for _, name := range []string{
		"internal/analysis/window.go", "internal/analysis/statistical.go", "internal/analysis/isolation.go",
		"internal/experiment/source.go", "internal/experiment/statistical.go", "internal/experiment/isolation.go",
		"internal/platform/storage/guard.go", "internal/platform/storage/guard_unix.go", "internal/platform/storage/guard_windows.go",
		"internal/replayer/reader.go", "internal/replayer/window.go", "internal/replayer/scheduler.go",
		"internal/modules/telemetry/domain/node.go", "internal/modules/telemetry/domain/record.go",
	} {
		write(filepath.Join(reference, name), read(filepath.Join(source, name)))
	}
	mod := bytes.Replace(read(filepath.Join(root, "go.mod")), []byte("module github.com/butorovv/cluster-anomaly-detection-v2"), []byte("module cluster-anomaly-detection"), 1)
	write(filepath.Join(reference, "go.mod"), mod)
	write(filepath.Join(reference, "go.sum"), read(filepath.Join(root, "go.sum")))
	write(filepath.Join(reference, "cmd/parity/main.go"), []byte(v1ParityMain))
	suffix := ""
	if runtime.GOOS == "windows" {
		suffix = ".exe"
	}
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go"+suffix)
	v1 := filepath.Join(work, "v1"+suffix)
	v2 := filepath.Join(work, "offline"+suffix)
	command(reference, goBinary, "build", "-buildvcs=false", "-mod=readonly", "-o", v1, "./cmd/parity")
	command(root, goBinary, "build", "-mod=readonly", "-o", v2, "./cmd/offline")
	expected := map[int][5]int{0: {1284, 750, 473, 285, 277}, 10: {2049, 711, 419, 292, 292}, 30: {3446, 654, 359, 295, 295}, 60: {5344, 596, 319, 287, 277}}
	for _, delta := range []int{0, 10, 30, 60} {
		t.Run(strconv.Itoa(delta), func(t *testing.T) {
			a, b := filepath.Join(work, fmt.Sprintf("v1-%d", delta)), filepath.Join(work, fmt.Sprintf("v2-%d", delta))
			command(reference, v1, assets, sha, a, w.Start.Format(time.RFC3339Nano), w.End.Format(time.RFC3339Nano), strconv.Itoa(delta))
			command(root, v2, fixtureArgs(assets, sha, b, w, delta)...)
			compareOutputs(t, a, b)
			var summary group.RBI2RunSummary
			if err := json.Unmarshal(read(filepath.Join(b, "summary.json")), &summary); err != nil {
				t.Fatal(err)
			}
			if realD2 {
				f, err := os.Open(filepath.Join(b, "raw.ndjson"))
				if err != nil {
					t.Fatal(err)
				}
				decoder := json.NewDecoder(bufio.NewReaderSize(f, 1<<20))
				i2Positive := 0
				for {
					var raw group.Raw
					err := decoder.Decode(&raw)
					if err == io.EOF {
						break
					}
					if err != nil {
						f.Close()
						t.Fatal(err)
					}
					if raw.IF.Available == 18 && raw.IF.Anomalous >= 2 {
						i2Positive++
					}
				}
				f.Close()
				if i2Positive != 1408 {
					t.Fatalf("raw I2 positive: %d != 1408", i2Positive)
				}
				got := [5]int{summary.PositiveTicks, summary.Events, summary.NewEvents, summary.RBIntervals, summary.RBSupportedEvents}
				if summary.Ticks != 46080 || got != expected[delta] {
					t.Fatal("D2 aggregates differ", summary)
				}
			}
			t.Logf("V1/V2 canonical raw/temporal/evidence/events/summary identical; delta=%d ticks=%d parents=%d real_D2=%t", delta, summary.Ticks, summary.Events, realD2)
		})
	}
}

const v1ParityMain = `package main
import (
 "bufio"
 "context"
 "encoding/json"
 "fmt"
 "os"
 "path/filepath"
 "strconv"
 "time"
 "cluster-anomaly-detection/internal/analysis"
 "cluster-anomaly-detection/internal/group"
)
func main(){if err:=run();err!=nil{fmt.Fprintln(os.Stderr,err);os.Exit(1)}}
func run()error{
 root,sha,out:=os.Args[1],os.Args[2],os.Args[3]
 start,err:=time.Parse(time.RFC3339Nano,os.Args[4]);if err!=nil{return err};end,err:=time.Parse(time.RFC3339Nano,os.Args[5]);if err!=nil{return err};delta,err:=strconv.Atoi(os.Args[6]);if err!=nil{return err}
 if err=os.Mkdir(out,0700);err!=nil{return err}
 var files []*os.File;var buffers []*bufio.Writer
 open:=func(name string)(func(any)error,error){f,e:=os.OpenFile(filepath.Join(out,name),os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600);if e!=nil{return nil,e};b:=bufio.NewWriter(f);files=append(files,f);buffers=append(buffers,b);return json.NewEncoder(b).Encode,nil}
 raw,err:=open("raw.ndjson");if err!=nil{return err};temporal,err:=open("temporal.ndjson");if err!=nil{return err};evidence,err:=open("evidence.ndjson");if err!=nil{return err};events,err:=open("events.ndjson");if err!=nil{return err}
 sink:=group.RBI2Sink{Raw:func(v group.Raw)error{return raw(v)},Temporal:func(v group.RBI2TemporalResult)error{return temporal(v)},Evidence:func(v group.RBI2Evidence)error{return evidence(v)},Event:func(v group.RBI2Event)error{return events(v)}}
 summary,runErr:=group.RunRBI2Offline(context.Background(),group.RBI2OfflineOptions{Files:group.SummitRBI2Files(root,sha),Input:filepath.Join(root,"data/prepared/summit-D1-pilot/D1.parquet"),Manifest:filepath.Join(root,"data/prepared/summit-D1-pilot/manifest.json"),Window:analysis.Window{Start:start,End:end},Groups:16,DeltaSeconds:delta,Sink:sink})
 for i,f:=range files{if e:=buffers[i].Flush();e!=nil{return e};if e:=f.Close();e!=nil{return e}}
 if runErr!=nil{return runErr};return group.SaveJSON(filepath.Join(out,"summary.json"),summary)
}
`
