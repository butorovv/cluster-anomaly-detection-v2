package replayer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	prepared "github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/parquet"

	pq "github.com/parquet-go/parquet-go"
)

func cliSourceSHA(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

func sourceArgs(path, digest string) []string {
	return []string{"--input", path, "--speed", "0", "--wire-format", "v1", "--completion-mode", "full-grid", "--start", "2022-01-18T08:00:00Z", "--end", "2022-01-18T08:00:30Z", "--execution-id", "c1-source-test", "--input-sha256", digest, "--profile-sha256", strings.Repeat("b", 64)}
}

func differentCLISource(t *testing.T, original, other string) {
	t.Helper()
	rows, err := pq.ReadFile[fixtureRow](original)
	if err != nil {
		t.Fatal(err)
	}
	value := 999.0
	rows[0].PSU0Power = &value
	if err := pq.WriteFile(other, rows, pq.KeyValueMetadata("schema_version", "F18-v1")); err != nil {
		t.Fatal(err)
	}
}

func TestC1RejectsWrongActualSource(t *testing.T) {
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.parquet"), filepath.Join(dir, "b.parquet")
	writeCLIFixture(t, a)
	differentCLISource(t, a, b)
	declared := cliSourceSHA(t, a)
	if declared == cliSourceSHA(t, b) {
		t.Fatal("repro inputs unexpectedly identical")
	}
	var out, stderr bytes.Buffer
	err := Replayer(context.Background(), sourceArgs(b, declared), func(string) string { return "" }, &out, &stderr)
	if !errors.Is(err, prepared.ErrSourceMismatch) || out.Len() != 0 || strings.Contains(stderr.String(), "replay finished") {
		t.Fatalf("wrong actual source accepted or published: err=%v output_bytes=%d success_summary=%t", err, out.Len(), strings.Contains(stderr.String(), "replay finished"))
	}
	t.Log("wrong actual SHA rejected before any record/marker")
}

func TestC1PinnedWireCompatibility(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.parquet")
	writeCLIFixture(t, path)
	digest := cliSourceSHA(t, path)
	cases := map[string][]string{
		"v1-full-grid": sourceArgs(path, digest),
		"v1-observed":  append(sourceArgs(path, digest), "--completion-mode", "observed"),
		"legacy":       {"--input", path, "--speed", "0"},
		"legacy-grid":  {"--input", path, "--speed", "0", "--completion-mode", "full-grid", "--start", "2022-01-18T08:00:00Z", "--end", "2022-01-18T08:00:30Z"},
	}
	// Exact d748eeb wire fingerprints of this generated fixture, not V1 science golden.
	pinnedSHA := map[string]string{
		"v1-full-grid": "4b9c5d228b579270a368cf4cdae9162960f5fbc90a42d9c51b4010e7d426ac8a",
		"v1-observed":  "57886a1ae0b8dab74c3903614df641031905d704511c52819d7aac30d559d7e4",
		"legacy":       "4adf1fc12d4653119922904ea88a53dc68fea5dcc4e4352e95d4acfe0846ceda",
		"legacy-grid":  "15c9302cd347f15f5b7a644bdf7001d4b05e85c9aef7c95ceba70e446851a998",
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			if err := Replayer(context.Background(), args, func(string) string { return "" }, &out, io.Discard); err != nil {
				t.Fatal(err)
			}
			if out.Len() == 0 {
				t.Fatal("empty replay")
			}
			if got := fmt.Sprintf("%x", sha256.Sum256(out.Bytes())); got != pinnedSHA[name] {
				t.Fatal("wire differs from pinned production baseline", got, pinnedSHA[name])
			}
			if root := os.Getenv("P16_C1_WIRE_BASELINE"); root != "" {
				pinned := filepath.Join(root, name+".ndjson")
				if os.Getenv("P16_C1_CAPTURE_WIRE") == "1" {
					// Explicit audit evidence only; this is not a scientific golden.
					if err := os.WriteFile(pinned, out.Bytes(), 0600); err != nil {
						t.Fatal(err)
					}
				} else {
					want, err := os.ReadFile(pinned)
					if err != nil || !bytes.Equal(want, out.Bytes()) {
						t.Fatal("wire bytes differ from d748eeb baseline", err)
					}
				}
			}
			t.Logf("input_sha256=%s bytes=%d wire_sha256=%x", digest, out.Len(), sha256.Sum256(out.Bytes()))
		})
	}
}

func TestC1FullFileHashAndSetupFailuresPublishNothing(t *testing.T) {
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.parquet"), filepath.Join(dir, "b.parquet")
	writeCLIFixture(t, a)
	differentCLISource(t, a, b) // changed row at08:00, outside the selected08:00:20 window
	badSchema := filepath.Join(dir, "schema.parquet")
	rows, err := pq.ReadFile[fixtureRow](a)
	if err != nil {
		t.Fatal(err)
	}
	if err := pq.WriteFile(badSchema, rows); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		append(sourceArgs(b, cliSourceSHA(t, a)), "--start", "2022-01-18T08:00:20Z"),
		sourceArgs(badSchema, cliSourceSHA(t, badSchema)),
		sourceArgs(a+".missing", cliSourceSHA(t, a)),
		sourceArgs(dir, cliSourceSHA(t, a)),
		sourceArgs(a, ""), sourceArgs(a, "wrong"), sourceArgs(a, strings.Repeat("A", 64)),
	} {
		var out, stderr bytes.Buffer
		if err := Replayer(context.Background(), args, func(string) string { return "" }, &out, &stderr); err == nil || out.Len() != 0 || strings.Contains(stderr.String(), "replay finished") {
			t.Fatal("setup failure published data or success", args, out.Len(), err)
		}
	}
}

type sourceLogWriter func([]byte) (int, error)

func (f sourceLogWriter) Write(b []byte) (int, error) { return f(b) }

func TestC1CancelOrMutationAfterVerificationBeforePublication(t *testing.T) {
	for _, mutate := range []bool{false, true} {
		t.Run(fmt.Sprint(mutate), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "source.parquet")
			writeCLIFixture(t, path)
			digest := cliSourceSHA(t, path)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var out, stderr bytes.Buffer
			triggered := false
			log := sourceLogWriter(func(b []byte) (int, error) {
				if bytes.Contains(b, []byte("source verified")) {
					triggered = true
					if mutate {
						writer, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
						if err != nil {
							t.Fatal(err)
						}
						_, writeErr := writer.Write([]byte{0})
						if err := errors.Join(writeErr, writer.Close()); err != nil {
							t.Fatal(err)
						}
					} else {
						cancel()
					}
				}
				return stderr.Write(b)
			})
			err := Replayer(ctx, sourceArgs(path, digest), func(string) string { return "" }, &out, log)
			want := context.Canceled
			if mutate {
				want = prepared.ErrSourceChanged
			}
			if !triggered || !errors.Is(err, want) || out.Len() != 0 || strings.Contains(stderr.String(), "replay finished") {
				t.Fatal("verification boundary did not fail closed", triggered, out.Len(), err, stderr.String())
			}
		})
	}
}

func TestC1WrongSHAStopsBeforeKafkaSetup(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var connections atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			_ = c.Close()
		}
	}()
	defer func() { _ = listener.Close(); <-done }()
	path := filepath.Join(t.TempDir(), "source.parquet")
	writeCLIFixture(t, path)
	args := append(sourceArgs(path, strings.Repeat("0", 64)), "--output", "kafka", "--kafka-brokers", listener.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var out, stderr bytes.Buffer
	err = Replayer(ctx, args, func(string) string { return "" }, &out, &stderr)
	if !errors.Is(err, prepared.ErrSourceMismatch) || connections.Load() != 0 || out.Len() != 0 || strings.Contains(stderr.String(), "replay finished") {
		t.Fatal("wrong source reached Kafka or publication", connections.Load(), out.Len(), err)
	}
}

func TestC1CLIProcessExit(t *testing.T) {
	if os.Getenv("P16_C1_SUBPROCESS") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append([]string{"replayer"}, os.Args[i+1:]...)
				os.Exit(Run())
			}
		}
		os.Exit(99)
	}
	path := filepath.Join(t.TempDir(), "source.parquet")
	writeCLIFixture(t, path)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, wrong := range []bool{false, true} {
		digest := cliSourceSHA(t, path)
		if wrong {
			digest = strings.Repeat("0", 64)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := exec.CommandContext(ctx, executable, append([]string{"-test.run=^TestC1CLIProcessExit$", "--"}, sourceArgs(path, digest)...)...)
		cmd.Env = append(os.Environ(), "P16_C1_SUBPROCESS=1")
		var out, stderr bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		err := cmd.Run()
		cancel()
		if wrong {
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 || out.Len() != 0 || strings.Contains(stderr.String(), "replay finished") {
				t.Fatal("CLI wrong SHA exit", err, out.Len(), stderr.String())
			}
		} else if err != nil || out.Len() == 0 || !strings.Contains(stderr.String(), "source verified") || !strings.Contains(stderr.String(), "replay finished") {
			t.Fatal("CLI valid SHA exit", err, out.Len(), stderr.String())
		}
	}
}

func TestC1ComposeSourceBindingContract(t *testing.T) {
	entrypoint, err := os.ReadFile("../../../deploy/entrypoint.sh")
	if err != nil {
		t.Fatal(err)
	}
	var command string
	for _, line := range strings.Split(string(entrypoint), "\n") {
		if strings.Contains(line, "exec /app/replayer ") {
			command = line
			break
		}
	}
	for _, required := range []string{"--input /assets/data/prepared/summit-D1-pilot/D1.parquet", `--input-sha256 "$INPUT_SHA256"`, `--profile-sha256 "$PROFILE_SHA256"`, "--wire-format v1", "--output kafka"} {
		if !strings.Contains(command, required) {
			t.Fatal("Compose Replayer binding lost", required, command)
		}
	}
	compose, err := os.ReadFile("../../../deploy/compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"read_only: true", "target: /assets", "environment: *runtime", "command: [replayer]", "INPUT_SHA256:"} {
		if !bytes.Contains(compose, []byte(required)) {
			t.Fatal("Compose mount/env contract lost", required)
		}
	}
	// Docker/container execution is deliberately not performed by this contract test.
}
