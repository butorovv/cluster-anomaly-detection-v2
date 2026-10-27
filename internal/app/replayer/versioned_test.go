package replayer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/inputcodec"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

func TestVersionedCLIAndLegacyDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "synthetic.parquet")
	writeCLIFixture(t, path)
	base := []string{"--input", path, "--speed", "0"}
	getenv := func(string) string { return "" }
	var defaultOut, explicitOut, stderr bytes.Buffer
	if err := Replayer(context.Background(), base, getenv, &defaultOut, &stderr); err != nil {
		t.Fatal(err)
	}
	if err := Replayer(context.Background(), append(append([]string{}, base...), "--completion-mode", "observed", "--wire-format", "legacy"), getenv, &explicitOut, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(defaultOut.Bytes(), explicitOut.Bytes()) {
		t.Fatal("legacy defaults changed")
	}
	args := append(append([]string{}, base...), "--completion-mode", "full-grid", "--start", "2022-01-18T08:00:00Z", "--end", "2022-01-18T08:00:30Z", "--wire-format", "v1", "--execution-id", "p03-test", "--input-sha256", strings.Repeat("a", 64), "--profile-sha256", strings.Repeat("b", 64))
	var out bytes.Buffer
	if err := Replayer(context.Background(), args, getenv, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	start := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	window := domain.InputWindow{Start: start, End: start.Add(30 * time.Second)}
	rows, markers := 0, 0
	for _, line := range bytes.Split(bytes.TrimSpace(out.Bytes()), []byte{'\n'}) {
		m, err := inputcodec.Decode(line, window)
		if err != nil {
			t.Fatal(err)
		}
		if m.Record != nil {
			rows++
		} else {
			markers++
		}
	}
	if rows != 4 || markers != 48 {
		t.Fatal(rows, markers)
	}
	for _, extra := range [][]string{{"--wire-format", "other"}, {"--wire-format", "v1"}, {"--execution-id", "ignored"}, {"--completion-mode", "full-grid", "--start", "2022-01-18T08:00:01Z", "--end", "2022-01-18T08:00:30Z"}} {
		if _, err := load(append(append([]string{}, base...), extra...), getenv, io.Discard); err == nil {
			t.Fatal(extra)
		}
	}
}

type brokenOutput struct{ err error }

func (w brokenOutput) Write([]byte) (int, error) { return 0, w.err }
func TestPartialOutputHasNoSuccessSummary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "synthetic.parquet")
	writeCLIFixture(t, path)
	boom := errors.New("stdout failed")
	var stderr bytes.Buffer
	err := Replayer(context.Background(), []string{"--input", path, "--speed", "0", "--completion-mode", "full-grid", "--start", "2022-01-18T08:00:00Z", "--end", "2022-01-18T08:00:30Z"}, func(string) string { return "" }, brokenOutput{boom}, &stderr)
	if !errors.Is(err, boom) || strings.Contains(stderr.String(), "replay finished") {
		t.Fatal(err, stderr.String())
	}
}
