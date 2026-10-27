package offline

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
)

type ndjsonOutput struct {
	file *os.File
	buf  *bufio.Writer
}

func openNDJSON(path string) (*ndjsonOutput, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	return &ndjsonOutput{file: f, buf: bufio.NewWriterSize(f, 1<<20)}, nil
}

func (o *ndjsonOutput) encode(v any) error { return json.NewEncoder(o.buf).Encode(v) }

func (o *ndjsonOutput) close() error {
	if o == nil {
		return nil
	}
	flushErr := o.buf.Flush()
	closeErr := o.file.Close()
	return errors.Join(flushErr, closeErr)
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) (retErr error) {
	fs := flag.NewFlagSet("offline", flag.ContinueOnError)
	fs.SetOutput(stderr)
	assetsRoot := fs.String("assets", "", "trusted assets root (required)")
	profileSHA := fs.String("profile-sha256", "", "trusted profile SHA256 (required)")
	outputDir := fs.String("output", "", "new output directory (required)")
	windowFlag := fs.String("window", "D2", "analysis window: D1 or D2")
	startFlag := fs.String("start", "", "optional RFC3339 UTC subwindow start (requires end)")
	endFlag := fs.String("end", "", "optional RFC3339 UTC subwindow end (requires start)")
	groups := fs.Int("groups", 16, "nested group universe: 4, 8, or 16")
	delta := fs.Int("delta", 0, "RBI2 TTL seconds: 0, 10, 30, or 60")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if *assetsRoot == "" || *profileSHA == "" || *outputDir == "" {
		return fmt.Errorf("--assets ROOT, --profile-sha256 SHA256, and --output NEW_DIRECTORY are required")
	}
	if !validSHA256(*profileSHA) {
		return fmt.Errorf("--profile-sha256 must be 64 hexadecimal characters")
	}
	var split int
	switch *windowFlag {
	case "D1":
		split = 0
	case "D2":
		split = 1
	default:
		return fmt.Errorf("--window must be D1 or D2")
	}
	if *groups != 4 && *groups != 8 && *groups != 16 {
		return fmt.Errorf("--groups must be 4, 8, or 16")
	}
	if *delta != 0 && *delta != 10 && *delta != 30 && *delta != 60 {
		return fmt.Errorf("--delta must be 0, 10, 30, or 60")
	}
	w := analysis.NIRWindows()[split]
	if *startFlag != "" || *endFlag != "" {
		start, e1 := time.Parse(time.RFC3339Nano, *startFlag)
		end, e2 := time.Parse(time.RFC3339Nano, *endFlag)
		if e1 != nil || e2 != nil || !end.After(start) || start.Before(w.Start) || end.After(w.End) || !start.Equal(start.Truncate(analysis.Step)) || !end.Equal(end.Truncate(analysis.Step)) {
			return fmt.Errorf("--start and --end must define a grid subwindow within --window")
		}
		w = analysis.Window{Start: start.UTC(), End: end.UTC()}
	}
	if _, err := os.Stat(*outputDir); !os.IsNotExist(err) {
		if err == nil {
			return fmt.Errorf("output directory already exists")
		}
		return fmt.Errorf("cannot inspect output directory: %w", err)
	}
	if err := os.Mkdir(*outputDir, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	root, err := filepath.Abs(*assetsRoot)
	if err != nil {
		return err
	}
	files := group.SummitRBI2Files(root, strings.ToLower(*profileSHA))
	input := filepath.Join(root, "data", "prepared", "summit-D1-pilot", "D1.parquet")
	manifest := filepath.Join(root, "data", "prepared", "summit-D1-pilot", "manifest.json")

	outputs := make([]*ndjsonOutput, 0, 4)
	defer func() {
		for _, o := range outputs {
			if e := o.close(); e != nil {
				retErr = errors.Join(retErr, e)
			}
		}
	}()
	open := func(name string) (*ndjsonOutput, error) {
		o, e := openNDJSON(filepath.Join(*outputDir, name))
		if e == nil {
			outputs = append(outputs, o)
		}
		return o, e
	}
	raw, err := open("raw.ndjson")
	if err != nil {
		return err
	}
	temporal, err := open("temporal.ndjson")
	if err != nil {
		return err
	}
	evidence, err := open("evidence.ndjson")
	if err != nil {
		return err
	}
	events, err := open("events.ndjson")
	if err != nil {
		return err
	}
	sink := group.RBI2Sink{
		Raw:      func(v group.Raw) error { return raw.encode(v) },
		Temporal: func(v group.RBI2TemporalResult) error { return temporal.encode(v) },
		Evidence: func(v group.RBI2Evidence) error { return evidence.encode(v) },
		Event:    func(v group.RBI2Event) error { return events.encode(v) },
	}
	summary, runErr := group.RunRBI2Offline(ctx, group.RBI2OfflineOptions{
		Files: files, Input: input, Manifest: manifest, Window: w, Groups: *groups, DeltaSeconds: *delta, Sink: sink,
	})
	for _, o := range outputs {
		if e := o.close(); runErr == nil && e != nil {
			runErr = e
		}
	}
	outputs = nil
	if runErr != nil {
		return runErr
	}
	if err := group.SaveJSON(filepath.Join(*outputDir, "summary.json"), summary); err != nil {
		return err
	}
	if err := json.NewEncoder(stdout).Encode(summary); err != nil {
		return fmt.Errorf("print summary: %w", err)
	}
	return nil
}

func validSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
