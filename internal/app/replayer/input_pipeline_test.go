package replayer

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/delivery/inputcodec"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/usecase"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
	prepared "github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/parquet"
)

// Pipeline compares each completed tick with a second direct streaming reader.
// Neither expected records nor the encoded stream are materialized for all D2.
type pipelineSink struct {
	window                              domain.InputWindow
	binding                             domain.InputBinding
	assemblers                          map[string]*usecase.TickAssembler
	expected                            usecase.RecordReader
	lookahead                           *domain.TelemetryRecord
	eof                                 bool
	rows, ticks, zero, partial, invalid int64
}

func (s *pipelineSink) accept(m domain.InputMessage) (*domain.GroupTick, error) {
	b, err := inputcodec.Encode(m, s.window)
	if err != nil {
		return nil, err
	}
	decoded, err := inputcodec.Decode(b, s.window)
	if err != nil {
		return nil, err
	}
	return s.assemblers[m.GroupID].Accept(decoded)
}
func (s *pipelineSink) Send(_ context.Context, r domain.TelemetryRecord) error {
	tick, err := s.accept(domain.InputMessage{Version: domain.InputVersion, Type: domain.RecordMessage, Binding: s.binding, Timestamp: r.Timestamp, GroupID: r.GroupID, Record: &r})
	if err != nil {
		return err
	}
	if tick != nil {
		return fmt.Errorf("assembled without source marker")
	}
	s.rows++
	if !r.Valid {
		s.invalid++
	}
	return nil
}
func (s *pipelineSink) Complete(ctx context.Context, c domain.TimestampComplete) error {
	tick, err := s.accept(domain.InputMessage{Version: domain.InputVersion, Type: domain.CompleteMessage, Binding: s.binding, Timestamp: c.Timestamp, GroupID: c.GroupID, Completion: &c})
	if err != nil {
		return err
	}
	if tick == nil {
		return fmt.Errorf("marker produced no tick")
	}
	want := map[string]domain.TelemetryRecord{}
	for {
		if s.lookahead == nil && !s.eof {
			r, err := s.expected.Next(ctx)
			if err == io.EOF {
				s.eof = true
			} else if err != nil {
				return err
			} else {
				s.lookahead = &r
			}
		}
		if s.lookahead == nil || s.lookahead.Timestamp != c.Timestamp || s.lookahead.GroupID != c.GroupID {
			break
		}
		want[s.lookahead.Hostname] = *s.lookahead
		s.lookahead = nil
	}
	if len(want) != len(tick.Records) || len(tick.Records) != c.Records || tick.ValidRecords != c.ValidRecords {
		return fmt.Errorf("counts differ at %s/%s", c.GroupID, c.Timestamp)
	}
	valid := 0
	missing := []string{}
	for h := 1; h <= 18; h++ {
		host := fmt.Sprintf("%sn%02d", c.GroupID, h)
		w, ok := want[host]
		got, present := tick.Records[host]
		if present != ok {
			return fmt.Errorf("presence differs for %s", host)
		}
		if !ok {
			missing = append(missing, host)
			continue
		}
		if w.Valid {
			valid++
		}
		if !equalInputRecord(w, got) {
			return fmt.Errorf("values/identity/missingness changed for %s/%s", host, c.Timestamp)
		}
	}
	if valid != c.ValidRecords || strings.Join(missing, ",") != strings.Join(tick.Missing, ",") {
		return fmt.Errorf("valid/missing hosts differ")
	}
	if len(want) == 0 {
		s.zero++
	} else if len(want) < 18 || valid < 18 {
		s.partial++
	}
	s.ticks++
	return nil
}
func equalInputRecord(a, b domain.TelemetryRecord) bool {
	if a.Timestamp != b.Timestamp || a.GroupID != b.GroupID || a.Hostname != b.Hostname || a.NodeIndex != b.NodeIndex || a.Valid != b.Valid || a.AvailabilityMask != b.AvailabilityMask {
		return false
	}
	af, bf := a.Features(), b.Features()
	for i := range af {
		if math.IsNaN(af[i]) {
			if !math.IsNaN(bf[i]) {
				return false
			}
		} else if math.Float64bits(af[i]) != math.Float64bits(bf[i]) {
			return false
		}
	}
	return true
}
func runInputPipeline(t *testing.T, path, profileSHA string) {
	t.Helper()
	ctx := context.Background()
	start := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	window := domain.InputWindow{Start: start, End: start.Add(8 * time.Hour)}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, f); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	binding := domain.InputBinding{ExecutionID: "p03-input-parity", InputSHA256: fmt.Sprintf("%x", hash.Sum(nil)), ProfileSHA256: profileSHA}
	s := &pipelineSink{window: window, binding: binding, assemblers: map[string]*usecase.TickAssembler{}}
	for _, g := range topology.CanonicalGroups() {
		a, err := usecase.NewTickAssembler(g.Code, binding, window)
		if err != nil {
			t.Fatal(err)
		}
		s.assemblers[g.Code] = a
	}
	open := func() *prepared.ParquetReader {
		r, err := prepared.OpenParquet(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		})
		return r
	}
	s.expected = &usecase.WindowReader{Source: open(), Start: window.Start, End: window.End}
	reader := &usecase.WindowReader{Source: open(), Start: window.Start, End: window.End}
	stats, err := usecase.ReplayFullGrid(ctx, reader, s, s, usecase.RealClock{}, 0, window.Start, window.End)
	if err != nil {
		t.Fatal(err)
	}
	for group, a := range s.assemblers {
		if err := a.Finish(); err != nil {
			t.Fatal(group, err)
		}
	}
	if s.lookahead != nil || !s.eof || stats.Completions != 46080 || s.ticks != 46080 || stats.Rows != s.rows || stats.Invalid != s.invalid {
		t.Fatal("incomplete pipeline", stats, s.ticks, s.rows, s.zero, s.partial, s.invalid)
	}
	t.Logf("input parity: assembled=%d markers=%d rows=%d invalid=%d zero_ticks=%d partial_ticks=%d; exact identity/float bits and NaN-aware missing equality", s.ticks, stats.Completions, s.rows, s.invalid, s.zero, s.partial)
}
func TestSyntheticInputPipeline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prepared.parquet")
	writeCLIFixture(t, path)
	runInputPipeline(t, path, strings.Repeat("a", 64))
}
func TestRealD2InputPipeline(t *testing.T) {
	root := os.Getenv("P03_REAL_ROOT")
	if root == "" {
		t.Skip("P03_REAL_ROOT required; real input parity NOT_RUN")
	}
	path := filepath.Join(root, "data/prepared/summit-D1-pilot/D1.parquet")
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	_, err = io.Copy(h, f)
	_ = f.Close()
	if err != nil || fmt.Sprintf("%x", h.Sum(nil)) != "cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7" {
		t.Fatal("original prepared SHA mismatch", err)
	}
	runInputPipeline(t, path, "a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36")
}
