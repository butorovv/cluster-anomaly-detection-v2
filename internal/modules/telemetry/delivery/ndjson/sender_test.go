package ndjson

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

type brokenWriter struct{ err error }

func (w brokenWriter) Write([]byte) (int, error) { return 0, w.err }

func TestNDJSONNullTypesAndErrors(t *testing.T) {
	r := domain.TelemetryRecord{Timestamp: time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC), Hostname: "a01n01", GroupID: "a01", NodeIndex: 1, AvailabilityMask: domain.FullAvailabilityMask &^ 1}
	r.PSUPower[0] = math.NaN()
	var output bytes.Buffer
	s := NewJSONSender(&output)
	if err := s.Send(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	c := domain.TimestampComplete{Timestamp: r.Timestamp, GroupID: r.GroupID, Records: 1, ValidRecords: 0}
	if err := s.Complete(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatal(output.String())
	}
	var got struct {
		Type             string
		F18              []*float64
		AvailabilityMask uint32 `json:"availability_mask"`
		Valid            bool   `json:"is_valid"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Fatal(err)
	}
	if got.Type != "telemetry" || len(got.F18) != 18 || got.F18[0] != nil || got.F18[1] == nil || *got.F18[1] != 0 || got.Valid || got.AvailabilityMask != r.AvailabilityMask {
		t.Fatal(got)
	}
	var complete struct {
		Type string
		domain.TimestampComplete
	}
	if err := json.Unmarshal([]byte(lines[1]), &complete); err != nil {
		t.Fatal(err)
	}
	if complete.Type != "timestamp_complete" || complete.TimestampComplete != c {
		t.Fatal(complete)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(s.Send(ctx, r), context.Canceled) || !errors.Is(s.Complete(ctx, c), context.Canceled) {
		t.Fatal("cancellation lost")
	}
	boom := errors.New("write failure")
	broken := NewJSONSender(brokenWriter{boom})
	if !errors.Is(broken.Send(context.Background(), r), boom) {
		t.Fatal("sender error lost")
	}
	broken = NewJSONSender(brokenWriter{boom})
	if !errors.Is(broken.Complete(context.Background(), c), boom) {
		t.Fatal("completion error lost")
	}
}
