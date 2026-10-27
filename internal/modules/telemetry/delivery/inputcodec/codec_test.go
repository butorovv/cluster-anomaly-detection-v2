package inputcodec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

func codecFixture() (domain.InputMessage, domain.InputWindow) {
	at := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	r := domain.TelemetryRecord{Timestamp: at, GroupID: "a01", Hostname: "a01n01", NodeIndex: 1, Valid: true, AvailabilityMask: domain.FullAvailabilityMask}
	var f [domain.FeatureCount]float64
	for i := range f {
		f[i] = float64(i)
	}
	r.SetFeatures(f)
	return domain.InputMessage{Version: domain.InputVersion, Type: domain.RecordMessage, Binding: domain.InputBinding{ExecutionID: "exec-1", InputSHA256: strings.Repeat("a", 64), ProfileSHA256: strings.Repeat("b", 64)}, GroupID: r.GroupID, Timestamp: at, Record: &r}, domain.InputWindow{Start: at, End: at.Add(20 * time.Second)}
}

func TestRoundTripF18FullPartialMissingZero(t *testing.T) {
	for _, missing := range []int{0, 1, 18} {
		t.Run(string(rune('A'+missing)), func(t *testing.T) {
			m, w := codecFixture()
			f := m.Record.Features()
			for i := 0; i < missing; i++ {
				f[17-i] = math.NaN()
				m.Record.AvailabilityMask &^= 1 << uint(17-i)
			}
			m.Record.Valid = missing == 0
			m.Record.SetFeatures(f)
			b, err := Encode(m, w)
			if err != nil {
				t.Fatal(err)
			}
			got, err := Decode(b, w)
			if err != nil {
				t.Fatal(err)
			}
			if got.Binding != m.Binding || got.Record.Valid != m.Record.Valid || got.Record.AvailabilityMask != m.Record.AvailabilityMask || got.Record.Timestamp.Location() != time.UTC {
				t.Fatal("identity/availability lost")
			}
			gf := got.Record.Features()
			for i, v := range f {
				if math.IsNaN(v) {
					if !math.IsNaN(gf[i]) {
						t.Fatal("missing became zero", i)
					}
				} else if gf[i] != v {
					t.Fatal("changed F18", i, v, gf[i])
				}
			}
			if missing < 18 && gf[0] != 0 {
				t.Fatal("finite zero lost")
			}
			if bytes.Count(b, []byte("null")) != missing {
				t.Fatal("missing wire coordinates", string(b))
			}
		})
	}
	m, w := codecFixture()
	m.Record = nil
	m.Type = domain.CompleteMessage
	m.Completion = &domain.TimestampComplete{Timestamp: m.Timestamp, GroupID: m.GroupID}
	b, err := Encode(m, w)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(b, w)
	if err != nil || *got.Completion != *m.Completion {
		t.Fatal(got, err)
	}
}

func TestFrozenWireFixture(t *testing.T) {
	m, w := codecFixture()
	want := fmt.Sprintf(`{"version":"telemetry-input-v1","message_type":"telemetry_record","binding":{"execution_id":"exec-1","input_sha256":"%s","profile_sha256":"%s"},"group_id":"a01","timestamp":"2022-01-18T08:00:00Z","payload":{"hostname":"a01n01","node_index":1,"f18":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17],"is_valid":true,"availability_mask":262143}}`, strings.Repeat("a", 64), strings.Repeat("b", 64))
	b, err := Encode(m, w)
	if err != nil || string(b) != want {
		t.Fatalf("wire schema changed: %s %v", b, err)
	}
	if _, err := Decode([]byte(want), w); err != nil {
		t.Fatal(err)
	}
}

func TestRejectCorruptWire(t *testing.T) {
	m, w := codecFixture()
	b, err := Encode(m, w)
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(map[string]any){
		"version":      func(e map[string]any) { e["version"] = "unknown" },
		"type":         func(e map[string]any) { e["message_type"] = "future" },
		"group":        func(e map[string]any) { e["group_id"] = "a17" },
		"utc":          func(e map[string]any) { e["timestamp"] = "2022-01-18T08:00:00+00:00" },
		"grid":         func(e map[string]any) { e["timestamp"] = "2022-01-18T08:00:01Z" },
		"end":          func(e map[string]any) { e["timestamp"] = "2022-01-18T08:00:20Z" },
		"identity":     func(e map[string]any) { e["payload"].(map[string]any)["hostname"] = "a02n01" },
		"noncanonical": func(e map[string]any) { e["payload"].(map[string]any)["hostname"] = "a1n1" },
		"node":         func(e map[string]any) { e["payload"].(map[string]any)["node_index"] = 2 },
		"f18length":    func(e map[string]any) { e["payload"].(map[string]any)["f18"] = []float64{1} },
		"mask":         func(e map[string]any) { e["payload"].(map[string]any)["availability_mask"] = 0 },
		"valid":        func(e map[string]any) { e["payload"].(map[string]any)["is_valid"] = false },
		"missingvalid": func(e map[string]any) { delete(e["payload"].(map[string]any), "is_valid") },
		"missingmask":  func(e map[string]any) { delete(e["payload"].(map[string]any), "availability_mask") },
		"unknownfield": func(e map[string]any) { e["payload"].(map[string]any)["extra"] = 1 },
		"binding":      func(e map[string]any) { e["binding"].(map[string]any)["input_sha256"] = "abc" },
		"execution":    func(e map[string]any) { delete(e["binding"].(map[string]any), "execution_id") },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			var e map[string]any
			if err := json.Unmarshal(b, &e); err != nil {
				t.Fatal(err)
			}
			mutate(e)
			data, _ := json.Marshal(e)
			if _, err := Decode(data, w); err == nil {
				t.Fatal("accepted corruption", string(data))
			}
		})
	}
	for _, data := range []string{"", "{", "null", string(b) + "{}", strings.Replace(string(b), "[0,1,", "[1e999,1,", 1), strings.Replace(string(b), `"group_id":"a01"`, `"group_id":"a02","group_id":"a01"`, 1)} {
		if _, err := Decode([]byte(data), w); err == nil {
			t.Fatal("accepted invalid JSON", data)
		}
	}
	m.Record.PSUPower[0] = math.Inf(1)
	if _, err := Encode(m, w); err == nil {
		t.Fatal("Inf accepted")
	}
	m, _ = codecFixture()
	m.Completion = &domain.TimestampComplete{}
	if _, err := Encode(m, w); err == nil {
		t.Fatal("two payloads accepted")
	}
	for _, payload := range []string{`{}`, `{"records":null,"valid_records":0}`, `{"records":19,"valid_records":0}`, `{"records":0,"valid_records":1}`, `{"records":-1,"valid_records":0}`} {
		var e map[string]any
		_ = json.Unmarshal(b, &e)
		e["message_type"] = domain.CompleteMessage
		e["payload"] = json.RawMessage(payload)
		data, _ := json.Marshal(e)
		if _, err := Decode(data, w); err == nil {
			t.Fatal("bad completion accepted", payload)
		}
	}
}

type shortWriter struct{}

func (shortWriter) Write(b []byte) (int, error) { return len(b) - 1, nil }

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }
func TestSenderErrorsAndCancellation(t *testing.T) {
	m, w := codecFixture()
	sentinel := errors.New("sink down")
	for _, tc := range []struct {
		writer io.Writer
		want   error
	}{{shortWriter{}, io.ErrShortWrite}, {errorWriter{sentinel}, sentinel}} {
		s := Sender{Writer: tc.writer, Binding: m.Binding, Window: w}
		if err := s.Send(context.Background(), *m.Record); !errors.Is(err, tc.want) {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	s := Sender{Writer: &buf, Binding: m.Binding, Window: w}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Send(ctx, *m.Record); !errors.Is(err, context.Canceled) || buf.Len() != 0 {
		t.Fatal(err)
	}
}
