// Package inputcodec implements the transport-neutral telemetry-input-v1 JSON contract.
package inputcodec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

type envelope struct {
	Version   string              `json:"version"`
	Type      string              `json:"message_type"`
	Binding   domain.InputBinding `json:"binding"`
	GroupID   string              `json:"group_id"`
	Timestamp string              `json:"timestamp"`
	Payload   json.RawMessage     `json:"payload"`
}

type recordPayload struct {
	Hostname  string     `json:"hostname"`
	NodeIndex int        `json:"node_index"`
	F18       []*float64 `json:"f18"`
	Valid     *bool      `json:"is_valid"`
	Mask      *uint32    `json:"availability_mask"`
}

type completionPayload struct {
	Records      *int `json:"records"`
	ValidRecords *int `json:"valid_records"`
}

func strict(data []byte, out any) error {
	if err := uniqueFields(json.NewDecoder(bytes.NewReader(data)), 0); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("expected exactly one JSON object")
	}
	return nil
}

// Duplicate names are ambiguous bindings; encoding/json otherwise accepts the last.
func uniqueFields(d *json.Decoder, depth int) error {
	if depth > 32 {
		return fmt.Errorf("input JSON nesting exceeds schema limit")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("duplicate/invalid JSON field %q", name)
			}
			seen[name] = true
			if err := uniqueFields(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := uniqueFields(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter")
	}
	_, err = d.Token()
	return err
}

func Encode(m domain.InputMessage, window domain.InputWindow) ([]byte, error) {
	if err := m.Validate(window); err != nil {
		return nil, err
	}
	var payload any
	if m.Record != nil {
		r := m.Record
		values := make([]*float64, domain.FeatureCount)
		for i, value := range r.Features() {
			if !math.IsNaN(value) {
				v := value
				values[i] = &v
			}
		}
		payload = recordPayload{r.Hostname, r.NodeIndex, values, &r.Valid, &r.AvailabilityMask}
	} else {
		c := m.Completion
		payload = completionPayload{&c.Records, &c.ValidRecords}
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{m.Version, m.Type, m.Binding, m.GroupID, m.Timestamp.UTC().Format(time.RFC3339Nano), b})
}

func Decode(data []byte, window domain.InputWindow) (domain.InputMessage, error) {
	var e envelope
	if err := strict(data, &e); err != nil {
		return domain.InputMessage{}, err
	}
	at, err := time.Parse(time.RFC3339Nano, e.Timestamp)
	if err != nil || e.Timestamp != at.UTC().Format(time.RFC3339Nano) {
		return domain.InputMessage{}, fmt.Errorf("canonical UTC timestamp required")
	}
	m := domain.InputMessage{Version: e.Version, Type: e.Type, Binding: e.Binding, GroupID: e.GroupID, Timestamp: at.UTC()}
	switch e.Type {
	case domain.RecordMessage:
		var p recordPayload
		if err := strict(e.Payload, &p); err != nil {
			return domain.InputMessage{}, err
		}
		if len(p.F18) != domain.FeatureCount || p.Valid == nil || p.Mask == nil {
			return domain.InputMessage{}, fmt.Errorf("exact F18, is_valid and availability_mask required")
		}
		r := domain.TelemetryRecord{Timestamp: m.Timestamp, GroupID: m.GroupID, Hostname: p.Hostname, NodeIndex: p.NodeIndex, Valid: *p.Valid, AvailabilityMask: *p.Mask}
		var values [domain.FeatureCount]float64
		for i, v := range p.F18 {
			if v == nil {
				values[i] = math.NaN()
			} else {
				values[i] = *v
			}
		}
		r.SetFeatures(values)
		m.Record = &r
	case domain.CompleteMessage:
		var p completionPayload
		if err := strict(e.Payload, &p); err != nil {
			return domain.InputMessage{}, err
		}
		if p.Records == nil || p.ValidRecords == nil {
			return domain.InputMessage{}, fmt.Errorf("exact records and valid_records required")
		}
		m.Completion = &domain.TimestampComplete{Timestamp: m.Timestamp, GroupID: m.GroupID, Records: *p.Records, ValidRecords: *p.ValidRecords}
	default:
		return domain.InputMessage{}, fmt.Errorf("unknown input message_type %q", e.Type)
	}
	if err := m.Validate(window); err != nil {
		return domain.InputMessage{}, err
	}
	return m, nil
}

// Sender writes one versioned envelope per line; errors and cancellation propagate.
// It is sequential and deliberately has no concurrent worker or transport runtime.
type Sender struct {
	Writer  io.Writer
	Binding domain.InputBinding
	Window  domain.InputWindow
}

func (s *Sender) send(ctx context.Context, m domain.InputMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.Writer == nil {
		return fmt.Errorf("input writer required")
	}
	b, err := Encode(m, s.Window)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	n, err := s.Writer.Write(b)
	if err == nil && n != len(b) {
		return io.ErrShortWrite
	}
	return err
}

func (s *Sender) Send(ctx context.Context, r domain.TelemetryRecord) error {
	return s.send(ctx, domain.InputMessage{Version: domain.InputVersion, Type: domain.RecordMessage, Binding: s.Binding, GroupID: r.GroupID, Timestamp: r.Timestamp, Record: &r})
}

func (s *Sender) Complete(ctx context.Context, c domain.TimestampComplete) error {
	return s.send(ctx, domain.InputMessage{Version: domain.InputVersion, Type: domain.CompleteMessage, Binding: s.Binding, GroupID: c.GroupID, Timestamp: c.Timestamp, Completion: &c})
}
