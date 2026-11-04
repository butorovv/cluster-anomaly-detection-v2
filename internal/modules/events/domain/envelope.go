// Package domain defines the shared transport-independent result envelope v1.
package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

const MaxMessageBytes = 4 << 20

// Message has one wire schema for every transport. The typed Worker payload and
// opaque receiver payload serialize identically; receivers never reconstruct it.
type Message[T any] struct {
	SchemaVersion int       `json:"schema_version"`
	ExecutionID   string    `json:"execution_id"`
	RunID         string    `json:"run_id"`
	ConfigID      string    `json:"config_id"`
	GroupID       string    `json:"group_id"`
	MessageID     string    `json:"message_id"`
	Kind          string    `json:"kind"`
	Revision      int64     `json:"revision"`
	EventTime     time.Time `json:"event_time"`
	Terminal      bool      `json:"terminal"`
	EventID       string    `json:"event_id,omitempty"`
	Payload       T         `json:"payload"`
}

type Envelope = Message[json.RawMessage]
type EventEnvelope = Message[group.RBI2Event]
type GroupResult struct {
	Raw      group.Raw                `json:"raw"`
	Temporal group.RBI2TemporalResult `json:"temporal"`
	Evidence *group.RBI2Evidence      `json:"evidence"`
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var shaPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func ValidExecution(s string) bool { return uuidPattern.MatchString(s) }
func Hash(b []byte) string         { v := sha256.Sum256(b); return hex.EncodeToString(v[:]) }
func MessageID(run, groupID, kind, key string, revision int64) string {
	b, _ := json.Marshal([]any{"cad-output-v1", run, groupID, kind, key, revision})
	return Hash(b)
}

// UniqueJSON rejects duplicate keys at every depth and trailing JSON values.
// jsonb would otherwise silently discard conflicting duplicates before SQL dedup.
func UniqueJSON(b []byte) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var walk func() error
	walk = func() error {
		t, e := d.Token()
		if e != nil {
			return e
		}
		switch t {
		case json.Delim('{'):
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return e
				}
				s, ok := k.(string)
				if !ok || seen[s] {
					return fmt.Errorf("duplicate JSON key %v", k)
				}
				seen[s] = true
				if e := walk(); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		case json.Delim('['):
			for d.More() {
				if e := walk(); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		}
		return nil
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON content")
	}
	return nil
}
func StrictJSON(b []byte, v any) error {
	if err := UniqueJSON(b); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(v)
}
func Decode(b []byte) (Envelope, error) {
	var e Envelope
	if len(b) == 0 || len(b) > MaxMessageBytes {
		return e, fmt.Errorf("result size outside 1..%d", MaxMessageBytes)
	}
	if err := StrictJSON(b, &e); err != nil {
		return e, err
	}
	return e, Header(e)
}
func Header(e Envelope) error {
	if (e.SchemaVersion != 1 && e.SchemaVersion != 2) || !ValidExecution(e.ExecutionID) || !shaPattern.MatchString(e.RunID) || !shaPattern.MatchString(e.ConfigID) || len(e.MessageID) < 1 || len(e.MessageID) > 256 || e.Revision < 1 || !telemetry.OnGrid(e.EventTime) {
		return fmt.Errorf("invalid result identity/version/position")
	}
	if len(e.Payload) < 2 || e.Payload[0] != '{' {
		return fmt.Errorf("result payload object required")
	}
	if (e.SchemaVersion == 2 && e.Kind != "tick_fragment") || (e.SchemaVersion == 1 && e.Kind != "rbi2_event" && e.Kind != "group_result") {
		return fmt.Errorf("unsupported result kind")
	}
	return nil
}

// Validate checks identity and materialized payload invariants, never votes or
// temporal history. SQL remains authoritative for dedup/revision concurrency.
func Validate(e Envelope, id group.RBI2Identity) error {
	if err := Header(e); err != nil {
		return err
	}
	groupOK := false
	for i := 1; i <= id.Groups; i++ {
		groupOK = groupOK || e.GroupID == fmt.Sprintf("a%02d", i)
	}
	if !groupOK || e.RunID != id.RunID || e.ConfigID != id.ConfigID || e.EventTime.Before(id.Window.Start) || e.EventTime.After(id.Window.End) {
		return fmt.Errorf("result outside registered scientific execution")
	}
	if e.Kind == "group_result" || e.Kind == "tick_fragment" {
		return validateTick(e, id, e.Kind == "tick_fragment")
	}
	var p group.RBI2Event
	if err := StrictJSON(e.Payload, &p); err != nil {
		return err
	}
	if p.Version != id.Version || p.ID != e.EventID || p.RunID != e.RunID || p.KernelConfigID != e.ConfigID || p.Group != e.GroupID || p.DeltaSeconds != id.DeltaSeconds || !telemetry.OnGrid(p.Detected) || !telemetry.OnGrid(p.LastPositive) || p.Detected.Before(id.Window.Start) || p.LastPositive.Before(p.Detected) || !p.LastPositive.Before(id.Window.End) || p.LastPositive.After(e.EventTime) || p.ID != group.RBI2EventID(e.RunID, e.GroupID, p.Detected.UTC().Format(time.RFC3339Nano)) {
		return fmt.Errorf("event payload identity/interval mismatch")
	}
	if e.Terminal {
		reason := p.ClosureReason
		if !telemetry.OnGrid(p.Closed) || !p.Closed.After(p.LastPositive) || p.Closed.After(id.Window.End) || p.Closed.After(e.EventTime) || (reason != "negative" && reason != "unknown" && reason != "gap" && reason != "missing_tail" && reason != "window") || p.PositiveTicks < 1 || len(p.VoteSegments) == 0 || p.DurationSeconds != int(p.LastPositive.Sub(p.Detected)/time.Second)+10 || p.DelaySeconds != int(p.Detected.Sub(p.FirstEvidence)/time.Second) {
			return fmt.Errorf("invalid terminal event payload")
		}
	} else if !p.Closed.IsZero() {
		return fmt.Errorf("open event carries closed time")
	}
	return nil
}
