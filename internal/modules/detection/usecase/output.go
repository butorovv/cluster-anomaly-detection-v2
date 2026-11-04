package usecase

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
)

// EventEnvelope uses terminal-only revision 1; stable IDs do not depend on an
// owner epoch, checkpoint cadence, delivery attempt or processing clock.
type EventEnvelope struct {
	SchemaVersion int             `json:"schema_version"`
	ExecutionID   string          `json:"execution_id"`
	RunID         string          `json:"run_id"`
	ConfigID      string          `json:"config_id"`
	GroupID       string          `json:"group_id"`
	MessageID     string          `json:"message_id"`
	Kind          string          `json:"kind"`
	Revision      int64           `json:"revision"`
	EventTime     time.Time       `json:"event_time"`
	Terminal      bool            `json:"terminal"`
	EventID       string          `json:"event_id"`
	Payload       group.RBI2Event `json:"payload"`
}

func TerminalEvents(execution string, events []group.RBI2Event) ([]EventEnvelope, error) {
	outputs := make([]EventEnvelope, 0, len(events))
	for _, e := range events {
		if e.Closed.IsZero() || !e.Closed.After(e.LastPositive) || e.ID == "" || e.Version != group.RBI2Version {
			return nil, fmt.Errorf("terminal RBI2 event required")
		}
		key, err := json.Marshal([]any{"cad-output-v1", e.RunID, e.Group, "rbi2_event", e.ID, 1})
		if err != nil {
			return nil, err
		}
		h := sha256.Sum256(key)
		outputs = append(outputs, EventEnvelope{1, execution, e.RunID, e.KernelConfigID, e.Group, hex.EncodeToString(h[:]), "rbi2_event", 1, e.Closed.UTC(), true, e.ID, e})
	}
	return outputs, nil
}
