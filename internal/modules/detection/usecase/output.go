package usecase

import (
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

// EventEnvelope uses the shared v1 wire contract, with a typed frozen payload.
type EventEnvelope = domain.EventEnvelope

func TerminalEvents(execution string, events []group.RBI2Event) ([]EventEnvelope, error) {
	outputs := make([]EventEnvelope, 0, len(events))
	for _, e := range events {
		if e.Closed.IsZero() || !e.Closed.After(e.LastPositive) || e.ID == "" || e.Version != group.RBI2Version {
			return nil, fmt.Errorf("terminal RBI2 event required")
		}
		outputs = append(outputs, EventEnvelope{SchemaVersion: 1, ExecutionID: execution, RunID: e.RunID, ConfigID: e.KernelConfigID, GroupID: e.Group, MessageID: domain.MessageID(e.RunID, e.Group, "rbi2_event", e.ID, 1), Kind: "rbi2_event", Revision: 1, EventTime: e.Closed.UTC(), Terminal: true, EventID: e.ID, Payload: e})
	}
	return outputs, nil
}
