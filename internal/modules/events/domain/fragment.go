package domain

import (
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
)

func TickFragment(execution string, id group.RBI2Identity, result group.GroupProcessorResult) (Envelope, error) {
	payload, err := json.Marshal(GroupResult{Raw: result.Raw, Temporal: result.Temporal, Evidence: result.Evidence})
	if err != nil {
		return Envelope{}, err
	}
	e := Envelope{SchemaVersion: 2, ExecutionID: execution, RunID: id.RunID, ConfigID: id.ConfigID, GroupID: result.Raw.Group, MessageID: MessageID(id.RunID, result.Raw.Group, "tick_fragment", result.Raw.Timestamp.UTC().Format("2006-01-02T15:04:05Z07:00"), 1), Kind: "tick_fragment", Revision: 1, EventTime: result.Raw.Timestamp.UTC(), Terminal: false, Payload: payload}
	return e, Validate(e, id)
}
func validateTick(e Envelope, id group.RBI2Identity, fragment bool) error {
	var p GroupResult
	if err := StrictJSON(e.Payload, &p); err != nil {
		return err
	}
	if e.Revision != 1 || e.EventID != "" || !e.EventTime.Before(id.Window.End) || p.Raw.Group != e.GroupID || !p.Raw.Timestamp.Equal(e.EventTime) || p.Temporal.Group != e.GroupID || !p.Temporal.Timestamp.Equal(e.EventTime) || p.Temporal.DeltaSeconds != id.DeltaSeconds || p.Raw.Complete < 0 || p.Raw.Complete > 18 || len(p.Raw.Local) != 18 {
		return fmt.Errorf("invalid diagnostic tick binding")
	}
	if fragment {
		if e.Terminal || p.Raw.IF.OutsideGroups != nil {
			return fmt.Errorf("fragment must carry local unannotated diagnostics")
		}
	} else {
		if !e.Terminal || p.Raw.IF.OutsideGroups == nil || len(p.Raw.IF.OutsideGroups) != id.Groups-1 {
			return fmt.Errorf("final diagnostics require complete outside join")
		}
	}
	if (p.Temporal.Vote == group.Positive) != (p.Evidence != nil) {
		return fmt.Errorf("positive tick evidence mismatch")
	}
	if p.Evidence != nil {
		if p.Evidence.Version != id.Version || p.Evidence.RunID != e.RunID || p.Evidence.Raw.Group != e.GroupID || !p.Evidence.Raw.Timestamp.Equal(e.EventTime) {
			return fmt.Errorf("evidence binding mismatch")
		}
		a, _ := json.Marshal(p.Raw)
		b, _ := json.Marshal(p.Evidence.Raw)
		if string(a) != string(b) {
			return fmt.Errorf("evidence raw differs from diagnostic tick")
		}
		a, _ = json.Marshal(p.Temporal)
		b, _ = json.Marshal(p.Evidence.Temporal)
		if string(a) != string(b) {
			return fmt.Errorf("evidence temporal differs from accepted decision")
		}
	}
	return nil
}
