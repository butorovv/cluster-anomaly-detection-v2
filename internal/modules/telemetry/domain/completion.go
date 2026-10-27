package domain

import (
	"fmt"
	"time"

	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

// TimestampComplete follows all available records of one (group, event-time) pair.
// Replay emits this only for encountered pairs, including partially valid pairs.
type TimestampComplete struct {
	Timestamp    time.Time `json:"timestamp"`
	GroupID      string    `json:"group_id"`
	Records      int       `json:"records"`
	ValidRecords int       `json:"valid_records"`
}

func (c TimestampComplete) Validate() error {
	group, _, err := topology.ParseHostname(c.GroupID + "n01")
	if err != nil || group != c.GroupID || c.Timestamp.IsZero() {
		return fmt.Errorf("invalid timestamp completion identity")
	}
	if c.Records < 0 || c.ValidRecords < 0 || c.ValidRecords > c.Records {
		return fmt.Errorf("invalid timestamp completion counts")
	}
	return nil
}
