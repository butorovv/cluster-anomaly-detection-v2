package domain

import (
	"time"
)

type ResultCursor struct {
	Tick    time.Time `json:"tick"`
	GroupID string    `json:"group_id"`
}
type ResultPage struct {
	Items []GroupResult `json:"items"`
	Next  ResultCursor  `json:"next"`
}
type Summary struct {
	ExecutionID        string `json:"execution_id"`
	RunID              string `json:"run_id"`
	ConfigID           string `json:"config_id"`
	ExpectedTicks      int64  `json:"expected_ticks"`
	Ticks              int64  `json:"group_ticks"`
	PositiveTicks      int64  `json:"positive_ticks"`
	UnknownTicks       int64  `json:"unknown_ticks"`
	I2PositiveTicks    int64  `json:"i2_positive_ticks"`
	Events             int64  `json:"parent_events"`
	NewEvents          int64  `json:"new_events"`
	RBSubspans         int64  `json:"rb_subspans"`
	RBSupportedParents int64  `json:"rb_supported_parents"`
	Complete           bool   `json:"complete"`
}
