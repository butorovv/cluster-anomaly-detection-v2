package domain

import (
	"encoding/json"
	"time"
)

type Cursor struct {
	Revision int64 `json:"revision"`
	Ordinal  int   `json:"ordinal"`
}

func (c Cursor) After(other Cursor) bool {
	return c.Revision > other.Revision || (c.Revision == other.Revision && c.Ordinal > other.Ordinal)
}

type Owner struct {
	ExecutionID string
	GroupID     string
	OwnerID     string
	Epoch       int64
}
type Delivery struct {
	Cursor    Cursor          `json:"cursor"`
	Envelope  json.RawMessage `json:"envelope"`
	CreatedAt time.Time       `json:"created_at"`
}
type Page struct {
	ExecutionID string     `json:"execution_id"`
	GroupID     string     `json:"group_id"`
	OwnerID     string     `json:"owner_id"`
	Epoch       int64      `json:"epoch"`
	Items       []Delivery `json:"items"`
}
type Endpoint struct {
	URL     string
	OwnerID string
	Epoch   int64
}
