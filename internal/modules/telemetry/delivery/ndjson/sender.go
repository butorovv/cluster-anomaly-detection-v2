package ndjson

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

type JSONSender struct{ encoder *json.Encoder }

func NewJSONSender(w io.Writer) *JSONSender { return &JSONSender{encoder: json.NewEncoder(w)} }

func (s *JSONSender) Send(ctx context.Context, r domain.TelemetryRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var values [domain.FeatureCount]*float64
	for i, v := range r.Features() {
		if !math.IsNaN(v) && !math.IsInf(v, 0) {
			value := v
			values[i] = &value
		}
	}
	return s.encoder.Encode(struct {
		Type             string                        `json:"type"`
		Timestamp        time.Time                     `json:"timestamp"`
		Hostname         string                        `json:"hostname"`
		GroupID          string                        `json:"group_id"`
		NodeIndex        int                           `json:"node_index"`
		F18              [domain.FeatureCount]*float64 `json:"f18"`
		Valid            bool                          `json:"is_valid"`
		AvailabilityMask uint32                        `json:"availability_mask"`
	}{"telemetry", r.Timestamp, r.Hostname, r.GroupID, r.NodeIndex, values, r.Valid, r.AvailabilityMask})
}
func (s *JSONSender) Complete(ctx context.Context, c domain.TimestampComplete) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.encoder.Encode(struct {
		Type string `json:"type"`
		domain.TimestampComplete
	}{"timestamp_complete", c})
}
