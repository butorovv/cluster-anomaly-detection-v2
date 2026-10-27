package usecase

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

// WindowReader filters the existing prepared Parquet at read time. It preserves
// missingness and checks order/duplicates in consumed rows, including the skipped
// prefix. It stops at End instead of scanning the held-out suffix. No file is made.
type WindowReader struct {
	Source     RecordReader
	Start, End time.Time
	previous   domain.TelemetryRecord
	seen       bool
	done       bool
}

func (r *WindowReader) Next(ctx context.Context) (domain.TelemetryRecord, error) {
	if err := ctx.Err(); err != nil {
		return domain.TelemetryRecord{}, err
	}
	if r.Source == nil || r.Start.IsZero() || !r.End.After(r.Start) {
		return domain.TelemetryRecord{}, fmt.Errorf("invalid reader window")
	}
	if r.done {
		return domain.TelemetryRecord{}, io.EOF
	}
	for {
		v, err := r.Source.Next(ctx)
		if err != nil {
			return v, err
		}
		if r.seen && compareRecords(r.previous, v) >= 0 {
			return domain.TelemetryRecord{}, fmt.Errorf("non-increasing source key in window reader")
		}
		r.previous, r.seen = v, true
		// Sorted input cannot return into this window. Stop before traversing the
		// held-out suffix; order validation still covers every consumed record.
		if !v.Timestamp.Before(r.End) {
			r.done = true
			return domain.TelemetryRecord{}, io.EOF
		}
		if !v.Timestamp.Before(r.Start) && v.Timestamp.Before(r.End) {
			return v, nil
		}
	}
}
