package parquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

// PhysicalSample contains the measured F18 channels in domain.FeatureNames order.
// A nil coordinate represents a channel that was absent at the original time.
type PhysicalSample struct {
	Time   time.Time                     `json:"time"`
	Values [domain.FeatureCount]*float64 `json:"values"`
}

// PhysicalWindow reads an inclusive window in the timestamp-ordered prepared D2
// source. The result includes only rows recorded for the requested hostname.
func (r *ParquetReader) PhysicalWindow(ctx context.Context, host string, from, to time.Time) ([]PhysicalSample, error) {
	const step = 10 * time.Second
	const maxWindow = 10 * time.Minute
	if to.Before(from) || to.Sub(from) > maxWindow || from.UnixMicro()%step.Microseconds() != 0 ||
		to.UnixMicro()%step.Microseconds() != 0 {
		return nil, fmt.Errorf("expected an inclusive window on the 10-second grid of at most 10 minutes")
	}
	seek := func(index int64) error {
		if err := r.reader.SeekToRow(index); err != nil {
			return err
		}
		r.index, r.count, r.pending = 0, 0, nil
		return nil
	}
	lo, hi := int64(0), r.Rows()
	for lo < hi {
		mid := lo + (hi-lo)/2
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := r.reader.SeekToRow(mid); err != nil {
			return nil, err
		}
		var probe [1]preparedRow
		if n, err := r.reader.Read(probe[:]); n != 1 {
			return nil, fmt.Errorf("seek physical archive row %d: %v", mid, err)
		}
		if probe[0].Timestamp < from.UnixMicro() {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	samples := make([]PhysicalSample, 0, 61)
	if lo == r.Rows() {
		return samples, r.checkSourceUnchanged()
	}
	if err := seek(lo); err != nil {
		return nil, err
	}
	var previous, previousHost time.Time
	// The registered source has at most 288 rows per timestamp.
	for i := 0; i <= 61*288; i++ {
		row, err := r.Next(ctx)
		if errors.Is(err, io.EOF) {
			return samples, nil
		}
		if err != nil {
			return nil, err
		}
		if row.Timestamp.Before(from) || row.Timestamp.Before(previous) {
			return nil, fmt.Errorf("physical archive is not in timestamp order")
		}
		if row.Timestamp.After(to) {
			return samples, nil
		}
		previous = row.Timestamp
		if row.Hostname != host {
			continue
		}
		if row.Timestamp.Equal(previousHost) {
			return nil, fmt.Errorf("duplicate physical observation for %s at %s", host, row.Timestamp)
		}
		previousHost = row.Timestamp
		sample := PhysicalSample{Time: row.Timestamp}
		for index, value := range row.Features() {
			if !math.IsNaN(value) {
				value := value
				sample.Values[index] = &value
			}
		}
		samples = append(samples, sample)
	}
	return nil, fmt.Errorf("physical archive exceeds the allowed row count")
}
