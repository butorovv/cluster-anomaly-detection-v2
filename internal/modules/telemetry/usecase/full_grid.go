package usecase

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

// ReplayFullGrid emits the complete canonical group/tick grid for [start,end).
// It keeps at most one source row in lookahead memory.
func ReplayFullGrid(ctx context.Context, reader RecordReader, sender TelemetrySender, completer TimestampCompleter, clock Clock, speed float64, start, end time.Time) (Stats, error) {
	var stats Stats
	if speed < 0 || math.IsNaN(speed) || math.IsInf(speed, 0) {
		return stats, fmt.Errorf("speed must be finite and non-negative")
	}
	if reader == nil || sender == nil || completer == nil || clock == nil {
		return stats, fmt.Errorf("reader, sender, completer and clock are required")
	}
	window := domain.InputWindow{Start: start, End: end}
	if err := window.Validate(); err != nil {
		return stats, err
	}
	groups := topology.CanonicalGroups()
	tickCount := end.Sub(start) / (10 * time.Second)
	var previous domain.TelemetryRecord
	hasPrevious := false
	var pending domain.TelemetryRecord
	hasPending := false
	read := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		v, err := reader.Next(ctx)
		if errors.Is(err, io.EOF) {
			hasPending = false
			return nil
		}
		if err != nil {
			return fmt.Errorf("read record: %w", err)
		}
		if err := v.Validate(); err != nil {
			return err
		}
		if err := topology.ValidateSelectedNode(v.Hostname, v.GroupID, v.NodeIndex); err != nil {
			return err
		}
		if !window.Contains(v.Timestamp) {
			return fmt.Errorf("record outside full-grid window or tick: %s", v.Timestamp)
		}
		if hasPrevious && compareRecords(previous, v) >= 0 {
			return fmt.Errorf("non-increasing replay key: %s/%s/%s", v.Timestamp, v.GroupID, v.Hostname)
		}
		previous, hasPrevious, pending, hasPending = v, true, v, true
		return nil
	}
	wallStart := clock.Now()
	if err := read(); err != nil {
		return stats, err
	}
	hosts := map[string]struct{}{}
	for ti := int64(0); ti < int64(tickCount); ti++ {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		timestamp := start.Add(time.Duration(ti) * 10 * time.Second)
		if speed > 0 {
			ns := float64(timestamp.Sub(start)) / speed
			if ns >= float64(math.MaxInt64) {
				return stats, fmt.Errorf("replay pacing exceeds duration range; use a higher speed")
			}
			if wait := wallStart.Add(time.Duration(ns)).Sub(clock.Now()); wait > 0 {
				if err := clock.Sleep(ctx, wait); err != nil {
					return stats, err
				}
			}
		}
		stats.Timestamps++
		for _, group := range groups {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			complete := domain.TimestampComplete{Timestamp: timestamp, GroupID: group.Code}
			for hasPending && pending.Timestamp.Equal(timestamp) && pending.GroupID == group.Code {
				if err := sender.Send(ctx, pending); err != nil {
					return stats, fmt.Errorf("send record: %w", err)
				}
				stats.Rows++
				complete.Records++
				if pending.Valid {
					complete.ValidRecords++
				} else {
					stats.Invalid++
				}
				hosts[pending.Hostname] = struct{}{}
				if err := read(); err != nil {
					return stats, err
				}
			}
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			if err := completer.Complete(ctx, complete); err != nil {
				return stats, fmt.Errorf("timestamp completion: %w", err)
			}
			stats.Completions++
		}
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if hasPending {
		return stats, fmt.Errorf("record remains beyond emitted full-grid ticks")
	}
	stats.Groups = 16
	stats.Hosts = len(hosts)
	return stats, nil
}
