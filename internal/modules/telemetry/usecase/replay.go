package usecase

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

type TelemetrySender interface {
	Send(context.Context, domain.TelemetryRecord) error
}

type TimestampCompleter interface {
	Complete(context.Context, domain.TimestampComplete) error
}

type RecordReader interface {
	Next(context.Context) (domain.TelemetryRecord, error)
}

type Clock interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }
func (RealClock) Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type Stats struct {
	Rows, Timestamps, Completions, Invalid int64
	Groups, Hosts                          int
}

// Replay sends every record, including invalid records, and marks only completed groups.
// The reader must yield strictly ascending timestamp/group/hostname keys.
func Replay(ctx context.Context, reader RecordReader, sender TelemetrySender, completer TimestampCompleter, clock Clock, speed float64) (Stats, error) {
	var stats Stats
	if speed < 0 || math.IsNaN(speed) || math.IsInf(speed, 0) {
		return stats, fmt.Errorf("speed must be finite and non-negative")
	}
	if reader == nil || sender == nil || completer == nil || clock == nil {
		return stats, fmt.Errorf("reader, sender, completer and clock are required")
	}
	var previous domain.TelemetryRecord
	var complete domain.TimestampComplete
	var logicalStart, wallStart time.Time
	groups, hosts := map[string]struct{}{}, map[string]struct{}{}
	finishGroup := func() error {
		if err := completer.Complete(ctx, complete); err != nil {
			return fmt.Errorf("timestamp completion: %w", err)
		}
		stats.Completions++
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		record, err := reader.Next(ctx)
		if errors.Is(err, io.EOF) {
			if stats.Rows > 0 {
				if err := finishGroup(); err != nil {
					return stats, err
				}
			}
			stats.Groups, stats.Hosts = len(groups), len(hosts)
			return stats, nil
		}
		if err != nil {
			return stats, fmt.Errorf("read record: %w", err)
		}
		if err := record.Validate(); err != nil {
			return stats, err
		}
		if stats.Rows > 0 && compareRecords(previous, record) >= 0 {
			return stats, fmt.Errorf("non-increasing replay key: %s/%s/%s", record.Timestamp, record.GroupID, record.Hostname)
		}
		if stats.Rows == 0 {
			logicalStart, wallStart = record.Timestamp, clock.Now()
		}
		newTimestamp := stats.Rows == 0 || !record.Timestamp.Equal(previous.Timestamp)
		newGroup := newTimestamp || record.GroupID != previous.GroupID
		if newGroup {
			if stats.Rows > 0 {
				if err := finishGroup(); err != nil {
					return stats, err
				}
			}
			complete = domain.TimestampComplete{Timestamp: record.Timestamp, GroupID: record.GroupID}
		}
		if newTimestamp {
			if speed > 0 {
				ns := float64(record.Timestamp.Sub(logicalStart)) / speed
				if ns >= float64(math.MaxInt64) {
					return stats, fmt.Errorf("replay pacing exceeds duration range; use a higher speed")
				}
				target := wallStart.Add(time.Duration(ns))
				if wait := target.Sub(clock.Now()); wait > 0 {
					if err := clock.Sleep(ctx, wait); err != nil {
						return stats, err
					}
				}
			}
			stats.Timestamps++
		}
		if err := sender.Send(ctx, record); err != nil {
			return stats, fmt.Errorf("send record: %w", err)
		}
		stats.Rows++
		complete.Records++
		if record.Valid {
			complete.ValidRecords++
		} else {
			stats.Invalid++
		}
		groups[record.GroupID], hosts[record.Hostname] = struct{}{}, struct{}{}
		previous = record
	}
}

func compareRecords(a, b domain.TelemetryRecord) int {
	if c := a.Timestamp.Compare(b.Timestamp); c != 0 {
		return c
	}
	if a.GroupID < b.GroupID || (a.GroupID == b.GroupID && a.Hostname < b.Hostname) {
		return -1
	}
	if a.GroupID == b.GroupID && a.Hostname == b.Hostname {
		return 0
	}
	return 1
}
