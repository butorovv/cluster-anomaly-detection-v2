// Package analysis implements the NIR mathematical model independently of I/O.
package analysis

import "time"

const Step = 10 * time.Second

type Window struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end_exclusive"`
}

func (w Window) Contains(t time.Time) bool { return !t.Before(w.Start) && t.Before(w.End) }
func (w Window) Valid() bool               { return !w.Start.IsZero() && w.End.After(w.Start) }

// NIRWindows replaces the obsolete three-calendar-day experimental split.
func NIRWindows() [3]Window {
	t := time.Date(2022, 1, 18, 0, 0, 0, 0, time.UTC)
	return [3]Window{{t, t.Add(8 * time.Hour)}, {t.Add(8 * time.Hour), t.Add(16 * time.Hour)}, {t.Add(16 * time.Hour), t.Add(24 * time.Hour)}}
}
