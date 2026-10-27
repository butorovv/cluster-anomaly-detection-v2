package usecase

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

func assemblerFixture(t *testing.T, ticks int) (*TickAssembler, domain.InputMessage) {
	t.Helper()
	at := time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)
	binding := domain.InputBinding{ExecutionID: "exec", InputSHA256: strings.Repeat("a", 64), ProfileSHA256: strings.Repeat("b", 64)}
	a, err := NewTickAssembler("a01", binding, domain.InputWindow{Start: at, End: at.Add(time.Duration(ticks) * 10 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	r := validGridRecord(at, "a01n01", 1)
	return a, domain.InputMessage{Version: domain.InputVersion, Type: domain.RecordMessage, Binding: binding, GroupID: "a01", Timestamp: at, Record: &r}
}
func marker(m domain.InputMessage, records, valid int) domain.InputMessage {
	m.Type = domain.CompleteMessage
	m.Record = nil
	m.Completion = &domain.TimestampComplete{Timestamp: m.Timestamp, GroupID: m.GroupID, Records: records, ValidRecords: valid}
	return m
}
func mustAccept(t *testing.T, a *TickAssembler, m domain.InputMessage) *domain.GroupTick {
	t.Helper()
	v, err := a.Accept(m)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func requireInputError(t *testing.T, err error, fatal bool) {
	t.Helper()
	var e *InputError
	if !errors.As(err, &e) || e.Fatal != fatal {
		t.Fatalf("want fatal=%t, got %v", fatal, err)
	}
}

func TestAssemblerMarkerRequiredFullPartialEmptyAndOwnership(t *testing.T) {
	a, m := assemblerFixture(t, 3)
	for h := 1; h <= 18; h++ {
		r := validGridRecord(m.Timestamp, fmt.Sprintf("a01n%02d", h), h)
		m.Record = &r
		if got := mustAccept(t, a, m); got != nil {
			t.Fatal("tick before marker")
		}
		r.PSUPower[0] = 999
	}
	full := mustAccept(t, a, marker(m, 18, 18))
	if len(full.Records) != 18 || len(full.Missing) != 0 || full.ValidRecords != 18 || full.Records["a01n01"].PSUPower[0] != 0 {
		t.Fatal(full)
	}
	full.Records["a01n01"] = domain.TelemetryRecord{}
	delete(full.Records, "a01n02")
	m.Timestamp = m.Timestamp.Add(10 * time.Second)
	r := validGridRecord(m.Timestamp, "a01n01", 1)
	f := r.Features()
	f[0] = math.NaN()
	r.SetFeatures(f)
	r.Valid = false
	r.AvailabilityMask &^= 1
	m.Record = &r
	mustAccept(t, a, m)
	r.PSUPower[1] = 999
	partial := mustAccept(t, a, marker(m, 1, 0))
	if len(partial.Records) != 1 || len(partial.Missing) != 17 || partial.ValidRecords != 0 || !math.IsNaN(partial.Records["a01n01"].PSUPower[0]) || partial.Records["a01n01"].PSUPower[1] == 999 {
		t.Fatal(partial)
	}
	m.Timestamp = m.Timestamp.Add(10 * time.Second)
	empty := mustAccept(t, a, marker(m, 0, 0))
	if len(empty.Records) != 0 || len(empty.Missing) != 18 {
		t.Fatal(empty)
	}
	if err := a.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := a.Finish(); err != nil {
		t.Fatal(err)
	}
	_, err := a.Accept(marker(m, 0, 0))
	requireInputError(t, err, true)
}

func TestAssemblerRecoverableRejectionAtomic(t *testing.T) {
	a, m := assemblerFixture(t, 1)
	bad := *m.Record
	bad.AvailabilityMask = 0
	m.Record = &bad
	_, err := a.Accept(m)
	requireInputError(t, err, false)
	r := validGridRecord(m.Timestamp, "a01n01", 1)
	m.Record = &r
	mustAccept(t, a, m)
	_, err = a.Accept(marker(m, 0, 0))
	requireInputError(t, err, false)
	_, err = a.Accept(marker(m, 1, 0))
	requireInputError(t, err, false)
	got := mustAccept(t, a, marker(m, 1, 1))
	if len(got.Records) != 1 || got.ValidRecords != 1 {
		t.Fatal("rejection mutated accepted records", got)
	}
	if err := a.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestAssemblerAllMissingRecordIsPresent(t *testing.T) {
	a, m := assemblerFixture(t, 1)
	var f [domain.FeatureCount]float64
	for i := range f {
		f[i] = math.NaN()
	}
	m.Record.SetFeatures(f)
	m.Record.Valid = false
	m.Record.AvailabilityMask = 0
	mustAccept(t, a, m)
	got := mustAccept(t, a, marker(m, 1, 0))
	if len(got.Records) != 1 || len(got.Missing) != 17 {
		t.Fatal(got)
	}
	if _, ok := got.Records["a01n01"]; !ok {
		t.Fatal("all-missing row discarded")
	}
}

func TestAssemblerConflictingInvalidDuplicateIsFatalAndAtomic(t *testing.T) {
	a, m := assemblerFixture(t, 1)
	mustAccept(t, a, m)
	m.Record.AvailabilityMask = 0
	_, err := a.Accept(m)
	requireInputError(t, err, true)
	if err.(*InputError).Code != "conflicting_duplicate_host" || len(a.records) != 1 || a.records["a01n01"].AvailabilityMask != domain.FullAvailabilityMask {
		t.Fatal("accepted state changed", err, a.records)
	}
}

func TestAssemblerFatalStreamPolicies(t *testing.T) {
	for name, mutate := range map[string]func(*domain.InputMessage){
		"binding":         func(m *domain.InputMessage) { m.Binding.ExecutionID = "other" },
		"source":          func(m *domain.InputMessage) { m.Binding.InputSHA256 = strings.Repeat("c", 64) },
		"profile":         func(m *domain.InputMessage) { m.Binding.ProfileSHA256 = strings.Repeat("c", 64) },
		"group":           func(m *domain.InputMessage) { m.GroupID = "a02" },
		"version":         func(m *domain.InputMessage) { m.Version = "future" },
		"type":            func(m *domain.InputMessage) { m.Type = "other" },
		"grid":            func(m *domain.InputMessage) { m.Timestamp = m.Timestamp.Add(time.Second) },
		"future":          func(m *domain.InputMessage) { m.Timestamp = m.Timestamp.Add(10 * time.Second) },
		"outside":         func(m *domain.InputMessage) { m.Timestamp = m.Timestamp.Add(-10 * time.Second) },
		"payloadidentity": func(m *domain.InputMessage) { m.Record.GroupID = "a02" },
	} {
		t.Run(name, func(t *testing.T) {
			a, m := assemblerFixture(t, 2)
			good := marker(m, 0, 0)
			mutate(&m)
			_, err := a.Accept(m)
			requireInputError(t, err, true)
			_, again := a.Accept(good)
			if again != err {
				t.Fatal("fatal stream continued", again)
			}
			if a.Finish() != err {
				t.Fatal("EOF erased fatal")
			}
		})
	}
	for _, conflict := range []bool{false, true} {
		a, m := assemblerFixture(t, 1)
		mustAccept(t, a, m)
		if conflict {
			m.Record.PSUPower[0] = 123
		}
		_, err := a.Accept(m)
		requireInputError(t, err, true)
		code := err.(*InputError).Code
		if conflict && code != "conflicting_duplicate_host" || !conflict && code != "exact_duplicate_host" {
			t.Fatal(code)
		}
	}
	a, m := assemblerFixture(t, 1)
	m.Record.Hostname = "a01n02"
	m.Record.NodeIndex = 2
	mustAccept(t, a, m)
	m.Record.Hostname = "a01n01"
	m.Record.NodeIndex = 1
	_, err := a.Accept(m)
	requireInputError(t, err, true)
	for _, kind := range []string{"record", "marker"} {
		a, m := assemblerFixture(t, 2)
		mustAccept(t, a, marker(m, 0, 0))
		if kind == "marker" {
			m = marker(m, 0, 0)
		}
		_, err := a.Accept(m)
		requireInputError(t, err, true)
	}
}

func TestAssemblerEOFMissingTailAndConstruction(t *testing.T) {
	for _, rows := range []bool{false, true} {
		a, m := assemblerFixture(t, 2)
		if rows {
			mustAccept(t, a, m)
		}
		requireInputError(t, a.Finish(), true)
	}
	a, m := assemblerFixture(t, 2)
	mustAccept(t, a, marker(m, 0, 0))
	requireInputError(t, a.Finish(), true)
	_, m = assemblerFixture(t, 1)
	for _, group := range []string{"a00", "a1", "a17"} {
		if _, err := NewTickAssembler(group, m.Binding, domain.InputWindow{Start: m.Timestamp, End: m.Timestamp.Add(10 * time.Second)}); err == nil {
			t.Fatal(group)
		}
	}
	if _, err := NewTickAssembler("a01", m.Binding, domain.InputWindow{Start: m.Timestamp, End: m.Timestamp.Add(time.Second)}); err == nil {
		t.Fatal("offgrid window")
	}
}
