package group

import (
	"math"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

func boundFor(host, sensor, boundary, unit string, lower, upper float64) Bound {
	return Bound{ID: host + "-" + sensor, Hostname: host, Sensor: sensor, DeviceModel: "verified-device", Unit: unit, MeasurementBoundary: boundary, Aggregation: "10s_mean", SourceURL: "https://vendor.example/spec", Verified: true, Compatible: true, Lower: &lower, Upper: &upper}
}

func TestHardwareMissingSensorAndBoundSnapshot(t *testing.T) {
	b := boundFor("a01n01", "gpu0_power", "DC", "W", 0, 100)
	h, err := NewHardware([]Bound{b})
	if err != nil {
		t.Fatal(err)
	}
	*b.Upper = 1000 // approved limits are snapshotted, not mutable by the caller.
	r := recordWithFeature("a01n01", 1, 101)
	y := h.Evaluate([]string{"a01n01", "a01n02"}, map[string]domain.TelemetryRecord{"a01n01": r})
	if y.Hosts["a01n01"] != Positive {
		t.Fatal("caller mutated effective bound")
	}
	duplicate := h.Evaluate([]string{"a01n01", "a01n01"}, map[string]domain.TelemetryRecord{"a01n01": r})
	if duplicate.State == Positive {
		t.Fatal("duplicate hostname counted as distinct hosts")
	}
	r.GPUPower[0] = math.NaN()
	r.AvailabilityMask &^= 1 << 4
	r.Valid = false
	y = h.Evaluate([]string{"a01n01", "a01n02"}, map[string]domain.TelemetryRecord{"a01n01": r})
	if y.Hosts["a01n01"] != Unknown || y.State != Unknown {
		t.Fatalf("missing sensor treated as known: %+v", y)
	}
}

func recordWithFeature(host string, index int, value float64) domain.TelemetryRecord {
	r := domain.TelemetryRecord{Timestamp: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), Hostname: host, GroupID: "a01", NodeIndex: index, Valid: true, AvailabilityMask: domain.FullAvailabilityMask}
	values := [domain.FeatureCount]float64{}
	for i := range values {
		values[i] = 10
	}
	values[4] = value
	r.SetFeatures(values)
	return r
}

func TestHardwareRequiresVerifiedCompatibleSensorMetadata(t *testing.T) {
	b := boundFor("a01n01", "gpu0_power", "DC", "W", 0, 100)
	if _, err := NewHardware([]Bound{b}); err != nil {
		t.Fatalf("valid bound rejected: %v", err)
	}
	bad := b
	bad.Unit = "kW"
	if _, err := NewHardware([]Bound{bad}); err == nil {
		t.Fatal("unit mismatch accepted")
	}
	bad = b
	bad.Verified = false
	if _, err := NewHardware([]Bound{bad}); err == nil {
		t.Fatal("unverified bound accepted")
	}
	bad = b
	bad.Sensor = "gpu0_power"
	bad.Unit = "W"
	bad.MeasurementBoundary = "nominal"
	if _, err := NewHardware([]Bound{bad}); err == nil {
		t.Fatal("nominal boundary accepted")
	}
}

func TestHardwareEqualityAndDistinctHostVoting(t *testing.T) {
	h, err := NewHardware([]Bound{boundFor("a01n01", "gpu0_power", "DC", "W", 0, 100), boundFor("a01n01", "gpu1_power", "DC", "W", 0, 100)})
	if err != nil {
		t.Fatal(err)
	}
	hosts := []string{"a01n01", "a01n02"}
	r := map[string]domain.TelemetryRecord{"a01n01": recordWithFeature("a01n01", 1, 100)}
	y := h.Evaluate(hosts, r)
	if y.State != Negative {
		t.Fatalf("boundary equality should be negative and only one unknown host cannot make two breaches possible: %s", y.State)
	}
	r["a01n01"] = recordWithFeature("a01n01", 1, 101)
	twoDevices := r["a01n01"]
	twoDevices.GPUPower[1] = 101
	r["a01n01"] = twoDevices
	y = h.Evaluate(hosts, r)
	if y.State != Unknown {
		t.Fatalf("two device breaches on one host counted as two hosts: %s", y.State)
	}
	bounds := []Bound{boundFor("a01n01", "gpu0_power", "DC", "W", 0, 100), boundFor("a01n02", "gpu0_power", "DC", "W", 0, 100)}
	h, err = NewHardware(bounds)
	if err != nil {
		t.Fatal(err)
	}
	r["a01n02"] = recordWithFeature("a01n02", 2, 101)
	if got := h.Evaluate(hosts, r); got.State != Positive {
		t.Fatalf("distinct host breaches should vote positive: %s", got.State)
	}
	empty, err := NewHardware(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := empty.Evaluate(hosts, r); got.State != Unknown {
		t.Fatalf("empty bounds must be unknown: %s", got.State)
	}
}

func TestHardwareEvidenceCannotMutateApprovedLimits(t *testing.T) {
	h, err := NewHardware([]Bound{boundFor("a01n01", "gpu0_power", "DC", "W", 0, 100)})
	if err != nil {
		t.Fatal(err)
	}
	records := map[string]domain.TelemetryRecord{"a01n01": recordWithFeature("a01n01", 1, 101)}
	first := h.Evaluate([]string{"a01n01", "a01n02"}, records)
	if len(first.Evidence) != 1 || first.Hosts["a01n01"] != Positive {
		t.Fatal(first)
	}
	*first.Evidence[0].Upper = 1000
	*first.Evidence[0].Lower = -1000
	second := h.Evaluate([]string{"a01n01", "a01n02"}, records)
	if second.Hosts["a01n01"] != Positive || *second.Evidence[0].Upper != 100 || *second.Evidence[0].Lower != 0 {
		t.Fatalf("returned evidence mutated approved limits: %+v", second)
	}
}
