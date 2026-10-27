package domain

import (
	"math"
	"testing"
	"time"
)

func validRecord() TelemetryRecord {
	r := TelemetryRecord{Timestamp: time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC), Hostname: "a01n01", GroupID: "a01", NodeIndex: 1, Valid: true, AvailabilityMask: FullAvailabilityMask}
	var f [FeatureCount]float64
	for i := range f {
		f[i] = float64(i + 1)
	}
	r.SetFeatures(f)
	return r
}

func TestFeatureOrderAndRoundTrip(t *testing.T) {
	want := [18]string{"psu0_power", "psu1_power", "cpu0_power", "cpu1_power", "gpu0_power", "gpu1_power", "gpu2_power", "gpu3_power", "gpu4_power", "gpu5_power", "cpu0_mean_temp", "cpu1_mean_temp", "gpu0_core_temp", "gpu1_core_temp", "gpu2_core_temp", "gpu3_core_temp", "gpu4_core_temp", "gpu5_core_temp"}
	if FeatureNames != want {
		t.Fatal(FeatureNames)
	}
	r := validRecord()
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	for i, v := range r.Features() {
		if v != float64(i+1) {
			t.Fatalf("coordinate %d = %g", i, v)
		}
	}
	if r.PSUPower != [2]float64{1, 2} || r.CPUPower != [2]float64{3, 4} || r.GPUPower != [6]float64{5, 6, 7, 8, 9, 10} || r.CPUTemp != [2]float64{11, 12} || r.GPUTemp != [6]float64{13, 14, 15, 16, 17, 18} {
		t.Fatal(r)
	}
}

func TestMissingnessAndInvalidStates(t *testing.T) {
	r := validRecord()
	r.PSUPower[0] = math.NaN()
	r.Valid = false
	r.AvailabilityMask &^= 1
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*TelemetryRecord){
		"positive infinity":   func(r *TelemetryRecord) { r.PSUPower[0] = math.Inf(1) },
		"negative infinity":   func(r *TelemetryRecord) { r.PSUPower[0] = math.Inf(-1) },
		"nan with mask":       func(r *TelemetryRecord) { r.PSUPower[0] = math.NaN() },
		"finite without mask": func(r *TelemetryRecord) { r.AvailabilityMask &^= 1; r.Valid = false },
		"valid conflicts":     func(r *TelemetryRecord) { r.Valid = false },
		"high mask bit":       func(r *TelemetryRecord) { r.AvailabilityMask |= 1 << 18 },
		"host":                func(r *TelemetryRecord) { r.Hostname = "invalid" },
		"group":               func(r *TelemetryRecord) { r.GroupID = "a02" },
		"index":               func(r *TelemetryRecord) { r.NodeIndex = 2 },
		"timestamp":           func(r *TelemetryRecord) { r.Timestamp = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			r := validRecord()
			mutate(&r)
			if r.Validate() == nil {
				t.Fatal("invalid record accepted")
			}
		})
	}
	var missing [FeatureCount]float64
	for i := range missing {
		missing[i] = math.NaN()
	}
	r.SetFeatures(missing)
	r.AvailabilityMask = 0
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCompletionCounters(t *testing.T) {
	c := TimestampComplete{Timestamp: validRecord().Timestamp, GroupID: "a01", Records: 2, ValidRecords: 1}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, counts := range [][2]int{{0, 0}, {1, 0}, {18, 18}} {
		c.Records, c.ValidRecords = counts[0], counts[1]
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, counts := range [][2]int{{-1, 0}, {1, -1}, {1, 2}} {
		c.Records, c.ValidRecords = counts[0], counts[1]
		if c.Validate() == nil {
			t.Fatal(counts)
		}
	}
	c.Records, c.ValidRecords = 1, 1
	c.GroupID = "a1"
	if c.Validate() == nil {
		t.Fatal("noncanonical group")
	}
	c.GroupID = "a01"
	c.Timestamp = time.Time{}
	if c.Validate() == nil {
		t.Fatal("zero time")
	}
}
