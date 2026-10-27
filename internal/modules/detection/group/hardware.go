package group

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

// Bound is an explicitly approved, sensor-compatible hard limit.
type Bound struct {
	ID                  string   `json:"id"`
	Hostname            string   `json:"hostname"`
	Sensor              string   `json:"sensor"`
	DeviceModel         string   `json:"device_model"`
	Unit                string   `json:"unit"`
	MeasurementBoundary string   `json:"measurement_boundary"`
	Aggregation         string   `json:"aggregation"`
	SourceURL           string   `json:"source_url"`
	Verified            bool     `json:"verified"`
	Compatible          bool     `json:"compatible"`
	Lower               *float64 `json:"lower"`
	Upper               *float64 `json:"upper"`
}
type Hardware struct{ bounds []Bound }
type hardwareFile struct {
	ApprovedSensorBounds []Bound `json:"approved_sensor_bounds"`
}

func LoadHardware(path string) (*Hardware, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f hardwareFile
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(b, &fields); err != nil {
		return nil, err
	}
	if v, ok := fields["approved_sensor_bounds"]; !ok || string(v) == "null" {
		return nil, fmt.Errorf("approved_sensor_bounds array required")
	}
	if err = json.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	return NewHardware(f.ApprovedSensorBounds)
}
func NewHardware(bounds []Bound) (*Hardware, error) {
	seen := map[string]bool{}
	valid := map[string]bool{}
	for _, n := range domain.FeatureNames {
		valid[n] = true
	}
	for i, b := range bounds {
		if _, _, err := topology.ParseHostname(b.Hostname); err != nil {
			return nil, fmt.Errorf("invalid bound hostname")
		}
		if b.ID == "" || seen[b.ID] || b.Hostname == "" || strings.ContainsAny(b.Hostname, "*?[]") || b.Sensor == "" || !valid[b.Sensor] || b.DeviceModel == "" || b.Unit == "" || b.MeasurementBoundary == "" || b.Aggregation == "" || b.SourceURL == "" || !b.Verified || !b.Compatible {
			return nil, fmt.Errorf("invalid approved bound at index %d: provenance or approval", i)
		}
		seen[b.ID] = true
		boundary := ""
		if b.Sensor == "psu0_power" || b.Sensor == "psu1_power" {
			boundary = "AC"
		} else if len(b.Sensor) >= 5 && b.Sensor[:5] == "gpu0_" || len(b.Sensor) >= 5 && b.Sensor[:5] == "gpu1_" || len(b.Sensor) >= 5 && b.Sensor[:5] == "gpu2_" || len(b.Sensor) >= 5 && b.Sensor[:5] == "gpu3_" || len(b.Sensor) >= 5 && b.Sensor[:5] == "gpu4_" || len(b.Sensor) >= 5 && b.Sensor[:5] == "gpu5_" {
			if b.Sensor[len(b.Sensor)-5:] == "power" {
				boundary = "DC"
			} else {
				boundary = "core_temperature"
			}
		} else if b.Sensor == "cpu0_power" || b.Sensor == "cpu1_power" {
			boundary = "DC"
		} else {
			boundary = "mean_core_temperature"
		}
		u := "W"
		if boundary == "core_temperature" || boundary == "mean_core_temperature" {
			u = "degC"
		}
		if b.MeasurementBoundary != boundary || b.Unit != u || b.Aggregation != "10s_mean" {
			return nil, fmt.Errorf("invalid bound %s: sensor semantics mismatch", b.ID)
		}
		if b.Lower == nil && b.Upper == nil {
			return nil, fmt.Errorf("bound %s has no limit", b.ID)
		}
		for _, v := range []*float64{b.Lower, b.Upper} {
			if v != nil && (math.IsNaN(*v) || math.IsInf(*v, 0)) {
				return nil, fmt.Errorf("bound %s has nonfinite limit", b.ID)
			}
		}
		if b.Lower != nil && b.Upper != nil && *b.Lower > *b.Upper {
			return nil, fmt.Errorf("bound %s has inverted limits", b.ID)
		}
	}
	copyBounds := append([]Bound(nil), bounds...)
	for i := range copyBounds {
		if copyBounds[i].Lower != nil {
			v := *copyBounds[i].Lower
			copyBounds[i].Lower = &v
		}
		if copyBounds[i].Upper != nil {
			v := *copyBounds[i].Upper
			copyBounds[i].Upper = &v
		}
	}
	return &Hardware{bounds: copyBounds}, nil
}
func (h *Hardware) Evaluate(hosts []string, records map[string]domain.TelemetryRecord) YResult {
	result := YResult{Criterion: Criterion{State: Unknown}, Hosts: map[string]State{}}
	if h == nil || len(h.bounds) == 0 {
		result.Reason = "no_approved_sensor_bounds"
		for _, host := range hosts {
			result.Hosts[host] = Unknown
		}
		return result
	}
	idx := map[string]int{}
	for i, n := range domain.FeatureNames {
		idx[n] = i
	}
	positives, unknown := 0, 0
	seenHosts := map[string]bool{}
	for _, host := range hosts {
		if seenHosts[host] {
			continue
		}
		seenHosts[host] = true
		applicable, positive := 0, false
		r, ok := records[host]
		if ok && (r.Hostname != host || r.Validate() != nil) {
			ok = false
		}
		if ok {
			features := r.Features()
			for _, b := range h.bounds {
				if b.Hostname != host {
					continue
				}
				applicable++
				ev := DeviceEvidence{Hostname: host, BoundID: b.ID, Sensor: b.Sensor}
				// Evidence belongs to the caller; never expose the registry's pointers.
				if b.Lower != nil {
					ev.Lower = ptr(*b.Lower)
				}
				if b.Upper != nil {
					ev.Upper = ptr(*b.Upper)
				}
				v := features[idx[b.Sensor]]
				if r.AvailabilityMask&(1<<idx[b.Sensor]) == 0 || math.IsNaN(v) || math.IsInf(v, 0) {
					ev.State = Unknown
					ev.Reason = "missing_or_nonfinite_sensor"
				} else {
					ev.Value = &v
					ev.State = Negative
					if b.Lower != nil && v < *b.Lower || b.Upper != nil && v > *b.Upper {
						ev.State = Positive
						positive = true
					}
				}
				result.Evidence = append(result.Evidence, ev)
			}
		}
		switch {
		case positive:
			result.Hosts[host] = Positive
			positives++
		case applicable > 0 && ok:
			all := true
			for _, e := range result.Evidence {
				if e.Hostname == host && e.State != Negative {
					all = false
				}
			}
			if all {
				result.Hosts[host] = Negative
			} else {
				result.Hosts[host] = Unknown
				unknown++
			}
		default:
			result.Hosts[host] = Unknown
			unknown++
		}
	}
	if positives >= 2 {
		result.State = Positive
	} else if positives+unknown < 2 {
		result.State = Negative
	} else {
		result.State = Unknown
	}
	sort.Slice(result.Evidence, func(i, j int) bool {
		a, b := result.Evidence[i], result.Evidence[j]
		if a.Hostname != b.Hostname {
			return a.Hostname < b.Hostname
		}
		return a.BoundID < b.BoundID
	})
	return result
}
