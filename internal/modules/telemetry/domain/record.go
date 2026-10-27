package domain

import (
	"fmt"
	"math"
	"time"

	topology "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/topology/domain"
)

const FeatureCount = 18
const FullAvailabilityMask uint32 = (1 << FeatureCount) - 1

var FeatureNames = [FeatureCount]string{
	"psu0_power", "psu1_power", "cpu0_power", "cpu1_power",
	"gpu0_power", "gpu1_power", "gpu2_power", "gpu3_power", "gpu4_power", "gpu5_power",
	"cpu0_mean_temp", "cpu1_mean_temp",
	"gpu0_core_temp", "gpu1_core_temp", "gpu2_core_temp", "gpu3_core_temp", "gpu4_core_temp", "gpu5_core_temp",
}

// TelemetryRecord preserves logical time and missingness. Missing coordinates are NaN.
type TelemetryRecord struct {
	Timestamp        time.Time
	Hostname         string
	GroupID          string
	NodeIndex        int
	PSUPower         [2]float64
	CPUPower         [2]float64
	GPUPower         [6]float64
	CPUTemp          [2]float64
	GPUTemp          [6]float64
	Valid            bool
	AvailabilityMask uint32
}

func (r TelemetryRecord) Features() [FeatureCount]float64 {
	return [FeatureCount]float64{
		r.PSUPower[0], r.PSUPower[1], r.CPUPower[0], r.CPUPower[1],
		r.GPUPower[0], r.GPUPower[1], r.GPUPower[2], r.GPUPower[3], r.GPUPower[4], r.GPUPower[5],
		r.CPUTemp[0], r.CPUTemp[1],
		r.GPUTemp[0], r.GPUTemp[1], r.GPUTemp[2], r.GPUTemp[3], r.GPUTemp[4], r.GPUTemp[5],
	}
}

func (r *TelemetryRecord) SetFeatures(f [FeatureCount]float64) {
	copy(r.PSUPower[:], f[0:2])
	copy(r.CPUPower[:], f[2:4])
	copy(r.GPUPower[:], f[4:10])
	copy(r.CPUTemp[:], f[10:12])
	copy(r.GPUTemp[:], f[12:18])
}

func (r TelemetryRecord) Validate() error {
	group, index, err := topology.ParseHostname(r.Hostname)
	if err != nil {
		return err
	}
	if r.Timestamp.IsZero() || group != r.GroupID || index != r.NodeIndex {
		return fmt.Errorf("inconsistent telemetry identity: %s/%s", r.Hostname, r.Timestamp)
	}
	var mask uint32
	for i, value := range r.Features() {
		if !math.IsNaN(value) && !math.IsInf(value, 0) {
			mask |= 1 << i
		} else if !math.IsNaN(value) {
			return fmt.Errorf("infinite coordinate %s", FeatureNames[i])
		}
	}
	if r.AvailabilityMask != mask || r.Valid != (mask == FullAvailabilityMask) {
		return fmt.Errorf("inconsistent F18 availability for %s", r.Hostname)
	}
	return nil
}
