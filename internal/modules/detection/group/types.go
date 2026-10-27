// Package group implements the frozen research-only Summit group contract.
package group

import (
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/local"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
)

const Version = "summit-group-research-v1"
const HostsPerGroup = 18
const Channels = 5
const Dimensions = 10
const Floor = 1e-4
const RCond = 1e-12

type State string

const (
	Unknown  State = "unknown"
	Negative State = "negative"
	Positive State = "positive"
)

type HostVector [Channels]float64
type Vector [Dimensions]float64
type Matrix [Dimensions][Dimensions]float64
type Witness struct {
	Hostname  string  `json:"hostname"`
	Channel   int     `json:"channel"`
	Direction int     `json:"direction"`
	Value     float64 `json:"value"`
	Median    float64 `json:"median"`
	Scale     float64 `json:"scale"`
	Z         float64 `json:"z"`
	Amplitude float64 `json:"amplitude"`
	ZeroMAD   bool    `json:"zero_mad"`
}
type Criterion struct {
	State     State    `json:"state"`
	Reason    string   `json:"reason,omitempty"`
	Score     *float64 `json:"score"`
	Threshold *float64 `json:"threshold"`
}
type RResult struct {
	Criterion
	Channel      int       `json:"channel"`
	Direction    int       `json:"direction"`
	Pair         []Witness `json:"defining_pair"`
	Witnesses    []Witness `json:"witnesses"`
	ZeroMADAbove []Witness `json:"zero_mad_above_threshold"`
}
type DeviceEvidence struct {
	Hostname string   `json:"hostname"`
	BoundID  string   `json:"bound_id"`
	Sensor   string   `json:"sensor"`
	State    State    `json:"state"`
	Value    *float64 `json:"value"`
	Lower    *float64 `json:"lower"`
	Upper    *float64 `json:"upper"`
	Reason   string   `json:"reason,omitempty"`
}
type YResult struct {
	Criterion
	Hosts    map[string]State `json:"hosts"`
	Evidence []DeviceEvidence `json:"device_evidence"`
}
type Support struct {
	Available        int      `json:"available"`
	Anomalous        int      `json:"anomalous"`
	S                *float64 `json:"s"`
	OutsideAvailable int      `json:"outside_available"`
	OutsideAnomalous int      `json:"outside_anomalous"`
	OutsideGroups    []string `json:"outside_groups"`
	OutsideS         *float64 `json:"outside_s"`
	E                *float64 `json:"e"`
}
type Raw struct {
	Timestamp  time.Time             `json:"timestamp"`
	Group      string                `json:"group"`
	Complete   int                   `json:"complete_hosts"`
	Missing    []string              `json:"missing_hosts"`
	Vector     *Vector               `json:"vector"`
	HostValues map[string]HostVector `json:"host_values"`
	R          RResult               `json:"r"`
	B          Criterion             `json:"b"`
	Y          YResult               `json:"y"`
	IF         Support               `json:"if"`
	Local      []local.LocalResult   `json:"local"`
}

// Tick is the fixed-inventory input at one event-time tick, including absences.
type Tick struct {
	Timestamp time.Time
	Records   map[string]domain.TelemetryRecord
}
type Occurrence struct {
	State   State      `json:"state"`
	Witness *time.Time `json:"witness_time"`
}
type TemporalResult struct {
	Timestamp            time.Time  `json:"timestamp"`
	Group                string     `json:"group"`
	DeltaSeconds         int        `json:"delta_seconds"`
	R                    Occurrence `json:"r"`
	Y                    Occurrence `json:"y"`
	B                    Occurrence `json:"b"`
	Candidate            State      `json:"candidate"`
	Vote                 State      `json:"vote2"`
	Mode                 string     `json:"mode"`
	CriteriaIncomplete   bool       `json:"criteria_incomplete"`
	HardwareConfirmation string     `json:"hardware_confirmation"`
	FirstEvidence        *time.Time `json:"first_evidence_time"`
}
type Event struct {
	RunID                string     `json:"run_id"`
	ID                   string     `json:"id"`
	Group                string     `json:"group"`
	DeltaSeconds         int        `json:"delta_seconds"`
	Mode                 string     `json:"mode"`
	CriteriaIncomplete   bool       `json:"criteria_incomplete"`
	HardwareConfirmation string     `json:"hardware_confirmation"`
	FirstEvidence        time.Time  `json:"first_evidence_time"`
	Detected             time.Time  `json:"detected_event_time"`
	LastPositive         time.Time  `json:"last_positive_time"`
	Closed               time.Time  `json:"closed_event_time"`
	DurationSeconds      int        `json:"duration_grid_seconds"`
	DelaySeconds         int        `json:"confirmation_delay_seconds"`
	PositiveTicks        int        `json:"positive_ticks"`
	RawCoincidentTicks   int        `json:"raw_coincident_ticks"`
	TTLExtensionTicks    int        `json:"ttl_extension_ticks"`
	CensoredMissing      bool       `json:"right_censored_by_missing"`
	CensoredWindow       bool       `json:"right_censored_by_window"`
	LeftCensoredWindow   bool       `json:"left_censored_by_window"`
	RawFirst             *time.Time `json:"first_raw_coincident_time"`
	RawLast              *time.Time `json:"last_raw_coincident_time"`
	RawSpanSeconds       int        `json:"raw_coincident_span_seconds"`
	IFSupportedTicks     int        `json:"if_supported_ticks"`
	ZeroMADPairTicks     int        `json:"zero_mad_pair_ticks"`
	WitnessHosts         []string   `json:"witness_hosts"`
}

func ptr(x float64) *float64 { return &x }
