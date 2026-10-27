package group

import "encoding/json"

// MarshalJSON keeps the offline wire representation after AnnotateOutside,
// but does not present uncomputed outside counts as measured zeroes. Until
// OutsideGroups is non-nil, only Available/Anomalous/S describe observations.
func (s Support) MarshalJSON() ([]byte, error) {
	var available, anomalous *int
	if s.OutsideGroups != nil {
		available, anomalous = &s.OutsideAvailable, &s.OutsideAnomalous
	}
	return json.Marshal(struct {
		Available        int      `json:"available"`
		Anomalous        int      `json:"anomalous"`
		S                *float64 `json:"s"`
		OutsideAvailable *int     `json:"outside_available"`
		OutsideAnomalous *int     `json:"outside_anomalous"`
		OutsideGroups    []string `json:"outside_groups"`
		OutsideS         *float64 `json:"outside_s"`
		E                *float64 `json:"e"`
	}{s.Available, s.Anomalous, s.S, available, anomalous, s.OutsideGroups, s.OutsideS, s.E})
}
