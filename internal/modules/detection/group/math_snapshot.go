package group

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

const MathSnapshotSchemaVersion = 1

// Checksums use UTF-8 compact encoding/json of the typed DTO with its own
// SHA256="" (omitted by omitempty). Struct field order, sorted map keys,
// time.Time JSON instants/offsets, finite float encodings and nil/empty distinctions
// are retained. Nested checksums stay included. No wall-clock fields are added.
func mathSnapshotHash(value any) (string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func checkMathSnapshotHash(value any, expected string) error {
	if !validSHA(expected) || expected != strings.ToLower(expected) {
		return fmt.Errorf("invalid snapshot SHA256")
	}
	actual, err := mathSnapshotHash(value)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("snapshot SHA256 mismatch")
	}
	return nil
}

func copyMathSnapshot[T any](value T) (T, error) {
	var result T
	b, err := json.Marshal(value)
	if err == nil {
		err = json.Unmarshal(b, &result)
	}
	return result, err
}

// Inspect all object levels, including maps in identities and event arrays.
// DisallowUnknownFields alone would silently accept duplicate keys.
func mathSnapshotTokens(dec *json.Decoder) error {
	token, err := dec.Token()
	if err != nil {
		return err
	}
	switch token {
	case json.Delim('{'):
		seen := map[string]bool{}
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("duplicate or invalid snapshot key %v", key)
			}
			seen[name] = true
			if err := mathSnapshotTokens(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token()
	case json.Delim('['):
		for dec.More() {
			if err := mathSnapshotTokens(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token()
	}
	return err
}

func mathSnapshotFields(data []byte, required, nullable string) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := mathSnapshotTokens(dec); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing snapshot data")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	keys, nulls := strings.Fields(required), strings.Fields(nullable)
	if len(fields) != len(keys) {
		return nil, fmt.Errorf("missing or unknown snapshot fields")
	}
	for key, value := range fields {
		if !slices.Contains(keys, key) {
			return nil, fmt.Errorf("unknown snapshot field %s", key)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) && !slices.Contains(nulls, key) {
			return nil, fmt.Errorf("null snapshot field %s", key)
		}
	}
	return fields, nil
}

func decodeMathSnapshot(data []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func snapshotGrid(t time.Time) bool { return !t.IsZero() && t.Equal(t.Truncate(analysis.Step)) }

func snapshotPosition(split, last, end time.Time) error {
	if !snapshotGrid(split) || !snapshotGrid(end) || end.Before(split) ||
		(!last.IsZero() && (!snapshotGrid(last) || last.Before(split) || !last.Before(end))) {
		return fmt.Errorf("snapshot position outside grid window")
	}
	return nil
}

func validateSnapshotOccurrence(positive, unknown, split, last time.Time, finished bool) error {
	for _, at := range []time.Time{positive, unknown} {
		if !at.IsZero() && (last.IsZero() || finished || !snapshotGrid(at) || at.Before(split) || at.After(last)) {
			return fmt.Errorf("invalid snapshot occurrence time")
		}
	}
	if !positive.IsZero() && !unknown.IsZero() && !positive.After(unknown) {
		return fmt.Errorf("positive precedes unknown barrier")
	}
	return nil
}

func snapshotHostList(hosts []string, group string, sortedUnique bool) error {
	for i, host := range hosts {
		if !slices.Contains(canonicalSnapshotHosts(group), host) || (sortedUnique && i > 0 && hosts[i-1] >= host) {
			return fmt.Errorf("invalid snapshot host list")
		}
	}
	return nil
}

func canonicalSnapshotHosts(group string) []string {
	hosts := make([]string, HostsPerGroup)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("%sn%02d", group, i+1)
	}
	return hosts
}

func snapshotIdentityJSON(data []byte) error {
	f, err := mathSnapshotFields(data, "version config_id run_id legacy_run_id profile_sha256 input_sha256 groups delta_seconds window", "")
	if err != nil {
		return err
	}
	_, err = mathSnapshotFields(f["window"], "start end_exclusive", "")
	return err
}

const legacyEventSnapshotFields = "run_id id group delta_seconds mode criteria_incomplete hardware_confirmation first_evidence_time detected_event_time last_positive_time closed_event_time duration_grid_seconds confirmation_delay_seconds positive_ticks raw_coincident_ticks ttl_extension_ticks right_censored_by_missing right_censored_by_window left_censored_by_window first_raw_coincident_time last_raw_coincident_time raw_coincident_span_seconds if_supported_ticks zero_mad_pair_ticks witness_hosts"
const parentEventSnapshotFields = "version id run_id kernel_config_id group delta_seconds detected last_positive closed first_evidence duration_seconds confirmation_delay_seconds positive_ticks closure_reason right_censored_by_missing right_censored_by_window left_censored_by_window vote_segments rb_subspans new_disjoint_from_rb if_hosts witness_hosts if_simultaneous_max if_score_max r_ratio_max b_ratio_max"

func nullableSnapshotObject(data []byte, required, nullable string) (map[string]json.RawMessage, error) {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil, nil
	}
	return mathSnapshotFields(data, required, nullable)
}
