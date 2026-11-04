// Package experiment provides a bounded streaming comparator against independently
// generated pinned V1 artifacts. It performs no anomaly inference.
package experiment

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/jackc/pgx/v5/pgxpool"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
)

type Comparison struct {
	Status          string            `json:"status"`
	Raw             int               `json:"raw_rows"`
	Temporal        int               `json:"temporal_rows"`
	Evidence        int               `json:"evidence_rows"`
	Events          int               `json:"parent_events"`
	Positive        int               `json:"positive_ticks"`
	I2Positive      int               `json:"i2_positive_ticks"`
	NewEvents       int               `json:"new_events"`
	RBSubspans      int               `json:"rb_subspans"`
	RBParents       int               `json:"rb_supported_parents"`
	CanonicalSHA256 map[string]string `json:"canonical_sha256"`
}

func Compare(ctx context.Context, pool *pgxpool.Pool, execution, reference string) (Comparison, error) {
	report := Comparison{Status: "FAIL", CanonicalSHA256: map[string]string{}}
	var files []*os.File
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()
	open := func(name string) (*json.Decoder, error) {
		f, err := os.Open(filepath.Join(reference, name+".ndjson"))
		if err != nil {
			return nil, err
		}
		files = append(files, f)
		return json.NewDecoder(bufio.NewReaderSize(f, 1<<20)), nil
	}
	raw, err := open("raw")
	if err != nil {
		return report, err
	}
	temporal, err := open("temporal")
	if err != nil {
		return report, err
	}
	evidence, err := open("evidence")
	if err != nil {
		return report, err
	}
	events, err := open("events")
	if err != nil {
		return report, err
	}
	hashes := map[string]hash.Hash{}
	for _, name := range []string{"raw", "temporal", "evidence", "events"} {
		hashes[name] = sha256.New()
	}
	same := func(name, key string, got, want any) error {
		a, err := json.Marshal(got)
		if err != nil {
			return err
		}
		b, err := json.Marshal(want)
		if err != nil {
			return err
		}
		if !bytes.Equal(a, b) {
			return fmt.Errorf("%s mismatch %s at %s", name, key, firstDifference(a, b))
		}
		_, _ = hashes[name].Write(b)
		_, _ = hashes[name].Write([]byte{'\n'})
		return nil
	}
	rows, err := pool.Query(ctx, `SELECT payload FROM cad_v2.master_results WHERE execution_id=$1::uuid ORDER BY tick,group_id`, execution)
	if err != nil {
		return report, err
	}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			rows.Close()
			return report, err
		}
		var got domain.GroupResult
		if err := json.Unmarshal(b, &got); err != nil {
			rows.Close()
			return report, err
		}
		var wantRaw group.Raw
		var wantTemporal group.RBI2TemporalResult
		if err := raw.Decode(&wantRaw); err != nil {
			rows.Close()
			return report, fmt.Errorf("reference raw exhausted: %w", err)
		}
		if err := temporal.Decode(&wantTemporal); err != nil {
			rows.Close()
			return report, err
		}
		key := got.Raw.Group + "/" + got.Raw.Timestamp.String()
		if err := same("raw", key, got.Raw, wantRaw); err != nil {
			rows.Close()
			return report, err
		}
		report.Raw++
		if err := same("temporal", key, got.Temporal, wantTemporal); err != nil {
			rows.Close()
			return report, err
		}
		report.Temporal++
		if got.Temporal.LocalI2.State == group.Positive {
			report.I2Positive++
		}
		if wantTemporal.Vote == group.Positive {
			report.Positive++
			var want group.RBI2Evidence
			if err := evidence.Decode(&want); err != nil {
				rows.Close()
				return report, err
			}
			if got.Evidence == nil {
				rows.Close()
				return report, fmt.Errorf("evidence missing %s", key)
			}
			if err := same("evidence", key, *got.Evidence, want); err != nil {
				rows.Close()
				return report, err
			}
			report.Evidence++
		} else if got.Evidence != nil {
			rows.Close()
			return report, fmt.Errorf("unexpected evidence %s", key)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return report, err
	}
	for name, decoder := range map[string]*json.Decoder{"raw": raw, "temporal": temporal, "evidence": evidence} {
		var extra json.RawMessage
		if err := decoder.Decode(&extra); err != io.EOF {
			return report, fmt.Errorf("unmatched reference %s tail: %v", name, err)
		}
	}
	expected := []group.RBI2Event{}
	for {
		var e group.RBI2Event
		err := events.Decode(&e)
		if err == io.EOF {
			break
		}
		if err != nil {
			return report, err
		}
		expected = append(expected, e)
	}
	sort.Slice(expected, func(i, j int) bool {
		if expected[i].Group != expected[j].Group {
			return expected[i].Group < expected[j].Group
		}
		return expected[i].Detected.Before(expected[j].Detected)
	})
	eventRows, err := pool.Query(ctx, `SELECT payload,revision,terminal FROM cad_v2.master_events WHERE execution_id=$1::uuid ORDER BY group_id,(payload->>'detected')::timestamptz,event_id`, execution)
	if err != nil {
		return report, err
	}
	for eventRows.Next() {
		var b []byte
		var revision int64
		var terminal bool
		if err := eventRows.Scan(&b, &revision, &terminal); err != nil {
			eventRows.Close()
			return report, err
		}
		var got group.RBI2Event
		if err := json.Unmarshal(b, &got); err != nil {
			eventRows.Close()
			return report, err
		}
		if report.Events >= len(expected) || revision != 1 || !terminal {
			eventRows.Close()
			return report, fmt.Errorf("unexpected event/revision %s", got.ID)
		}
		if err := same("events", got.ID, got, expected[report.Events]); err != nil {
			eventRows.Close()
			return report, err
		}
		report.Events++
		report.RBSubspans += len(got.RBSubspans)
		if got.NewDisjointFromRB {
			report.NewEvents++
		} else {
			report.RBParents++
		}
	}
	eventRows.Close()
	if err := eventRows.Err(); err != nil {
		return report, err
	}
	if report.Events != len(expected) {
		return report, fmt.Errorf("missing parent events %d/%d", report.Events, len(expected))
	}
	summaryBytes, err := os.ReadFile(filepath.Join(reference, "summary.json"))
	if err != nil {
		return report, err
	}
	var summary group.RBI2RunSummary
	if err := json.Unmarshal(summaryBytes, &summary); err != nil {
		return report, err
	}
	var identityBytes []byte
	if err := pool.QueryRow(ctx, `SELECT r.identity_json FROM cad_v2.runs r JOIN cad_v2.executions x USING(run_id) WHERE x.execution_id=$1::uuid`, execution).Scan(&identityBytes); err != nil {
		return report, err
	}
	var identity group.RBI2Identity
	if err := json.Unmarshal(identityBytes, &identity); err != nil {
		return report, err
	}
	a, _ := json.Marshal(identity)
	b, _ := json.Marshal(summary.Identity)
	if !bytes.Equal(a, b) {
		return report, fmt.Errorf("scientific identity mismatch at %s", firstDifference(a, b))
	}
	if report.Raw != summary.Ticks || report.Positive != summary.PositiveTicks || report.Events != summary.Events || report.NewEvents != summary.NewEvents || report.RBSubspans != summary.RBIntervals || report.RBParents != summary.RBSupportedEvents {
		return report, fmt.Errorf("independent reference summary mismatch: %+v", report)
	}
	for name, h := range hashes {
		report.CanonicalSHA256[name] = hex.EncodeToString(h.Sum(nil))
	}
	report.Status = "PASS"
	return report, nil
}
func firstDifference(a, b []byte) string {
	var x, y any
	_ = json.Unmarshal(a, &x)
	_ = json.Unmarshal(b, &y)
	var diff func(any, any, string) string
	diff = func(a, b any, path string) string {
		switch v := a.(type) {
		case map[string]any:
			w, ok := b.(map[string]any)
			if !ok {
				return path
			}
			keys := []string{}
			for k := range v {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				d := diff(v[k], w[k], path+"."+k)
				if d != "" {
					return d
				}
			}
			if len(v) != len(w) {
				return path + ".keys"
			}
			return ""
		case []any:
			w, ok := b.([]any)
			if !ok || len(v) != len(w) {
				return path + ".length"
			}
			for i := range v {
				if d := diff(v[i], w[i], fmt.Sprintf("%s[%d]", path, i)); d != "" {
					return d
				}
			}
			return ""
		default:
			u, _ := json.Marshal(a)
			encodedB, _ := json.Marshal(b)
			if !bytes.Equal(u, encodedB) {
				return fmt.Sprintf("%s (%s != %s)", path, u, encodedB)
			}
			return ""
		}
	}
	return diff(x, y, "$")
}
