package group_test

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
)

// Freeze the previous calculation as a reference for numeric and wire parity.
func annotateOutsideReference(rows []group.Raw) {
	for i := range rows {
		s := &rows[i].IF
		s.OutsideAvailable, s.OutsideAnomalous = 0, 0
		s.OutsideGroups, s.OutsideS, s.E = nil, nil, nil
		for j := range rows {
			if i != j {
				s.OutsideGroups = append(s.OutsideGroups, rows[j].Group)
				s.OutsideAvailable += rows[j].IF.Available
				s.OutsideAnomalous += rows[j].IF.Anomalous
			}
		}
		if s.OutsideAvailable > 0 {
			outside := float64(s.OutsideAnomalous) / float64(s.OutsideAvailable)
			s.OutsideS = &outside
			if s.S != nil {
				e := *s.S - outside
				s.E = &e
			}
		}
	}
}

func TestAnnotateOutsideReferenceAndEvidenceParity(t *testing.T) {
	for _, n := range []int{0, 1, 2, 16, 64, 200} {
		for _, allMissing := range []bool{false, true} {
			want := outsideRows(n)
			for i := range want {
				if allMissing {
					want[i].IF.Available, want[i].IF.Anomalous, want[i].IF.S = 0, 0, nil
				}
				// Order belongs to the input, including repeated group names.
				if i%3 == 0 {
					want[i].Group = "repeated"
				}
			}
			got := make([]group.Raw, len(want))
			copy(got, want)
			annotateOutsideReference(want)
			group.AnnotateOutside(got)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("annotation differs for n=%d allMissing=%t", n, allMissing)
			}
			for i := range want {
				payload := domain.GroupResult{Raw: got[i], Evidence: &group.RBI2Evidence{Raw: got[i]}}
				reference := domain.GroupResult{Raw: want[i], Evidence: &group.RBI2Evidence{Raw: want[i]}}
				a, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				b, err := json.Marshal(reference)
				if err != nil || string(a) != string(b) {
					t.Fatalf("Raw/Evidence.Raw wire mismatch for n=%d group=%d: %v", n, i, err)
				}
			}
			// Re-annotation must clear any prior diagnostic fields.
			group.AnnotateOutside(got)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("repeated annotation differs for n=%d", n)
			}
		}
	}
}

// Re-annotate already saved golden rows. This reads immutable output files and
// performs no D2 replay, training, SQL or Kafka work.
func TestAnnotateOutsideSavedGolden(t *testing.T) {
	root := os.Getenv("OUTSIDE_GOLDEN_DIR")
	if root == "" {
		t.Skip("OUTSIDE_GOLDEN_DIR required for read-only golden annotation check")
	}
	const rawSHA = "dccdda0fb913e3059839361aec8ff6b2356fa61e7b9973b3c96e2bd420df7027"
	const evidenceSHA = "91403d489576acc01ff7f2308d41b7881f7d3d3279cee408b049a8527280e1ad"
	open := func(name, wantSHA string) *json.Decoder {
		t.Helper()
		f, err := os.Open(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })
		hash := sha256.New()
		if _, err := io.Copy(hash, f); err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(hash.Sum(nil)) != wantSHA {
			t.Fatalf("unrecognized golden SHA for %s", name)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		return json.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	}
	raw, evidence := open("raw.ndjson", rawSHA), open("evidence.ndjson", evidenceSHA)
	var current group.RBI2Evidence
	hasEvidence := false
	readEvidence := func() {
		t.Helper()
		current = group.RBI2Evidence{}
		err := evidence.Decode(&current)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		hasEvidence = err == nil
	}
	readEvidence()
	checkedEvidence := 0
	for tick := range 2880 {
		want := make([]group.Raw, 16)
		for i := range want {
			if err := raw.Decode(&want[i]); err != nil {
				t.Fatal(err)
			}
			if !want[i].Timestamp.Equal(want[0].Timestamp) {
				t.Fatal("golden does not group rows by timestamp")
			}
		}
		got := append([]group.Raw(nil), want...)
		group.AnnotateOutside(got)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("golden Raw mismatch at timestamp %d", tick)
		}
		for _, row := range got {
			if hasEvidence && current.Raw.Timestamp.Equal(row.Timestamp) && current.Raw.Group == row.Group {
				if !reflect.DeepEqual(row, current.Raw) {
					t.Fatalf("golden Evidence.Raw mismatch at %s/%s", row.Timestamp, row.Group)
				}
				checkedEvidence++
				readEvidence()
			}
		}
	}
	var extra group.Raw
	if err := raw.Decode(&extra); err != io.EOF || hasEvidence || checkedEvidence != 1284 {
		t.Fatalf("golden extent mismatch: raw error=%v, extraEvidence=%t, checkedEvidence=%d", err, hasEvidence, checkedEvidence)
	}
	t.Log("46080 Raw rows and 1284 Evidence.Raw entries match the pinned golden")
}
