package group

import (
	"testing"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

// Production identity can be checked without opening absent telemetry/IF bytes.
// This test does not claim that the actual production bundle was executed.
func TestFrozenProductionRBI2Identity(t *testing.T) {
	p, err := LoadProfile("../../../../configs/group-profile.summit-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewRBI2Identity(summitProfileSHA, p.InputSHA, 16, analysis.NIRWindows()[1], 0)
	if err != nil {
		t.Fatal(err)
	}
	if id.ConfigID != "b44f95bee95d00b908b30de77d89d8ae56c2a6a8a40d9d8fc8a508413b17da87" || id.RunID != "845af471d9ec5b8a503f09bb1395486cbe1658d8cb9786d94f1d6b4e6e5688ae" || id.LegacyRunID != summitD2RunID {
		t.Fatal("production identity changed", id)
	}
}
