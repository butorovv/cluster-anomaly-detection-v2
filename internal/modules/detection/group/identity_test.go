package group

import (
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

const summitProfileSHA = "a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36"
const summitD2RunID = "70bd4e3d0d926bc49d6d36aacc9c3d66d1ba9a7817d4bb7fa908225aaf63853f"

func TestRunIdentityPreservesSummitGoldenAndEventID(t *testing.T) {
	p, err := LoadProfile("../../../../configs/group-profile.summit-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	if got := mustSHA(t, "../../../../configs/group-profile.summit-v1.json"); got != summitProfileSHA {
		t.Fatal("historical profile bytes changed", got)
	}
	id, err := RunID(summitProfileSHA, p.InputSHA, 16, analysis.NIRWindows()[1])
	if err != nil || id != summitD2RunID {
		t.Fatal("historical run identity changed", id, err)
	}
	// Insertion order does not change Go's canonical map encoding.
	reversed := map[string]string{}
	for key, value := range p.InputSHA {
		reversed[key] = value
	}
	other, _ := RunID(summitProfileSHA, reversed, 16, analysis.NIRWindows()[1])
	if other != id {
		t.Fatal("map iteration changed identity")
	}
	for _, groups := range []int{4, 8} {
		other, err := RunID(summitProfileSHA, p.InputSHA, groups, analysis.NIRWindows()[1])
		if err != nil || other == id {
			t.Fatal("group universe missing from identity", groups, err)
		}
	}
	timestamp := time.Date(2022, 1, 18, 8, 10, 30, 0, time.UTC)
	engine, _ := NewEngine(id, "a14", 0, analysis.NIRWindows()[1].Start)
	raw := temporalRaw(timestamp, Positive, Unknown, Positive)
	raw.Group = "a14"
	if _, _, err := engine.Step(raw); err != nil {
		t.Fatal(err)
	}
	events, err := engine.Flush(timestamp.Add(analysis.Step))
	if err != nil || len(events) != 1 || events[0].ID != "755b81bb8fb3ed30c9a98aa2eeeb7f0ea9bdb1ff1fa984bb2bb1d8af630521ee" {
		t.Fatal("historical event ID changed", events, err)
	}
}

func TestRunIdentityRejectsIncompleteInput(t *testing.T) {
	good := strings.Repeat("a", 64)
	for _, tc := range []struct {
		sha    string
		inputs map[string]string
		groups int
		window analysis.Window
	}{
		{"", map[string]string{"input": good}, 16, analysis.NIRWindows()[1]},
		{good, nil, 16, analysis.NIRWindows()[1]},
		{good, map[string]string{"": good}, 16, analysis.NIRWindows()[1]},
		{good, map[string]string{"input": "wrong"}, 16, analysis.NIRWindows()[1]},
		{good, map[string]string{"input": good}, 1, analysis.NIRWindows()[1]},
		{good, map[string]string{"input": good}, 16, analysis.Window{}},
	} {
		if _, err := RunID(tc.sha, tc.inputs, tc.groups, tc.window); err == nil {
			t.Fatal("incomplete identity accepted")
		}
	}
}
