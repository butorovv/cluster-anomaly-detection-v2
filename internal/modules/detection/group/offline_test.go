package group

import (
	"fmt"
	"testing"
	"time"
)

func TestEntireAbsentTickKeepsStatisticsAndIFUnknown(t *testing.T) {
	p := &Profile{}
	for i := 0; i < 16; i++ {
		g := GroupProfile{Group: fmt.Sprintf("a%02d", i+1)}
		for j := 0; j < 18; j++ {
			g.Hosts = append(g.Hosts, fmt.Sprintf("%sn%02d", g.Group, j+1))
		}
		p.Groups = append(p.Groups, g)
	}
	hw, _ := NewHardware(nil)
	rows, err := Evaluate(p, Tick{Timestamp: time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC)}, hw, nil, 4, &Timing{})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.R.State != Unknown || r.B.State != Unknown || r.Y.State != Unknown || r.IF.S != nil || len(r.Missing) != 18 {
			t.Fatalf("missing tick became normal: %+v", r)
		}
	}
}

func TestAnnotateOutsideUsesAvailableHostWeights(t *testing.T) {
	rows := []Raw{
		{Group: "a", IF: Support{Available: 1, Anomalous: 1, S: ptr(1)}},
		{Group: "b", IF: Support{Available: 9, Anomalous: 0, S: ptr(0)}},
		{Group: "c", IF: Support{Available: 0, Anomalous: 0}},
	}
	AnnotateOutside(rows)
	if rows[0].IF.OutsideAvailable != 9 || rows[0].IF.OutsideAnomalous != 0 || rows[0].IF.OutsideS == nil || *rows[0].IF.OutsideS != 0 || rows[0].IF.E == nil || *rows[0].IF.E != 1 {
		t.Fatalf("weighted outside annotation: %+v", rows[0].IF)
	}
	if rows[2].IF.OutsideAvailable != 10 || rows[2].IF.OutsideS == nil || *rows[2].IF.OutsideS != 0.1 || rows[2].IF.E != nil {
		t.Fatalf("outside score/unknown own score: %+v", rows[2].IF)
	}
	zero := []Raw{{Group: "a", IF: Support{Available: 0}}, {Group: "b", IF: Support{Available: 0}}}
	AnnotateOutside(zero)
	if zero[0].IF.OutsideS != nil || zero[0].IF.E != nil {
		t.Fatal("zero denominator must remain unknown")
	}
}
