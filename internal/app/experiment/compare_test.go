package experiment

import (
	"strings"
	"testing"
)

func TestComparatorReportsExactScientificPath(t *testing.T) {
	a := []byte(`{"raw":{"if":{"outside_s":0.25}},"temporal":{"vote":"Positive"}}`)
	b := []byte(`{"raw":{"if":{"outside_s":0.5}},"temporal":{"vote":"Positive"}}`)
	if d := firstDifference(a, b); !strings.HasPrefix(d, "$.raw.if.outside_s") {
		t.Fatal("wrong mismatch attribution", d)
	}
}
