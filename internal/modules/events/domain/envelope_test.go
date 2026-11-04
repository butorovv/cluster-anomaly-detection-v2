package domain

import (
	"testing"
)

func TestUniqueJSON(t *testing.T) {
	for _, b := range []string{`{"a":1,"a":2}`, `{"a":{"b":1,"b":2}}`, `{} {}`} {
		if err := UniqueJSON([]byte(b)); err == nil {
			t.Fatal("accepted ambiguity", b)
		}
	}
	if err := UniqueJSON([]byte(`{"a":[1,2,{"b":true}]}`)); err != nil {
		t.Fatal(err)
	}
}
func TestMessageIdentityStable(t *testing.T) {
	a := MessageID("run", "a01", "rbi2_event", "event", 1)
	if a != MessageID("run", "a01", "rbi2_event", "event", 1) || a == MessageID("run", "a01", "rbi2_event", "event", 2) {
		t.Fatal("unstable or unversioned identity")
	}
}
