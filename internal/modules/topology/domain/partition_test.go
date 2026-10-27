package domain

import (
	"fmt"
	"testing"
)

func TestPartitionForGroupIsCanonicalBijection(t *testing.T) {
	seen := map[int]string{}
	for i := 1; i <= 16; i++ {
		group := fmt.Sprintf("a%02d", i)
		got, err := PartitionForGroup(group)
		if err != nil || got != i-1 {
			t.Fatalf("%s: partition=%d err=%v", group, got, err)
		}
		if prior, ok := seen[got]; ok {
			t.Fatalf("%s and %s map to %d", prior, group, got)
		}
		seen[got] = group
	}
	for _, bad := range []string{"", "a00", "a17", "a1", "a001", "A01", "a-1", "a 1"} {
		if _, err := PartitionForGroup(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}
