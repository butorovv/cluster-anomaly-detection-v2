package domain

import (
	"fmt"
	"testing"
)

func TestCanonicalGroupsAndInventoryValidation(t *testing.T) {
	groups := CanonicalGroups()
	if len(groups) != 16 || len(groups[0].Nodes) != 18 || groups[0].Nodes[0].Hostname != "a01n01" || groups[15].Nodes[17].Hostname != "a16n18" {
		t.Fatal("CanonicalGroups returned an unexpected inventory")
	}
	if err := ValidateInventory(groups); err != nil {
		t.Fatalf("ValidateInventory(canonical): %v", err)
	}
	seen := map[string]bool{}
	for gi, group := range groups {
		if group.Code != fmt.Sprintf("a%02d", gi+1) || len(group.Nodes) != 18 {
			t.Fatal(group)
		}
		for ni, node := range group.Nodes {
			if seen[node.Hostname] || node.Hostname != fmt.Sprintf("a%02dn%02d", gi+1, ni+1) {
				t.Fatal(node)
			}
			seen[node.Hostname] = true
		}
	}
	if len(seen) != 288 {
		t.Fatal(len(seen))
	}
}

func TestValidateInventoryRejectsDuplicateMissingAndReorderedNodes(t *testing.T) {
	cases := map[string]func([]Group){
		"duplicate and omission": func(groups []Group) { groups[0].Nodes[1] = groups[0].Nodes[0] },
		"node reorder":           func(groups []Group) { groups[0].Nodes[0], groups[0].Nodes[1] = groups[0].Nodes[1], groups[0].Nodes[0] },
		"group reorder":          func(groups []Group) { groups[0], groups[1] = groups[1], groups[0] },
		"group duplicate":        func(groups []Group) { groups[1] = groups[0] },
		"missing node":           func(groups []Group) { groups[0].Nodes = groups[0].Nodes[:17] },
		"unknown node":           func(groups []Group) { groups[0].Nodes[0].Hostname = "a17n01" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			groups := CanonicalGroups()
			mutate(groups)
			if err := ValidateInventory(groups); err == nil {
				t.Fatal("ValidateInventory unexpectedly succeeded")
			}
		})
	}
}

func TestCanonicalGroupsReturnsIndependentSlices(t *testing.T) {
	first := CanonicalGroups()
	first[0].Code = "changed"
	first[0].Nodes[0].Hostname = "changed"
	second := CanonicalGroups()
	if second[0].Code != "a01" || second[0].Nodes[0].Hostname != "a01n01" {
		t.Fatal("mutating one inventory changed a later CanonicalGroups result")
	}
}
