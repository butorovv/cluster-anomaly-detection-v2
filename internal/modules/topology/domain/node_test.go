package domain

import "testing"

func TestParseHostnamePreservesV1Syntax(t *testing.T) {
	for _, tc := range []struct {
		host  string
		group string
		index int
	}{
		{"h99n18", "h99", 18},
		{"a1n1", "a01", 1},
	} {
		group, index, err := ParseHostname(tc.host)
		if err != nil || group != tc.group || index != tc.index {
			t.Fatalf("ParseHostname(%q) = (%q, %d, %v), want (%q, %d, nil)", tc.host, group, index, err, tc.group, tc.index)
		}
	}
}

func TestValidateSelectedNode(t *testing.T) {
	valid := []struct {
		host, group string
		index       int
	}{
		{"a01n01", "a01", 1}, {"a16n18", "a16", 18},
	}
	for _, tc := range valid {
		if err := ValidateSelectedNode(tc.host, tc.group, tc.index); err != nil {
			t.Errorf("ValidateSelectedNode(%q, %q, %d): %v", tc.host, tc.group, tc.index, err)
		}
	}
	invalid := []struct {
		host, group string
		index       int
	}{
		{"a1n01", "a01", 1}, {"a17n01", "a17", 1}, {"b01n01", "b01", 1},
		{"a01n00", "a01", 0}, {"a01n01", "a02", 1}, {"a01n02", "a01", 1},
		{"a01n01", "a01", 2},
	}
	for _, tc := range invalid {
		if err := ValidateSelectedNode(tc.host, tc.group, tc.index); err == nil {
			t.Errorf("ValidateSelectedNode(%q, %q, %d) unexpectedly succeeded", tc.host, tc.group, tc.index)
		}
	}
}
