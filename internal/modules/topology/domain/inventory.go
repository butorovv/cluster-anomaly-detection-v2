package domain

import "fmt"

// Group describes a logical sample partition, not a verified physical failure domain.
type Group struct {
	Code  string
	Nodes []Node
}

// CanonicalGroups returns a fresh copy of the expected group and node inventory.
func CanonicalGroups() []Group {
	groups := make([]Group, 16)
	for groupNumber := 1; groupNumber <= 16; groupNumber++ {
		code := fmt.Sprintf("a%02d", groupNumber)
		nodes := make([]Node, 18)
		for nodeIndex := 1; nodeIndex <= 18; nodeIndex++ {
			nodes[nodeIndex-1] = Node{
				Hostname:  fmt.Sprintf("%sn%02d", code, nodeIndex),
				GroupID:   code,
				NodeIndex: nodeIndex,
			}
		}
		groups[groupNumber-1] = Group{Code: code, Nodes: nodes}
	}
	return groups
}

// ValidateInventory requires the complete canonical group and node order.
func ValidateInventory(groups []Group) error {
	want := CanonicalGroups()
	if len(groups) != len(want) {
		return fmt.Errorf("inventory has %d groups; want %d", len(groups), len(want))
	}
	for gi, group := range groups {
		if group.Code != want[gi].Code {
			return fmt.Errorf("group %d has code %q; want %q", gi, group.Code, want[gi].Code)
		}
		if len(group.Nodes) != len(want[gi].Nodes) {
			return fmt.Errorf("group %q has %d nodes; want %d", group.Code, len(group.Nodes), len(want[gi].Nodes))
		}
		for ni, node := range group.Nodes {
			expected := want[gi].Nodes[ni]
			if node.Hostname != expected.Hostname || node.GroupID != expected.GroupID || node.NodeIndex != expected.NodeIndex {
				return fmt.Errorf("group %q node %d is not canonical", group.Code, ni)
			}
		}
	}
	return nil
}

// ValidateSelectedNode verifies that the selected identity is one canonical inventory node.
func ValidateSelectedNode(hostname, groupID string, nodeIndex int) error {
	if nodeIndex < 1 || nodeIndex > 18 {
		return fmt.Errorf("node index %d is outside 1..18", nodeIndex)
	}
	group, index, err := ParseHostname(hostname)
	if err != nil {
		return err
	}
	if len(group) != 3 || group[0] != 'a' || group < "a01" || group > "a16" || hostname != fmt.Sprintf("%sn%02d", group, index) {
		return fmt.Errorf("hostname %q is not a canonical inventory node", hostname)
	}
	if groupID != group || nodeIndex != index {
		return fmt.Errorf("selected node identity does not match hostname %q", hostname)
	}
	return nil
}
