package domain

import "fmt"

// PartitionForGroup is the v1 bijection, independent of any Kafka client.
func PartitionForGroup(group string) (int, error) {
	if len(group) != 3 || group[0] != 'a' || group[1] < '0' || group[1] > '1' || group[2] < '0' || group[2] > '9' {
		return 0, fmt.Errorf("noncanonical group_id %q", group)
	}
	n := int(group[1]-'0')*10 + int(group[2]-'0')
	if n < 1 || n > 16 {
		return 0, fmt.Errorf("group_id outside a01..a16: %q", group)
	}
	return n - 1, nil
}
