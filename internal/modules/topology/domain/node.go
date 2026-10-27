package domain

import (
	"fmt"
	"regexp"
	"strconv"
)

type Node struct {
	ID        int
	Hostname  string
	GroupID   string
	NodeIndex int
}

var summitHostname = regexp.MustCompile(`^([a-h])(\d{1,2})n(\d{1,2})$`)

// ParseHostname interprets the hostname encoding, without guessing physical rack bounds.
func ParseHostname(host string) (string, int, error) {
	m := summitHostname.FindStringSubmatch(host)
	if m == nil {
		return "", 0, fmt.Errorf("invalid Summit hostname %q", host)
	}
	row, _ := strconv.Atoi(m[2])
	index, _ := strconv.Atoi(m[3])
	if index < 1 || index > 18 {
		return "", 0, fmt.Errorf("out-of-range Summit hostname %q", host)
	}
	return fmt.Sprintf("%s%02d", m[1], row), index, nil
}
