package kafkainput

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

func TestCheckCleanupPolicy(t *testing.T) {
	value := func(s string) *string { return &s }
	tests := []struct {
		name      string
		resources kadm.ResourceConfigs
		wantErr   bool
	}{
		{"delete", kadm.ResourceConfigs{{Name: "t", Configs: []kadm.Config{{Key: "cleanup.policy", Value: value("delete")}}}}, false},
		{"missing resource", nil, true},
		{"missing config", kadm.ResourceConfigs{{Name: "t"}}, true},
		{"unknown", kadm.ResourceConfigs{{Name: "t", Configs: []kadm.Config{{Key: "cleanup.policy"}}}}, true},
		{"compact", kadm.ResourceConfigs{{Name: "t", Configs: []kadm.Config{{Key: "cleanup.policy", Value: value("compact")}}}}, true},
		{"combined", kadm.ResourceConfigs{{Name: "t", Configs: []kadm.Config{{Key: "cleanup.policy", Value: value("delete,compact")}}}}, true},
		{"resource error", kadm.ResourceConfigs{{Name: "t", Err: errors.New("denied")}}, true},
		{"duplicate resource", kadm.ResourceConfigs{{Name: "t"}, {Name: "t"}}, true},
		{"duplicate config", kadm.ResourceConfigs{{Name: "t", Configs: []kadm.Config{{Key: "cleanup.policy", Value: value("delete")}, {Key: "cleanup.policy", Value: value("delete")}}}}, true},
		{"nil then duplicate", kadm.ResourceConfigs{{Name: "t", Configs: []kadm.Config{{Key: "cleanup.policy"}, {Key: "cleanup.policy", Value: value("delete")}}}}, true},
		{"unrecognized", kadm.ResourceConfigs{{Name: "t", Configs: []kadm.Config{{Key: "cleanup.policy", Value: value("unknown")}}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkCleanupPolicy(tt.resources, "t")
			if (err != nil) != tt.wantErr {
				t.Fatalf("checkCleanupPolicy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
