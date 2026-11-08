package resulttransport

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

type producerAdminStub struct {
	metadata                   kadm.Metadata
	configs                    kadm.ResourceConfigs
	metadataErr, configsErr    error
	metadataCalls, configCalls int
}

func (a *producerAdminStub) Metadata(_ context.Context, topics ...string) (kadm.Metadata, error) {
	a.metadataCalls++
	if len(topics) != 1 || topics[0] != ResultsTopic {
		panic("expected one explicit result topic")
	}
	return a.metadata, a.metadataErr
}
func (a *producerAdminStub) DescribeTopicConfigs(_ context.Context, topics ...string) (kadm.ResourceConfigs, error) {
	a.configCalls++
	if len(topics) != 1 || topics[0] != ResultsTopic {
		panic("expected one explicit result topic")
	}
	return a.configs, a.configsErr
}
func validProducerAdmin() *producerAdminStub {
	partitions := kadm.PartitionDetails{}
	for p := int32(0); p < resultPartitions; p++ {
		partitions[p] = kadm.PartitionDetail{Leader: 1}
	}
	policy := "delete"
	maxBytes := "4195328"
	return &producerAdminStub{
		metadata: kadm.Metadata{Cluster: "cluster", Topics: kadm.TopicDetails{ResultsTopic: {ID: kadm.TopicID{1}, Partitions: partitions}}},
		configs:  kadm.ResourceConfigs{{Name: ResultsTopic, Configs: []kadm.Config{{Key: "cleanup.policy", Value: &policy}, {Key: "max.message.bytes", Value: &maxBytes}}}},
	}
}
func TestProducerSourceFreshMetadataAndPolicy(t *testing.T) {
	a := validProducerAdmin()
	for i := 0; i < 2; i++ {
		cluster, topic, err := describeProducerSource(context.Background(), a, ResultsTopic)
		if err != nil || cluster != "cluster" || topic != (kadm.TopicID{1}).String() {
			t.Fatalf("identity %q/%q: %v", cluster, topic, err)
		}
	}
	if a.metadataCalls != 2 || a.configCalls != 2 {
		t.Fatal("checks must be fresh and use exactly two RPCs", a.metadataCalls, a.configCalls)
	}
}
func TestProducerSourceFailClosed(t *testing.T) {
	rpcErr := errors.New("RPC unavailable")
	for _, tc := range []struct {
		name    string
		edit    func(*producerAdminStub)
		want    string
		configs int
	}{
		{"metadata RPC", func(a *producerAdminStub) { a.metadataErr = rpcErr }, "RPC unavailable", 0},
		{"empty cluster", func(a *producerAdminStub) { a.metadata.Cluster = "" }, "cluster ID required", 0},
		{"missing topics", func(a *producerAdminStub) { a.metadata.Topics = nil }, "unavailable", 0},
		{"missing topic", func(a *producerAdminStub) { delete(a.metadata.Topics, ResultsTopic) }, "unavailable", 0},
		{"topic error", func(a *producerAdminStub) {
			v := a.metadata.Topics[ResultsTopic]
			v.Err = kerr.UnknownTopicOrPartition
			a.metadata.Topics[ResultsTopic] = v
		}, "unavailable", 0},
		{"missing ID", func(a *producerAdminStub) {
			v := a.metadata.Topics[ResultsTopic]
			v.ID = kadm.TopicID{}
			a.metadata.Topics[ResultsTopic] = v
		}, "topic ID required", 0},
		{"missing partitions", func(a *producerAdminStub) {
			v := a.metadata.Topics[ResultsTopic]
			v.Partitions = nil
			a.metadata.Topics[ResultsTopic] = v
		}, "require 16 partitions", 0},
		{"15 partitions", func(a *producerAdminStub) { delete(a.metadata.Topics[ResultsTopic].Partitions, 15) }, "require 16 partitions", 0},
		{"17 partitions", func(a *producerAdminStub) {
			a.metadata.Topics[ResultsTopic].Partitions[16] = kadm.PartitionDetail{Leader: 1}
		}, "require 16 partitions", 0},
		{"wrong partition IDs", func(a *producerAdminStub) {
			p := a.metadata.Topics[ResultsTopic].Partitions
			delete(p, 15)
			p[16] = kadm.PartitionDetail{Leader: 1}
		}, "partition 15 unavailable", 0},
		{"missing leader", func(a *producerAdminStub) {
			a.metadata.Topics[ResultsTopic].Partitions[3] = kadm.PartitionDetail{Leader: -1}
		}, "partition 3 unavailable", 0},
		{"partition error", func(a *producerAdminStub) {
			a.metadata.Topics[ResultsTopic].Partitions[4] = kadm.PartitionDetail{Leader: 1, Err: kerr.LeaderNotAvailable}
		}, "partition 4 unavailable", 0},
		{"configs RPC", func(a *producerAdminStub) { a.configsErr = rpcErr }, "RPC unavailable", 1},
		{"missing resource", func(a *producerAdminStub) { a.configs = nil }, "exactly one resource", 1},
		{"wrong resource", func(a *producerAdminStub) { a.configs[0].Name = "other" }, "exactly one resource", 1},
		{"duplicate resource", func(a *producerAdminStub) { a.configs = append(a.configs, a.configs[0]) }, "exactly one resource", 1},
		{"resource error", func(a *producerAdminStub) { a.configs[0].Err = kerr.TopicAuthorizationFailed }, "configuration unavailable", 1},
		{"missing policy", func(a *producerAdminStub) { a.configs[0].Configs = nil }, "cleanup.policy unavailable", 1},
		{"nil policy", func(a *producerAdminStub) { a.configs[0].Configs[0].Value = nil }, "cleanup.policy unavailable", 1},
		{"duplicate policy", func(a *producerAdminStub) {
			a.configs[0].Configs = append(a.configs[0].Configs, a.configs[0].Configs[0])
		}, "duplicate cleanup.policy", 1},
		{"nil then duplicate policy", func(a *producerAdminStub) {
			c := a.configs[0].Configs[0]
			a.configs[0].Configs[0].Value = nil
			a.configs[0].Configs = append(a.configs[0].Configs, c)
		}, "duplicate cleanup.policy", 1},
		{"compact policy", func(a *producerAdminStub) { s := "compact"; a.configs[0].Configs[0].Value = &s }, "unsafe cleanup.policy", 1},
		{"combined policy", func(a *producerAdminStub) { s := "delete,compact"; a.configs[0].Configs[0].Value = &s }, "unsafe cleanup.policy", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := validProducerAdmin()
			tc.edit(a)
			cluster, topic, err := describeProducerSource(context.Background(), a, ResultsTopic)
			if err == nil || !strings.Contains(err.Error(), tc.want) || cluster != "" || topic != "" {
				t.Fatalf("fail-closed: %q/%q %v", cluster, topic, err)
			}
			if a.metadataCalls != 1 || a.configCalls != tc.configs {
				t.Fatal("unexpected RPC count", a.metadataCalls, a.configCalls)
			}
			if (tc.name == "metadata RPC" || tc.name == "configs RPC") && !errors.Is(err, rpcErr) {
				t.Fatal("RPC cause not preserved", err)
			}
		})
	}
	for _, tc := range []struct {
		name  string
		admin producerTopicAdmin
		topic string
	}{{"nil admin", nil, ResultsTopic}, {"empty topic", validProducerAdmin(), ""}} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := describeProducerSource(context.Background(), tc.admin, tc.topic); err == nil {
				t.Fatal("invalid request accepted")
			}
		})
	}
}
