// Package kafkainput is the telemetry-input-v1 Kafka transport and recovery PoC.
// It does not implement a mathematical processor or a checkpoint repository.
package kafkainput

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const Topic = "telemetry.raw"
const Partitions = 16

type Broker struct {
	Seeds []string
	// Topic defaults to telemetry.raw. Tests use isolated topics with the same contract.
	Topic string
}

func (b Broker) topic() string {
	if b.Topic == "" {
		return Topic
	}
	return b.Topic
}

func (b Broker) options() []kgo.Opt {
	// This PoC explicitly uses PLAINTEXT; an EOF during broker restart is not
	// evidence of a TLS mismatch. Retries remain bounded by caller/deadlines.
	return []kgo.Opt{kgo.SeedBrokers(b.Seeds...), kgo.DialTimeout(5 * time.Second), kgo.RequestTimeoutOverhead(5 * time.Second), kgo.AlwaysRetryEOF()}
}

// CheckTopic checks metadata without creating or expanding a topic. A changed
// topic ID must invalidate external offsets even when its name has been reused.
// DescribeConfigs supplies the effective policy, including broker defaults;
// only confirmed delete is safe for the keyed, replayable telemetry log.
func CheckTopic(ctx context.Context, cl *kgo.Client, topic string) (string, error) {
	m, err := kadm.NewClient(cl).Metadata(ctx, topic)
	if err != nil {
		return "", err
	}
	t, ok := m.Topics[topic]
	if !ok || t.Err != nil {
		return "", fmt.Errorf("topic %s unavailable: %v", topic, t.Err)
	}
	if len(t.Partitions) != Partitions {
		return "", fmt.Errorf("topic %s: require 16 partitions, got %d", topic, len(t.Partitions))
	}
	for p := int32(0); p < Partitions; p++ {
		d, ok := t.Partitions[p]
		if !ok || d.Err != nil || d.Leader < 0 {
			return "", fmt.Errorf("partition %d unavailable: %v", p, d.Err)
		}
	}
	if t.ID == (kadm.TopicID{}) {
		return "", fmt.Errorf("topic ID required for recovery identity")
	}
	configs, err := kadm.NewClient(cl).DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return "", fmt.Errorf("describe configuration for topic %s: %w", topic, err)
	}
	if err := checkCleanupPolicy(configs, topic); err != nil {
		return "", err
	}
	return t.ID.String(), nil
}

func checkCleanupPolicy(resources kadm.ResourceConfigs, topic string) error {
	if len(resources) != 1 || resources[0].Name != topic {
		return fmt.Errorf("topic %s configuration unavailable: require exactly one resource", topic)
	}
	found := resources[0]
	if found.Err != nil {
		return fmt.Errorf("topic %s configuration unavailable: %w", topic, found.Err)
	}
	var policy *string
	seen := false
	for i := range found.Configs {
		if found.Configs[i].Key == "cleanup.policy" {
			if seen {
				return fmt.Errorf("topic %s has duplicate cleanup.policy configurations", topic)
			}
			seen = true
			policy = found.Configs[i].Value
		}
	}
	if policy == nil {
		return fmt.Errorf("topic %s cleanup.policy unavailable", topic)
	}
	if *policy != "delete" {
		return fmt.Errorf("topic %s has unsafe cleanup.policy %q: require delete", topic, *policy)
	}
	return nil
}

func offsetBounds(ctx context.Context, cl *kgo.Client, topic string) (kadm.ListedOffsets, kadm.ListedOffsets, error) {
	a := kadm.NewClient(cl)
	start, err := a.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, nil, err
	}
	end, err := a.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, nil, err
	}
	return start, end, nil
}

func checkOffset(start, end kadm.ListedOffsets, topic string, partition int32, next int64) error {
	lo, lok := start[topic][partition]
	hi, hik := end[topic][partition]
	if !lok || !hik || lo.Err != nil || hi.Err != nil {
		return fmt.Errorf("offset bounds unavailable for %s/%d: %v/%v", topic, partition, lo.Err, hi.Err)
	}
	if next < 0 || next < lo.Offset || next > hi.Offset {
		return fmt.Errorf("durable next_offset %d outside [%d,%d] for %s/%d", next, lo.Offset, hi.Offset, topic, partition)
	}
	return nil
}
