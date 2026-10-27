package kafkainput

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// These checks use the real opt-in Kafka fixture so resource defaults are
// resolved by the broker's DescribeConfigs implementation.
func TestKafkaCleanupPolicyGate(t *testing.T) {
	cfg, admin := integration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	policies := []struct {
		name     string
		policy   *string
		accepted bool
	}{
		{name: "delete", policy: kmsgString("delete"), accepted: true},
		{name: "compact", policy: kmsgString("compact")},
		{name: "compact-delete", policy: kmsgString("compact,delete")},
		{name: "delete-compact", policy: kmsgString("delete,compact")},
		{name: "inherited-delete-default", policy: nil, accepted: true},
	}
	for _, tc := range policies {
		topic := fmt.Sprintf("%s-policy-%s", cfg.topic(), tc.name)
		configs := map[string]*string{}
		if tc.policy != nil {
			configs["cleanup.policy"] = tc.policy
		}
		created, err := admin.CreateTopics(ctx, 16, 1, configs, topic)
		if err != nil || created[topic].Err != nil {
			t.Fatalf("create %s: %v %v", topic, err, created[topic].Err)
		}
		t.Cleanup(func() {
			clean, done := context.WithTimeout(context.Background(), 10*time.Second)
			defer done()
			if _, err := admin.DeleteTopics(clean, topic); err != nil {
				t.Errorf("delete %s: %v", topic, err)
			}
		})
		waitFor(t, ctx, func() bool {
			m, err := admin.Metadata(ctx, topic)
			return err == nil && m.Topics[topic].Err == nil && len(m.Topics[topic].Partitions) == 16
		})
		variant := cfg
		variant.Broker.Topic = topic
		id, checkErr := CheckTopic(ctx, admin.client, topic)
		if tc.accepted {
			if checkErr != nil || id == "" {
				t.Fatalf("CheckTopic(%s) = %q, %v", tc.name, id, checkErr)
			}
			if tc.policy == nil {
				resources, err := admin.DescribeTopicConfigs(ctx, topic)
				if err != nil {
					t.Fatal(err)
				}
				for _, resource := range resources {
					for _, config := range resource.Configs {
						if config.Key == "cleanup.policy" && config.Source == kmsg.ConfigSourceDynamicTopicConfig {
							t.Fatal("inherited fixture unexpectedly has a topic override")
						}
					}
				}
			}
			producer, err := NewProducer(ctx, ProducerConfig{Broker: variant.Broker, Binding: variant.Binding, Window: variant.Window})
			if err != nil {
				t.Fatalf("NewProducer(%s): %v", tc.name, err)
			}
			if err := producer.Complete(ctx, domain.TimestampComplete{GroupID: "a01", Timestamp: cfg.Window.Start}); err != nil {
				t.Fatal(err)
			}
			if err := producer.Close(); err != nil {
				t.Fatal(err)
			}
			called := false
			consumeCtx, stop := context.WithTimeout(ctx, 2*time.Second)
			consumeErr := ConsumePartition(consumeCtx, variant, Position{Identity: variant.identity(id, 0), NextOffset: 0}, func(context.Context, *kgo.Record, domain.InputMessage) error {
				called = true
				return errDone
			})
			stop()
			if !called || !errors.Is(consumeErr, errDone) {
				t.Fatalf("ConsumePartition(%s) did not handle the safe marker: %v", tc.name, consumeErr)
			}
			continue
		}
		if checkErr == nil || id != "" {
			t.Fatalf("CheckTopic(%s) = %q, %v; want rejection", tc.name, id, checkErr)
		}
		before, err := admin.ListEndOffsets(ctx, topic)
		if err != nil {
			t.Fatal(err)
		}
		producer, err := NewProducer(ctx, ProducerConfig{Broker: variant.Broker, Binding: variant.Binding, Window: variant.Window})
		if err == nil || producer != nil {
			if producer != nil {
				_ = producer.Close()
			}
			t.Fatalf("NewProducer(%s) = %v, %v; want rejection", tc.name, producer, err)
		}
		after, err := admin.ListEndOffsets(ctx, topic)
		if err != nil {
			t.Fatal(err)
		}
		if !sameTopicOffsets(before, after, topic) {
			t.Fatalf("producer rejection published records for %s: before=%v after=%v", tc.name, before[topic], after[topic])
		}

		called := false
		consumeCfg := variant
		consumeCfg.Load = func(context.Context, Identity) (Position, error) { return Position{}, nil }
		err = ConsumeGroup(ctx, consumeCfg, func(context.Context, *kgo.Record, domain.InputMessage) error { called = true; return nil })
		if err == nil || called {
			t.Fatalf("ConsumeGroup(%s): err=%v handlerCalled=%v", tc.name, err, called)
		}
	}
}

func sameTopicOffsets(a, b kadm.ListedOffsets, topic string) bool {
	if len(a[topic]) != len(b[topic]) {
		return false
	}
	for p, left := range a[topic] {
		right, ok := b[topic][p]
		if !ok || left.Offset != right.Offset {
			return false
		}
	}
	return true
}

func TestKafkaCleanupPolicyAdminUnavailable(t *testing.T) {
	cfg, admin := integration(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := CheckTopic(ctx, admin.client, cfg.topic())
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("CheckTopic canceled context error = %v", err)
	}
	// Permit real metadata traffic, but make DescribeConfigs unavailable on the
	// client transport. No fake Admin response or broker configuration is used.
	var blocked atomic.Int32
	client, err := kgo.NewClient(append(cfg.options(), kgo.Dialer(func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &denyDescribeConfigsConn{Conn: conn, blocked: &blocked}, nil
	}))...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	live, done := context.WithTimeout(context.Background(), 3*time.Second)
	defer done()
	metadata, err := kadm.NewClient(client).Metadata(live, cfg.topic())
	if err != nil || metadata.Topics[cfg.topic()].Err != nil {
		t.Fatalf("metadata prerequisite: %v", err)
	}
	id, err := CheckTopic(live, client, cfg.topic())
	if err == nil || id != "" || blocked.Load() == 0 || !strings.Contains(err.Error(), "describe configuration") {
		t.Fatalf("unavailable DescribeConfigs accepted: id=%q err=%v blocked=%d", id, err, blocked.Load())
	}
}

type denyDescribeConfigsConn struct {
	net.Conn
	blocked *atomic.Int32
}

func (c *denyDescribeConfigsConn) Write(p []byte) (int, error) {
	// Kafka request: int32 frame length followed by int16 API key.
	if len(p) >= 6 && int16(binary.BigEndian.Uint16(p[4:6])) == kmsg.NewPtrDescribeConfigsRequest().Key() {
		c.blocked.Add(1)
		return 0, io.ErrClosedPipe
	}
	return c.Conn.Write(p)
}
