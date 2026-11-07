package resulttransport

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func asyncWirePayload(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(domain.Envelope{SchemaVersion: 1, ExecutionID: "12345678-1234-1234-1234-123456789abc", RunID: strings.Repeat("a", 64), ConfigID: strings.Repeat("b", 64), GroupID: "a01", MessageID: "async-stable-id", Kind: "group_result", Revision: 1, EventTime: time.Date(2022, 1, 18, 8, 0, 0, 0, time.UTC), Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func asyncForTest(t *testing.T) *KafkaAsyncSender {
	t.Helper()
	seeds := producerIdentityBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s, err := NewKafkaAsync(ctx, seeds, true, KafkaAsyncOptions{MaxRecords: 4, MaxBytes: domain.MaxMessageBytes + 256})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func awaitAsync(t *testing.T, ctx context.Context, s *KafkaAsyncSender, raw []byte) error {
	t.Helper()
	done := make(chan error, 1)
	if err := s.SendAsync(ctx, raw, "a01", func(err error) { done <- err }); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func replaceAsyncClient(t *testing.T, s *KafkaAsyncSender, dial func(context.Context, string, string) (net.Conn, error)) {
	t.Helper()
	s.sender.client.Close()
	seeds := strings.Split(os.Getenv("P07_TEST_KAFKA_BROKERS"), ",")
	client, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.Dialer(dial), kgo.DialTimeout(5*time.Second), kgo.RequestTimeoutOverhead(5*time.Second), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerLinger(0), kgo.MaxBufferedRecords(4), kgo.MaxBufferedBytes(domain.MaxMessageBytes+256))
	if err != nil {
		t.Fatal(err)
	}
	s.sender.client = client
}

type ackFaultConn struct {
	net.Conn
	claim   *atomic.Bool
	armed   atomic.Bool
	hold    bool
	written chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (c *ackFaultConn) Write(p []byte) (int, error) {
	claimed := len(p) >= 6 && binary.BigEndian.Uint16(p[4:6]) == 0 && c.claim.CompareAndSwap(false, true)
	if claimed {
		c.armed.Store(true)
	}
	n, err := c.Conn.Write(p)
	if claimed && err == nil {
		if c.written != nil {
			close(c.written)
		}
	}
	return n, err
}
func (c *ackFaultConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.armed.Load() {
		if c.hold {
			<-c.closed
		}
		_ = c.Close()
		return 0, io.EOF
	}
	return n, err
}
func (c *ackFaultConn) Close() error { c.once.Do(func() { close(c.closed) }); return c.Conn.Close() }

func TestKafkaAsyncLostACKRetriesIdempotently(t *testing.T) {
	s := asyncForTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	seeds := strings.Split(os.Getenv("P07_TEST_KAFKA_BROKERS"), ",")
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()
	admin := kadm.NewClient(adminClient)
	before, err := admin.ListEndOffsets(ctx, ResultsTopic)
	if err != nil {
		t.Fatal(err)
	}
	var claimed atomic.Bool
	replaceAsyncClient(t, s, func(ctx context.Context, n, a string) (net.Conn, error) {
		c, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, n, a)
		if err != nil {
			return nil, err
		}
		return &ackFaultConn{Conn: c, claim: &claimed, closed: make(chan struct{})}, nil
	})
	var callbacks atomic.Int32
	done := make(chan error, 1)
	if err = s.SendAsync(ctx, asyncWirePayload(t), "a01", func(err error) { callbacks.Add(1); done <- err }); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	after, err := admin.ListEndOffsets(ctx, ResultsTopic)
	if err != nil {
		t.Fatal(err)
	}
	if !claimed.Load() || callbacks.Load() != 1 || after[ResultsTopic][0].Offset-before[ResultsTopic][0].Offset != 1 {
		t.Fatal("lost ACK did not preserve idempotent record", claimed.Load(), callbacks.Load(), before, after)
	}
}

func TestKafkaAsyncUnknownACKCancelFlushAndClose(t *testing.T) {
	s := asyncForTest(t)
	var claimed atomic.Bool
	written := make(chan struct{})
	replaceAsyncClient(t, s, func(ctx context.Context, n, a string) (net.Conn, error) {
		c, e := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, n, a)
		if e != nil {
			return nil, e
		}
		return &ackFaultConn{Conn: c, claim: &claimed, closed: make(chan struct{}), hold: true, written: written}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	if err := s.SendAsync(ctx, asyncWirePayload(t), "a01", func(e error) { done <- e }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-written:
	case <-ctx.Done():
		t.Fatal("produce not written", ctx.Err())
	}
	cancel()
	flushCtx, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	if err := s.Flush(flushCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unknown ACK was treated as resolved", err)
	}
	select {
	case err := <-done:
		t.Fatal("cancellation fabricated a terminal outcome", err)
	default:
	}
	_ = s.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Close fabricated successful publication")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not complete pending callback")
	}
}

func TestKafkaAsyncDisconnectBeforeAdmissionAndRecovery(t *testing.T) {
	s := asyncForTest(t)
	var blocked atomic.Bool
	blocked.Store(true)
	replaceAsyncClient(t, s, func(ctx context.Context, n, a string) (net.Conn, error) {
		if blocked.Load() {
			return nil, &net.OpError{Op: "dial", Net: n, Err: context.DeadlineExceeded}
		}
		return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, n, a)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	var callbacks atomic.Int32
	err := s.SendAsync(ctx, asyncWirePayload(t), "a01", func(error) { callbacks.Add(1) })
	cancel()
	if err == nil || callbacks.Load() != 0 {
		t.Fatal("failed metadata admission produced completion", err, callbacks.Load())
	}
	blocked.Store(false)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = awaitAsync(t, ctx, s, asyncWirePayload(t)); err != nil {
		t.Fatal(err)
	}
	s.sender.Identity.ClusterID = "wrong-cluster"
	if err = s.SendAsync(ctx, asyncWirePayload(t), "a01", func(error) { callbacks.Add(1) }); err == nil || callbacks.Load() != 0 {
		t.Fatal("cluster mismatch accepted")
	}
}

func TestKafkaAsyncActualRecreationRejectsOldPin(t *testing.T) {
	producerIdentityBroker(t)
	if !strings.HasPrefix(ResultsTopic, "p142-b2-") || os.Getenv("P142_OWNED_RESULTS_TOPIC") != ResultsTopic {
		t.Skip("requires test-only ResultsTopic overlay bound to a fresh owned topic")
	}
	s := asyncForTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	old := s.sender.Identity
	admin := kadm.NewClient(s.sender.client)
	rs, err := admin.DeleteTopics(ctx, ResultsTopic)
	if err != nil || rs[ResultsTopic].Err != nil {
		t.Fatal(rs, err)
	}
	for {
		m, e := admin.Metadata(ctx, ResultsTopic)
		if e == nil && m.Topics[ResultsTopic].Err != nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(30 * time.Millisecond):
		}
	}
	policy := "delete"
	created, err := admin.CreateTopics(ctx, 16, 1, map[string]*string{"cleanup.policy": &policy}, ResultsTopic)
	if err != nil || created[ResultsTopic].Err != nil {
		t.Fatal(created, err)
	}
	for {
		_, topic, e := describeProducerSource(ctx, admin, ResultsTopic)
		if e == nil {
			if topic == old.TopicID {
				t.Fatal("topic ID reused")
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(30 * time.Millisecond):
		}
	}
	var callback atomic.Bool
	if err = s.SendAsync(ctx, asyncWirePayload(t), "a01", func(error) { callback.Store(true) }); err == nil || !strings.Contains(err.Error(), "incarnation changed") || callback.Load() {
		t.Fatal(fmt.Sprint("old pin accepted: ", err))
	}
}
