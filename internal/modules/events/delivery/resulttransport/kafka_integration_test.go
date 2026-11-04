package resulttransport

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Count actual bootstrap TCP connections via a transparent seed proxy. Broker
// metadata still advertises its real endpoint. A new Describe client per message
// necessarily reconnects to the proxy; a shared client keeps a bounded pool.
func TestKafkaSenderBoundedConnections(t *testing.T) {
	if os.Getenv("SYSTEM_KAFKA_INTEGRATION") != "1" {
		t.Skip("real isolated Kafka opt-in")
	}
	seeds := strings.Split(os.Getenv("P07_TEST_KAFKA_BROKERS"), ",")
	if os.Getenv("SYSTEM_KAFKA_ISOLATED") != "1" || seeds[0] == "" {
		t.Fatal("isolated Kafka prerequisites required")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var connections atomic.Int64
	var mu sync.Mutex
	active := []net.Conn{}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			incoming, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			outgoing, err := net.DialTimeout("tcp", seeds[0], 3*time.Second)
			if err != nil {
				_ = incoming.Close()
				continue
			}
			mu.Lock()
			active = append(active, incoming, outgoing)
			mu.Unlock()
			go func() {
				defer incoming.Close()
				defer outgoing.Close()
				done := make(chan struct{})
				go func() { _, _ = io.Copy(outgoing, incoming); _ = outgoing.Close(); close(done) }()
				_, _ = io.Copy(incoming, outgoing)
				_ = incoming.Close()
				<-done
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-stopped
		mu.Lock()
		defer mu.Unlock()
		for _, c := range active {
			_ = c.Close()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sender, err := NewKafka(ctx, []string{listener.Addr().String()}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	f := workerfixture.New(t, 1, 0)
	p, err := group.NewGroupProcessor(f.Bundle, f.Options)
	if err != nil {
		t.Fatal(err)
	}
	records := map[string]domain.TelemetryRecord{}
	for h := 1; h <= 18; h++ {
		record := workerfixture.Record("a01", h, f.Options.Window.Start, 2)
		records[record.Hostname] = record
	}
	if _, err := p.Step(domain.GroupTick{Binding: f.Options.Binding, GroupID: "a01", Timestamp: f.Options.Window.Start, Records: records, ValidRecords: 18}); err != nil {
		t.Fatal(err)
	}
	events, err := p.Finish()
	if err != nil {
		t.Fatal(err)
	}
	envelopes, err := core.TerminalEvents(f.Options.Binding.ExecutionID, events)
	if err != nil || len(envelopes) != 1 {
		t.Fatal("terminal fixture", len(envelopes), err)
	}
	raw, err := json.Marshal(envelopes[0])
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 128; i++ {
		if err := sender.Send(ctx, raw, "a01"); err != nil {
			t.Fatal(err)
		}
	}
	if count := connections.Load(); count > 8 {
		t.Fatal("unbounded Kafka bootstrap connections", count)
	}
	t.Log(fmt.Sprintf("128 durable sends, %d bootstrap connections", connections.Load()))
	// A changed pin must reject the next send even though connections are reused.
	sender.Identity.TopicID = "different-topic-incarnation"
	if err := sender.Send(ctx, raw, "a01"); err == nil {
		t.Fatal("stale source pin accepted")
	}
}
