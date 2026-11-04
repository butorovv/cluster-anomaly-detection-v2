package worker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Disconnect/refuse every real Kafka connection of this Worker, including broker
// addresses returned by metadata. Other executions and the actual broker stay up.
func TestWorkerKafkaNetworkDisconnectRecovery(t *testing.T) {
	rig := workerIntegrationFixture(t, 30, "polling", nil, true)
	var blocked atomic.Bool
	var mu sync.Mutex
	connections := []net.Conn{}
	fault := rig.broker
	fault.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if blocked.Load() {
			return nil, &net.OpError{Op: "dial", Net: network, Err: context.DeadlineExceeded}
		}
		c, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		connections = append(connections, c)
		mu.Unlock()
		if blocked.Load() {
			_ = c.Close()
			return nil, context.DeadlineExceeded
		}
		return c, nil
	}
	throughFault := *rig
	throughFault.broker = fault
	runner, cancel, done := throughFault.start(t)
	defer func() {
		if cancel != nil {
			stopWorker(t, cancel, done)
		}
	}()
	for tick := 0; tick < 2; tick++ {
		rig.sendTick(t, tick)
		rig.waitRevision(t, int64(tick+1))
	}
	before, err := rig.store.Load(context.Background(), "a01")
	if err != nil {
		t.Fatal(err)
	}
	partial := rig.messages("a01", 2)
	for _, m := range partial[:2] {
		rig.send(t, m)
	}
	waitWorker(t, 10*time.Second, func() bool {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		p := runner.parts[0]
		return p != nil && p.nextOffset == before.NextOffset+2
	})
	blocked.Store(true)
	mu.Lock()
	closed := len(connections)
	for _, c := range connections {
		_ = c.Close()
	}
	mu.Unlock()
	if closed == 0 {
		t.Fatal("no real connections disconnected")
	}
	for _, m := range partial[2:] {
		rig.send(t, m)
	}
	for g := 2; g <= 16; g++ {
		for _, m := range rig.messages(fmt.Sprintf("a%02d", g), 2) {
			rig.send(t, m)
		}
	}
	time.Sleep(1500 * time.Millisecond)
	after, err := rig.store.Load(context.Background(), "a01")
	if err != nil || after.SHA256 != before.SHA256 || after.NextOffset != before.NextOffset {
		t.Fatal("progress changed while Kafka unreachable", err)
	}
	blocked.Store(false)
	restarted := false
	deadline := time.Now().Add(40 * time.Second)
	for {
		select {
		case err := <-done:
			if err == nil || restarted {
				t.Fatal("unexpected Worker termination during network recovery", err)
			}
			cancel()
			cancel = nil
			runner, cancel, done = rig.start(t)
			restarted = true
		default:
		}
		var count int
		if err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.group_checkpoints WHERE checkpoint_revision=3`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 16 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Kafka reconnection/restart did not resume from durable state")
		}
		time.Sleep(50 * time.Millisecond)
	}
	for tick := 3; tick < 6; tick++ {
		rig.sendTick(t, tick)
		rig.waitRevision(t, int64(tick+1))
	}
	rig.assertEvents(t)
	var fragments int
	if err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.worker_tick_fragments`).Scan(&fragments); err != nil || fragments != 96 {
		t.Fatal("disconnect lost/duplicated diagnostic rows", fragments, err)
	}
	t.Logf("closed %d Kafka sockets; partial tail replay/reconnect preserved the continuous oracle", closed)
}
