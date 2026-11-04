package worker

import (
	"context"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func pgOutageProxy(t *testing.T, backend string) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	connections := []net.Conn{}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			incoming, err := listener.Accept()
			if err != nil {
				return
			}
			outgoing, err := net.DialTimeout("tcp", backend, time.Second)
			if err != nil {
				_ = incoming.Close()
				continue
			}
			mu.Lock()
			connections = append(connections, incoming, outgoing)
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
	var once sync.Once
	stop := func() {
		once.Do(func() {
			_ = listener.Close()
			<-stopped
			mu.Lock()
			defer mu.Unlock()
			for _, c := range connections {
				_ = c.Close()
			}
		})
	}
	t.Cleanup(stop)
	return listener.Addr().String(), stop
}
func TestWorkerPostgresNetworkOutageRecovery(t *testing.T) {
	rig := workerIntegrationFixture(t, 30, "polling", nil, true)
	u, err := url.Parse(rig.dsn)
	if err != nil {
		t.Fatal(err)
	}
	address, disconnect := pgOutageProxy(t, u.Host)
	u.Host = address
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.ConnectTimeout = time.Second
	proxyPool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer proxyPool.Close()
	throughProxy := *rig
	throughProxy.store = &repo.Store{DB: proxyPool, Registration: rig.store.Registration}
	_, cancel, done := throughProxy.start(t)
	defer cancel()
	for tick := 0; tick < 2; tick++ {
		rig.sendTick(t, tick)
		rig.waitRevision(t, int64(tick+1))
	}
	before, err := rig.store.Load(context.Background(), "a01")
	if err != nil {
		t.Fatal(err)
	}
	disconnect()
	rig.sendTick(t, 2)
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "connect") && !strings.Contains(err.Error(), "EOF") && !strings.Contains(err.Error(), "closed") {
			t.Fatal("Worker did not report SQL outage", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Worker continued while PostgreSQL unreachable")
	}
	after, err := rig.store.Load(context.Background(), "a01")
	if err != nil || after.SHA256 != before.SHA256 || after.NextOffset != before.NextOffset {
		t.Fatal("RAM-only progress escaped outage", err)
	}
	next, cancelNext, doneNext := rig.start(t)
	defer stopWorker(t, cancelNext, doneNext)
	waitWorker(t, 35*time.Second, func() bool { return countAssigned(next) == 16 })
	rig.waitRevision(t, 3)
	for tick := 3; tick < 6; tick++ {
		rig.sendTick(t, tick)
		rig.waitRevision(t, int64(tick+1))
	}
	rig.assertEvents(t)
	var fragments int
	if err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.worker_tick_fragments`).Scan(&fragments); err != nil || fragments != 96 {
		t.Fatal("SQL outage lost/duplicated diagnostic rows", fragments, err)
	}
}
