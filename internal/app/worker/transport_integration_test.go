package worker

import (
	"context"
	"encoding/json"
	"errors"

	masterapp "github.com/butorovv/cluster-anomaly-detection-v2/internal/app/master"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/httpinput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/httpoutbox"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/resulttransport"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func discardLog() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }
func (r *workerRig) startDelivery(t *testing.T, mode, masterURL string) (*Runner, context.CancelFunc, <-chan error) {
	t.Helper()
	d := &usecase.Dispatcher{ExecutionID: r.fixture.Options.Binding.ExecutionID, Mode: mode, Store: repo.OutboxStore{DB: r.pool}, Logger: discardLog()}
	switch mode {
	case "polling":
		server := httptest.NewServer(httpoutbox.Handler(d))
		d.Endpoint = server.URL
		t.Cleanup(server.Close)
	case "http_push":
		sender, err := resulttransport.NewHTTP(masterURL, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		d.Sender = sender
	case "kafka":
		sender, err := resulttransport.NewKafka(context.Background(), r.broker.Seeds, true)
		if err != nil {
			t.Fatal(err)
		}
		d.Sender = sender
	default:
		t.Fatal("invalid mode")
	}
	return r.startOutput(t, d)
}
func assertMaster(t *testing.T, r *workerRig, store repo.MasterStore) map[string]string {
	t.Helper()
	want := r.expectedEvents(t)
	got := map[string]group.RBI2Event{}
	hashes := map[string]string{}
	events, err := store.Events(context.Background(), r.fixture.Options.Binding.ExecutionID, "", "", 500)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range events {
		var e group.RBI2Event
		if err := json.Unmarshal(b, &e); err != nil {
			t.Fatal(err)
		}
		if _, ok := got[e.ID]; ok {
			t.Fatal("duplicate parent")
		}
		got[e.ID] = e
		canonical, _ := json.Marshal(e)
		hashes[e.ID] = domain.Hash(canonical)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Master differs from continuous oracle: got %d want %d", len(got), len(want))
	}
	return hashes
}
func TestWorkerThreeTransportParityAndRestart(t *testing.T) {
	var reference map[string]string
	var frozen *workerfixture.Fixture
	for _, mode := range []string{"polling", "http_push", "kafka"} {
		t.Run(mode, func(t *testing.T) {
			rig := workerIntegrationFixture(t, 30, mode, frozen)
			if frozen == nil {
				value := rig.fixture
				frozen = &value
			}
			store := restrictedMaster(t, rig)
			service := usecase.Master{Store: store}
			// Push loses one ACK AFTER durable acceptance; it must retry the exact ID.
			var posts atomic.Int32
			lost := atomic.Bool{}
			handler := httpinput.Handler(service, discardLog())
			receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts.Add(1)
				}
				if mode == "http_push" && r.Method == http.MethodPost && lost.CompareAndSwap(false, true) {
					rec := httptest.NewRecorder()
					handler.ServeHTTP(rec, r)
					if rec.Code != 200 {
						t.Error("first durable acceptance", rec.Code)
					}
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = connection.Close()
					return
				}
				handler.ServeHTTP(w, r)
			}))
			defer receiver.Close()
			if mode == "kafka" {
				rig.bindResults(t, "p09-master-"+rig.group)
			}
			startMaster := func() (context.CancelFunc, <-chan error) {
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() {
					if mode == "polling" {
						done <- (usecase.Poller{ExecutionID: rig.fixture.Options.Binding.ExecutionID, Groups: 16, Interval: 20 * time.Millisecond, Store: store, Master: service, Logger: discardLog()}).Run(ctx)
					} else if mode == "kafka" {
						done <- masterapp.RunKafka(ctx, rig.broker.Seeds, "p09-master-"+rig.group, rig.fixture.Options.Binding.ExecutionID, true, store, service)
					} else {
						<-ctx.Done()
						done <- nil
					}
				}()
				return cancel, done
			}
			stopMaster := func(cancel context.CancelFunc, done <-chan error) {
				cancel()
				select {
				case err := <-done:
					if err != nil && !errors.Is(err, context.Canceled) {
						t.Error(err)
					}
				case <-time.After(15 * time.Second):
					t.Error("Master stop timeout")
				}
			}
			cancelMaster, doneMaster := startMaster()
			defer func() {
				if cancelMaster != nil {
					stopMaster(cancelMaster, doneMaster)
				}
			}()
			a, cancelA, doneA := rig.startDelivery(t, mode, receiver.URL)
			defer func() {
				if cancelA != nil {
					stopWorker(t, cancelA, doneA)
				}
			}()
			waitWorker(t, 30*time.Second, func() bool { return countAssigned(a) == 16 })
			for tick := 0; tick < 4; tick++ {
				rig.sendTick(t, tick)
				rig.waitRevision(t, int64(tick+1))
			}
			waitWorker(t, 20*time.Second, func() bool {
				var n int
				err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_events`).Scan(&n)
				return err == nil && n >= 16
			})
			stopMaster(cancelMaster, doneMaster)
			cancelMaster = nil
			b, cancelB, doneB := rig.startDelivery(t, mode, receiver.URL)
			defer stopWorker(t, cancelB, doneB)
			waitWorker(t, 30*time.Second, func() bool { na, nb := countAssigned(a), countAssigned(b); return na > 0 && nb > 0 && na+nb == 16 })
			stopWorker(t, cancelA, doneA)
			cancelA = nil
			waitWorker(t, 30*time.Second, func() bool { return countAssigned(b) == 16 })
			// Outputs remain durable while Master/poller/results consumer is stopped.
			for tick := 4; tick < 6; tick++ {
				rig.sendTick(t, tick)
				rig.waitRevision(t, int64(tick+1))
			}
			cancelMaster, doneMaster = startMaster()
			want := rig.expectedEvents(t)
			waitWorker(t, 30*time.Second, func() bool {
				var n int
				err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_events`).Scan(&n)
				return err == nil && n == len(want)
			})
			got := assertMaster(t, rig, store)
			if reference == nil {
				reference = got
			} else if !reflect.DeepEqual(reference, got) {
				t.Fatal("cross-transport payload hashes differ")
			}
			waitWorker(t, 20*time.Second, func() bool {
				var n int
				err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.outbox_messages WHERE delivered_at IS NULL`).Scan(&n)
				return err == nil && n == 0
			})
			var inbox int
			if err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_inbox`).Scan(&inbox); err != nil || inbox != len(want) {
				t.Fatal("logical duplicate in Master", inbox, err)
			}
			if mode == "http_push" && posts.Load() <= int32(len(want)) {
				t.Fatal("lost ACK did not cause retry")
			}
			if mode == "polling" {
				var cursors int
				if err := rig.pool.QueryRow(context.Background(), `SELECT count(*) FROM cad_v2.master_poll_cursors`).Scan(&cursors); err != nil || cursors != 16 {
					t.Fatal("durable polling cursors", cursors, err)
				}
			}
			t.Logf("mode=%s parents=%d payload_hashes_equal=true", mode, len(got))
		})
	}
}

func restrictedMaster(t *testing.T, r *workerRig) repo.MasterStore {
	t.Helper()
	ctx := context.Background()
	role := "p09_master_" + strings.ReplaceAll(r.fixture.Options.Binding.ExecutionID, "-", "")
	quoted := pgx.Identifier{role}.Sanitize()
	sql := `CREATE ROLE ` + quoted + ` NOLOGIN; GRANT USAGE ON SCHEMA cad_v2 TO ` + quoted + `; GRANT SELECT ON cad_v2.runs,cad_v2.executions,cad_v2.execution_partitions,cad_v2.master_inbox,cad_v2.master_events,cad_v2.master_results,cad_v2.worker_endpoints,cad_v2.partition_ownership,cad_v2.master_poll_cursors,cad_v2.result_topic_bindings,cad_v2.result_consumer_offsets,cad_v2.master_tick_fragments,cad_v2.master_tick_joins TO ` + quoted + `; GRANT EXECUTE ON FUNCTION cad_v2.accept_result(uuid,jsonb),cad_v2.advance_poll_cursor(uuid,text,bigint,integer),cad_v2.bind_result_source(uuid,text,text,text,jsonb),cad_v2.advance_result_offset(uuid,text,integer,bigint,bigint),cad_v2.accept_tick_fragment(uuid,jsonb) TO ` + quoted
	sql += "; GRANT INSERT ON cad_v2.master_tick_joins TO " + quoted
	if _, err := r.pool.Exec(ctx, sql); err != nil {
		t.Fatal(err)
	}
	cfg := r.pool.Config()
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error { _, err := c.Exec(ctx, "SET ROLE "+quoted); return err }
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := r.pool.Exec(context.Background(), "DROP OWNED BY "+quoted+"; DROP ROLE "+quoted); err != nil {
			t.Error(err)
		}
	})
	if _, err := pool.Exec(ctx, `SELECT * FROM cad_v2.outbox_messages LIMIT 1`); err == nil {
		t.Fatal("Master can read Worker outbox directly")
	}
	return repo.MasterStore{DB: pool}
}
