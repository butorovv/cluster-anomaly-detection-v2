package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/db"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/group"
	checkpoints "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/repository/postgres"
	core "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/usecase"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/httpinput"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/httpoutbox"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/resulttransport"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	repo "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/repository/postgres"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	telemetry "github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/telemetry/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/postgrestest"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/testutil/workerfixture"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestOutboxPageTakeoverTransportBoundaries(t *testing.T) {
	// All deltas/cadences, with rotating transport coverage rather than 36 costly
	// combinations. Kafka covers the real SQL intake/offset boundary here, not a
	// broker. Real producer/consumer/process recovery remains separately gated.
	modes := []string{"http_push", "polling", "kafka"}
	for di, delta := range []int{0, 10, 30, 60} {
		for ci, cadence := range []int{1, 3, 6} {
			mode := modes[(di+ci)%len(modes)]
			t.Run(fmt.Sprintf("d%d_c%d_%s", delta, cadence, mode), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				admin := postgrestest.New(t)
				f := workerfixture.New(t, 7, delta)
				f.Options.Groups = 4
				model, err := group.NewGroupProcessor(f.Bundle, f.Options)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := model.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				registration := checkpoints.Registration{Identity: snapshot.Identity, Binding: f.Options.Binding, ClusterID: "p16-r1", TopicID: "p16-r1-input", ConsumerGroup: "p16-r1", OutputTransport: mode, Diagnostics: true, CheckpointIntervalTicks: cadence, StartOffsets: map[string]int64{}}
				for p := 0; p < 16; p++ {
					registration.StartOffsets[fmt.Sprint(p)] = 0
				}
				if err := checkpoints.Register(ctx, admin, registration); err != nil {
					t.Fatal(err)
				}
				workerRole, masterRole := fmt.Sprintf("r1w_%d", time.Now().UnixNano()), fmt.Sprintf("r1m_%d", time.Now().UnixNano())
				if _, err := admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{workerRole}.Sanitize()+" NOLOGIN; CREATE ROLE "+pgx.Identifier{masterRole}.Sanitize()+" NOLOGIN"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
					defer done()
					for _, role := range []string{workerRole, masterRole} {
						q := pgx.Identifier{role}.Sanitize()
						if _, err := admin.Exec(cleanup, "DROP OWNED BY "+q+"; DROP ROLE "+q); err != nil {
							t.Error(err)
						}
					}
				})
				connection, err := admin.Acquire(ctx)
				if err != nil {
					t.Fatal(err)
				}
				err = db.GrantRuntime(ctx, connection.Conn(), workerRole, masterRole)
				connection.Release()
				if err != nil {
					t.Fatal(err)
				}
				rolePool := func(role string) *pgxpool.Pool {
					cfg := admin.Config()
					cfg.MaxConns = 4
					cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
						_, err := c.Exec(ctx, "SET ROLE "+pgx.Identifier{role}.Sanitize())
						return err
					}
					pool, err := pgxpool.NewWithConfig(ctx, cfg)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(pool.Close)
					return pool
				}
				worker, masterPool := rolePool(workerRole), rolePool(masterRole)
				store := &checkpoints.Store{DB: worker, Registration: registration}
				lease, _, err := store.Acquire(ctx, "a01", "00000000-0000-4000-8000-000000000091", 5*time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				partition, err := core.NewPartitionWithCadence(f.Bundle, f.Options, 0, nil, cadence)
				if err != nil {
					t.Fatal(err)
				}
				for tick := 0; tick < 7; tick++ {
					at := f.Options.Window.Start.Add(time.Duration(tick) * telemetry.GridStep)
					for h := 1; h <= 18; h++ {
						record := workerfixture.Record("a01", h, at, 2)
						if _, err := partition.Accept(int64(tick*19+h-1), telemetry.InputMessage{Version: telemetry.InputVersion, Type: telemetry.RecordMessage, Binding: f.Options.Binding, GroupID: "a01", Timestamp: at, Record: &record}); err != nil {
							t.Fatal(err)
						}
					}
					completion := telemetry.TimestampComplete{GroupID: "a01", Timestamp: at, Records: 18, ValidRecords: 18}
					tr, err := partition.Accept(int64(tick*19+18), telemetry.InputMessage{Version: telemetry.InputVersion, Type: telemetry.CompleteMessage, Binding: f.Options.Binding, GroupID: "a01", Timestamp: at, Completion: &completion})
					if err != nil {
						t.Fatal(err)
					}
					if tr != nil {
						revision, err := store.Save(ctx, lease, *tr)
						if err != nil {
							t.Fatal(err)
						}
						if err := partition.Ack(revision); err != nil {
							t.Fatal(err)
						}
					}
				}
				old := domain.Owner{ExecutionID: registration.Binding.ExecutionID, GroupID: lease.Group, OwnerID: lease.Owner, Epoch: lease.Epoch}
				outbox := repo.OutboxStore{DB: worker}
				cached, err := outbox.Page(ctx, old, domain.Cursor{}, 128, true)
				if err != nil || len(cached) != 8 {
					t.Fatal("expected seven durable fragments and one terminal event", len(cached), err)
				}
				before, err := store.Load(ctx, "a01")
				if err != nil || before == nil || !before.Finalized || before.NextOffset != 133 || before.Revision != int64((7+cadence-1)/cadence) {
					t.Fatal("durable complete boundary", before, err)
				}
				if err := store.Release(ctx, lease); err != nil {
					t.Fatal(err)
				}
				nextLease, saved, err := store.Acquire(ctx, "a01", "00000000-0000-4000-8000-000000000092", 5*time.Minute)
				if err != nil || nextLease.Epoch != lease.Epoch+1 || saved == nil || !bytes.Equal(before.Snapshot, saved.Snapshot) || before.SHA256 != saved.SHA256 || before.NextOffset != saved.NextOffset {
					t.Fatal("takeover changed durable scientific state", nextLease, saved, err)
				}
				if _, err := core.NewPartitionWithCadence(f.Bundle, f.Options, 0, saved, cadence); err != nil {
					t.Fatal("restoring actual checkpoint after takeover", err)
				}
				next := domain.Owner{ExecutionID: old.ExecutionID, GroupID: old.GroupID, OwnerID: nextLease.Owner, Epoch: nextLease.Epoch}
				if _, err := outbox.Page(ctx, old, domain.Cursor{}, 128, true); err == nil {
					t.Fatal("fresh old-owner Page succeeded")
				}
				replayed, err := outbox.Page(ctx, next, domain.Cursor{}, 128, true)
				if err != nil || !reflect.DeepEqual(cached, replayed) {
					t.Fatal("takeover changed pending messages/cursors", err)
				}
				masterStore := repo.MasterStore{DB: masterPool}
				master := usecase.Master{Store: masterStore}
				var sender *resulttransport.HTTPSender
				var dispatcher *usecase.Dispatcher
				var polling *httptest.Server
				if mode == "http_push" {
					server := httptest.NewServer(httpinput.Handler(master, slog.Default()))
					defer server.Close()
					sender, err = resulttransport.NewHTTP(server.URL, 3*time.Second)
					if err != nil {
						t.Fatal(err)
					}
					defer sender.Close()
				} else if mode == "polling" {
					dispatcher = &usecase.Dispatcher{ExecutionID: old.ExecutionID, Mode: mode, Store: outbox}
					polling = httptest.NewServer(httpoutbox.Handler(dispatcher))
					defer polling.Close()
					dispatcher.Endpoint = polling.URL
					if err := dispatcher.Track(ctx, next); err != nil {
						t.Fatal(err)
					}
				} else if err := masterStore.BindResults(ctx, old.ExecutionID, "p16-r1", "p16-r1-results", "p16-r1-master", registration.StartOffsets); err != nil {
					t.Fatal(err)
				}
				for attempt := 0; attempt < 2; attempt++ {
					for i, item := range cached {
						switch mode {
						case "http_push":
							err = sender.Send(ctx, item.Envelope, "a01")
						case "polling":
							_, err = master.AcceptAt(ctx, item.Envelope, item.Cursor)
						default:
							err = master.AcceptKafka(ctx, old.ExecutionID, "p16-r1-results", 0, int64(attempt*len(cached)+i), item.Envelope)
						}
						if err != nil {
							t.Fatal("late delivery or exact retry", mode, err)
						}
					}
				}
				envelope, err := domain.Decode(cached[0].Envelope)
				if err != nil {
					t.Fatal(err)
				}
				var stale *pgconn.PgError
				if err := outbox.Ack(ctx, old, envelope.MessageID); !errors.As(err, &stale) || stale.Code != "55000" {
					t.Fatal("late old-owner callback acknowledged new ownership", err)
				}
				if mode == "polling" {
					last := cached[len(cached)-1].Cursor
					request, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/v1/outbox?execution_id=%s&group_id=a01&revision=%d&ordinal=%d&limit=128", polling.URL, old.ExecutionID, last.Revision, last.Ordinal), nil)
					if err != nil {
						t.Fatal(err)
					}
					response, err := http.DefaultClient.Do(request)
					if err != nil {
						t.Fatal(err)
					}
					var page domain.Page
					decodeErr := json.NewDecoder(response.Body).Decode(&page)
					response.Body.Close()
					if response.StatusCode != 200 || decodeErr != nil || page.Epoch != next.Epoch || len(page.Items) != 0 {
						t.Fatal("polling HTTP cursor ACK/page", response.StatusCode, page, decodeErr)
					}
					if cursor, err := masterStore.PollCursor(ctx, old.ExecutionID, "a01"); err != nil || cursor != last {
						t.Fatal("durable polling prefix", cursor, err)
					}
				} else {
					for _, item := range replayed {
						e, err := domain.Decode(item.Envelope)
						if err != nil {
							t.Fatal(err)
						}
						if err := outbox.Ack(ctx, next, e.MessageID); err != nil {
							t.Fatal(err)
						}
					}
					if mode == "kafka" {
						if offset, err := masterStore.ResultOffset(ctx, old.ExecutionID, 0); err != nil || offset != 16 {
							t.Fatal("durable Kafka SQL prefix", offset, err)
						}
					}
				}
				if remaining, err := outbox.Page(ctx, next, domain.Cursor{}, 128, true); err != nil || len(remaining) != 0 {
					t.Fatal("delivery lost or left a pending event/fragment", len(remaining), err)
				}
				var events, fragments, inbox int
				if err := admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM cad_v2.master_events),(SELECT count(*) FROM cad_v2.master_tick_fragments),(SELECT count(*) FROM cad_v2.master_inbox)`).Scan(&events, &fragments, &inbox); err != nil || events != 1 || fragments != 7 || inbox != 1 {
					t.Fatal("duplicate effect or missing output", events, fragments, inbox, err)
				}
			})
		}
	}
}
