package resulttransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/delivery/httpoutbox"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/domain"
	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/events/usecase"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Synthetic transport fixture only; padding is not a scientific result.
func payloadOfSize(t *testing.T, size int, fill byte) []byte {
	t.Helper()
	base := asyncWirePayload(t)
	prefix := bytes.TrimSuffix(base, []byte("{}}"))
	prefix = append(prefix, []byte(`{"padding":"`)...)
	suffix := []byte(`"}}`)
	if size < len(prefix)+len(suffix) {
		t.Fatal("size too small")
	}
	raw := append(prefix, bytes.Repeat([]byte{fill}, size-len(prefix)-len(suffix))...)
	return append(raw, suffix...)
}

func TestPayloadProducerConfigurationAndFraming(t *testing.T) {
	baseline, err := kgo.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer baseline.Close()
	old := baseline.OptValue(kgo.ProducerBatchMaxBytes).(int32)
	if old != 1000012 {
		t.Fatalf("pinned SDK default changed: %d", old)
	}
	cl, err := kgo.NewClient(kafkaProducerOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if got := cl.OptValue(kgo.ProducerBatchMaxBytes).(int32); got != KafkaBatchMaxBytes {
		t.Fatalf("batch cap %d", got)
	}
	for _, size := range []int{1000011, 1000012, 1000013, (1 << 20) - 1, 1 << 20, (1 << 20) + 1, domain.MaxMessageBytes - 1, domain.MaxMessageBytes, domain.MaxMessageBytes + 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			raw := payloadOfSize(t, size, 'x')
			if len(raw) != size {
				t.Fatal("fixture size")
			}
			_, err := domain.Decode(raw)
			if size > domain.MaxMessageBytes {
				if err == nil {
					t.Fatal("oversize accepted")
				}
				// Fail before dereferencing a client and before any async callback.
				if !domain.IsPermanentDelivery((&KafkaSender{}).Send(context.Background(), raw, "a01")) {
					t.Fatal("sync rejection not permanent")
				}
				calls := 0
				err = (&KafkaAsyncSender{}).SendAsync(context.Background(), raw, "a01", func(error) { calls++ })
				if !domain.IsPermanentDelivery(err) || calls != 0 {
					t.Fatal("async oversize contract", err, calls)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			// Encode the worst bounded key, no headers, and maximal timestamp/offset
			// varints. 65 includes SDK's batch framing and array prefix.
			record := kmsg.Record{Length: int32(size + 300), Key: bytes.Repeat([]byte{'k'}, 256), Value: raw, TimestampDelta64: 1 << 62, OffsetDelta: 1 << 30}
			batch := len(record.AppendTo(nil)) + 65
			if batch > KafkaBatchMaxBytes {
				t.Fatalf("framing %d > %d", batch, KafkaBatchMaxBytes)
			}
			if size >= 1<<20 && batch <= int(old) {
				t.Fatal("baseline mismatch not reproduced")
			}
			t.Logf("payload=%d conservative_batch=%d old_batch_cap=%d C2_batch_cap=%d", size, batch, old, KafkaBatchMaxBytes)
		})
	}
}

func TestPayloadFreshEffectiveTopicLimit(t *testing.T) {
	for _, value := range []string{"1000012", "1048588", "4195327", "0", "invalid", "2147483648", ""} {
		t.Run(value, func(t *testing.T) {
			a := validProducerAdmin()
			if _, _, err := describeProducerSource(context.Background(), a, ResultsTopic); err != nil {
				t.Fatal(err)
			}
			a.configs[0].Configs[1].Value = &value
			_, _, err := describeProducerSource(context.Background(), a, ResultsTopic)
			if !domain.IsPermanentDelivery(err) || a.configCalls != 2 {
				t.Fatal("fresh limit must fail closed", err)
			}
		})
	}
	for _, kind := range []string{"missing", "nil", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			a := validProducerAdmin()
			switch kind {
			case "missing":
				a.configs[0].Configs = a.configs[0].Configs[:1]
			case "nil":
				a.configs[0].Configs[1].Value = nil
			case "duplicate":
				a.configs[0].Configs = append(a.configs[0].Configs, a.configs[0].Configs[1])
			}
			if _, _, err := describeProducerSource(context.Background(), a, ResultsTopic); !domain.IsPermanentDelivery(err) {
				t.Fatal(err)
			}
		})
	}
	a := validProducerAdmin()
	outage := errors.New("admin outage")
	a.configsErr = outage
	if _, _, err := describeProducerSource(context.Background(), a, ResultsTopic); !errors.Is(err, outage) || domain.IsPermanentDelivery(err) {
		t.Fatal("outage must remain transient", err)
	}
}

func TestPayloadDeliveryClassification(t *testing.T) {
	for _, err := range []error{kerr.MessageTooLarge, kerr.RecordListTooLarge, kerr.InvalidRecord} {
		wrapped := kafkaDeliveryError(fmt.Errorf("broker: %w", err))
		if !domain.IsPermanentDelivery(wrapped) || !errors.Is(wrapped, err) {
			t.Fatal(wrapped)
		}
	}
	for _, err := range []error{nil, context.Canceled, context.DeadlineExceeded, kerr.NotLeaderForPartition, kerr.RequestTimedOut, errors.New("socket EOF")} {
		if domain.IsPermanentDelivery(kafkaDeliveryError(err)) {
			t.Fatal("transient classified permanent", err)
		}
	}
}

type payloadPageStore struct{ raw []byte }

func (s payloadPageStore) Advertise(context.Context, domain.Owner, string) error { return nil }
func (s payloadPageStore) Page(context.Context, domain.Owner, domain.Cursor, int, bool) ([]domain.Delivery, error) {
	return []domain.Delivery{{Cursor: domain.Cursor{Revision: 1, Ordinal: 1}, Envelope: s.raw}}, nil
}
func (s payloadPageStore) Ack(context.Context, domain.Owner, string) error              { return nil }
func (s payloadPageStore) AckCursor(context.Context, domain.Owner, domain.Cursor) error { return nil }

func TestPayloadHTTPPushPollingBoundaryParity(t *testing.T) {
	for _, size := range []int{(1 << 20) - 1, 1 << 20, (1 << 20) + 1, domain.MaxMessageBytes - 1, domain.MaxMessageBytes} {
		for _, fill := range []byte{'x', '<'} {
			t.Run(fmt.Sprintf("%d/%c", size, fill), func(t *testing.T) {
				raw := payloadOfSize(t, size, fill)
				push := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					got, err := io.ReadAll(http.MaxBytesReader(w, r.Body, domain.MaxMessageBytes))
					if err != nil || !bytes.Equal(raw, got) {
						t.Error("push bytes changed", err)
						http.Error(w, "size", 413)
						return
					}
					_ = json.NewEncoder(w).Encode(usecase.Receipt{MessageID: "async-stable-id", Accepted: true})
				}))
				defer push.Close()
				sender, err := NewHTTP(push.URL, 5*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer sender.Close()
				if err := sender.Send(context.Background(), raw, "a01"); err != nil {
					t.Fatal(err)
				}
				e, err := domain.Decode(raw)
				if err != nil {
					t.Fatal(err)
				}
				d := &usecase.Dispatcher{Store: payloadPageStore{raw}, ExecutionID: e.ExecutionID, Mode: "polling"}
				o := domain.Owner{ExecutionID: e.ExecutionID, GroupID: "a01", OwnerID: "owner", Epoch: 1}
				if err := d.Track(context.Background(), o); err != nil {
					t.Fatal(err)
				}
				poll := httptest.NewServer(httpoutbox.Handler(d))
				defer poll.Close()
				response, err := http.Get(poll.URL + "/v1/outbox?execution_id=" + e.ExecutionID + "&group_id=a01&revision=0&ordinal=0&limit=64")
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				pageBytes, err := io.ReadAll(io.LimitReader(response.Body, 2*domain.MaxMessageBytes+1))
				if err != nil {
					t.Fatal(err)
				}
				if response.StatusCode != 200 || len(pageBytes) > 2*domain.MaxMessageBytes {
					t.Fatal("page exceeds consumer bound", response.StatusCode, len(pageBytes))
				}
				var page domain.Page
				if err := domain.StrictJSON(pageBytes, &page); err != nil {
					t.Fatal(err)
				}
				if len(page.Items) != 1 || !bytes.Equal(page.Items[0].Envelope, raw) {
					t.Fatal("polling bytes changed")
				}
				t.Logf("push=%d polling_page=%d; bytes preserved", len(raw), len(pageBytes))
			})
		}
	}
}

func TestPayloadHTTP413Permanent(t *testing.T) {
	for _, status := range []int{413, 429, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "rejected", status) }))
			defer s.Close()
			sender, err := NewHTTP(s.URL, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer sender.Close()
			err = sender.Send(context.Background(), asyncWirePayload(t), "a01")
			if err == nil || domain.IsPermanentDelivery(err) != (status == 413) {
				t.Fatal(status, err)
			}
		})
	}
}

// Opt-in: a dedicated disposable broker is required; it creates group.results,
// but never deletes topics or changes an existing topic's configuration.
func TestPayloadRealKafkaSyncAsyncBoundary(t *testing.T) {
	seeds := producerIdentityBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	syncSender, err := NewKafka(ctx, seeds, true)
	if err != nil {
		t.Fatal(err)
	}
	defer syncSender.Close()
	async, err := NewKafkaAsync(ctx, seeds, false, KafkaAsyncOptions{MaxRecords: 1, MaxBytes: domain.MaxMessageBytes + 256})
	if err != nil {
		t.Fatal(err)
	}
	defer async.Close()
	start, err := kadm.NewClient(syncSender.client).ListEndOffsets(ctx, ResultsTopic)
	if err != nil {
		t.Fatal(err)
	}
	pos, ok := start[ResultsTopic][0]
	if !ok || pos.Err != nil {
		t.Fatal("partition bound unavailable", start)
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{ResultsTopic: {0: kgo.NewOffset().At(pos.Offset)}}), kgo.FetchMaxBytes(8<<20), kgo.FetchMaxPartitionBytes(KafkaBatchMaxBytes))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, size := range []int{1000011, 1000012, 1000013, (1 << 20) - 1, 1 << 20, (1 << 20) + 1, domain.MaxMessageBytes - 1, domain.MaxMessageBytes, domain.MaxMessageBytes + 1} {
		raw := payloadOfSize(t, size, 'x')
		for _, mode := range []string{"sync", "async"} {
			var err error
			if mode == "sync" {
				err = syncSender.Send(ctx, raw, "a01")
			} else {
				err = awaitAsync(t, ctx, async, raw)
			}
			if size > domain.MaxMessageBytes {
				if !domain.IsPermanentDelivery(err) {
					t.Fatal(mode, size, err)
				}
				continue
			}
			if err != nil {
				t.Fatal(mode, size, err)
			}
			fetch := reader.PollRecords(ctx, 1)
			if errs := fetch.Errors(); len(errs) > 0 {
				t.Fatal(errs)
			}
			records := fetch.Records()
			if len(records) != 1 || !bytes.Equal(records[0].Value, raw) || string(records[0].Key) != "a01" {
				t.Fatal("wire changed", mode, size)
			}
			t.Logf("%s payload=%d offset=%d bytes preserved", mode, size, records[0].Offset)
		}
	}
	// Successful preflight already inspected the effective topic limit, not only
	// SDK config. Prove a live pin change cannot publish either path.
	syncSender.Identity.TopicID = "changed"
	if err := syncSender.Send(ctx, asyncWirePayload(t), "a01"); err == nil || !strings.Contains(err.Error(), "incarnation") {
		t.Fatal(err)
	}
}
