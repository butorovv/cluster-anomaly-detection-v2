# Result payload contract (P16-C2)

The limit applies to the **encoded envelope delivered by the outbox**, not to an
unencoded scientific payload, a checkpoint snapshot, an entire poll response or
the process RSS. `domain.MaxMessageBytes` remains **4,194,304 bytes (4 MiB)**.
No event, evidence, vote segment, identity or scientific asset is truncated,
split or recomputed by this change.

| Layer | Before C2 | C2 contract/configuration |
|---|---|---|
| Envelope decode; HTTP Push request | 4,194,304 bytes | Unchanged; larger messages rejected |
| Outbox PostgreSQL schema | JSONB, no application 4 MiB constraint at staging | Unchanged; Page enforces encoded message <=4 MiB and total page <=4 MiB |
| Producer batch, franz-go v1.21.0 | Implicit 1,000,012 bytes | Explicit 4,195,328 bytes, sync and async |
| Async admission / SDK buffered bytes | Default 4,194,560; configurable bounded budget | Unchanged; budget counts key + value, not record-batch framing |
| New `group.results` topic | `cleanup.policy=delete`, inherited size | `cleanup.policy=delete`, `max.message.bytes=4195328` |
| Existing topic | Inherited/administrator policy | Fresh effective `max.message.bytes >=4195328` required at initialization and each send; never auto-altered |
| Compose broker `message.max.bytes` | Unset (Kafka 4.1 default 1,048,588) | 4,195,328; a topic override can replace the broker default |
| Compose `replica.fetch.max.bytes` | Unset (default 1,048,576) | 4,195,328; Kafka's first-batch progress exception still applies |
| Master result fetch | 8 MiB total; 4 MiB +1 KiB per partition | Numerically unchanged; shared batch constant and effective topic check |
| SDK broker write/read | Defaults 100 MiB / 100 MiB | Unchanged; broker socket request default also 100 MiB |
| HTTP Push receipt | 64 KiB | Unchanged; ACK only after valid matching receipt |
| HTTP Polling | <=4 MiB of stored envelopes; <=8 MiB response reader | Same bounds; page encoder disables HTML escaping to avoid sixfold expansion of `<`, `>` and `&` |

Kafka uses no compression, no headers, a group key <=256 bytes and all-ISR
acknowledgement. The 1 KiB framing allowance covers the key, record varints and
batch metadata. Boundary tests encode a conservative worst-case record. Kafka
record bytes and partition mapping remain unchanged. Async queue/task/byte
budgets, FIFO, owner generation checks and SQL ACK fencing remain unchanged.

These are source/configuration facts, **not evidence of a running broker's
effective settings**. Actual broker delivery, consumer recovery and PostgreSQL
restarts require opt-in integration on a disposable isolated stand.

## Failure and recovery

`MessageTooLarge`, `RecordListTooLarge`, `InvalidRecord`, invalid local Kafka
envelopes, changed identity pins, insufficient/malformed effective topic size,
oversized SQL Page messages, and HTTP 413 are permanent delivery failures. The
sender preserves the underlying cause. Dispatcher returns an error, increments
`output_permanent_errors` and does not ACK the failed row. Worker propagates the
output failure and stops. Async handles both immediate errors and callback
errors; other in-flight successful deliveries may still ACK during bounded drain.
A stale generation's callback cannot ACK a replacement owner's work.

Admin RPC outages, network/cancellation/timeout errors and retriable broker
errors retain the existing retry/recovery behavior. This is a classification of
known payload failures, not an assertion that every Kafka error is classified.
SQL ACK is still attempted only after successful external delivery. Unknown
publication outcomes remain pending and may replay with the same message ID.

## Existing deployments and pending messages

Before deploying C2, inspect the actual effective topic configuration, broker
request/replication limits and Master consumer settings on an explicitly owned
stand. Approve any topic/broker configuration change separately for that stand;
this application does not alter existing topics. Retain ClusterID, TopicID,
16 partitions and exactly `cleanup.policy=delete`. Do not recreate a topic to
raise its size: recreation invalidates its recovery identity.

After approved configuration repair, restart the Worker; the durable pending
envelope replays without a scientific rewrite. The new minimum topic policy is
required even when current records are small, so an unprepared existing topic
will fail closed at startup. Compose changes affect newly started services;
they do not change an already running broker or an explicit topic override.

PostgreSQL JSONB normalizes serialization. Its returned JSON text can be larger
than the original compact serialization. The transport limit is checked on
the actual Page bytes. Already stored rows above 4 MiB remain durable and now
cause a visible permanent failure in push/Kafka dispatch; C2 neither migrates
nor truncates them. Polling preserves its existing HTTP error/cursor retry
behavior for an unreadable page. Such rows require a separate explicit recovery
decision. Raising Kafka caps does not fix every possible SQL/runtime failure.

## Checks and integration handoff

- Fast: `go test ./... -count=1 -p=1`; external integration opt-ins unset.
- Race: `go test -race ./internal/modules/events/... ./internal/app/worker -count=1 -p=1`.
- Static: `go vet ./...`.
- Disposable Kafka only: set `SYSTEM_KAFKA_INTEGRATION=1`,
  `SYSTEM_KAFKA_ISOLATED=1`, `P07_TEST_KAFKA_BROKERS=<dedicated seeds>`, then
  `go test ./internal/modules/events/delivery/resulttransport -run '^TestPayloadRealKafkaSyncAsyncBoundary$' -count=1 -v`.
  It creates/uses `group.results` and does not delete or alter existing topics.
- Disposable PostgreSQL only: set `SYSTEM_TEST_DATABASE_URL=<dedicated admin URL>`,
  then `go test ./internal/modules/events/repository/postgres -run '^TestPayloadSQLPendingDispatcherRestart$' -count=1 -v`.
  The helper creates uniquely named test databases. This test uses synthetic
  administrative outbox rows and models a Dispatcher restart, not compute->Save
  atomicity or an actual Worker process restart.

Before acceptance, add/perform the combined real Kafka+PostgreSQL failure and
Worker restart gate, lost broker/SQL ACK recovery, same-ID replay and exact
scientific/golden comparison. Preserve initial failures and explicit SKIPs.
Do not infer system acceptance from mock pending/restart tests or from a
configured producer option. Full D2 is outside this change.

Reference: [Apache Kafka 4.1 broker configuration](https://kafka.apache.org/41/configuration/broker-configs/).
The pinned SDK source documents the 1,000,012-byte batch default and key/value
buffer accounting (`pkg/kgo/config.go`, `record_and_fetch.go`, `sink.go`).
