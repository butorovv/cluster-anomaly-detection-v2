# HTTP Polling: reuse one strict envelope decode

Base: `249dc0bb5890785c2821c252d5acfb8196f60751`. This change can be taken
independently of the HTTP Push pool.

Poller strictly decodes each item with the transport-independent
`domain.DecodeEnvelope`, checks its execution/group, then passes the proof-bearing
`DecodedEnvelope` to `Master.AcceptAtEnvelope`. The proof's zero value is invalid.
The byte `Master.AcceptAt` API remains strict and preserves its decode-start
measurement boundary. Both APIs share payload/identity validation and the same
`positionedStore.AcceptAt` call. The existing SQL transaction still couples
result acceptance/deduplication and durable cursor advance.

Page strict JSON, size, execution/group, discovered owner/epoch, increasing
cursor and registered scientific payload checks are retained. Poll scheduling,
interval, concurrency, deadlines, page size, HTTP wire format and SQL are unchanged.
This does not address historical page timeouts or missing measurement timestamps.
Kafka reception, runner/provenance, checkpointing, lease and golden are unchanged.

## Local checks

An independent export of the base plus only this change passed race tests:
**14 tests/subtests PASS, 0 FAIL, 0 SKIP**. Cases cover:

- Distinct valid fragments produce identical MessageIDs, payloads and cursor
  sequence through the frozen old path and the new path. A duplicate ID at a
  later cursor advances the cursor with one logical stored result.
- Unknown/duplicate header fields, valid JSON envelopes above 4 MiB, wrong
  execution/group, page identity/owner/epoch mismatch, nonincreasing cursor and
  invalid payloads fail without advancing past the failed item. An earlier
  valid item remains accepted when a later cursor is invalid.
- Storage failure leaves the fake result/cursor unchanged and returns an
  unaccepted receipt; the byte API agrees and rejects malformed input.
- A zero `DecodedEnvelope` cannot enter storage.

The fake models atomic success/error and duplicate-safe storage **in memory**.
These tests do not establish PostgreSQL isolation or crash durability. Production
SQL was not modified. Combined local unit and race suites across events usecase,
domain, HTTP input/outbox and resulttransport passed **164 tests/subtests** with
**18 optional Kafka/PostgreSQL integration skips**, zero failures. `go vet`
passed for those packages.

## Short A/B

[Samples and validation hashes](evidence/P16_HTTP_POLLING_LOCAL.json).
Go 1.27.2, Windows/amd64, i7-12650H, GOMAXPROCS=4, median of five repetitions at
`-benchtime=300ms`. One operation is one complete page path. Both sides use
identical valid pages of distinct tick fragments. The test freezes both old
`Poller.poll` and old byte `Master.AcceptAt`, renamed only.

A deterministic in-memory RoundTripper supplies the HTTP response; a discard-mode
Store supplies identity and successful acceptance without retaining benchmark
history. Strict page/envelope parsing, URL/request construction, metrics and
scientific payload validation are included. Sockets, PostgreSQL execution and
durability, fixture construction and Worker computation are excluded.

| Page items | ns/op before → after | B/op before → after | allocs/op before → after |
|---|---:|---:|---:|
| 1 | 229,921 → 181,010 | 204,426 → 158,593 | 3,268 → 2,477 |
| 16 | 3,693,809 → 2,824,033 | 3,115,905 → 2,381,844 | 51,087 → 38,421 |
| 64 | 15,019,650 → 11,861,681 | 12,490,238 → 9,554,472 | 204,003 → 153,344 |

This page path took about 21–24% less measured time with lower allocation in this
local fixture. It demonstrates removal of repeated parsing. It does not measure
production HTTP throughput, full D2 or any SQL speedup.

## Reproduce

Use Go 1.27.2 and a supported C compiler for race. Unset external database/Kafka
integration variables for a local-only run:

```powershell
$env:GOMAXPROCS = '4'
go test -count=1 -timeout=120s ./internal/modules/events/usecase -run '^(TestPollStrictDecode|TestPollPageSequence|TestAcceptAtEnvelope)' -v
go test -race -count=1 -timeout=120s ./internal/modules/events/usecase ./internal/modules/events/domain ./internal/modules/events/delivery/httpinput ./internal/modules/events/delivery/httpoutbox ./internal/modules/events/delivery/resulttransport
go vet ./internal/modules/events/usecase ./internal/modules/events/domain ./internal/modules/events/delivery/httpinput ./internal/modules/events/delivery/httpoutbox ./internal/modules/events/delivery/resulttransport
go test -run '^$' -bench '^BenchmarkPollEnvelopeDecode$' -benchmem -benchtime=300ms -count=5 ./internal/modules/events/usecase
git diff --check 249dc0bb5890785c2821c252d5acfb8196f60751..HEAD
```

Raw logs and isolated exports are retained outside Git in the parent workspace's
`_worknotes/p16-http-delivery`. VM, SSH, cloud services, full D2 and new real-DB
durability/recovery runs were not used.
