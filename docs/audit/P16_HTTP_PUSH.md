# AUD-010: bounded HTTP Push dispatcher

Base: `249dc0bb5890785c2821c252d5acfb8196f60751`. Only `http_push` enters the new
pool. The synchronous Kafka loop, asynchronous Kafka dispatcher, Polling, SQL,
lease/checkpoint logic, experiment runner and defaults remain unchanged.

## Behavior and bounds

One Worker has **four fixed sending workers**. W2 can have at most **8** active
HTTP requests, W4 at most **16**, regardless of how many groups a Worker owns.
The same group has at most one active task through HTTP receipt and SQL outbox
ACK. Each task handles one record, then yields to the next ready group. This
prevents a 64-record page from monopolizing a pool slot.

The scheduler retains at most one existing page per valid group: 64 records and
4 MiB each, at most 16 groups per Worker. Thus staged envelope bytes are bounded
by 64 MiB per Worker, plus metadata. Jobs are unbuffered; the completion mailbox
has four slots. There is no per-record goroutine or unbounded page queue.
The initial 50 ms scan, empty-page scan pause, 3 s page/send-and-ACK deadlines
and exponential retry pause (100 ms through 3.2 s) are retained.

The order remains Master acceptance, verified HTTP receipt, SQL ACK. Failure or
lost reply retains the head record and its original MessageID. The existing
HTTP sender accepts a matching duplicate receipt, allowing safe retry after
Master acceptance with a lost response. SQL ACK errors also retain the head.

Owner, epoch and local tracking generation are checked before delivery and
before ACK. ACK always receives the captured owner tuple. PostgreSQL remains
the final fence if ownership changes during ACK itself. No ownership mutex is
held over page reads, HTTP or SQL. Forget/retrack of even the same tuple retires
old work. A late malformed page from an old generation cannot stop a new owner.
Current permanent delivery errors stop admission and retain pending records.
Cancellation/Close stops admission, cancels active I/O and joins all four workers
before returning. Outbox calls and the HTTP sender must honor their contexts.

## Local verification

All synchronization regressions use channels rather than sleeps:

- `a02` ACKs while `a01` is held in either Send or SQL ACK; `a01` record two
  cannot overtake record one's ACK.
- `httptest` loses the first reply after an in-memory receiver accepts the ID.
  Until a valid duplicate receipt arrives there is no outbox ACK; both attempts
  use the same ID and the receiver has one logical result. Retry pause is checked.
- Owner/epoch replacement and same-tuple retracking suppress old ACKs.
- A full four-slot pool cancels or closes without delivering waiting records,
  and all active calls exit. The bound is checked with 16 ready groups.
- A large page yields to a fifth ready group; SQL ACK retry preserves FIFO;
  a nil Send return after its context deadline does not ACK.
- Permanent failure retains pending data; stale malformed Page is retired.

These use an **in-memory outbox/receiver**, including the lost-response test.
They prove the dispatcher contract, not PostgreSQL crash durability.
An independent export of the base plus only this change passed race tests:
**32 tests/subtests PASS, 0 FAIL, 0 SKIP**, including existing sync/async tests.

## Short A/B

[Machine-readable samples and check hashes](evidence/P16_HTTP_PUSH_LOCAL.json).
Go 1.27.2, Windows/amd64, i7-12650H, GOMAXPROCS=4. Median of five repetitions,
`-benchtime=300ms`. One operation is the complete Run/shutdown of **16 groups
times 16 records**, including the common initial 50 ms scan. Fixture creation
and Track are outside the timer. The baseline is the exact old Run, renamed
in a test file. Page/ACK use an in-memory fake, Send is a no-op. The slow variant
explicitly injects 2 ms into each `a01` Send. No sockets or SQL are measured.

| Scenario | ns/batch before → after | B/batch before → after | allocs/batch before → after | Other groups' last ACK, ms before → after |
|---|---:|---:|---:|---:|
| No added delay | 52,470,333 → 51,731,600 | 1,142,549 → 1,150,872 | 18,482 → 18,805 | 52.106 → 51.578 |
| Slow a01 | 93,546,125 → 93,740,975 | 1,149,116 → 1,158,308 | 18,561 → 18,914 | 93.237 → 52.378 |

No material ordinary-case throughput improvement is established: the difference
is about 1.4% and includes timer/scheduler noise. With slow `a01`, other groups'
ACK latency falls about 44%, while whole-batch time is essentially unchanged
because the slow group still must finish. The pool adds some allocation cost.
These are local dispatcher measurements, **not full D2 throughput**.

## Reproduce

Use Go 1.27.2 and a supported C compiler for race on Windows. No external service
environment variables are needed; unset database/Kafka integration variables to
keep the optional integration tests skipped. From the repository root:

```powershell
$env:GOMAXPROCS = '4'
go test -count=1 -timeout=120s ./internal/modules/events/usecase -run '^TestHTTPPush' -v
go test -race -count=1 -timeout=120s ./internal/modules/events/usecase -run '^(TestHTTPPush|TestSync|TestAsync)'
go test -run '^$' -bench '^BenchmarkHTTPPushDispatcher$' -benchmem -benchtime=300ms -count=5 ./internal/modules/events/usecase
git diff --check 249dc0bb5890785c2821c252d5acfb8196f60751..HEAD
```

Raw logs and the isolated source exports are retained outside Git in the parent
workspace's `_worknotes/p16-http-delivery`; their hashes are in the JSON.
VM/SSH/cloud and full D2 were not used. AUD-001/005 ownership and cleanup rules
are preserved; rejected SQL batching/counter work and AUD-006/009/011/012 are
outside this change.
