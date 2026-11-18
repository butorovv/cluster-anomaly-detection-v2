# P16: unique-fragment counter, 16 groups

Decision on 2026-11-19: **keep `COUNT(*)`**. An atomic counter prototype passed
correctness checks but did not demonstrate a reproducible improvement in whole
transactions. No production SQL, migration, grants or Go code is changed by this
study. The two Go optimizations at `d6ec91e` and `a1c4cb0` remain included.

Machine-readable measurements, offsets, digests and retained artifact hashes:
[P16_FRAGMENT_COUNTER_16.json](evidence/P16_FRAGMENT_COUNTER_16.json).

## Compared implementations

Both variants use the source archive of
`a1c4cb01a3a6e5c9b8c69ffdea17768296255b37` and one test executable. The baseline
keeps the production `acceptFragmentTx` completeness `COUNT(*)`.

The experimental database adds a table keyed by `(execution_id, event_time)` with
`unique_fragments` constrained to 1..16. Its function is the existing V9
`accept_tick_fragment` with one additional upsert **after** a successful fragment
insert, under the existing timestamp advisory lock:

```sql
INSERT INTO cad_v2.master_tick_fragment_counts
       (execution_id,event_time,unique_fragments)
VALUES (x,(m->>'event_time')::timestamptz,1)
ON CONFLICT (execution_id,event_time) DO UPDATE
SET unique_fragments=cad_v2.master_tick_fragment_counts.unique_fragments+1;
```

Existing fragments are backfilled by grouped counts before using the prototype.
The existing exact-duplicate early return and conflicting-retry rejection run
before this upsert. The Go overlay replaces only the completeness scalar with a
counter lookup in the same separate READ COMMITTED statement. Thus its snapshot
is acquired after the potentially waiting fragment statement. Final writes
remain 16 individual writes. No SQL batching is involved.

## Method and isolation

- New private PostgreSQL 17.5 cluster, loopback only, dedicated data directory and
  disposable database per block. No existing service or data was used. The
  private cluster was stopped normally after the tests.
- Go 1.27.2 Windows/amd64, GOMAXPROCS=4, runtime `SET ROLE counter_master`.
  `fsync`, `synchronous_commit` and `full_page_writes` enabled, 128 MiB shared
  buffers, autovacuum disabled for both variants, 30 minute checkpoint timeout.
- Existing `newMicroFixture`: **16 groups**, 18 node observations per group.
  Identical frozen envelopes, identity and input hash in all 12 blocks. This is a
  synthetic local SQL fixture, not the full D2 input or its golden verification.
- Three alternating series at one and four connections. Order is COUNT then
  counter in series 1/3, counter then COUNT in series 2. Each block has 8 warmup
  timestamps and 48 measured timestamps: 720 ordinary and 48 final transactions.
  A checkpoint is completed after warmup, before the WAL snapshot.
- Each worker owns a fixed subset of group/partition streams and consumes their
  timestamps in order. Four workers may process different timestamps concurrently
  while preserving each partition's offset order.
- Calls use the real `MasterStore.AcceptKafka` SQL repository entry point,
  without a Kafka broker. Latency spans BEGIN start through COMMIT completion,
  including fragment insertion, completeness lookup, final join when required,
  offset CAS and durable COMMIT. Pool acquisition is excluded from latency and
  included in workload throughput. Quantiles use nearest rank.
- A QueryPerformanceCounter tracer times both variants identically and detects
  the final transaction by its actual `master_tick_joins` insert. It does not
  assume which group arrives last.
- WAL is the isolated cluster's insert-LSN difference. `pg_stat_wal`
  record/byte/FPI/write/sync deltas are retained as diagnostics in the JSON, but
  asynchronous flushing includes delayed setup contributions in some blocks
  (stats bytes exceed the exact LSN delta). Those stats deltas are **not accepted
  for A/B attribution**; all reported WAL comparisons use only exact LSN deltas.
  Active runtime backend waits
  are sampled from `pg_stat_activity` by a nominal 2 ms ticker. **Lock samples are
  observations, not distinct waits or wait duration.** Sampling rounds are
  reported because the ticker and SQL observer do not guarantee exact intervals.
- Before measurement, acceptance was set to at least 5% throughput gain in all
  three series without worsening ordinary or final p95. The observed signs also
  fail the broader requirement of a reproducible gain independently of this
  numerical threshold.

## Full transaction results

Throughput is fragment transactions/s, not D2 group-ticks/s. The table preserves
execution order. Every row passed digest, cardinality and offset verification.

| Series | Connections | Variant | tx/s | Ordinary p50 / p95, ms | Final p50 / p95, ms | WAL LSN bytes | Lock samples / rounds |
|---|---:|---|---:|---:|---:|---:|---:|
| 1 | 1 | COUNT | 148.44 | 2.285 / 3.693 | 68.038 / 88.071 | 14168672 | 0 / 2416 |
| 1 | 1 | counter | 147.15 | 2.278 / 4.012 | 67.719 / 87.148 | 14277800 | 0 / 2437 |
| 1 | 4 | COUNT | 163.72 | 4.251 / 8.793 | 78.255 / 126.215 | 14204864 | 183 / 2122 |
| 1 | 4 | counter | 164.60 | 4.388 / 9.601 | 74.722 / 133.530 | 14325648 | 190 / 2110 |
| 2 | 1 | counter | 158.11 | 2.009 / 3.675 | 62.554 / 93.355 | 14277848 | 0 / 2241 |
| 2 | 1 | COUNT | 176.88 | 1.871 / 2.885 | 57.373 / 71.568 | 14168632 | 0 / 2032 |
| 2 | 4 | counter | 230.53 | 2.923 / 8.095 | 59.965 / 76.430 | 14319040 | 118 / 1522 |
| 2 | 4 | COUNT | 246.47 | 2.488 / 5.920 | 55.450 / 66.695 | 14211352 | 102 / 1407 |
| 3 | 1 | COUNT | 118.85 | 3.089 / 5.059 | 85.268 / 108.759 | 14168656 | 0 / 2979 |
| 3 | 1 | counter | 160.45 | 2.147 / 3.672 | 63.228 / 74.234 | 14277832 | 0 / 2219 |
| 3 | 4 | COUNT | 226.45 | 2.484 / 5.817 | 60.233 / 82.103 | 14204960 | 137 / 1539 |
| 3 | 4 | counter | 134.36 | 5.114 / 10.186 | 103.899 / 137.106 | 14331504 | 227 / 2175 |

Counter throughput changes against its paired baseline:

| Connections | Series 1 | Series 2 | Series 3 |
|---|---:|---:|---:|
| 1 | -0.87% | -10.61% | +35.00% |
| 4 | +0.54% | -6.47% | -40.67% |

Large drift between series prevents attributing the isolated +35% result to the
counter. Neither concurrency demonstrates repeatable improvement. Ordinary and
final p95 also regress in some pairs. WAL increases in every pair, by about
0.76..0.89%. At four connections most sampled heavyweight lock waits are the
existing advisory lock. Series 1's counter additionally has two `Lock/extend`
backend observations. Lightweight waits are listed separately in the JSON.
There were no deadlocks or SQL/lock timeouts in accepted blocks.

## Correctness and evidence status

`TestCounterRegression`: **PASS**, six subtests, no SKIP:

- Exact retry returns unaccepted and does not increment the counter. A changed
  payload with the same message ID is rejected with SQLSTATE 23505.
- Explicit rollback removes both the new fragment and its increment. An injected
  failure during final-result insertion rolls back the last fragment, counter,
  partial final rows and join; retry succeeds at the same offset.
- Backfill from seven existing fragments yields seven, an existing duplicate
  keeps seven, and completion yields sixteen.
- Competing arrivals 15/16 are forced to overlap. The second backend is observed
  waiting on a lock before the first transaction commits. Completion and retries
  yield exactly **one join and 16 final results**.
- Each scenario compares baseline/counter final message ID plus typed payload
  digest and all 16 durable offsets. The independent fixture expected digest is
  also checked.

`TestCounterMeasurements`: **PASS**, 12 blocks, no SKIP. Each has 56 total joins
and 896 final results including warmup, identical final digest across variants
and all 16 partition next-offsets equal to 56. All 9,216 timed calls have a
recorded BEGIN-to-COMMIT sample, including 576 final transactions.

Two earlier measurement attempts failed due to harness faults: timestamp
scheduling violated per-partition offset order, and the coarse Windows clock
reported zero-duration COMMITs. Their regression phases passed. Their logs are
retained and explicitly excluded from accepted measurements. The corrected run
uses FIFO streams and QueryPerformanceCounter. Production code was unchanged
during all attempts.

The local study directory outside the repository is
`_worknotes/p16-counter-local` in the parent workspace. It retains the exact
source archive, SQL prototype, Go overlay/regressions, launcher, executable,
raw per-transaction samples and all test logs. `qpc-run/artifacts.sha256.json`
records their hashes, also copied into the committed JSON. No raw logs, test
overlay, temporary PostgreSQL setup files or local absolute paths are committed.
VM, full D2, Kafka end-to-end throughput and production golden were **not run**.
Historical D2 slowdown remains open. Kafka pipeline, outside groups, lease,
offset semantics and golden are unchanged; production SQL batching stays rejected.
