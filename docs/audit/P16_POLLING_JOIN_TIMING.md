# Polling: page cancellation at the COMMIT acknowledgement boundary

Base: `987ce713935906458e78315bd4f1a4e1b0985b55`.
Branch: `fix/p16-polling-join-timing`. No VM invocation or D2 rerun was used.

## Historical evidence

The original reports and `http-d2-artifacts.tgz` remain unchanged. The archive
SHA-256 is `f2ec17395e886fc7abaa81dcdd478d99bcf023a2db750ab97c08996ac641d771`.
Read-only correlation is retained outside Git in the parent workspace's
`_worknotes/p16-polling-join-timing/correlation.json`; its inputs are the copied
`results/evidence/` and logs extracted into a new directory from that archive.

Both runs have golden PASS: 46,080 raw/temporal rows, 1,284 positive ticks,
750 parent events, matching canonical digests. Independent lifecycle review
confirms child shutdown. Worker measurement, durable checkpoint, and one output
attempt exist for every missing fragment. There is **no** Master
`result measurement` for its deterministic MessageID, whether accepted or
duplicate. A parser cannot recover an absent event.

Start with W4, a11, `2022-01-18T11:49:40Z`:

- MessageID: `120130784b65fbbc91fb29598a74624ca0062b374065a1cc105e148f01cfcc1f`.
- Worker-00 lines 23636–23638 record compute, durable confirmation, and checkpoint.
- Its output attempt is worker-00 line 39053 at `11:15:44.028382185Z`.
- The preceding a11 tick (11:49:30) is confirmed at `11:15:46.918068714Z`
  (Master line 39107).
- Poller logs a11 `timeout: context deadline exceeded` at
  `11:15:46.988865473Z`.
- The next a11 tick (11:49:50) is received at `11:15:48.423882469Z` and
  confirmed at `11:15:48.484821459Z` (Master line 39662).

All other holes have the same observable frontier: one output attempt, the
preceding group's confirmed tick, a same-group deadline error, then the next
tick's receipt. Times below are UTC; `next` is received_at, not committed_at.

| Run/group | Missing tick | Output attempt | Previous confirmed tick | Deadline error | Next receipt |
|---|---|---|---|---|---|
| W4/a11 | 08:18:50 | 11:13:37.703827126 | 11:13:40.598677961 | 11:13:40.668403910 | 11:13:41.901316180 |
| W4/a11 | 09:08:50 | 11:14:05.366346645 | 11:14:08.272439335 | 11:14:08.338400911 | 11:14:09.295746534 |
| W4/a11 | 09:52:20 | 11:14:30.363224918 | 11:14:33.267023159 | 11:14:33.325194784 | 11:14:35.591596316 |
| W4/a11 | 11:27:40 | 11:15:30.830676946 | 11:15:33.753574579 | 11:15:33.804849442 | 11:15:35.326599662 |
| W4/a11 | 11:34:20 | 11:15:35.269862955 | 11:15:38.116233896 | 11:15:38.199165649 | 11:15:39.625587731 |
| W4/a11 | 11:49:40 | 11:15:44.028382185 | 11:15:46.918068714 | 11:15:46.988865473 | 11:15:48.423882469 |
| W4/a11 | 11:57:10 | 11:15:48.369423266 | 11:15:51.280681679 | 11:15:51.340079241 | 11:15:53.058756963 |
| W4/a11 | 14:26:10 | 11:16:50.340717507 | 11:16:53.257493875 | 11:16:53.311458068 | 11:16:53.488674103 |
| W2/a13 | 08:47:10 | 11:25:43.121251720 | 11:25:46.035755440 | 11:25:46.099577197 | 11:25:46.278625023 |
| W2/a13 | 09:51:30 | 11:26:18.286987773 | 11:26:21.191396944 | 11:26:21.257108791 | 11:26:21.410139631 |
| W2/a13 | 15:50:30 | 11:29:46.000336076 | 11:29:48.906181529 | 11:29:48.975686628 | 11:29:49.152838072 |

`output_retries=0` does not rule out Polling retries: that counter is not a
per-page/per-transaction retry trace. No historical PostgreSQL protocol trace,
transaction ID/COMMIT acknowledgement, or per-call Master durable-cursor snapshot
was retained. Input `next_offsets` and Worker checkpoint revisions are **not**
the Master polling cursor. Thus these artifacts do not directly establish the
individual historical transaction's error or exact COMMIT/ACK time.

## Where confirmation can disappear

`MasterStore.AcceptAt` writes acceptance and `advance_poll_cursor` in one
Read Committed transaction. It formerly called `tx.Commit(pageContext)`.
With pgx's default cancellation handler, expiry of that context can close the
connection while a successful PostgreSQL COMMIT reply is in flight.
`Master.acceptAtEnvelope` then returns the error; `Master.observed` deliberately
omits successful `result measurement` on an error. Meanwhile a subsequent page
reads the advanced durable cursor and can skip the already committed fragment.

The three cases must remain distinct:

1. **Rollback, then accepted retry:** neither the result nor cursor persists;
   a successful retry records its own real acceptance. This is reproduced in
   the pre-COMMIT-cancellation test, not established for the historical holes.
2. **COMMIT succeeds but its acknowledgement is lost:** result and cursor
   persist together, while the original acceptance observation is absent.
   A real local PostgreSQL wire proxy reproduces this exactly on the base code
   for both AcceptAt APIs. It explains all eleven historical frontiers and the
   complete golden output, but is not a replacement for missing historical
   per-transaction evidence.
3. **Observation exists but the reader misses it:** ruled out for these eleven
   MessageIDs by the raw-log scan; no reader change is needed.

The old runs retain **incomplete timing**: W4 has 2,872/2,880 complete joins in
timing logs, W2 2,877/2,880. Their precise original committed_at is unknown.
Neither later retries, cursor reads, nor process duration fill those holes.

## Fix and limits

Only Polling's `AcceptAt` COMMIT completion changes. Queries and cursor update
still use the page context. A canceled context checked **before the commit
decision** rolls the entire transaction back. Once that decision is made, a
fresh context retaining request values gives COMMIT at most two seconds to
receive its own acknowledgement, independent of subsequent page cancellation.
This follows the existing bounded transaction-cleanup policy; it is not tuning
the page deadline. Transaction finalization may finish up to that bounded budget
after page cancellation, while admission and subsequent SQL work remain canceled.

Only an actual successful original ACK produces the existing committed_at
observation. A permanently lost reply, broken connection, or completion timeout
still returns an error and produces no successful measurement. A retry keeps
the same MessageID, remains a duplicate, and never replaces the first observation.
The result/cursor transaction and dedup SQL are unchanged. Poller deadlines,
100-ms interval, page size, concurrency, Kafka and HTTP Push paths, measurement
formulas, and old artifacts are unchanged.

## Local verification

Private PostgreSQL 17.5 (Windows/amd64), loopback-only, fsync and
synchronous_commit on; Go 1.27.2. This differs from VM PostgreSQL 17.12 and is a
controlled mechanism reproduction, not a new D2 performance result.

Before the production change, both new cancellation-at-ACK subtests failed:
PostgreSQL had 4 fragments / 4 final results / 1 join and advanced polling cursor,
but the caller returned `context canceled` and emitted no measurement.

Regression coverage includes both byte/decoded APIs, held real successful ACK,
actual permanent ACK loss, bounded held-ACK timeout, cancellation after the
cursor UPDATE but before COMMIT, rollback cursor/result atomicity, duplicate-safe
retry, and rejection of an incomplete join measurement. Exact check outcomes and
commands are retained in `_worknotes/p16-polling-join-timing/`.

Observed results: focused PostgreSQL regressions 10 PASS / 0 FAIL / 0 SKIP;
the complete unit/integration and race runs each 160 PASS / 0 FAIL / 2 SKIP.
The two skips are the opt-in `TestSQLMicroMeasurements` and
`TestTimestampSQLMeasurements`, not the new regressions. `go vet` passed for
events usecase, events PostgreSQL repository and experiment packages.
`git diff --check` passed. SHA-256 rechecking confirmed all 84 original local
artifact files (including the archive) unchanged.
