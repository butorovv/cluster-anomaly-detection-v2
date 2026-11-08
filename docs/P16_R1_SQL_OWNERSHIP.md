# P16-R1: committed outbox reads and ownership fencing

Baseline: `d748eebce3f41ba691f6196328a1814c90cffc41`.
Scope: AUD-001 only. No detector, checkpoint, Master join, transport protocol,
timeout, lifecycle or experimental infrastructure changes.

## Contract and choice (written before the production change)

The V7 `read_outbox` calls V3 `assert_owner`, taking an ownership row
`FOR UPDATE` until transaction completion. A held empty page is sufficient
to block `renew_partition` and produce `55P03` at the unchanged 2s lock timeout.

Two designs were considered:

1. A separate owner-validated, read-only page API, with a single statement
   snapshot and no ownership row lock. All mutations keep V3 fencing.
2. Compatible read/renew lock modes and a conditional renewal implementation.
   This would also change a writer's serialization/expiry contract and require
   broader takeover proofs. Changing Page to KEY SHARE alone would still
   conflict with the existing renewal's FOR UPDATE.

Choose (1). A page neither claims nor removes messages nor acknowledges delivery.
V4 already permits a committed envelope to finish network delivery after revoke.
Master deduplicates the complete envelope; new ownership replays pending output
with the same identity. Mutation fencing is the required exclusive boundary.

### Read linearization

`read_outbox_page` is SECURITY DEFINER, STABLE, with fixed
`search_path = pg_catalog, cad_v2` and no PUBLIC EXECUTE. Its statements use the
calling query's one MVCC snapshot S. Ownership tuple and both output tables are
read in S. Lease validity is `lease_until > statement_timestamp()` (T, the
current command start), not transaction_timestamp or return time. This is a
read authorization as of the command; it is not a delivery-time lease promise.
Using clock_timestamp inside a STABLE function would make repeated identical
calls time-dependent, so it is deliberately avoided.

The API requires READ COMMITTED and rejects other isolation levels rather than
authorizing new pages from an old REPEATABLE READ/SERIALIZABLE snapshot. Null
credentials, wrong owner/epoch, released or expired visible ownership fail even
for an empty page. Missing execution/group fails, never an empty success.
Cursor/batch/pending arguments are validated explicitly. Schema/execution/group
and ordering/filtering of the union of events and fragments remain unchanged.

With concurrent renewal, S may conservatively see the previous expiry and fail;
the read does not wait for or extend ownership. With concurrent takeover, an
already authorized page may return its old committed output. A fresh READ
COMMITTED command after takeover rejects the old owner. In-flight delivery and
stale mutation are different operations.

Checkpoint state, offset, events and fragments commit atomically, so S sees the
whole committed boundary or its predecessor. An uncommitted/rolled-back output
is never visible. A concurrent ACK may make a returned pending row redundant;
exact delivery retries remain safe. Page cursor is an enumeration position,
not a durable ACK. It cannot advance Master progress by itself.

The adapter issues Page as a standalone pool query, never inside its Save TX.
As with any PostgreSQL SELECT, a caller that instead composes Page inside its
own writer transaction can see its own earlier uncommitted writes. This is not
a commit attestation API; publishing from that transaction remains forbidden
by V4. The no-dirty-read tests use a separate reader, as production does.

### Unchanged write linearization

Acquire, Save/commit_checkpoint, stage_tick_fragment, renew, release,
ack_outbox/ack_delivery/ack_poll_cursor and advertise continue to use V3
assert_owner FOR UPDATE. Owner/epoch/expiry are checked after lock acquisition;
checkpoint rechecks expiry after staging. An old transaction which lawfully
acquired the write fence still serializes before takeover. Nothing claims
that COMMIT occurs before the lease's wall-clock expiry.

```
Before: Page TX -- ownership FOR UPDATE -- COMMIT
                     ^ renew waits -> 55P03
After:  Page TX -- snapshot owner+committed page -- COMMIT
        Renew   -- ownership FOR UPDATE + lease UPDATE -- COMMIT
        ACK/Save -- same exclusive fence -- COMMIT
```

## Migration and deployment

V10 adds the API; V1-V9 and all writer function definitions stay immutable.
The Go adapter calls only the new API. Runtime grants authorize it for Worker,
not Master/PUBLIC, and remove the old API from the reviewed Worker allowlist.
The old function remains for historical/admin verification; it still locks.

Quiesce Worker input/output and Worker HTTP Polling requests; finish/rollback
their transactions. Apply V10 as migration owner, apply runtime grants, deploy
the new Worker binary, then resume. Master schema/protocol are unchanged.
This is not an authorization for mixed-version live rollout. Old binaries
using the refreshed grants fail closed on the old function. A rollback is
controlled quiescence plus old binaries AND their old runtime grants, retaining
V10/history/data; no down migration or checksum rewriting. Rolling back also
restores the known contention risk. Retain the V10-capable administrative
migration runner: the old V9 Apply correctly refuses a history containing V10;
do not run an old auto-bootstrap during rollback.

## Required evidence

Deterministic V9 red control (empty/nonempty page, observed pg_blocking_pids,
55P03), V10 green with the Page TX still open, and renewal headroom. Also:
fresh/upgrade/rollback/retry/grants; no dirty read; stale Page and all stale
mutations; expiry/takeover/release/reacquire; late delivery exact retry/conflict;
cursor and checkpoint/recovery regressions. No full D2 or P15-4B resources.

STABLE snapshot semantics: [PostgreSQL 17 function volatility](https://www.postgresql.org/docs/17/xfunc-volatility.html).
Execution evidence and outstanding gates belong in P16-R1-REPORT.md.
