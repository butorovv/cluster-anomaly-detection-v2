# P16-C1 — source bytes before publication

Baseline: `d748eebce3f41ba691f6196328a1814c90cffc41`. Scope: AUD-002 only.
No R1/V10, scientific/config/golden, migration, runner/lifecycle or infrastructure
changes. Red control and four wire transcripts were captured before this change.

## Contract selected before implementation

Every v1 replay, including Kafka and Compose, must compare the complete regular
Parquet file's SHA256 with the configured trusted digest before creating the
producer or publishing any record/marker. Do not derive a new binding from the
file or offer a bypass. Error/cancellation during verification means no output
and no replay success. Legacy NDJSON has no source binding; its defaults and wire
remain unchanged. Kafka requires v1, so legacy cannot bypass this check into Kafka.

Open once; hash and parse/read through that same owned `*os.File`. Hash the full
file, not just the selected time window. Use a fixed 64KiB buffer and a fixed-size
SectionReader, checking context between reads and after the scan. Size mismatch,
read error, canceled context or wrong digest fails closed. Regular-file checks
reject directories/devices and obvious FIFOs before opening them.

Path replacement/retargeting cannot redirect an already opened handle to another
file. Some OSes reject replacing the open path; on systems permitting it, replay
continues from the verified original object. A same-bytes relocation/symlink is
valid. There is no hash-then-reopen step. Footer/schema parsing must also complete
before any publication, and file metadata is rechecked after parsing.

### In-place mutation and cancellation boundaries

The source object must remain immutable throughout verification and replay,
including writes from the host outside a read-only container mount. This is a
trusted local prepared dataset contract, not an adversarial filesystem snapshot.
Compare size/mtime/identity before and after hashing, before/after each 256-row
decode batch, and on Close. Ordinary observed mutation fails the run. After
publication begins, errors can leave a confirmed prefix; it is not a success
and must not be blindly replayed into the same execution.

Metadata checks cannot prove immutability against a writer that restores size
and timestamps, malicious storage or concurrent writes hidden by filesystem
metadata. Supporting that threat model requires a filesystem snapshot/immutable
object or full private copy; this patch neither claims it nor makes large copies.
Context cancellation is cooperative at I/O chunk/batch boundaries; a currently
blocked local kernel read/open is not forcibly interrupted by a leaked goroutine.

## Implementation boundary

The platform Parquet reader owns the file and can verify before handing it to
the existing schema/row reader. The unverified OpenParquet path remains available
to existing offline/legacy callers. Only v1 Replayer selects the verified opener.
Frozen feature order, time/window validation, buffering, codec and scheduler are
unchanged. Existing producer source identity, bounded queues, record/marker ACK
order and failure cleanup stay unchanged.

Log the verified digest, bytes and hash-scan elapsed time on stderr. This is setup
work before publication; do not label it detector compute or infer throughput
speedup. The existing experiment runner already performs its own preflight hash
and is deliberately unchanged: a process-wall metric may include the additional
scan and must be interpreted accordingly in future comparisons.

## Entry points and acceptance

`cmd/replayer` calls the same Replayer function. `deploy/entrypoint.sh` supplies
v1/Kafka, the configured INPUT_SHA256 and the read-only `/assets` path. Bootstrap
registers identity but does not publish telemetry, so registration alone cannot
bypass verification in the Replayer. Experiment runner passes explicit v1 flags;
its earlier preflight is defense in depth, not a substitute for this entrypoint.

Required tests: fileA/hashA; fileB/hashA with zero sends; malformed/missing digest;
read/cancel failures; schema error with a correct hash; same-handle replacement,
symlink/relocation and ordinary in-place mutation; cancellation after verification
before first send; actual CLI process exit; Kafka setup not reached on mismatch;
byte-for-byte baseline wire parity. Compose command/mount binding must be reviewed
and tested without using the historical stack. Real assets/broker/Docker, if not
available independently, remain NOT_RUN. No D2 or golden changes.

Standard library contracts: [File.ReadAt](https://pkg.go.dev/os#File.ReadAt),
[CopyBuffer](https://pkg.go.dev/io#CopyBuffer). The context wrapper deliberately
exposes only Read; the pinned SHA writer exposes no ReaderFrom optimization.
