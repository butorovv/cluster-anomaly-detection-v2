#!/usr/bin/env bash
set -euo pipefail
# P01 verification + integration handoff; no additional Markdown documents.
# Native: PGDATABASE=p01_test P01_TEST_ALLOW_RESET=1 ./scripts/test.sh
# Needs Python 3 stdlib + psql; a FRESH p01_* database and test administrator
# (temporary role and backend termination). Never use production data.
# Isolated Docker alternative, no published ports:
# docker run -d --name p01-postgres-test --network none \
#   -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=p01_test postgres:17
# P01_PSQL="$PWD/scripts/psql-docker.sh" P01_TEST_ALLOW_RESET=1 ./scripts/test.sh
# docker rm -f p01-postgres-test
# trust applies only to this disposable isolated fixture DB.
# Runner: up -> five SQL suites -> concurrent sessions -> down -> up.
# Leaves a clean schema without fixtures. On failure preserve diagnostics and use
# a fresh DB. Migrations are applied individually/transactionally, no version ledger.
#
# Integration in MAIN V2 still required:
# 1. Compare final TZ/V1 sources with hashes in 100_schema.up.sql.
# 2. Agree execution scope, snapshot DTO/codec/version, open envelope/revisions.
# 3. Transfer into a V2 branch, renumber for its migration runner; preserve atomicity.
# 4. Separate migration/runtime roles; minimal rights from scripts/grants.sql.
# 5. Implement explicit full Snapshot/Restore, validation and continuation parity
#    for open events, legacy R+B, evidence, Flush and delta=0/10/30/60.
# 6. Kafka assignment/revoke/seek PoC: exact DB next_offset after DB commit but before
#    Kafka commit; stale owner fencing, initial offset availability/retention gaps.
# 7. Connect atomic checkpoint/outbox to actual Polling/Push/Kafka adapters.
#    Polling remains HTTP Worker->Master; direct SQL polling Master is forbidden.
# 8. Test golden payloads/IDs/evidence and real fault injection in MAIN repository:
#    DB outage, corrupt snapshot, partial tick, ambiguous commit, revoke inside open
#    event, final Flush. SQL tests alone prove neither mathematical parity nor SLOs.
# Open questions: accepted-input/duplicate/late policy state; actual Go DTOs/message
# canonicalization; polling discovery/cursor; lease/timeouts; retention/cleanup/
# snapshot history; exact latency clocks and performance limits. See SQL comments.
# No Worker/Master/Kafka/Go repositories, V1 edits or V2 main merge/push in P01.
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
psql_bin=${P01_PSQL:-psql}
if [[ ${P01_TEST_ALLOW_RESET:-} != 1 ]]; then
  echo 'Set P01_TEST_ALLOW_RESET=1 and use a dedicated p01_* test database.' >&2
  exit 1
fi
"$psql_bin" -X -v ON_ERROR_STOP=1 -qAtc \
  "DO \$\$ BEGIN IF current_database() NOT LIKE 'p01\\_%' ESCAPE '\\' THEN RAISE EXCEPTION 'dedicated p01_* database required'; END IF; END \$\$;"
for f in "$root"/migrations/*.up.sql; do
  "$psql_bin" -X -v ON_ERROR_STOP=1 -q -1 -f - < "$f"
done
for f in "$root"/tests/0[1-5]_*.sql; do
  { echo 'BEGIN;'; cat "$root/tests/fixtures.sql" "$f"; } | "$psql_bin" -X -v ON_ERROR_STOP=1 -qAt -f -
done
python3 "$root/tests/concurrency.py"
for f in $(printf '%s\n' "$root"/migrations/*.down.sql | sort -r); do
  "$psql_bin" -X -v ON_ERROR_STOP=1 -q -1 -f - < "$f"
done
"$psql_bin" -X -v ON_ERROR_STOP=1 -qAtc \
  "DO \$\$ BEGIN IF to_regnamespace('cad_v2') IS NOT NULL THEN RAISE EXCEPTION 'down left schema behind'; END IF; END \$\$;"
for f in "$root"/migrations/*.up.sql; do
  "$psql_bin" -X -v ON_ERROR_STOP=1 -q -1 -f - < "$f"
done
echo 'PASS full up -> SQL tests -> concurrency -> down -> up'
