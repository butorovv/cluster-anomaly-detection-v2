# PostgreSQL V1 baseline

`migrations/V1__core_schema.sql` is byte-identical to V1 commit
`c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc`. Its upstream origin is in
`source.json`; V1/V2 checksums are in `../docs/migration/02-provenance.json`.
Five canonical `metric_type` rows belong to production core. All other sample
DML is isolated in `demo/` and is never read by the runner.

Requires PostgreSQL 17 (integration tests print the actual server version), Go
1.27.2, a new database, and a role allowed to create schema/tables/functions:

```sh
DATABASE_URL='postgresql://...' go run ./db/cmd/migrate
# Re-running prints applied 0 migration(s).
TEST_DATABASE_URL='postgresql://.../postgres' go test ./db -v
```

The integration test requires CREATEDB and creates/drops only its randomly named
`p02_test_*` databases. It does not reset the configured database or volumes.
The runner supports up only, embeds the single `V<number>__description.sql`
series, serializes runs with a transaction advisory lock, and commits DDL plus
`public.schema_migrations` in one transaction. Versions must start at 1 and be
contiguous; duplicate numbers, changed checksums/names, unknown applied versions,
and an existing untracked `project` schema are errors. It never guesses history
for an already populated database. Use a separate reviewed adoption plan there.
There is no automatic down: V1 has no safe lossless inverse.

Optional demonstration, only in a separate demo database after applying core:

```sh
psql "$DEMO_DATABASE_URL" -v ON_ERROR_STOP=1 -1 -f db/demo/V1__demo_data.sql
```

`queries/f18.sql` retains V1's historical long-format query ($1 node ID, $2 UTC
timestamp). No importer, PostgreSQL telemetry adapter, or worker-state repository
is added here. `project.event` is an operational event, not RBI2 `master_events`.

## P01 integration contract

Reserve **V1 for this core**. P01 migrations follow at V2+ in their dependency
order. No P01 files have been changed or renumbered. At integration choose exactly
one runner and one history table; the runner here is provisional while P01 is
not visible. If retaining P01's runner, replace this runner/ledger integration
and adapt this test suite; do not operate both ledgers. Check duplicate schemas,
objects, constraint/index/type names, FK targets and `search_path`. Never rename
already applied migrations on shared/persistent databases; require a controlled
upgrade plan. Test both fresh V1→P01 and upgrade from an existing tracked V1,
including preservation of telemetry. See the migration report for the checklist.
