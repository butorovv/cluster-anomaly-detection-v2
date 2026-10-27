# Frozen detection

`group` and `local` preserve V1 `group-rbi2-v1` inference, identity and journal
semantics. `analysis` supplies the unchanged scientific numerical primitives.
`experiment` retains only source scanning, saved model envelopes and validators
required by these packages; the historical package name keeps the adaptation
small. It composes V2 telemetry/topology/Parquet and never imports PostgreSQL,
Kafka or HTTP. Go training primitives remain for V1 unit tests; `cmd/offline`
only loads frozen models and does not train/calibrate them.

`internal/app/offline` owns CLI/filesystem composition. No Worker/Snapshot API
is invented here. Provenance, input pins, validation status and the P01 merge
protocol are in `docs/migration/02-v1-db-frozen-rbi2.md`.
