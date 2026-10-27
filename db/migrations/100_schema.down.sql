-- Destructive: export/backup state first. No CASCADE into external objects.
DROP TRIGGER executions_seed ON cad_v2.executions;
DROP FUNCTION cad_v2.seed_partitions();
DROP TABLE cad_v2.master_events, cad_v2.master_results, cad_v2.master_inbox;
DROP TABLE cad_v2.outbox_messages, cad_v2.group_checkpoints, cad_v2.partition_ownership;
DROP TABLE cad_v2.execution_partitions, cad_v2.executions, cad_v2.runs;
DROP FUNCTION cad_v2.immutable_identity();
DROP DOMAIN cad_v2.sha256_hex;
DROP SCHEMA cad_v2;
