-- Optional integration step, NOT a migration; run as migration owner/admin.
-- Supply EXISTING non-owner, non-superuser roles using psql variables:
-- psql "$DATABASE_URL" -v worker_role=cad_worker -v master_role=cad_master -f scripts/grants.sql
-- Neither role may inherit migration ownership. No role/password is created here.
BEGIN;
GRANT USAGE ON SCHEMA cad_v2 TO :"worker_role", :"master_role";
REVOKE ALL ON ALL TABLES IN SCHEMA cad_v2 FROM :"worker_role", :"master_role";
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA cad_v2 FROM :"worker_role", :"master_role";
GRANT SELECT ON cad_v2.runs, cad_v2.executions, cad_v2.execution_partitions,
  cad_v2.partition_ownership, cad_v2.group_checkpoints, cad_v2.outbox_messages TO :"worker_role";
GRANT SELECT ON cad_v2.runs, cad_v2.executions, cad_v2.execution_partitions,
  cad_v2.master_inbox, cad_v2.master_results, cad_v2.master_events TO :"master_role";
GRANT EXECUTE ON FUNCTION
  cad_v2.acquire_partition(uuid,text,uuid,interval),
  cad_v2.renew_partition(uuid,text,uuid,bigint,interval),
  cad_v2.release_partition(uuid,text,uuid,bigint),
  cad_v2.commit_checkpoint(uuid,text,uuid,bigint,bigint,bigint,timestamptz,integer,text,bytea,cad_v2.sha256_hex,boolean,jsonb),
  cad_v2.ack_outbox(uuid,text,uuid,bigint,text) TO :"worker_role";
GRANT EXECUTE ON FUNCTION cad_v2.accept_result(uuid,jsonb) TO :"master_role";
COMMIT;

-- P09 extensions (run after applying V6).
GRANT SELECT ON cad_v2.worker_endpoints,cad_v2.partition_ownership,
 cad_v2.master_poll_cursors,cad_v2.result_topic_bindings,cad_v2.result_consumer_offsets TO :"master_role";
GRANT EXECUTE ON FUNCTION
 cad_v2.advertise_worker(uuid,text,uuid,bigint,text),
 cad_v2.read_outbox(uuid,text,uuid,bigint,bigint,integer,integer,boolean),
 cad_v2.ack_poll_cursor(uuid,text,uuid,bigint,bigint,integer) TO :"worker_role";
GRANT EXECUTE ON FUNCTION
 cad_v2.advance_poll_cursor(uuid,text,bigint,integer),
 cad_v2.bind_result_source(uuid,text,text,text,jsonb),
 cad_v2.advance_result_offset(uuid,text,integer,bigint,bigint) TO :"master_role";

-- P10 extensions (run after applying V7).
GRANT SELECT ON cad_v2.worker_tick_fragments TO :"worker_role";
GRANT SELECT ON cad_v2.master_tick_fragments,cad_v2.master_tick_joins TO :"master_role";
GRANT EXECUTE ON FUNCTION cad_v2.stage_tick_fragment(uuid,text,uuid,bigint,bigint,integer,jsonb),cad_v2.ack_delivery(uuid,text,uuid,bigint,text) TO :"worker_role";
GRANT EXECUTE ON FUNCTION cad_v2.accept_tick_fragment(uuid,jsonb) TO :"master_role";
GRANT INSERT ON cad_v2.master_tick_joins TO :"master_role";
