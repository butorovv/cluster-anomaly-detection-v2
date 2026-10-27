-- fixtures.sql is prepended by the runner.
SELECT p01_test.assert((SELECT count(*)=16 FROM cad_v2.execution_partitions),'all mapped partitions');
SELECT p01_test.expect_state($q$UPDATE cad_v2.executions SET partition_count=8$q$,'55000');
SELECT p01_test.expect_state($q$UPDATE cad_v2.runs SET delta_seconds=30$q$,'55000');
SELECT p01_test.expect_state($q$UPDATE cad_v2.execution_partitions SET partition_id=7 WHERE group_id='a01'$q$,'55000');
SELECT p01_test.expect_state($q$INSERT INTO cad_v2.execution_partitions VALUES
 ('00000000-0000-0000-0000-000000000001','a17',16,0)$q$,'23514');
SELECT p01_test.expect_state($q$SELECT 'bad'::cad_v2.sha256_hex$q$,'23514');
SELECT p01_test.expect_state($q$INSERT INTO cad_v2.executions
 SELECT '00000000-0000-0000-0000-000000000002',run_id,kafka_cluster_id,input_topic,consumer_group,
 partition_count,output_transport,checkpoint_interval_ticks,snapshot_schema_version,snapshot_codec,
 '{}',created_at FROM cad_v2.executions$q$,'23505');
SELECT p01_test.expect_state($q$INSERT INTO cad_v2.executions
 SELECT '00000000-0000-0000-0000-000000000002',run_id,kafka_cluster_id,input_topic,'missing-offsets',
 partition_count,output_transport,checkpoint_interval_ticks,snapshot_schema_version,snapshot_codec,
 '{}',created_at FROM cad_v2.executions$q$,'23502');
SELECT p01_test.assert(cad_v2.acquire_partition('00000000-0000-0000-0000-000000000001','a01',
 '00000000-0000-0000-0000-000000000011',interval '5 minutes')=1,'first epoch');
SELECT p01_test.expect_state($q$SELECT cad_v2.acquire_partition('00000000-0000-0000-0000-000000000001',
 'a01','00000000-0000-0000-0000-000000000012',interval '1 minute')$q$,'55000');
SELECT p01_test.expect_state($q$SELECT p01_test.commit(0,1501,'2022-01-18 08:00Z','[]',2)$q$,'55000');
SELECT p01_test.assert(p01_test.commit(0,1501,'2022-01-18 08:00Z',jsonb_build_array(p01_test.message()))=1,'first checkpoint');
SELECT p01_test.expect_state($q$SELECT p01_test.commit(0,1502,'2022-01-18 08:00:10Z')$q$,'40001');
SELECT p01_test.expect_state($q$SELECT p01_test.commit(1,1500,'2022-01-18 08:00:10Z')$q$,'55000');
SELECT p01_test.expect_state($q$SELECT p01_test.commit(1,1502,'2022-01-18 08:00:05Z')$q$,'22023');
SELECT p01_test.expect_state($q$SELECT p01_test.commit(1,1502,'2022-01-18 08:00:10Z',
 jsonb_build_array(p01_test.message() || '{"payload":{"changed":true}}'))$q$,'23505');
SELECT p01_test.expect_state($q$SELECT p01_test.commit(1,1502,'2022-01-18 08:00:10Z',
 jsonb_build_array(p01_test.message('group_result',1,'2022-01-18T08:01:00Z')))$q$,'22023');
SELECT p01_test.expect_state($q$SELECT cad_v2.commit_checkpoint(
 '00000000-0000-0000-0000-000000000001','a01','00000000-0000-0000-0000-000000000011',
 1,1,1502,'2022-01-18 08:00:10Z',1,'opaque-v1','\x010203',repeat('0',64),false,'[]')$q$,'22023');
SELECT p01_test.expect_state($q$SELECT cad_v2.commit_checkpoint(
 '00000000-0000-0000-0000-000000000001','a01','00000000-0000-0000-0000-000000000011',
 1,1,1502,'2022-01-18 08:00:10Z',2,'opaque-v1','\x010203',
 encode(sha256('\x010203'::bytea),'hex'),false,'[]')$q$,'22023');
SELECT p01_test.assert((SELECT next_offset=1501 AND checkpoint_revision=1
 FROM cad_v2.group_checkpoints),'failed calls leave durable checkpoint unchanged');
SELECT p01_test.assert((SELECT count(*)=1 FROM cad_v2.outbox_messages),'failed calls leave outbox unchanged');
SELECT p01_test.expect_state($q$SELECT cad_v2.renew_partition('00000000-0000-0000-0000-000000000001',
 'a01','00000000-0000-0000-0000-000000000011',1,interval '-1 second')$q$,'22023');
UPDATE cad_v2.partition_ownership SET lease_until=clock_timestamp()-interval '1 second' WHERE group_id='a01';
SELECT p01_test.expect_state($q$SELECT cad_v2.renew_partition('00000000-0000-0000-0000-000000000001',
 'a01','00000000-0000-0000-0000-000000000011',1,interval '1 minute')$q$,'55000');
SELECT p01_test.expect_state($q$SELECT p01_test.commit(1,1502,'2022-01-18 08:00:10Z')$q$,'55000');
-- Test-only admin restores lease so graceful release is exercised below.
UPDATE cad_v2.partition_ownership SET lease_until=clock_timestamp()+interval '5 minutes' WHERE group_id='a01';

-- Release + acquisition by same instance must still increment epoch.
SELECT cad_v2.release_partition('00000000-0000-0000-0000-000000000001','a01','00000000-0000-0000-0000-000000000011',1);
SELECT p01_test.assert(cad_v2.acquire_partition('00000000-0000-0000-0000-000000000001','a01',
 '00000000-0000-0000-0000-000000000011',interval '5 minutes')=2,'epoch survives release');
SELECT p01_test.expect_state($q$SELECT p01_test.commit(1,1502,'2022-01-18 08:00:10Z')$q$,'55000');
SELECT cad_v2.renew_partition('00000000-0000-0000-0000-000000000001','a01',
 '00000000-0000-0000-0000-000000000011',2,interval '5 minutes');

-- Final Flush can preserve offset and last_tick, but may occur only once.
SELECT p01_test.commit(1,1501,'2022-01-18 08:00Z','[]',2,true);
SELECT p01_test.expect_state($q$SELECT p01_test.commit(2,1502,'2022-01-18 08:00:10Z','[]',2)$q$,'55000');
SELECT 'PASS 01 invariants' AS result;
ROLLBACK;
