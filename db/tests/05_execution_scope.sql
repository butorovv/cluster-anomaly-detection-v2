-- Two output experiments keep the same mathematical IDs and stable message ID.
INSERT INTO cad_v2.executions
SELECT '00000000-0000-0000-0000-000000000002',run_id,kafka_cluster_id,input_topic,'p01-second',
 partition_count,'http_push',checkpoint_interval_ticks,snapshot_schema_version,snapshot_codec,
 start_offsets,created_at FROM cad_v2.executions;
SELECT cad_v2.acquire_partition('00000000-0000-0000-0000-000000000001','a01',
 '00000000-0000-0000-0000-000000000011',interval '5 minutes');
SELECT cad_v2.acquire_partition('00000000-0000-0000-0000-000000000002','a01',
 '00000000-0000-0000-0000-000000000012',interval '5 minutes');
SELECT p01_test.commit(0,1501,'2022-01-18 08:00Z',jsonb_build_array(p01_test.message()));
SELECT cad_v2.commit_checkpoint('00000000-0000-0000-0000-000000000002','a01',
 '00000000-0000-0000-0000-000000000012',1,0,1501,'2022-01-18 08:00Z',1,'opaque-v1',
 '\x010203',encode(sha256('\x010203'::bytea),'hex'),false,jsonb_build_array(p01_test.message()
 || '{"execution_id":"00000000-0000-0000-0000-000000000002"}'));
SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',p01_test.message());
SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000002',p01_test.message()
 || '{"execution_id":"00000000-0000-0000-0000-000000000002"}');
SELECT p01_test.assert((SELECT count(*)=2 AND count(DISTINCT message_id)=1 FROM cad_v2.outbox_messages),'independent outboxes, stable IDs');
SELECT p01_test.assert((SELECT count(*)=2 FROM cad_v2.master_results),'independent Master projections');
SELECT p01_test.expect_state($q$SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000002',
 p01_test.message())$q$,'22023');
SELECT 'PASS 05 execution scope' AS result;
ROLLBACK;
