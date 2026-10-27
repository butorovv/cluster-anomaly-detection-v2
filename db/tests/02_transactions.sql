SELECT cad_v2.acquire_partition('00000000-0000-0000-0000-000000000001','a01',
 '00000000-0000-0000-0000-000000000011',interval '5 minutes');
SAVEPOINT crash_before_commit;
SELECT p01_test.commit(0,1501,'2022-01-18 08:00Z',jsonb_build_array(p01_test.message()));
ROLLBACK TO crash_before_commit;
SELECT p01_test.assert(NOT EXISTS (SELECT 1 FROM cad_v2.group_checkpoints),'crash before commit: no state');
SELECT p01_test.assert(NOT EXISTS (SELECT 1 FROM cad_v2.outbox_messages),'crash before commit: no output');
SELECT p01_test.commit(0,1501,'2022-01-18 08:00Z',jsonb_build_array(p01_test.message()));

-- Batched outputs become visible with their final durable boundary.
SELECT p01_test.commit(1,1507,'2022-01-18 08:00:30Z',jsonb_build_array(
 p01_test.message('group_result',1,'2022-01-18T08:00:10Z'),
 p01_test.message('group_result',1,'2022-01-18T08:00:20Z'),
 p01_test.message('group_result',1,'2022-01-18T08:00:30Z')));
SELECT p01_test.assert((SELECT count(*)=3 FROM cad_v2.outbox_messages
 WHERE checkpoint_revision=2),'one barrier for three outputs');

-- Mid-batch failure rolls back both new checkpoint and already-inserted output.
SELECT p01_test.expect_state($q$SELECT p01_test.commit(2,1510,'2022-01-18 08:01:00Z',jsonb_build_array(
 p01_test.message('group_result',1,'2022-01-18T08:00:40Z'),
 p01_test.message('group_result',1,'2022-01-18T08:00:50Z') || '{"config_id":"wrong"}'))$q$,'22023');
SELECT p01_test.assert((SELECT next_offset=1507 FROM cad_v2.group_checkpoints),'failed batch: offset');
SELECT p01_test.assert((SELECT count(*)=4 FROM cad_v2.outbox_messages),'failed batch: no leaked output');

SELECT p01_test.assert(cad_v2.accept_result('00000000-0000-0000-0000-000000000001',p01_test.message()),'Master first delivery');
SELECT p01_test.assert(NOT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',p01_test.message()),'lost ack retry');
SELECT cad_v2.ack_outbox('00000000-0000-0000-0000-000000000001','a01',
 '00000000-0000-0000-0000-000000000011',1,p01_test.message()->>'message_id');
SELECT cad_v2.ack_outbox('00000000-0000-0000-0000-000000000001','a01',
 '00000000-0000-0000-0000-000000000011',1,p01_test.message()->>'message_id');
SELECT p01_test.assert((SELECT count(*)=1 FROM cad_v2.master_results),'duplicate-free final tick');
SELECT p01_test.expect_state($q$SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',
 p01_test.message() || '{"payload":{"conflict":true}}')$q$,'23505');
SELECT p01_test.expect_state($q$SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',
 p01_test.message() || '{"message_id":"different-id-same-logical-key"}')$q$,'23505');
SELECT 'PASS 02 transactions' AS result;
ROLLBACK;
