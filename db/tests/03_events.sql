SELECT cad_v2.acquire_partition('00000000-0000-0000-0000-000000000001','a01',
 '00000000-0000-0000-0000-000000000011',interval '5 minutes');
SELECT p01_test.commit(0,2,'2022-01-18 08:00Z',
 jsonb_build_array(p01_test.message('rbi2_event',1,'2022-01-18T08:00:00Z',false)));
SELECT p01_test.commit(1,4,'2022-01-18 08:00:10Z',
 jsonb_build_array(p01_test.message('rbi2_event',2,'2022-01-18T08:00:10Z',false)));
SELECT p01_test.expect_state($q$SELECT p01_test.commit(2,6,'2022-01-18 08:00:20Z',jsonb_build_array(
 p01_test.message('rbi2_event',3,'2022-01-18T08:00:20Z',false) || jsonb_build_object('payload',
 (p01_test.message('rbi2_event',3,'2022-01-18T08:00:20Z',false)->'payload') ||
 '{"detected":"2022-01-18T08:00:10Z","last_positive":"2022-01-18T08:00:10Z"}')))$q$,'55000');
SELECT p01_test.commit(2,6,'2022-01-18 08:00:20Z',
 jsonb_build_array(p01_test.message('rbi2_event',3,'2022-01-18T08:00:20Z',true)));
SELECT p01_test.expect_state($q$SELECT p01_test.commit(3,8,'2022-01-18 08:00:30Z',
 jsonb_build_array(p01_test.message('rbi2_event',4,'2022-01-18T08:00:30Z',false)))$q$,'55000');

-- Delivery order: closed v3, open v1, open v2, retry closed v3.
SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',p01_test.message('rbi2_event',3,'2022-01-18T08:00:20Z',true));
SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',p01_test.message('rbi2_event',1,'2022-01-18T08:00:00Z',false));
SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',p01_test.message('rbi2_event',2,'2022-01-18T08:00:10Z',false));
SELECT p01_test.assert(NOT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',
 p01_test.message('rbi2_event',3,'2022-01-18T08:00:20Z',true)),'terminal retry');
SELECT p01_test.assert((SELECT count(*)=1 AND bool_and(terminal AND revision=3) FROM cad_v2.master_events),'one closed event');
SELECT p01_test.assert((SELECT count(*)=3 FROM cad_v2.master_inbox),'all historical revisions retained');
SELECT p01_test.expect_state($q$SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',
 p01_test.message('rbi2_event',4,'2022-01-18T08:00:30Z',false))$q$,'55000');
SELECT p01_test.expect_state($q$SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',
 p01_test.message('rbi2_event',3,'2022-01-18T08:00:20Z',true) || '{"payload":{}}')$q$,'22023');
SELECT p01_test.expect_state($q$SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',
 p01_test.message('rbi2_event',3,'2022-01-18T08:00:20Z',true) || '{"message_id":"other-terminal-id"}')$q$,'23505');
SELECT 'PASS 03 event revisions' AS result;
ROLLBACK;
