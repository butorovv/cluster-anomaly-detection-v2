CREATE ROLE p01_test_worker NOLOGIN;
GRANT USAGE ON SCHEMA cad_v2, p01_test TO p01_test_worker;
GRANT SELECT ON cad_v2.group_checkpoints, cad_v2.outbox_messages TO p01_test_worker;
GRANT EXECUTE ON FUNCTION cad_v2.acquire_partition(uuid,text,uuid,interval),
  cad_v2.commit_checkpoint(uuid,text,uuid,bigint,bigint,bigint,timestamptz,integer,text,bytea,cad_v2.sha256_hex,boolean,jsonb)
  TO p01_test_worker;
SET LOCAL ROLE p01_test_worker;
SELECT p01_test.expect_state($q$UPDATE cad_v2.partition_ownership SET epoch=99$q$,'42501');
SELECT p01_test.expect_state($q$DELETE FROM cad_v2.outbox_messages$q$,'42501');
SELECT p01_test.expect_state($q$INSERT INTO cad_v2.group_checkpoints DEFAULT VALUES$q$,'42501');
SELECT p01_test.expect_state($q$SELECT cad_v2.assert_owner('00000000-0000-0000-0000-000000000001',
 'a01','00000000-0000-0000-0000-000000000011',1)$q$,'42501');
SELECT p01_test.expect_state($q$SELECT cad_v2.accept_result('00000000-0000-0000-0000-000000000001',p01_test.message())$q$,'42501');
SELECT cad_v2.acquire_partition('00000000-0000-0000-0000-000000000001','a01',
 '00000000-0000-0000-0000-000000000011',interval '5 minutes');
SELECT p01_test.commit(0,1501,'2022-01-18 08:00Z',jsonb_build_array(p01_test.message()));
SELECT p01_test.assert((SELECT next_offset=1501 FROM cad_v2.group_checkpoints),'restricted worker uses atomic API');
RESET ROLE;
SELECT 'PASS 04 privileges' AS result;
ROLLBACK;
