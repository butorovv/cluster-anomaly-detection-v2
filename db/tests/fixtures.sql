-- Synthetic SQL fixtures; these are NOT golden RBI2 IDs or real snapshots.
CREATE SCHEMA p01_test;
CREATE FUNCTION p01_test.assert(ok boolean, label text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN IF ok IS NOT TRUE THEN RAISE EXCEPTION 'assertion failed: %', label; END IF; END $$;
CREATE FUNCTION p01_test.expect_state(statement text, expected text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE got text;
BEGIN
  BEGIN EXECUTE statement;
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS got = RETURNED_SQLSTATE;
    IF got = expected THEN RETURN; END IF;
    RAISE EXCEPTION 'expected SQLSTATE %, got %: %', expected, got, SQLERRM;
  END;
  RAISE EXCEPTION 'statement succeeded, expected SQLSTATE %: %', expected, statement;
END $$;

INSERT INTO cad_v2.runs VALUES
  (repeat('1',64),repeat('2',64),repeat('3',64),'group-rbi2-v1',repeat('4',64),
   jsonb_build_object('input',repeat('5',64)), repeat('6',64),repeat('7',64),repeat('8',64),
   16,0,'2022-01-18 08:00:00Z','2022-01-18 16:00:00Z','{}',DEFAULT);
INSERT INTO cad_v2.executions
  (execution_id,run_id,kafka_cluster_id,input_topic,consumer_group,partition_count,
   output_transport,checkpoint_interval_ticks,snapshot_schema_version,snapshot_codec,start_offsets)
SELECT '00000000-0000-0000-0000-000000000001',repeat('1',64),'test-cluster','telemetry.raw',
  'p01-test',16,'polling',3,1,'opaque-v1',jsonb_object_agg(i::text,0)
FROM generate_series(0,15) i;

CREATE FUNCTION p01_test.message(
  p_kind text DEFAULT 'group_result', p_revision bigint DEFAULT 1,
  p_time text DEFAULT '2022-01-18T08:00:00Z', p_terminal boolean DEFAULT true,
  p_group text DEFAULT 'a01'
) RETURNS jsonb LANGUAGE sql AS $$
SELECT jsonb_build_object(
  'schema_version',1,'execution_id','00000000-0000-0000-0000-000000000001',
  'run_id',repeat('1',64),'config_id',repeat('2',64),'group_id',p_group,
  'kind',p_kind,'revision',p_revision,'event_time',p_time,'terminal',p_terminal,
  'message_id',p_kind||'/'||p_group||'/'||p_time||'/'||p_revision,
  'payload',CASE WHEN p_kind='rbi2_event' THEN jsonb_build_object(
    'version','group-rbi2-v1','id',repeat('e',64),'run_id',repeat('1',64),
    'kernel_config_id',repeat('2',64),'group',p_group,'delta_seconds',0,
    'detected','2022-01-18T08:00:00Z','last_positive','2022-01-18T08:00:00Z')
    || CASE WHEN p_terminal THEN jsonb_build_object('closed',p_time,'closure_reason','negative') ELSE '{}'::jsonb END
    ELSE jsonb_build_object('raw',jsonb_build_object('R','Unknown','B','Negative','I2','Unknown'),
      'temporal',jsonb_build_object('vote','Unknown'),'evidence',NULL) END)
  || CASE WHEN p_kind='rbi2_event' THEN jsonb_build_object('event_id',repeat('e',64)) ELSE '{}'::jsonb END;
$$;

CREATE FUNCTION p01_test.commit(p_expected bigint, p_offset bigint, p_time timestamptz,
  p_outputs jsonb DEFAULT '[]', p_epoch bigint DEFAULT 1, p_finalized boolean DEFAULT false)
RETURNS bigint LANGUAGE sql AS $$
SELECT cad_v2.commit_checkpoint('00000000-0000-0000-0000-000000000001','a01',
  '00000000-0000-0000-0000-000000000011',p_epoch,p_expected,p_offset,p_time,1,'opaque-v1',
  '\x010203',encode(sha256('\x010203'::bytea),'hex'),p_finalized,p_outputs);
$$;
