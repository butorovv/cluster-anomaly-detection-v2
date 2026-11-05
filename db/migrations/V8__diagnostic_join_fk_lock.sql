-- P14 revised diagnostic join: remove unnecessary Master/Worker FK contention.
-- V1-V7 remain byte-for-byte unchanged. No tables, identity, checkpoint,
-- offsets, final materialization or ownership semantics are changed.
-- NO KEY UPDATE remains exclusive among Master writers, but is compatible
-- with FK KEY SHARE. The global canonical Master guard is retained; this
-- migration does NOT introduce concurrent joins or RAM aggregation.
CREATE OR REPLACE FUNCTION cad_v2.accept_result(p_execution uuid, m jsonb)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, cad_v2 AS $$
DECLARE prior cad_v2.master_inbox; latest cad_v2.master_events; latest_time timestamptz;
BEGIN
  PERFORM cad_v2.validate_envelope(p_execution, m);
  PERFORM 1 FROM cad_v2.execution_partitions
    WHERE execution_id = p_execution AND group_id = m->>'group_id' FOR NO KEY UPDATE;
  SELECT * INTO prior FROM cad_v2.master_inbox
    WHERE execution_id = p_execution AND message_id = m->>'message_id';
  IF FOUND THEN
    IF prior.envelope IS DISTINCT FROM m THEN
      RAISE EXCEPTION 'conflicting Master retry' USING ERRCODE = '23505';
    END IF;
    RETURN false;
  END IF;
  IF m->>'kind' = 'rbi2_event' THEN
    -- Serialize even a malformed cross-group collision of the same event ID.
    -- Hash collisions only add locking; they never define logical identity.
    PERFORM pg_advisory_xact_lock(hashtextextended(p_execution::text || '/' || (m->>'event_id'), 0));
    SELECT * INTO latest FROM cad_v2.master_events
      WHERE execution_id = p_execution AND event_id = m->>'event_id' FOR UPDATE;
    IF FOUND THEN
      SELECT event_time INTO STRICT latest_time FROM cad_v2.master_inbox
        WHERE execution_id = p_execution AND message_id = latest.message_id;
      IF latest.group_id <> m->>'group_id'
         OR latest.payload->>'detected' IS DISTINCT FROM m#>>'{payload,detected}'
         OR (latest.terminal AND (m->>'revision')::bigint > latest.revision)
         OR ((m->>'terminal')::boolean AND (m->>'revision')::bigint < latest.revision)
         OR ((m->>'revision')::bigint > latest.revision
           AND (m#>>'{payload,last_positive}')::timestamptz < (latest.payload->>'last_positive')::timestamptz)
         OR ((m->>'revision')::bigint < latest.revision
           AND (m#>>'{payload,last_positive}')::timestamptz > (latest.payload->>'last_positive')::timestamptz)
         OR ((m->>'revision')::bigint > latest.revision AND (m->>'event_time')::timestamptz < latest_time)
         OR ((m->>'revision')::bigint < latest.revision AND (m->>'event_time')::timestamptz > latest_time) THEN
        RAISE EXCEPTION 'inconsistent event history/terminal rewrite' USING ERRCODE = '55000';
      END IF;
    END IF;
  END IF;
  INSERT INTO cad_v2.master_inbox
    (execution_id, message_id, group_id, kind, revision, event_time, event_id, terminal, envelope)
  VALUES (p_execution, m->>'message_id', m->>'group_id', m->>'kind', (m->>'revision')::bigint,
    (m->>'event_time')::timestamptz, m->>'event_id', (m->>'terminal')::boolean, m);
  -- Logical unique indexes also reject alternate message IDs for one version.
  IF m->>'kind' = 'group_result' THEN
    INSERT INTO cad_v2.master_results VALUES
      (p_execution, m->>'group_id', (m->>'event_time')::timestamptz, m->>'message_id', m->'payload');
  ELSE
    INSERT INTO cad_v2.master_events VALUES
      (p_execution, m->>'event_id', m->>'group_id', (m->>'revision')::bigint,
       (m->>'terminal')::boolean, m->>'message_id', m->'payload')
    ON CONFLICT (execution_id, event_id) DO UPDATE SET
      revision = EXCLUDED.revision, terminal = EXCLUDED.terminal,
      message_id = EXCLUDED.message_id, payload = EXCLUDED.payload
    WHERE EXCLUDED.revision > cad_v2.master_events.revision;
  END IF;
  RETURN true;
END $$;
CREATE OR REPLACE FUNCTION cad_v2.accept_tick_fragment(x uuid,m jsonb) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,cad_v2 AS $$
DECLARE old cad_v2.master_tick_fragments;
BEGIN
 PERFORM cad_v2.validate_tick_fragment(x,m);
 -- Keep the canonical execution-row guard exclusive among Master writers,
 -- while allowing independent Worker FK KEY SHARE checks to proceed. These
 -- immutable identity keys are never changed by intake. Other Master fragment
 -- transactions wait on the first row before holding any fragment FK locks.
 -- Final group identity guards use the same mode, avoiding an FK-conflicting
 -- upgrade at final COMMIT. V3 ownership fencing remains FOR UPDATE.
 PERFORM 1 FROM cad_v2.execution_partitions WHERE execution_id=x ORDER BY group_id FOR NO KEY UPDATE;
 -- One short transaction joins at most 16 groups; no full D2 RAM accumulation.
 PERFORM pg_advisory_xact_lock(hashtextextended(x::text || '/tick/' || (m->>'event_time'),0));
 SELECT * INTO old FROM cad_v2.master_tick_fragments WHERE execution_id=x AND group_id=m->>'group_id' AND event_time=(m->>'event_time')::timestamptz;
 IF FOUND THEN IF old.envelope IS DISTINCT FROM m THEN RAISE EXCEPTION 'conflicting Master fragment retry' USING ERRCODE='23505'; END IF; RETURN false; END IF;
 INSERT INTO cad_v2.master_tick_fragments VALUES(x,m->>'group_id',(m->>'event_time')::timestamptz,m->>'message_id',m);RETURN true;
END $$;
REVOKE EXECUTE ON FUNCTION cad_v2.accept_result(uuid,jsonb),cad_v2.accept_tick_fragment(uuid,jsonb) FROM PUBLIC;
