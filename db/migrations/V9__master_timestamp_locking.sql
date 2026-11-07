-- Independent diagnostic timestamps share no Master identity-row guard.
-- V1-V8, immutable identities, FK/unique constraints and V3 fencing are unchanged.
-- Deployment: quiesce Master intake and finish/rollback its open transactions
-- before Apply, then restart intake. V8's execution/legacy timestamp guards do
-- not coordinate with this namespace; mixed-version in-flight intake is unsafe.
-- Callers validate the timestamp before locking. Typed UTC epoch seconds make
-- equivalent JSON spellings and session TimeZone settings use the same key.
-- Hash collisions can only add serialization; the hash is not logical identity.
CREATE FUNCTION cad_v2.lock_master_tick(x uuid, t timestamptz) RETURNS void
LANGUAGE sql STRICT SET search_path = pg_catalog, cad_v2 AS $$
  SELECT pg_advisory_xact_lock(hashtextextended(
    'cad_v2/master-tick/v1/' || x::text || '/' || (extract(epoch FROM t)::bigint)::text, 0));
$$;
REVOKE EXECUTE ON FUNCTION cad_v2.lock_master_tick(uuid,timestamptz) FROM PUBLIC;

CREATE OR REPLACE FUNCTION cad_v2.accept_result(p_execution uuid, m jsonb)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, cad_v2 AS $$
DECLARE prior cad_v2.master_inbox; latest cad_v2.master_events; latest_time timestamptz;
BEGIN
  PERFORM cad_v2.validate_envelope(p_execution, m);
  IF m->>'kind' = 'group_result' THEN
    -- The same guard as fragment intake, retained across all final writes.
    -- Existence/identity are protected by validation, immutable triggers and FK;
    -- exact retries and logical tick uniqueness are protected by this tick lock.
    PERFORM cad_v2.lock_master_tick(p_execution, (m->>'event_time')::timestamptz);
  ELSE
    -- Event revision/history synchronization is deliberately unchanged from V8.
    PERFORM 1 FROM cad_v2.execution_partitions
      WHERE execution_id = p_execution AND group_id = m->>'group_id' FOR NO KEY UPDATE;
  END IF;
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
 -- Lock before INSERT/FK acquisition. Every arrival of this tick and its final
 -- results use this one xact lock. Other timestamps need no shared identity lock.
 -- The caller's separate READ COMMITTED count statement observes earlier commits
 -- after a lock wait; retain this guard through count, final writes and COMMIT.
 PERFORM cad_v2.lock_master_tick(x,(m->>'event_time')::timestamptz);
 SELECT * INTO old FROM cad_v2.master_tick_fragments WHERE execution_id=x AND group_id=m->>'group_id' AND event_time=(m->>'event_time')::timestamptz;
 IF FOUND THEN IF old.envelope IS DISTINCT FROM m THEN RAISE EXCEPTION 'conflicting Master fragment retry' USING ERRCODE='23505'; END IF; RETURN false; END IF;
 INSERT INTO cad_v2.master_tick_fragments VALUES(x,m->>'group_id',(m->>'event_time')::timestamptz,m->>'message_id',m);RETURN true;
END $$;
-- CREATE OR REPLACE preserves existing runtime grants; the helper stays private.
REVOKE EXECUTE ON FUNCTION cad_v2.accept_result(uuid,jsonb),cad_v2.accept_tick_fragment(uuid,jsonb) FROM PUBLIC;
