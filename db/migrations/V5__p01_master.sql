-- Integrated P01 103_master.up.sql as V5; use go run ./db/cmd/migrate.
-- The original P01 commentary below is historical; versions 100-103 now follow
-- core V1 as V2-V5 in public.schema_migrations. No standalone up/down runner.
-- Caller passes a transport envelope, not an outbox row reference.
-- Locks one execution/group to serialize conflicting first deliveries as well.
/* Receive transport envelope → BEGIN → accept_result → COMMIT → HTTP ack /
   advance polling cursor / commit OUTPUT Kafka offset. true=new, false=exact retry.
   Inbox и projection атомарны; lost ack не удваивает эффект. Conflicting payload/
   alternate message_id одной версии — ошибка. Старые версии остаются в inbox,
   не откатывают latest. Terminal запрещает новые версии. ID/group/detected immutable.
   Parent count по (execution,event_id), не по revisions. Master не пересчитывает
   Temporal/Journal; полный Raw/evidence/segments/subspans предоставляет DTO.
   Отдельная Master DB потребует регистрации тех же identity/execution metadata.
*/
CREATE FUNCTION cad_v2.accept_result(p_execution uuid, m jsonb)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, cad_v2 AS $$
DECLARE prior cad_v2.master_inbox; latest cad_v2.master_events; latest_time timestamptz;
BEGIN
  PERFORM cad_v2.validate_envelope(p_execution, m);
  PERFORM 1 FROM cad_v2.execution_partitions
    WHERE execution_id = p_execution AND group_id = m->>'group_id' FOR UPDATE;
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
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA cad_v2 FROM PUBLIC;
