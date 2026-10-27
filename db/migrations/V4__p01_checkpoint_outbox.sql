-- Integrated P01 102_checkpoint_outbox.up.sql as V4; use go run ./db/cmd/migrate.
-- The original P01 commentary below is historical; versions 100-103 now follow
-- core V1 as V2-V5 in public.schema_migrations. No standalone up/down runner.
-- Envelope v1 is a P01 integration proposal; frozen payload IDs are supplied by Go.
/* TRANSACTION ORDER / RECOVERY:
   - Обработать complete ticks в RAM; outputs cadence 1/3/6 ждут общего barrier.
   - Построить полный snapshot DTO, проверить все bindings, SHA точных bytes.
   - BEGIN → commit_checkpoint(owner,epoch,expected_revision,...,ALL outputs) → COMMIT.
     expected_revision=0 для первого checkpoint; CAS/lease/version/hash/progress
     проверяются под owner lock. Ошибка в конце batch откатывает ВСЁ.
     next_offset — первый offset, НЕ включённый в state, а не последний прочитанный.
     Обычная запись продвигает offset+tick; finalized допускает final Flush без
     продвижения. После finalized новые checkpoint запрещены.
   - Только после COMMIT разрешены доставка и вспомогательный Kafka commit.
     Kafka commit не входит в DB tx; RAM outputs нельзя публиковать до barrier.
   - SQL error → rollback + Restore durable state; неподтверждённую RAM не продолжать.
     Потеря соединения во время COMMIT → перечитать revision/offset/snapshot/outbox;
     слепой retry старого expected_revision даёт 40001, а не новый output.

   RESULT ENVELOPE v1 — проектное предложение, утвердить DTO при Go интеграции:
   {schema_version:1,execution_id,run_id,config_id,group_id,message_id,kind,revision,
    event_time,terminal,payload,event_id[только rbi2_event]}.
   group_result: immutable tick, revision=1, terminal=true; key execution/group/time.
   Payload предлагается {raw,temporal,evidence}, точная схема ещё не определена.
   SQL проверяет object, не подменяет scientific validator. Missing = frozen Unknown.
   rbi2_event: полная версия event payload (не diff), прежние ID/detected, terminal close.
   Open DTO предполагает closed absent/NULL, НЕ Go zero time: такого DTO ещё нет.
   Альтернатива — terminal-only доставка одной версии закрытого события.
   Revision — deterministic transition counter, сохраняемый в snapshot, не epoch/
   checkpoint number/wall clock/attempt. Должен быть одинаков при cadence 1/3/6.
   Предложение message_id = SHA256 canonical JSON массива
   ["cad-output-v1",run_id,group_id,kind,logical_key,revision]; logical_key = canonical
   UTC tick или frozen EventID. Canonicalization реализовать один раз в Go.
   Это транспортный ID, НЕ новая формула frozen EventID. Execution — DB scope.
   event_time = UTC Z causal transition; gap close может быть раньше last_tick.
   Processing/attempt timestamps не входят в envelope, иначе retry конфликтует.
   created_at внутри tx не равен t_durable_commit: измерять после server COMMIT ack.

   DELIVERY: один последовательный output loop/group текущего owner. Cursor — пара
   (checkpoint_revision,ordinal), scoped execution/group, не глобальный sequence:
   WHERE execution_id=... AND group_id=...
     AND (checkpoint_revision,ordinal)>(cursor_revision,cursor_ordinal)
   ORDER BY checkpoint_revision,ordinal LIMIT batch_size.
   Такой SQL выполняет Worker. Polling Master обращается HTTP к Worker, НЕ SQL outbox.
   Discovery/хранение acknowledged cursor ещё не реализованы. Push/Kafka используют
   pending rows. Не применять parallel SKIP LOCKED внутри group без порядка версий.
   ack_outbox только после durable доставки; Kafka broker ack до Master требует
   retention/replication гарантий output topic. SQL не отменяет старую сетевую
   отправку после revoke: committed envelopes остаются безопасны через Master dedup.

   CRASH MATRIX:
   RAM/partial tick/до COMMIT → Restore + replay хвоста от DB next_offset;
   после DB COMMIT до Kafka COMMIT → Restore + seek DB position;
   pending outbox → новый owner отправляет прежние IDs; lost ack → retry;
   open event даже δ=0 → Restore Journal/legacy/evidence, не новый EventID;
   DB outage → остановить durable progress/публикацию RAM;
   corrupt snapshot/version/retention gap → остановка, не ложное empty-state recovery.
   Штатный replay всего D2 запрещён. Checksum не доказывает полноту DTO.
*/
CREATE FUNCTION cad_v2.validate_envelope(p_execution uuid, m jsonb)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, cad_v2 AS $$
DECLARE r cad_v2.runs; x cad_v2.executions; et timestamptz; d timestamptz; lp timestamptz; c timestamptz;
BEGIN
  SELECT * INTO STRICT x FROM cad_v2.executions WHERE execution_id = p_execution;
  SELECT * INTO STRICT r FROM cad_v2.runs WHERE run_id = x.run_id;
  IF (jsonb_typeof(m) = 'object' AND m->>'schema_version' = '1'
      AND m->>'execution_id' = p_execution::text
      AND m->>'run_id' = r.run_id AND m->>'config_id' = r.config_id
      AND length(m->>'message_id') BETWEEN 1 AND 256
      AND m->>'kind' IN ('group_result', 'rbi2_event')
      AND (m->>'revision')::bigint > 0
      AND jsonb_typeof(m->'terminal') = 'boolean'
      AND jsonb_typeof(m->'payload') = 'object'
      AND m->>'event_time' ~ 'Z$') IS NOT TRUE THEN
    RAISE EXCEPTION 'invalid result envelope' USING ERRCODE = '22023';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM cad_v2.execution_partitions
    WHERE execution_id = p_execution AND group_id = m->>'group_id') THEN
    RAISE EXCEPTION 'group outside execution' USING ERRCODE = '22023';
  END IF;
  et := (m->>'event_time')::timestamptz;
  IF (isfinite(et) AND et >= r.window_start AND et <= r.window_end
      AND mod(extract(epoch FROM et), 10) = 0) IS NOT TRUE THEN
    RAISE EXCEPTION 'invalid result time' USING ERRCODE = '22023';
  END IF;
  IF m->>'kind' = 'group_result' THEN
    IF ((m->>'revision')::bigint = 1 AND (m->>'terminal')::boolean
        AND et < r.window_end AND NOT (m ? 'event_id')) IS NOT TRUE THEN
      RAISE EXCEPTION 'group result must be immutable tick revision 1' USING ERRCODE = '22023';
    END IF;
  ELSE
    -- Structural envelope checks, never recompute or rewrite mathematical IDs.
    IF (m->>'event_id' ~ '^[0-9a-f]{64}$'
        AND m#>>'{payload,id}' = m->>'event_id'
        AND m#>>'{payload,version}' = r.kernel_version
        AND m#>>'{payload,run_id}' = r.run_id
        AND m#>>'{payload,kernel_config_id}' = r.config_id
        AND m#>>'{payload,group}' = m->>'group_id'
        AND (m#>>'{payload,delta_seconds}')::integer = r.delta_seconds
        AND m#>>'{payload,detected}' ~ 'Z$'
        AND m#>>'{payload,last_positive}' ~ 'Z$') IS NOT TRUE THEN
      RAISE EXCEPTION 'event identity mismatch' USING ERRCODE = '22023';
    END IF;
    d := (m#>>'{payload,detected}')::timestamptz;
    lp := (m#>>'{payload,last_positive}')::timestamptz;
    IF (isfinite(d) AND isfinite(lp) AND d >= r.window_start AND d <= lp
        AND lp < r.window_end AND lp <= et
        AND mod(extract(epoch FROM d), 10) = 0 AND mod(extract(epoch FROM lp), 10) = 0) IS NOT TRUE THEN
      RAISE EXCEPTION 'invalid event interval' USING ERRCODE = '22023';
    END IF;
    IF (m->>'terminal')::boolean THEN
      c := (m#>>'{payload,closed}')::timestamptz;
      IF (m#>>'{payload,closed}' ~ 'Z$' AND isfinite(c) AND c > lp AND c <= r.window_end
          AND c <= et AND mod(extract(epoch FROM c), 10) = 0
          AND m#>>'{payload,closure_reason}' IN ('negative', 'unknown', 'gap', 'missing_tail', 'window')) IS NOT TRUE THEN
        RAISE EXCEPTION 'invalid terminal event' USING ERRCODE = '22023';
      END IF;
    ELSIF m#>>'{payload,closed}' IS NOT NULL THEN
      RAISE EXCEPTION 'open event cannot have closed time' USING ERRCODE = '22023';
    END IF;
  END IF;
END $$;

CREATE FUNCTION cad_v2.commit_checkpoint(
  p_execution uuid, p_group text, p_owner uuid, p_epoch bigint,
  p_expected_revision bigint, p_next_offset bigint, p_last_tick timestamptz,
  p_schema_version integer, p_codec text, p_snapshot bytea,
  p_snapshot_sha256 cad_v2.sha256_hex, p_finalized boolean, p_outputs jsonb
) RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, cad_v2 AS $$
DECLARE old cad_v2.group_checkpoints; x cad_v2.executions; r cad_v2.runs;
  part cad_v2.execution_partitions; n bigint; m jsonb; ord integer := 0;
  prior cad_v2.outbox_messages;
BEGIN
  PERFORM cad_v2.assert_owner(p_execution, p_group, p_owner, p_epoch);
  SELECT * INTO STRICT x FROM cad_v2.executions WHERE execution_id = p_execution;
  SELECT * INTO STRICT r FROM cad_v2.runs WHERE run_id = x.run_id;
  SELECT * INTO STRICT part FROM cad_v2.execution_partitions
    WHERE execution_id = p_execution AND group_id = p_group;
  SELECT * INTO old FROM cad_v2.group_checkpoints
    WHERE execution_id = p_execution AND group_id = p_group;
  IF p_expected_revision IS NULL OR p_expected_revision <> coalesce(old.checkpoint_revision, 0) THEN
    RAISE EXCEPTION 'checkpoint compare-and-swap conflict' USING ERRCODE = '40001';
  END IF;
  IF (p_schema_version = x.snapshot_schema_version AND p_codec = x.snapshot_codec
      AND octet_length(p_snapshot) > 0 AND p_snapshot_sha256 = encode(sha256(p_snapshot), 'hex')
      AND p_next_offset >= part.start_offset AND isfinite(p_last_tick)
      AND p_last_tick >= r.window_start AND p_last_tick < r.window_end
      AND mod(extract(epoch FROM p_last_tick), 10) = 0
      AND p_finalized IS NOT NULL AND jsonb_typeof(p_outputs) = 'array') IS NOT TRUE THEN
    RAISE EXCEPTION 'invalid checkpoint metadata/checksum' USING ERRCODE = '22023';
  END IF;
  IF old.checkpoint_revision IS NOT NULL THEN
    IF old.finalized OR p_next_offset < old.next_offset OR p_last_tick < old.last_tick
       OR (NOT p_finalized AND (p_next_offset = old.next_offset OR p_last_tick = old.last_tick)) THEN
      RAISE EXCEPTION 'checkpoint regression or write after finalization' USING ERRCODE = '55000';
    END IF;
  END IF;
  n := coalesce(old.checkpoint_revision, 0) + 1;
  INSERT INTO cad_v2.group_checkpoints VALUES
    (p_execution, p_group, part.partition_id, n, p_epoch, p_next_offset, p_last_tick,
     p_finalized, p_schema_version, p_codec, p_snapshot, p_snapshot_sha256, clock_timestamp())
  ON CONFLICT (execution_id, group_id) DO UPDATE SET
    checkpoint_revision = EXCLUDED.checkpoint_revision, owner_epoch = EXCLUDED.owner_epoch,
    next_offset = EXCLUDED.next_offset, last_tick = EXCLUDED.last_tick, finalized = EXCLUDED.finalized,
    snapshot_schema_version = EXCLUDED.snapshot_schema_version, snapshot_codec = EXCLUDED.snapshot_codec,
    snapshot = EXCLUDED.snapshot, snapshot_sha256 = EXCLUDED.snapshot_sha256, updated_at = EXCLUDED.updated_at;
  FOR m IN SELECT value FROM jsonb_array_elements(p_outputs) LOOP
    ord := ord + 1;
    PERFORM cad_v2.validate_envelope(p_execution, m);
    IF m->>'group_id' <> p_group OR (m->>'event_time')::timestamptz >
       (CASE WHEN p_finalized THEN r.window_end ELSE p_last_tick END) THEN
      RAISE EXCEPTION 'output outside checkpoint boundary' USING ERRCODE = '22023';
    END IF;
    -- Same logical message retry is allowed only if its entire envelope agrees.
    SELECT * INTO prior FROM cad_v2.outbox_messages
      WHERE execution_id = p_execution AND message_id = m->>'message_id';
    IF FOUND THEN
      IF prior.envelope IS DISTINCT FROM m THEN
        RAISE EXCEPTION 'conflicting outbox retry' USING ERRCODE = '23505';
      END IF;
      CONTINUE;
    END IF;
    IF m->>'kind' = 'rbi2_event' AND EXISTS (
      SELECT 1 FROM cad_v2.outbox_messages WHERE execution_id = p_execution
        AND event_id = m->>'event_id' AND (terminal OR revision >= (m->>'revision')::bigint
          OR group_id <> p_group
          OR envelope#>>'{payload,detected}' IS DISTINCT FROM m#>>'{payload,detected}'
          OR event_time > (m->>'event_time')::timestamptz
          OR (envelope#>>'{payload,last_positive}')::timestamptz > (m#>>'{payload,last_positive}')::timestamptz)
    ) THEN
      RAISE EXCEPTION 'non-monotone event revision or terminal rewrite' USING ERRCODE = '55000';
    END IF;
    INSERT INTO cad_v2.outbox_messages
      (execution_id, message_id, group_id, checkpoint_revision, ordinal, kind, revision,
       event_time, event_id, terminal, envelope)
    VALUES (p_execution, m->>'message_id', p_group, n, ord, m->>'kind', (m->>'revision')::bigint,
      (m->>'event_time')::timestamptz, m->>'event_id', (m->>'terminal')::boolean, m);
  END LOOP;
  -- Recheck expiry after serialization/validation. Ownership lock still held.
  PERFORM cad_v2.assert_owner(p_execution, p_group, p_owner, p_epoch);
  RETURN n;
END $$;

CREATE FUNCTION cad_v2.ack_outbox(p_execution uuid, p_group text, p_owner uuid, p_epoch bigint, p_message_id text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, cad_v2 AS $$
BEGIN
  PERFORM cad_v2.assert_owner(p_execution, p_group, p_owner, p_epoch);
  UPDATE cad_v2.outbox_messages SET delivered_at = coalesce(delivered_at, clock_timestamp())
    WHERE execution_id = p_execution AND group_id = p_group AND message_id = p_message_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'unknown outbox message' USING ERRCODE = '22023'; END IF;
END $$;

REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA cad_v2 FROM PUBLIC;
