-- PostgreSQL 17 baseline. Apply each migration in a transaction.
/* P01: автономный комплект, пока НЕ интегрирован в V2.
   Диапазон P01: 100–103; исходная V1__core_schema.sql сохраняет номер и имя.
   Перенос V1 и математического Go-ядра выполняется в отдельной ветке.
   Совместный migration runner и Snapshot/Restore ещё требуют интеграции.
   Источники использованы только для чтения:
   - Финальное ТЗ, 27.10.2026, разделы 3.7, 6.2, 8–10, 15, 18.2:
     https://github.com/butorovv/cluster-anomaly-detection-v2/blob/main/docs/spec/FINAL_TZ_KR_Distributed_Group_Anomaly_Detection_V2.md
     Локальная версия V2: 3fa11eb5249040a64c515f13d10d7f73ae1a0926.
     SHA256 ТЗ: d179b4e454a5f427fcd722cef77d1575786cddcf3a9005ac5118e62ca5502091
   - V1 https://github.com/butorovv/cluster-anomaly-detection/tree/c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc
     internal/group/rbi2_identity.go, rbi2_event.go, rbi2_temporal.go.
     Старый kr-distributed-detector-contract содержит предложения; финальное ТЗ выше.

   Up 100→103; down 103→100; каждый файл одной транзакцией:
   psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -1 -f FILE
   Runner версий выбирается V2. Повторный up должен завершаться ошибкой.
   Down удаляет cad_v2 data: сначала backup/export; внешние зависимости не CASCADE.

   ПРОЕКТНЫЕ ДОПУЩЕНИЯ (Snapshot/Restore ещё не реализован):
   1. execution_id — UUID эксперимента, не математическая identity. Один frozen
      RunID может иметь разные транспортные executions без смешения результатов.
   2. IDs и identity_json проверяет frozen Go validator при административной
      регистрации. SQL не переизобретает canonical JSON/ConfigID/RunID/EventID,
      не подтверждает SHA реальных файлов. Проверяет формат SHA и связи.
   3. Базовый run: 16 групп/16 partitions; 4/8 групп разрешены для V1 compatibility
      в первых partitions. Mapping immutable. start_offsets — обязательный JSON
      object с ключами номеров partitions и явными offsets, НЕ latest/earliest.
   4. Snapshot BYTEA, SHA256 точных bytes, codec/version pinned в execution.
      json-utf8/opaque-v1 — предложения codecs, SQL их не декодирует. Compression,
      upgrade и миграция DTO требуют решения. Один актуальный snapshot, без архива.
   5. Полноту future DTO должен доказать Restore validator: identity/hosts/δ/window/
      SHA; cursor/markers/accepted/rejected/dedup state; все RBI2Engine occurrenceState
      positive/unknown/causal witnesses/last/end/Flush; Journal.identity/last/rHosts/
      iHosts/open/counters/VoteSegments/evidence/maxima; legacy R+B Engine/subspans/
      historical IDs; deterministic output revision counters. Даже δ=0 требует open.
      Marshal private struct не заменяет явный DTO. SQL fixture bytes — НЕ snapshot.
   6. Complete tick boundary: prefetched/partial tail не включается в next_offset;
      иначе нужен доказанный partial assembler DTO. Не хранить immutable IF trees
      в каждом snapshot — только bindings/SHA отдельно загружаемых frozen assets.
   7. JSONB equality семантическое: object key order и 1/1.0 равны, arrays ordered.
      Byte-for-byte wire parity не заявляется; при необходимости canonical bytes/hash.
      Полные frozen payloads передаёт приложение; SQL не пересчитывает votes/Journal.
   8. Retention/cleanup/immutable metadata deletion/snapshot history ещё не выбраны.
      Не удалять dedup/outbox до конца возможного replay/polling cursor.

   Ограничения прав — часть протокола: runtime не владеет таблицами, не имеет DML,
   CREATE, superuser или наследования migration owner. Пример: scripts/grants.sql.
   Admin/migration owner доверен. SQL UUID/epoch защищают от stale process, не от
   злонамеренного клиента с общим DSN. Kafka assignment проверяет приложение.
   Нет FK outbox→старый checkpoint revision: checkpoint обновляется на месте;
   atomic API связывает durable outputs с состоянием. Для аудита нужен history.
*/
CREATE SCHEMA cad_v2;
REVOKE ALL ON SCHEMA cad_v2 FROM PUBLIC;
CREATE DOMAIN cad_v2.sha256_hex AS text
  CHECK (VALUE ~ '^[0-9a-f]{64}$');

-- Mathematical IDs come ONLY from the frozen Go contract, never from SQL.
CREATE TABLE cad_v2.runs (
  run_id cad_v2.sha256_hex PRIMARY KEY,
  config_id cad_v2.sha256_hex NOT NULL,
  legacy_run_id cad_v2.sha256_hex NOT NULL,
  kernel_version text NOT NULL CHECK (kernel_version = 'group-rbi2-v1'),
  profile_sha256 cad_v2.sha256_hex NOT NULL,
  input_sha256 jsonb NOT NULL CHECK (jsonb_typeof(input_sha256) = 'object' AND input_sha256 <> '{}'),
  bundle_sha256 cad_v2.sha256_hex NOT NULL,
  topology_sha256 cad_v2.sha256_hex NOT NULL,
  feature_schema_sha256 cad_v2.sha256_hex NOT NULL,
  groups_count integer NOT NULL CHECK (groups_count IN (4, 8, 16)),
  delta_seconds integer NOT NULL CHECK (delta_seconds IN (0, 10, 30, 60)),
  window_start timestamptz NOT NULL,
  window_end timestamptz NOT NULL,
  identity_json jsonb NOT NULL CHECK (jsonb_typeof(identity_json) = 'object'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (isfinite(window_start) AND isfinite(window_end) AND window_start < window_end),
  CHECK (mod(extract(epoch FROM window_start), 10) = 0 AND mod(extract(epoch FROM window_end), 10) = 0)
);

-- A new transport experiment retains RunID but gets a fresh execution_id.
CREATE TABLE cad_v2.executions (
  execution_id uuid PRIMARY KEY,
  run_id cad_v2.sha256_hex NOT NULL REFERENCES cad_v2.runs,
  kafka_cluster_id text NOT NULL CHECK (length(kafka_cluster_id) > 0),
  input_topic text NOT NULL CHECK (input_topic = 'telemetry.raw'),
  consumer_group text NOT NULL CHECK (length(consumer_group) > 0),
  partition_count integer NOT NULL CHECK (partition_count = 16),
  output_transport text NOT NULL CHECK (output_transport IN ('polling', 'http_push', 'kafka')),
  checkpoint_interval_ticks integer NOT NULL CHECK (checkpoint_interval_ticks IN (1, 3, 6)),
  snapshot_schema_version integer NOT NULL CHECK (snapshot_schema_version > 0),
  snapshot_codec text NOT NULL CHECK (snapshot_codec IN ('json-utf8', 'opaque-v1')),
  start_offsets jsonb NOT NULL CHECK (jsonb_typeof(start_offsets) = 'object'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (kafka_cluster_id, input_topic, consumer_group)
);

CREATE TABLE cad_v2.execution_partitions (
  execution_id uuid NOT NULL REFERENCES cad_v2.executions,
  group_id text NOT NULL CHECK (group_id ~ '^a(0[1-9]|1[0-6])$'),
  partition_id integer NOT NULL CHECK (partition_id BETWEEN 0 AND 15),
  start_offset bigint NOT NULL CHECK (start_offset >= 0),
  PRIMARY KEY (execution_id, group_id),
  UNIQUE (execution_id, partition_id),
  UNIQUE (execution_id, group_id, partition_id),
  CHECK (partition_id = substring(group_id FROM 2)::integer - 1)
);

CREATE TABLE cad_v2.partition_ownership (
  execution_id uuid NOT NULL,
  group_id text NOT NULL,
  partition_id integer NOT NULL,
  owner_id uuid,
  epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0),
  lease_until timestamptz,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (execution_id, group_id),
  FOREIGN KEY (execution_id, group_id, partition_id)
    REFERENCES cad_v2.execution_partitions (execution_id, group_id, partition_id),
  CHECK ((owner_id IS NULL) = (lease_until IS NULL)),
  CHECK (owner_id IS NULL OR (epoch > 0 AND isfinite(lease_until)))
);
CREATE INDEX ownership_expiry_idx ON cad_v2.partition_ownership (lease_until)
  WHERE owner_id IS NOT NULL;

CREATE TABLE cad_v2.group_checkpoints (
  execution_id uuid NOT NULL,
  group_id text NOT NULL,
  partition_id integer NOT NULL,
  checkpoint_revision bigint NOT NULL CHECK (checkpoint_revision > 0),
  owner_epoch bigint NOT NULL CHECK (owner_epoch > 0),
  next_offset bigint NOT NULL CHECK (next_offset >= 0),
  last_tick timestamptz NOT NULL CHECK (isfinite(last_tick) AND mod(extract(epoch FROM last_tick), 10) = 0),
  finalized boolean NOT NULL,
  snapshot_schema_version integer NOT NULL CHECK (snapshot_schema_version > 0),
  snapshot_codec text NOT NULL CHECK (snapshot_codec IN ('json-utf8', 'opaque-v1')),
  snapshot bytea NOT NULL CHECK (octet_length(snapshot) > 0),
  snapshot_sha256 cad_v2.sha256_hex NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (execution_id, group_id),
  FOREIGN KEY (execution_id, group_id, partition_id)
    REFERENCES cad_v2.execution_partitions (execution_id, group_id, partition_id),
  CHECK (snapshot_sha256 = encode(sha256(snapshot), 'hex'))
);

-- Delivery order cursor is (checkpoint_revision, ordinal), scoped to group.
-- No bigserial cursor: concurrent commits can leave invisible sequence holes.
CREATE TABLE cad_v2.outbox_messages (
  execution_id uuid NOT NULL,
  message_id text NOT NULL CHECK (length(message_id) BETWEEN 1 AND 256),
  group_id text NOT NULL,
  checkpoint_revision bigint NOT NULL CHECK (checkpoint_revision > 0),
  ordinal integer NOT NULL CHECK (ordinal > 0),
  kind text NOT NULL CHECK (kind IN ('group_result', 'rbi2_event')),
  revision bigint NOT NULL CHECK (revision > 0),
  event_time timestamptz NOT NULL CHECK (isfinite(event_time)),
  event_id cad_v2.sha256_hex,
  terminal boolean NOT NULL,
  envelope jsonb NOT NULL CHECK (jsonb_typeof(envelope) = 'object'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  delivered_at timestamptz,
  PRIMARY KEY (execution_id, message_id),
  FOREIGN KEY (execution_id, group_id) REFERENCES cad_v2.execution_partitions,
  UNIQUE (execution_id, group_id, checkpoint_revision, ordinal),
  CHECK ((kind = 'rbi2_event') = (event_id IS NOT NULL)),
  CHECK (kind <> 'group_result' OR (revision = 1 AND terminal))
);
CREATE UNIQUE INDEX outbox_result_key ON cad_v2.outbox_messages (execution_id, group_id, event_time)
  WHERE kind = 'group_result';
CREATE UNIQUE INDEX outbox_event_key ON cad_v2.outbox_messages (execution_id, event_id, revision)
  WHERE kind = 'rbi2_event';
CREATE INDEX outbox_pending_idx ON cad_v2.outbox_messages
  (execution_id, group_id, checkpoint_revision, ordinal) WHERE delivered_at IS NULL;

-- No FK to outbox: Master must accept the same envelope over any transport.
CREATE TABLE cad_v2.master_inbox (
  execution_id uuid NOT NULL,
  message_id text NOT NULL,
  group_id text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('group_result', 'rbi2_event')),
  revision bigint NOT NULL CHECK (revision > 0),
  event_time timestamptz NOT NULL,
  event_id cad_v2.sha256_hex,
  terminal boolean NOT NULL,
  envelope jsonb NOT NULL,
  received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (execution_id, message_id),
  FOREIGN KEY (execution_id, group_id) REFERENCES cad_v2.execution_partitions,
  CHECK ((kind = 'rbi2_event') = (event_id IS NOT NULL))
);
CREATE UNIQUE INDEX inbox_result_key ON cad_v2.master_inbox (execution_id, group_id, event_time)
  WHERE kind = 'group_result';
CREATE UNIQUE INDEX inbox_event_key ON cad_v2.master_inbox (execution_id, event_id, revision)
  WHERE kind = 'rbi2_event';

CREATE TABLE cad_v2.master_results (
  execution_id uuid NOT NULL,
  group_id text NOT NULL,
  tick timestamptz NOT NULL,
  message_id text NOT NULL,
  payload jsonb NOT NULL,
  PRIMARY KEY (execution_id, group_id, tick),
  FOREIGN KEY (execution_id, message_id) REFERENCES cad_v2.master_inbox,
  FOREIGN KEY (execution_id, group_id) REFERENCES cad_v2.execution_partitions
);
CREATE INDEX master_results_tick_idx ON cad_v2.master_results (execution_id, tick, group_id);

CREATE TABLE cad_v2.master_events (
  execution_id uuid NOT NULL,
  event_id cad_v2.sha256_hex NOT NULL,
  group_id text NOT NULL,
  revision bigint NOT NULL CHECK (revision > 0),
  terminal boolean NOT NULL,
  message_id text NOT NULL,
  payload jsonb NOT NULL,
  PRIMARY KEY (execution_id, event_id),
  FOREIGN KEY (execution_id, message_id) REFERENCES cad_v2.master_inbox,
  FOREIGN KEY (execution_id, group_id) REFERENCES cad_v2.execution_partitions
);
CREATE INDEX master_events_group_idx ON cad_v2.master_events (execution_id, group_id, event_id);

CREATE FUNCTION cad_v2.immutable_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'immutable identity: %', TG_TABLE_NAME USING ERRCODE = '55000';
END $$;
CREATE TRIGGER runs_immutable BEFORE UPDATE OR DELETE ON cad_v2.runs
  FOR EACH ROW EXECUTE FUNCTION cad_v2.immutable_identity();
CREATE TRIGGER executions_immutable BEFORE UPDATE OR DELETE ON cad_v2.executions
  FOR EACH ROW EXECUTE FUNCTION cad_v2.immutable_identity();
CREATE TRIGGER partitions_immutable BEFORE UPDATE OR DELETE ON cad_v2.execution_partitions
  FOR EACH ROW EXECUTE FUNCTION cad_v2.immutable_identity();

CREATE FUNCTION cad_v2.seed_partitions() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, cad_v2 AS $$
BEGIN
  INSERT INTO cad_v2.execution_partitions (execution_id, group_id, partition_id, start_offset)
  SELECT NEW.execution_id, 'a' || lpad(i::text, 2, '0'), i - 1,
    (NEW.start_offsets ->> (i - 1)::text)::bigint
  FROM cad_v2.runs r CROSS JOIN LATERAL generate_series(1, r.groups_count) i
  WHERE r.run_id = NEW.run_id;
  INSERT INTO cad_v2.partition_ownership (execution_id, group_id, partition_id)
  SELECT execution_id, group_id, partition_id FROM cad_v2.execution_partitions
  WHERE execution_id = NEW.execution_id;
  RETURN NEW;
END $$;
CREATE TRIGGER executions_seed AFTER INSERT ON cad_v2.executions
  FOR EACH ROW EXECUTE FUNCTION cad_v2.seed_partitions();

REVOKE ALL ON ALL TABLES IN SCHEMA cad_v2 FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA cad_v2 FROM PUBLIC;
