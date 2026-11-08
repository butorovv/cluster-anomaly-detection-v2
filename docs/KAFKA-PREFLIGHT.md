# P16-C2 Kafka preflight (PR #18)

Интеграционный baseline: `65dc089d3078f308430cc448934a6b41a3bc789f`.
Существующий C2 commit: `d79ba3150a02dea30b7055d9afab1748dc8c8796`.
Контракт: encoded envelope **4,194,304 bytes**, Kafka batch cap
**4,195,328 bytes (4 MiB +1 KiB framing)**. Payload не дробится/не обрезается.

## Статус этой проверки

Локальный Docker endpoint — `desktop-linux`, named pipe
`//./pipe/dockerDesktopLinuxEngine`; daemon отсутствует (pipe not found).
Подтверждённого выделенного Kafka endpoint нет. Docker/VMware/Ubuntu/P15-4B,
общие topics, identities и volumes не изменялись; Kafka не запускался.

**Actual Kafka Admin readback / sync-async delivery / Master Kafka consumer:
NOT_RUN. SYSTEM_ACCEPTED не заявляется.** Значения Compose и исходников не
являются измеренными настройками существующего broker. Команды ниже подготовлены
для будущего явно выделенного стенда; в этой задаче они не исполнялись.

## Требуемые и проверенные limits

| Уровень | Требование C2 | Фактически проверено здесь |
|---|---|---|
| Domain envelope | <=4,194,304 bytes inclusive | Domain/boundary/HTTP unit tests; >cap fail-closed |
| Producer, sync и async | `ProducerBatchMaxBytes=4195328` | Общая producer factory; SDK runtime-option и framing unit tests |
| Producer byte budget | Key+value <= budget; default4,194,560 | Source/unit; framing не входит в SDK `userSize`; bounded admission сохранён |
| `group.results` effective `max.message.bytes` | **>=4195328**, включая inherited value | Fresh DescribeTopicConfigs gate в коде/mock tests; actual Kafka **NOT_RUN** |
| Broker `message.max.bytes` | Для согласованного стенда4195328; это default для topic | Compose4195328; actual **NOT_RUN**. Explicit topic override заменяет broker default; это не два независимых min-cap |
| Broker `replica.fetch.max.bytes` | Рекомендуется >=4195328 на всех brokers | Compose4195328; actual **NOT_RUN**. Kafka first-batch progress exception делает fetch limit мягким |
| Broker `replica.fetch.response.max.bytes` | Не создавать response bottleneck ниже полного batch | Default Kafka4.1=10MiB по документации; actual **NOT_RUN** |
| Broker `socket.request.max.bytes` | Согласовать с producer request ceiling, обычно100MiB | SDK BrokerMaxWriteBytes default100MiB; documented broker default100MiB; actual **NOT_RUN** |
| Master fetch | `FetchMaxPartitionBytes=4195328`; `FetchMaxBytes=8388608` | Source-confirmed; actual Master Kafka чтение **NOT_RUN** |
| ClusterID / TopicID | Совпадают с зарегистрированными pins, не пустые | Fresh fail-closed code/mock tests; actual IDs **NOT_READ** |
| Partitions / cleanup | Ровно16; exactly `delete` | Source/mock tests; actual **NOT_READ** |
| Outbox pending | Failed/permanent send не ACK-ить; restart replay same bytes/ID | Реальный isolated PostgreSQL + synthetic sender и Dispatcher restart; real Kafka/Worker process gate **NOT_RUN** |

Kafka batch allowance включает key<=256bytes, varints и record-batch framing;
headers отсутствуют, compression отключена, ACK=all ISR. Менять MaxBufferedBytes
вместо batch/topic cap недостаточно. Consumer/replica fetch limits имеют Kafka
first-batch exception; preflight всё равно задаёт достаточные явные значения.
Дополнительно проверьте, что effective topic `segment.bytes` не меньше batch cap.

## 1. Read-only preflight на явно выделенном стенде

Пример для Bash / Apache Kafka4.1.x и отдельного plaintext стенда. Значения
должны быть взяты из инвентаря этого стенда, **без fallback наlocalhost или
унаследованные P15/R1 env URLs**. Для SASL/TLS CLI требуется соответствующий
admin client config; этот пример не утверждает поддержку такого deployment
текущими application entrypoints.

```bash
set -euo pipefail
KAFKA_BIN="${KAFKA_HOME:?approved Kafka installation required}/bin"
BOOTSTRAP="${C2_KAFKA_BOOTSTRAP:?explicit isolated broker seeds required}"
EVIDENCE="${C2_PREFLIGHT_DIR:?new private evidence directory required}"
mkdir -p "$EVIDENCE"

"$KAFKA_BIN/kafka-cluster.sh" cluster-id --bootstrap-server "$BOOTSTRAP" \
  | tee "$EVIDENCE/cluster-before.txt"
"$KAFKA_BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" \
  > "$EVIDENCE/broker-inventory.txt"
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --describe --topic group.results | tee "$EVIDENCE/topic-before.txt"
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type topics --entity-name group.results --describe --all \
  | tee "$EVIDENCE/topic-config-before.txt"

# Подставить ВСЕ broker IDs из подтверждённого inventory стенда; здесь ID1
# соответствует только одноузловому примеру deploy/compose.yaml.
BROKER_IDS=(1)
for id in "${BROKER_IDS[@]}"; do
  "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
    --entity-type brokers --entity-name "$id" --describe --all \
    | tee "$EVIDENCE/broker-$id-before.txt"
done
```

Не ограничиваться dynamic-only describe: нужны effective inherited/default
values и config sources/synonyms. Сверить все16 partitions/leaders, exact
cleanup.policy, ClusterID и TopicID с durable execution/result source binding
в PostgreSQL (разрешённый read-only доступ владельца стенда). Одного совпадения
имени topic недостаточно. Если topic отсутствует или identity изменилась — STOP;
не создавать его заново для продолжаемой execution. Для новой execution новый
topic/binding — отдельный явный bootstrap, не обход existing recovery pins.

## 2. Предлагаемые изменения — только после отдельного разрешения владельца

Сначала предъявить before-evidence, точные target/project/path и план ниже.
**Никакие ALTER/restart команды здесь не выполнены.** Если effective topic cap
уже >=4195328, не снижать его и не менять конфигурацию без причины.

Только если topic cap недостаточен и изменение одобрено для выделенного стенда:

```bash
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type topics --entity-name group.results --alter \
  --add-config max.message.bytes=4195328
```

Эффект: меняется только topic size override, сохраняются TopicID/ClusterID,
partitions, existing records, cleanup.policy и recovery offsets. Допускаются
более крупные batches, растут потенциальные network/memory/I/O расходы. Это
не подтверждение доставки и не ACK pending. Не применять `--delete`,
`--create`, `--alter --partitions`, `--delete-config` или storage format.

Broker configuration для одноузлового **собственного** Compose стенда уже
присутствует в этой ветке:

```yaml
KAFKA_MESSAGE_MAX_BYTES: 4195328
KAFKA_REPLICA_FETCH_MAX_BYTES: 4195328
```

Для native Kafka эквиваленты в утверждённом `server.properties`:

```properties
message.max.bytes=4195328
replica.fetch.max.bytes=4195328
```

Kafka4.1 считает `replica.fetch.max.bytes` read-only broker setting: не
предлагать dynamic `--alter` для него. Применение требует одобренного broker
restart. `message.max.bytes` задаёт default для topics без override;
его увеличение влияет на все такие topics. Не требуется повышать этот default
ради одного topic, если его explicit override уже достаточен; всё равно
прочитать и зафиксировать broker/replica/socket values.

Пример точной команды применения **после** утверждения отдельного проекта,
файла Compose и прежнего immutable volume/cluster identity:

```bash
PROJECT="${C2_COMPOSE_PROJECT:?explicit approved private project required}"
COMPOSE_FILE="${C2_COMPOSE_FILE:?explicit approved Compose file required}"
docker compose -p "$PROJECT" -f "$COMPOSE_FILE" config \
  > "$EVIDENCE/approved-compose.txt"
# Сначала review config/volume mappings; команда ниже разрешается отдельно.
docker compose -p "$PROJECT" -f "$COMPOSE_FILE" up -d --no-deps kafka
```

Последствия: Kafka service может быть recreated/restarted и кратковременно
недоступен. Сохранить существующий data volume, CLUSTER_ID и node.id; не выполнять
`down -v`, volume removal, cleanup/format или запуск с пустым replacement volume.
Никакого исполнения на Ubuntu/P15-4B или общих topics. Увеличение heap, памяти
VM и изменение эксплуатационных таймаутов в C2 не входят.

Повторить все read-only команды, сохранить after-evidence и сравнить IDs и
partitions byte-for-byte с before. Совпадение IDs обязательно; одного вывода
Compose config недостаточно. При смешанных брокерах проверить каждый broker.

## 3. Delivery/recovery gate после successful preflight

На disposable stand явно установить только свои opt-ins:

```bash
export SYSTEM_KAFKA_INTEGRATION=1
export SYSTEM_KAFKA_ISOLATED=1
export P07_TEST_KAFKA_BROKERS="$BOOTSTRAP"
go test ./internal/modules/events/delivery/resulttransport \
  -run '^TestPayloadRealKafkaSyncAsyncBoundary$' -count=1 -p=1 -v
```

Этот test публикует synthetic envelopes через sync/async и читает точные
key/value через direct Kafka reader: около1,000,012bytes,1MiB и4MiB, включая
4MiB inclusive и +1 rejection. Он использует fixed group.results, поэтому
недопустим на общих/исторических topics. Не заявлять им покрытие production
Master consumer group/SQL final join, actual Worker restart или scientific
validity synthetic padding.

Для отдельного PostgreSQL C2:

```bash
export SYSTEM_TEST_DATABASE_URL="${C2_TEST_DATABASE_URL:?private test admin URL required}"
go test ./internal/modules/events/repository/postgres \
  -run '^TestPayloadSQLPendingDispatcherRestart$' -count=1 -p=1 -v
```

Далее требуется combined real Kafka+PG gate: Master ConsumeKafka с durable
offsets, valid scientific result/events, permanent send без SQL ACK, pending
после failed/unknown delivery, restart Worker, same-ID replay, lost broker/SQL
ACK, FIFO/rebalance/takeover и independent golden. Имеющиеся SQL/synthetic
sender tests и direct Kafka reader по отдельности его не заменяют.

R1/V10 при rollout требует завершить old Worker TXs/intake/output/HTTP,
применить V10, обновить grants и Worker binary согласованно. Page использует
`read_outbox_page` в READ COMMITTED; все mutations сохраняют SQL fencing.
V10 устраняет Page/renewal contention, не все возможные SQL блокировки.

**До фактического readback и delivery/recovery gate — CONDITIONALLY_READY,
Kafka NOT_RUN, не SYSTEM_ACCEPTED. D2 и сравнительный pilot здесь не запускать.**

Первичные источники:
[Kafka4.1 broker settings](https://kafka.apache.org/41/configuration/broker-configs/),
[Kafka4.1 operations](https://kafka.apache.org/41/operations/basic-kafka-operations/),
[ClusterTool4.1.2](https://github.com/apache/kafka/blob/4.1.2/tools/src/main/java/org/apache/kafka/tools/ClusterTool.java),
и pinned franz-go v1.21.0 `pkg/kgo/config.go`.
