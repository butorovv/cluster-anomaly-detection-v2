# cluster-anomaly-detection-v2

Go-реализация курсовой системы обнаружения групповых аномалий Summit.
Главный архитектурный и математический контракт —
[утверждённое ТЗ](docs/spec/FINAL_TZ_KR_Distributed_Group_Anomaly_Detection_V2.md).

Реализованы frozen offline R/B/I2, Parquet replayer, versioned input codec,
TickAssembler, Kafka input/recovery PoC и PostgreSQL foundation V1–V5. Worker/Master,
Snapshot/Restore и прикладной checkpoint/outbox runtime — следующие этапы.
Runtime и тесты — Go; Python для запуска не требуется.

## Требования и исходные данные

Нужен Go **1.27.2**, для SQL integration tests — PostgreSQL **17**.
Assets передаются отдельно и не хранятся в Git. `ASSETS_ROOT` должен сохранять
исходные логические пути V1:

| Файл | SHA256 |
|---|---|
| `data/prepared/summit-D1-pilot/D1.parquet` | `cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7` |
| `data/prepared/summit-D1-pilot/manifest.json` | `6d603a5445d1a76be24408eed84933e96a4a10af6c3bfd32471d93ca7dd9f291` |
| `results/statistical-20220118-8h-v1.json` | `36da27a4a4f485baa4b1b1b899c26a36efc15401327fd85abc3d63ea3c1ade80` |
| `results/isolation-20220118-v1.json` | `6aa1ca4f2f76b56418aafaa1ea25e6812a3b9fdcc0d309aeed44dc1db03f2da3` |
| `configs/group-profile.summit-v1.json` | `a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36` |

Остальные frozen inputs — `configs/group-detection.summit-v1.json`,
`configs/local-b1.summit-20220118.json`, `configs/summit-hardware-reference.json`.
Их точные SHA закреплены в `input_sha256` group profile. Эти конфигурации
сохранены в репозитории без изменений. Bundle проверяет SHA и bindings;
модели не переобучаются, пороги не подбираются.

Историческое имя `D1.parquet` обозначает исходный подготовленный файл, из которого
выбирается окно D2 `[2022-01-18T08:00:00Z,2022-01-18T16:00:00Z)`.
Подмена новым D2-only файлом нарушает binding. Нужен prepared **F18-v1**, а не raw
Summit Parquet: metadata/schema, UTC-сетка 10 секунд в одном дне и возрастающие
уникальные ключи `(timestamp, group_id, hostname)` проверяются reader.
Inventory — `a01`–`a16`, по 18 hosts; порядок F18 — `domain.FeatureNames`.
Пропущенные координаты — `NaN` в Go и `null` в JSON, finite zero сохраняется.

## Запуск

```sh
go mod download
go run ./cmd/replayer --input /path/to/assets/data/prepared/summit-D1-pilot/D1.parquet --speed 0
go run ./cmd/offline --assets /path/to/assets \
  --profile-sha256 a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36 \
  --window D2 --groups 16 --delta 0 --output /path/to/new-output
```

Offline создаёт `raw.ndjson`, `temporal.ndjson`, `evidence.ndjson`,
`events.ndjson`, `summary.json`; summary также идёт в stdout. Каталог должен
быть новым. Summary записывается только после успешного inference, закрытия
файлов и повторной проверки SHA. Поддерживаются δ=0/10/30/60, вложенные группы
4/8/16, D1/D2 и явное подокно `--start/--end`. Полная D2-сетка — 46 080 group-ticks,
включая пустые группы с Unknown. При ошибке возможен неполный выходной префикс.

У replayer по умолчанию `--completion-mode observed --wire-format legacy`:
старый NDJSON `telemetry`/`timestamp_complete`, markers только встреченных пар.
`REPLAYER_INPUT` и `REPLAYER_SPEED` поддерживаются; явные flags приоритетны.
Stdout — данные, stderr — диагностика, ошибка возвращает ненулевой код,
прерывание — 130. `--speed 0` отключает ожидания, положительный speed масштабирует
общую временную линию; исторический event time не является processing latency.

Полный входной поток:

```sh
go run ./cmd/replayer --input /path/to/assets/data/prepared/summit-D1-pilot/D1.parquet \
  --completion-mode full-grid --wire-format v1 --speed 0 \
  --start 2022-01-18T08:00:00Z --end 2022-01-18T16:00:00Z \
  --execution-id local-d2 \
  --input-sha256 cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7 \
  --profile-sha256 a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36
```

Full-grid требует явное окно `[start,end)` на UTC-сетке и перечисляет все 16 групп:
порядок timestamp → group_id → hostname, records пары до единственного marker.
Пустая пара получает counts 0/0; на D2 ровно 46 080 markers независимо от полноты.
Pacing выполняется один раз на tick, включая пустой. Подмножества inventory
в full-grid не поддерживаются. Source/order/sink/cancellation ошибки возвращаются
без success summary; повтор всего execution автоматически не выполняется.

`--wire-format v1` требует окно и явные execution/input/profile bindings.
Формат — один JSON object на строку:
`{version:"telemetry-input-v1",message_type:"telemetry_record"|"timestamp_complete",
binding:{execution_id,input_sha256,profile_sha256},group_id,timestamp,payload}`.
Payload record — `{hostname,node_index,f18:[18 numbers/null],is_valid,availability_mask}`;
payload marker — `{records,valid_records}`: все доступные строки / полные F18.
Codec проверяет canonical identity, UTC/grid/window, F18/mask/Valid, версию/тип,
отклоняет Inf, неизвестные и дублированные JSON-поля. Invalid/partial строки
сохраняются. Key обоих типов — group_id; mapping `a01→0,...,a16→15` явный, без hash.
Execution binding не переопределяет scientific RunID/ConfigID/EventID.

TickAssembler завершает tick только по marker, даже после 18 rows, хранит текущие
18 records, выдаёт явные missing hosts и отличает отсутствие записи от partial.
Массивы копируются на входе; ownership выходной map передаётся получателю.
`Finish()` обязателен: EOF без полной сетки — ошибка. Availability/payload-shape/count
rejects атомарны и допускают исправление сообщения. Identity/order/position,
duplicate/late ошибки fatal и останавливают сборку; exact/conflicting duplicates
различаются. Полный durable recovery математики и exactly-once в этом входном срезе отсутствуют.

`group.AnnotateOutside` использует другие группы того же tick. Его IF Outside-поля
не меняют голоса, но входят в golden Raw/Evidence. До независимого GroupProcessor
нужно решение о детерминированном обогащении вне него. Поля и семантика сохранены.

## Проверки и воспроизведение parity

```sh
go test ./...
go vet ./...
go build ./cmd/replayer
go build ./cmd/offline
git diff --check
```

Автономные тесты генерируют synthetic fixtures в `t.TempDir`; внешние проверки
без prerequisites дают явный SKIP. Synthetic E2E — `TestSyntheticInputPipeline`,
real input parity — `TestRealD2InputPipeline`: сравнение с прямым чтением Parquet,
NaN-aware missingness, исходные значения и 46 080 assembled ticks.

Scientific V1 закреплён на `c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc`.
Для проверки original source SHA нужен чистый checkout с LF, без CRLF-конверсии:

```sh
git -c core.autocrlf=false clone --no-checkout https://github.com/butorovv/cluster-anomaly-detection /path/to/v1-reference
git -C /path/to/v1-reference checkout --detach c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc
```

Минимальный [manifest исходных scientific Go-файлов](internal/app/offline/testdata/v1-scientific-files.json)
сохраняет все 40 source/SHA/target записей. `TestV1ScientificDeclarationsUnchanged`
проверяет исходные SHA и declarations; разрешены только прежние import/parser/path
relocations. Исследовательские loaders и численные зависимости `analysis`/`experiment`,
legacy Engine внутри RBI2Journal и Hardware/Y diagnostic schema сохранены.

Strict real D2 acceptance (PowerShell; заменить `/path/to/...` реальными путями):

```powershell
$env:OFFLINE_REAL_ROOT = (Resolve-Path '/path/to/assets').Path
$env:GROUP_REAL_ROOT = $env:OFFLINE_REAL_ROOT
$env:P03_REAL_ROOT = $env:OFFLINE_REAL_ROOT
$env:OFFLINE_V1_SOURCE = (Resolve-Path '/path/to/v1-reference').Path
$env:GROUP_REAL_REFERENCE = (Resolve-Path '/path/to/research/baseline-d2').Path
$env:GROUP_RBI2_RESEARCH = (Resolve-Path '/path/to/research/i2-focused').Path
$env:P03_REAL_ACCEPTANCE = '1'
go test ./internal/acceptance -run '^TestRealAcceptance$' -count=1 -timeout 40m -v
go test ./internal/app/offline -run '^TestV1ScientificDeclarationsUnchanged$' -count=1 -v
```

Entrypoint проверяет assets и запускает существующие canonical V1→V2 comparators
для δ0/10/30/60, independent research δ0 comparator и real input parity.
Сравниваются полные raw/temporal/evidence/events/summary; агрегаты не заменяют
поэлементное сравнение. Отсутствие обязательных env/файлов или неверные SHA дают
FAIL. TMP/TEMP/TMPDIR должны иметь >5 GiB свободного места для V1 disk guard.
V1 read-only; сборки и outputs comparator находятся во временных каталогах.

Независимые oracles — сохранённые до PR14 `raw.ndjson` и research
`journal-ticks.csv`, `event-tick-evidence.csv`, `two-level-journal.ndjson`.
Они не генерируются текущим V2; свежий вывод pinned V1 является отдельным
regression reference. Historical R+B metadata не заменяют RBI2 oracle.
Для остальных сохранённых regressions:

```powershell
$env:GROUP_I2_GOLDEN_TICKS = Join-Path $env:GROUP_RBI2_RESEARCH 'journal-ticks.csv'
go test ./internal/modules/detection/group -run '^(TestRealPerGroupD2Parity|TestI2RealD2SavedDecisions)$' -count=1 -v
```

Legacy replayer comparator отдельно закреплён на V1
`891e0ef71a0da0f9ad256f91d199fef38d1355a9`: указать чистый checkout этого commit
в `REPLAYER_V1_SOURCE`, затем выполнить
`go test ./internal/app/replayer -run '^TestV1Compatibility$' -count=1 -v`.
Пользовательские V1 checkouts ради переключения commit не менять.

## PostgreSQL V1–V5

```sh
DATABASE_URL='postgresql://...' go run ./db/cmd/migrate
TEST_DATABASE_URL='postgresql://.../postgres' go test ./db -count=1 -v
```

Единая executable серия `db/migrations/V1__core_schema.sql` … `V5__p01_master.sql`
не изменяется: V1 — исходная project schema, V2 — cad_v2 tables, V3 — ownership,
V4 — atomic checkpoint/outbox, V5 — Master acceptance. Номера 100–103 в исторических
SQL-комментариях относятся к прежним именам P01; текущие версии V2–V5.

Runner `db.Apply` применяет pending migrations одной транзакцией под advisory lock,
проверяет SHA в `public.schema_migrations`, восстанавливает search_path.
Повторный запуск применяет 0 миграций; изменённая/неизвестная история, пропуски
версий и существующие untracked project/cad_v2 отклоняются без автоматического
adoption или reset. `project.event` — эксплуатационный факт,
`project.telemetry` — long-format history; master events/checkpoints отдельны.

`db/tests/*.sql` нужны integration suites. Тесты создают/удаляют только собственные
случайные `p02_test_*` БД; нужны CREATEDB, права для временных test roles и завершения
своего backend в crash test. Проверяются fresh/upgrade/history/checksum/rollback,
сохранность telemetry, SQL invariants/privileges и concurrent fencing/crash/dedup.
SQL foundation не доказывает полноту будущего Snapshot/Restore.

`db/queries/f18.sql` сохранён как каноническая F18-проекция long-format истории;
текущий Parquet runtime её не вызывает. `db/scripts/grants.sql` нужен для выдачи
ограниченных прав существующим runtime roles; запускать migration owner:

```sh
psql "$DATABASE_URL" -v worker_role=cad_worker -v master_role=cad_master -f db/scripts/grants.sql
```

Runtime roles не должны владеть таблицами или наследовать migration owner.

## P04: Kafka input и consumer recovery PoC

Библиотека — [franz-go v1.21.0](https://github.com/twmb/franz-go/tree/v1.21.0),
admin API — `pkg/kadm v1.17.2`. Выбор основан на API: `ManualPartitioner`,
idempotent producer с all-ISR acknowledgements, `AdjustFetchOffsetsFn` до fetch
нового assignment, cooperative-sticky group protocol, lifecycle callbacks,
`BlockRebalanceOnPoll` и `NoResetOffset`. См.
[точный API/source](https://github.com/twmb/franz-go/blob/v1.21.0/pkg/kgo/config.go).
CGO/librdkafka и Python не требуются. Kafka commits не являются checkpoint.

### Локальный стенд (Docker Desktop, Linux containers)

Из корня репозитория, одинаково в PowerShell и sh:

```sh
docker compose up -d --wait kafka
docker compose run --rm topic-init
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic telemetry.raw
docker compose stop
docker compose start kafka
docker compose down
```

Закреплён официальный JVM image `apache/kafka:4.1.2`, KRaft single broker.
Host Go-приложения используют `localhost:9092`; контейнеры в Compose network —
`kafka:19092`. Внешний listener опубликован только на loopback. PLAINTEXT стенд
предназначен для локального PoC. Healthcheck проверяет Kafka API.
`topic-init` создаёт topic с 16 partitions, повторно проверяет именно фактический
count и завершается ошибкой при несовпадении; существующий topic не расширяется.
Автосоздание topics выключено. Kafka хранит log в named volume `kafka-data`;
`stop`/`down` сохраняют его. Намеренный сброс **только стенда**:
`docker compose down -v` (удаляет его Kafka-данные).
Один broker с replication factor 1 проверяет рестарты, но не отказоустойчивость
к потере диска. Kafka retention не заменяет долгосрочный SQL-архив.

К предыдущему full-grid примеру Replayer добавить:

```text
--output kafka --kafka-brokers localhost:9092 --kafka-delivery-timeout 30s
```

Обязателен явный `--wire-format v1`; default остаётся stdout/legacy/observed.
Topic runtime — `telemetry.raw`. Перед отправкой проверяются metadata, topic ID,
16 физических partitions с лидерами и canonical mapping. Record и marker имеют
key `group_id` и **явно** назначенную partition через `PartitionForGroup`;
hash partitioner/fallback отсутствует. Tests используют отдельные случайные
topics с тем же 16-partition контрактом; CLI/synthetic-Parquet/D2 tests проходят
именно через `telemetry.raw`, начиная с зафиксированных текущих end offsets.
Нельзя параллельно публиковать в него другой execution во время acceptance.

Producer вызывается последовательно существующим replay, сохраняет максимум
18 queued records одной пары `(group,timestamp)` и максимум 2 MiB SDK buffer.
`Send` означает принятие в bounded queue, **не доставку**. `Complete` сначала
ждёт всех acknowledgements records (`Flush` и callbacks), затем отдельно ждёт
all-ISR acknowledgement marker (`ProduceSync`). Поэтому marker не опережает
записи даже при retry; idempotence SDK включена, не отключается. После ошибки
producer latch-ится и следующий marker не публикуется. Replay success выдаётся
только после всех подтверждений и успешного Close. Deadline/cancellation/broker
errors возвращаются вызывающему; timeout может означать неоднозначную доставку
префикса, а не разрешение повторить весь input поверх того же execution.
Ошибку нельзя трактовать как пустой tick или двигать checkpoint. Пустой tick
имеет настоящий подтверждённый marker 0/0. Buffered records при неуспешном
завершении отменяются; неподтверждённый префикс не объявляется завершённым.

### Consumer harness и внешняя позиция

PoC находится в `internal/modules/telemetry/delivery/kafkainput` и его opt-in
Go integration harness; отдельного пустого `cmd/worker` нет.
`ConsumePartition` проверяет прямое чтение назначенной partition;
`ConsumeGroup` использует настоящий Consumer Group с cooperative-sticky
rebalance. Auto-commit отключён; оба пути проверяют topic/partition/key/group,
versioned codec, execution/source/profile bindings и непрерывность offsets.
Синтетический и D2 harness передают сообщения **существующему** TickAssembler,
проверяют отсутствующие hosts, counts, timestamps и вызывают `Finish()` на
достигнутом окне. До полного marker tick не публикуется. Любая ошибка данных
останавливает stream, а не пропускает сообщение. Сетевые group-session ошибки
диагностируются и повторяются до caller deadline; auth/data-loss/offset/schema
ошибки не классифицируются как сетевой retry. Для известного PLAINTEXT стенда
EOF во время старта брокера повторяется (`AlwaysRetryEOF`).

Assignment protocol:

1. Kafka назначает partitions; assigned/revoked/lost callbacks дают наблюдения.
2. `AdjustFetchOffsetsFn` заново загружает внешнюю позицию **для каждого нового
   assignment**, после Kafka OffsetFetch, до первого fetch. Отдельный admin client
   инициализирован до запуска group client; callback ограничен 10 секундами.
3. Сверяется полная `Identity`: topic name + immutable Kafka topic ID, partition,
   logical group, InputBinding и window. Topic recreation, чужая identity или
   отсутствующая позиция — ошибка. Kafka committed offset всегда перекрывается.
4. Проверяется `log_start <= next_offset <= log_end`, без отрицательных sentinel
   offsets. `NoResetOffset().At(next_offset).WithEpoch(-1)` исключает reset на
   earliest/latest и случайное использование Kafka committed leader epoch.
   Out-of-retention проверяется реальным `DeleteRecords` test; `NoResetOffset`
   дополнительно останавливает offset-out-of-range после начала чтения.
5. Poll выдаёт максимум 128 сообщений; callback обработки синхронный.
   Rebalance заблокирован до окончания batch, затем `AllowRebalance` освобождает
   его. На выходе `CloseAllowingRebalance` не зависает на финальном revoke.
   Здесь обработка короткая; будущая медленная DB/inference обработка обязана
   укладываться в rebalance timeout и использовать PostgreSQL ownership fencing.

Durable-offset fixture живёт вне consumer clients и не продвигается при
simulated crash. Он намеренно ограничен транспортом: проверено продолжение с
фактического offset 4 при Kafka commit 8, после crash прочитаны 4,5,6; затем
assignment `A=16 → A=8/B=8 → B=16 → A2=8/B=8`, повторная загрузка позиции и
обработка новых сообщений. Каждый assignment/revoke и владелец обработки
контролируются assertions. Это подтверждение protocol API при корректной group
сессии, а не обещание fencing при зависшем процессе или сетевом разделении.

Произвольный mid-tick offset сам по себе **не восстанавливает TickAssembler или
математическое состояние**. Recovery fixture использует marker-only ticks;
полный D2 harness начинает с начала окна. P05/P06 должны атомарно хранить
mathematical snapshot + input position, либо восстанавливать bounded input prefix
от безопасной границы tick перед продолжением. Raw/evidence/results из
неподтверждённого state нельзя публиковать: будущий checkpoint/outbox transaction
с ownership fencing остаётся обязательным. PoC не пишет detection results.

### Испытания

Обычный `go test ./...` автономен, Kafka suites opt-in. Явно включённый Kafka
режим падает при отсутствии брокера; strict также требует outage test.
Outage test останавливает **только Kafka этого Compose-проекта**, запускается
последовательно; не запускать его одновременно с D2 или другими Kafka suites.

```powershell
$env:P04_KAFKA_STRICT = '1'
$env:P04_KAFKA_OUTAGE = '1'
go test ./internal/modules/telemetry/delivery/kafkainput -count=1 -timeout 6m -v
Remove-Item Env:P04_KAFKA_STRICT, Env:P04_KAFKA_OUTAGE
$env:P04_KAFKA = '1'
go test ./internal/app/replayer -run '^TestKafkaReplayerSynthetic$' -count=1 -v
go test -race ./internal/modules/telemetry/delivery/kafkainput -run 'TestKafka(Synthetic|Invalid|Group|Cancellation)' -count=1 -timeout 6m
Remove-Item Env:P04_KAFKA
$env:P03_REAL_ROOT = (Resolve-Path '/path/to/original/assets').Path
$env:P04_REAL_ACCEPTANCE = '1'
go test ./internal/app/replayer -run '^TestKafkaRealD2$' -count=1 -timeout 30m -v
Remove-Item Env:P04_REAL_ACCEPTANCE
```

Synthetic: 560 records + 48 markers, 48 ticks всех 16 групп: полные 18-host,
неполные, partial F18/NaN и полностью пустые. Проверяются содержимое, порядок,
invalid JSON/schema/version/binding/key/physical routing, count mismatch,
cancellation, invalid/missing/expired checkpoints, broker stop/start и rebalance.
Отдельный generated Parquet→Replayer Kafka→assembler test проверяет 4 records,
48 ticks и полное поэлементное содержимое.

Real D2 test проверяет SHA оригинального Parquet, передаёт его потоково через
тот же Replayer и настоящий `telemetry.raw`, отдельно напрямую читает оригинал
и сравнивает per-partition SHA256 последовательностей **всех полей** GroupTick:
identities/bindings, timestamp, sorted hosts, NodeIndex, finite F18 float bits,
NaN-aware missingness, masks/Valid, missing hosts и counts. Порядок ticks каждой
группы входит в hash; TickAssembler дополнительно проверяет порядок hosts и
markers. Оба пути хранят лишь текущие ticks и 16 hash states, не весь Parquet.
Ожидаются 829 440 records + 46 080 markers = 875 520 Kafka messages и 46 080
assembled ticks. Это отдельная проверка от P03 mathematical golden acceptance,
команды которой приведены выше; synthetic не заменяет original D2.

### Граница будущего архива project.telemetry

Дополнительный контракт КР: доступные F18 измерения из Kafka долговременно
сохраняются в **существующей** `project.telemetry`; автоматического удаления нет.
Наличие таблицы ещё не означает надёжного Kafka→SQL ingestion. P04 проверяет её
DDL/каноническую F18 query и существующие SQL integration suites, но не создаёт
архивирующий runtime, новую архивную таблицу или V6. V1–V5 остаются неизменными.

`project` хранит topology/metric sources/значения; `cad_v2` — executions,
ownership/checkpoints/outbox/results. Перед ingestion должны быть заполнены
16 групп, 288 nodes, компоненты и ровно 18 canonical sources на node. Mapping
по `domain.FeatureNames` и `db/queries/f18.sql`:

| F18 keys | component/local_index | metric_type.code |
|---|---|---|
| `psu0_power`, `psu1_power` | psu 0..1 | PSU_INPUT_POWER |
| `cpu0_power`, `cpu1_power` | cpu 0..1 | CPU_POWER |
| `gpu0_power`..`gpu5_power` | gpu 0..5 | GPU_POWER |
| `cpu0_mean_temp`, `cpu1_mean_temp` | cpu 0..1 | CPU_MEAN_TEMP |
| `gpu0_core_temp`..`gpu5_core_temp` | gpu 0..5 | GPU_CORE_TEMP |

Проверять canonical source_key, совместимость вида/типа и единственность каждого
F18 key на node до записи. DDL защищает FK, `(component_id,metric_type_id)` и вид
компонента, но не глобальную canonical уникальность source_key. Записывать только
доступные finite координаты согласно mask, включая finite zero; NaN/отсутствие
никогда не заменять нулём и не записывать как PostgreSQL numeric NaN.
`telemetry.value` — `numeric NOT NULL`, `ts` — `timestamptz`, FK restrict;
уникальность `(metric_source_id,ts)` уже есть. Для float→numeric использовать
одну каноническую round-trip decimal конверсию; golden F18 остаётся отдельно
проверяемым исходным input, без обещания хранения IEEE payload bits в numeric.

Будущий writer: bounded batch/COPY в временный staging, проверка конфликтующих
повторов и insert отсутствующих значений одной транзакцией. Exact повтор
идемпотентен; другой value при той же паре — явная ошибка с rollback, а не
безусловный `ON CONFLICT DO NOTHING` или overwrite. Проверка должна оставаться
верной при конкуренции (serialization/locks + повторное сравнение). Все telemetry
данные и **отдельный архивный next_offset** фиксируются атомарно в PostgreSQL
до продвижения позиции; Kafka commit лишь вспомогателен. Если archive consumer
независим от detector — отдельная Consumer Group, позиция и fencing, чтобы
checkpoint детектора не выдавал недописанный архив за готовый. Архивируются и
partial/invalid records: каждая доступная координата; marker не измерение.

В старой таблице нет execution/source/profile binding, availability mask,
причины missing, признака отсутствующей строки/полностью пустого tick,
CPU valid-core count и preprocessing provenance. Пропуск coordinate сохраняет
отсутствие значения, но не объясняет его причину; timestamptz сохраняет instant,
а не исходную timezone. Эти ограничения не исправлять скрытой заменой нулями
или автоматической миграцией. Дизайн provenance отдельно согласовать в P05/P06;
immutable manifests/hashes исходных assets сохраняют научную воспроизводимость.

### AnnotateOutside: детерминированное обогащение для P05/P06

Код: `group.Evaluate` в `offline.go` сначала вычисляет локальные Raw всех групп,
затем `AnnotateOutside(rows)`; `rbi2_offline.go` вызывает Journal после аннотации.
Аннотация меняет **только** `raw.if.{outside_available,
outside_anomalous,outside_groups,outside_s,e}` (Go `Raw.IF`). Эти же пять полей
копируются в `RBI2Evidence.Raw` положительного tick через JSON snapshot в
`RBI2Journal.Step`. `support.go` отличает ещё не вычисленные outside counts
(`null`) от вычисленных нулей. Temporal и event не имеют outside-полей;
event IFScoreMax/IFSimultaneousMax/IFHosts используют локальный IF. R/B/I2,
2-of-3, δ, temporal transitions, EventID/RunID/ConfigID и Journal intervals
от outside-аннотации не зависят. Локальные S/Available/Anomalous не изменяются.

Предлагаемая граница — durable deterministic diagnostic join **после** независимых
GroupProcessor, без повторного Temporal/RBI2Journal на Master:

1. Worker считает локальную математику и Journal один раз, сохраняет Raw с
   локальными IF-счётчиками, evidence и events в своём atomic checkpoint/outbox.
   Outside пока не вычислен; provisional payload не публикуется как golden Raw.
2. Join keyed по `(scientific run_id, execution scope, timestamp)` получает
   полный **ожидаемый набор групп именно run (4/8/16)**, включая Unknown/empty.
   Проверяет profile/config/input binding и fencing версии execution; canonical
   duplicate с тем же content hash идемпотентен, конфликт — ошибка.
3. После полноты сортирует groups канонически и вызывает ту же неизменённую
   `AnnotateOutside` над копиями Raw. Обогащает только пять полей Raw и уже
   сохранённого Evidence.Raw по `(run,group,timestamp)`. Evidence ID и решение
   положительности не пересчитываются; events/Temporal остаются прежними.
4. Durable join input/completion/outbox допускает replay после crash без повторной
   публикации. Finalization идёт по timestamp watermark, затем group; внутри
   группы порядок worker sequence сохраняется. Events имеют собственный тип и
   sequence: если общий exporter требует causal evidence-before-event, event
   ждёт watermark последнего зависимого evidence/закрытия; порядок равных времён
   берётся из existing canonical exporter, не из порядка прихода Kafka.
5. Нет полного набора — нет final outside и нет «нулей вместо ещё не пришло».
   Timeout — диагностический blocked/incomplete run, не научное решение.
   Recovery использует durable join state и те же keys; задержка одной группы
   задерживает только final diagnostics/export, не локальный detector state.

Приёмка реализации P05/P06: для frozen windows/δ0/10/30/60 сравнить canonical
raw/temporal/evidence/events/summary с теми же V1 golden, затем перестановки
межгруппового прихода, exact/conflicting duplicates, crash на каждой join/outbox
границе и reassign Worker. P04 фиксирует проектное решение; сам join,
Snapshot/Restore, production checkpoint/archive writer и Master не реализованы.
