# cluster-anomaly-detection-v2

Go-реализация курсовой системы обнаружения групповых аномалий Summit.
Главный архитектурный и математический контракт —
[утверждённое ТЗ](docs/spec/FINAL_TZ_KR_Distributed_Group_Anomaly_Detection_V2.md).

Реализованы frozen offline R/B/I2, Parquet replayer, versioned input codec,
TickAssembler, Kafka Producer/Consumer, PostgreSQL foundation V1–V5,
полный математический Snapshot/Restore и [Worker runtime](#worker-runtime-checkpoint-и-recovery)
с Consumer Group, SQL ownership/fencing и atomic checkpoint/outbox. Archive Writer
поддерживает Summit bootstrap/F18 mapping и идемпотентную запись в
`project.telemetry`. Production archival Kafka Consumer и координация его progress с архивом остаются
отдельным расширением. P08–P10 добавляют Master, три транспорта и durable outside
join. Статусы parity, отказов и измерений подтверждаются JSON-артефактами ниже.
Runtime и тесты — Go; Python для запуска не требуется.
Сохранённые результаты `experiments/artifacts/` и независимые golden находятся
во внешнем архиве, который **не включён в GitHub**. Все пути к историческим
артефактам ниже доступны после восстановления по разделу
[хранения результатов](#хранение-экспериментальных-артефактов).

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
не меняют голоса, но входят в golden Raw/Evidence. В P10 обогащение выполняется после независимого GroupProcessor на Master;
поля и семантика сохранены.

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
SQL foundation проверяется отдельно от полноты математического Snapshot/Restore;
интеграцию с ним выполняет P07 Worker.

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
Effective `cleanup.policy` также проверяется через Admin API (`--describe --all`):
разрешён только `delete`, включая унаследованный broker default. Неизвестная или
недоступная конфигурация и любые комбинации с `compact` дают отказ. Существующая
конфигурация topic автоматически не исправляется. Для отдельного стенда можно
задать `KAFKA_HOST_PORT` и явно передать `docker compose -p <project> ...`.
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
полный D2 harness начинает с начала окна. P07 Worker поверх этого adapter хранит
полный mathematical snapshot + input position на complete boundary и переигрывает
только неподтверждённый хвост. Raw/evidence/results из неподтверждённого state
не публикуются; terminal events проходят SQL checkpoint/outbox transaction с
ownership fencing. Сам транспортный PoC не пишет detection results.

### Испытания

Обычный `go test ./...` автономен, Kafka suites opt-in. Явно включённый Kafka
режим падает при отсутствии брокера; strict также требует outage test.
Обычные Kafka suites используют `P04_KAFKA_BROKERS` (default `localhost:9092`).
Outage test создаёт отдельный Compose-проект и свободный loopback port; адрес
обычных suites не является разрешением остановить их broker. Можно явно задать
`P04_KAFKA_OUTAGE_PROJECT=p04-test-...` и `P04_KAFKA_OUTAGE_PORT`, но проект должен
быть новым, а порт свободным. Reservation исключает одновременный захват одного
имени двумя worktree. Перед stop/start проверяются случайный ownership token,
project/service labels, port binding и неизменный container ID. При неизвестном
владельце операции запрещены; cleanup удаляет только проверенные собственные
ресурсы. Docker недоступен или стенд не создан — strict FAIL. Без opt-in outage
даёт SKIP, а при strict отсутствие `P04_KAFKA_OUTAGE=1` даёт FAIL.

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
Отдельные тесты отклоняют `compact`, `compact,delete`, `delete,compact` до первой
публикации; разрешают explicit/inherited `delete` и проверяют fail-closed при
недоступности DescribeConfigs после успешного чтения metadata.
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

### PostgreSQL Archive Writer и граница будущего Kafka ingestion

Дополнительный контракт КР: доступные F18 измерения из Kafka долговременно
сохраняются в **существующей** `project.telemetry`; автоматического удаления нет.
P04-DB реализует самостоятельный `internal/modules/telemetry/archive/postgres`:
Summit bootstrap, F18 mapping и Archive Writer. Его можно вызывать напрямую для
prepared D2 без Kafka и пересчёта математики. Production Kafka→SQL consumer и
атомарное сохранение его progress ещё отсутствуют. Новых архивных таблиц или V6
нет; V1–V5 остаются неизменными.

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

Writer проверяет вид/индекс компонента, тип/единицы метрики и единственность каждого
F18 source на node. Исторические spelling `source_key` сохраняются; mapping строится
по компоненту и типу метрики. DDL защищает FK, `(component_id,metric_type_id)` и вид
компонента. Writer записывает только
доступные finite координаты согласно mask, включая finite zero; NaN/отсутствие
никогда не заменять нулём и не записывать как PostgreSQL numeric NaN.
`telemetry.value` — `numeric NOT NULL`, `ts` — `timestamptz`, FK restrict;
уникальность `(metric_source_id,ts)` уже есть. Для float→numeric использовать
одну каноническую round-trip decimal конверсию; golden F18 остаётся отдельно
проверяемым исходным input, без обещания хранения IEEE payload bits в numeric.

`WriteBatch` принимает до 4096 records; SQL statements содержат до 1024 измерений.
Запись выполняется через `INSERT ... SELECT FROM UNNEST ... ON CONFLICT DO NOTHING`,
затем отдельный READ COMMITTED запрос с row locks проверяет равенство значений.
Весь batch атомарен: exact повтор идемпотентен, конфликтующий value вызывает
rollback, существующие измерения не перезаписываются. Успех возвращается после
COMMIT; неоднозначный ответ допускает безопасный повтор тех же records.

Будущая интеграция должна фиксировать telemetry и **отдельный архивный next_offset**
атомарно в PostgreSQL
до продвижения позиции; Kafka commit лишь вспомогателен. Если archive consumer
независим от detector — отдельная Consumer Group, позиция и fencing, чтобы
checkpoint детектора не выдавал недописанный архив за готовый. Архивируются и
partial/invalid records: каждая доступная координата; marker не измерение.

В старой таблице нет execution/source/profile binding, availability mask,
причины missing, признака отсутствующей строки/полностью пустого tick,
CPU valid-core count и preprocessing provenance. Пропуск coordinate сохраняет
отсутствие значения, но не объясняет его причину; timestamptz сохраняет instant,
а не исходную timezone. Эти ограничения не исправлять скрытой заменой нулями
или автоматической миграцией. Дизайн архивного provenance отдельно согласовать;
immutable manifests/hashes исходных assets сохраняют научную воспроизводимость.

### AnnotateOutside: межгрупповой diagnostic join

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

Реализованная P10 граница — durable deterministic diagnostic join **после** независимых
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
4. Durable fragments, финальные results и completion marker допускают replay.
   Каждый timestamp завершается своей транзакцией. Глобальный watermark здесь
   не вводится. Канонический export читает завершённый execution в порядке
   timestamp/group; events — самостоятельные terminal сообщения.
5. Нет полного набора — нет final outside и нет «нулей вместо ещё не пришло».
   Timeout — диагностический blocked/incomplete run, не научное решение.
   Recovery использует durable join state и те же keys; задержка одной группы
   задерживает только final diagnostics/export, не локальный detector state.

Приёмка P10 сопоставляет каждый Raw/Temporal/Evidence/parent field с независимым
pinned V1, включая δ0/10/30/60, разные Worker counts, перестановки и recovery.
Результаты и ограничения — в разделе P10 ниже. Самостоятельный Archive Writer
реализован в P04-DB; его отдельный archival Kafka consumer не входит в этот runtime.

## Worker runtime: checkpoint и recovery

Реализованы P07-A–F: один процесс обслуживает все назначенные ему Kafka
partitions, несколько процессов используют общий `group.id`. Partition `0..15`
соответствует `a01..a16`; конкретный Worker выбирает Kafka Consumer Group.
Математика, P06 Snapshot/Restore, frozen assets и SQL-миграции V1–V5 не менялись.

### Архитектура и API

```
cmd/worker → internal/app/worker.Runner
    → kafkainput.ConsumeGroup
    → detection/usecase.Partition.Accept(offset, InputMessage)
    → TickAssembler → GroupProcessor.Step / Finish / Snapshot
    → postgres.Store.Save(lease, transition)
    → cad_v2.commit_checkpoint → COMMIT → Partition.Ack(revision)
```

`detection/usecase` не импортирует Kafka/pgx. `Partition` последовательно принимает
сообщения с точными offsets, проверяет binding/order через существующий assembler
и возвращает `Transition` только после complete marker. Результаты batch доступны
в `Transition.Results`; `Transition.Result.Events` объединяет закрытые events
batch. Raw/Evidence не публикуются Worker как окончательные
`group_result`: outside diagnostics ещё не вычислены. Терминальные `RBI2Event`
сохраняются целиком, с существующими научными RunID/ConfigID/EventID.

`Store` предоставляет `Load`, `Acquire`, `Renew`, `Release`, `Save`, `Pending`.
`Register` — отдельная административная операция. Runtime использует SELECT и
существующие security-definer функции V3/V4, без прямого DML и без второго fencing.
Master использует V5 `accept_result`; его runtime описан в разделе P08 ниже.

### Assignment, progress и recovery

1. `OnPartitionsAssigned` синхронно регистрирует фактическое назначение.
2. `AdjustFetchOffsetsFn` вызывает `Load` до первого fetch новой partition.
   `Load` выполняет `acquire_partition`, читает checkpoint в той же короткой
   транзакции, затем проверяет DTO и вызывает `RestoreGroupProcessor`.
3. Проверяются cluster/topic incarnation, input/profile/execution binding,
   математическая identity, version/codec, точный внешний SHA и внутренние SHA DTO.
   Начало без checkpoint разрешено только для явно зарегистрированного execution
   с известными start offsets и без существующего outbox этой группы.
4. PostgreSQL `next_offset` заменяет Kafka committed offset. После acquisition/
   restore повторно читаются Kafka bounds; потерянный tail — явная ошибка.
5. На каждом complete tick математический snapshot, offset **сразу после marker**
   и все терминальные outputs фиксируются одной транзакцией через
   `cad_v2.commit_checkpoint`. Функция проверяет CAS revision и SQL owner epoch.
   Только после COMMIT приложение вызывает `Ack`.
6. Ошибка вычисления/входа/commit останавливает consumer и уничтожает его RAM.
   При неизвестном исходе COMMIT store читает durable revision/offset/bytes/hash/
   outbox на отдельном bounded запросе. Подтверждённый точный результат разрешает
   ACK; иначе Worker завершается. Слепого write retry нет.
7. `renew_partition` поддерживает SQL lease. Ошибка renew отменяет consumer.
   Revoke/lost удаляет состояние и вызывает `release_partition`. Старый epoch
   не может выполнить checkpoint, renew или release после takeover.

`BlockRebalanceOnPoll` и `PollRecords(128)` ограничивают обрабатываемый batch;
`AllowRebalance` вызывается после него, а `CloseAllowingRebalance` обеспечивает
финальный revoke на ошибке/завершении. Lifecycle hooks — синхронные barriers с
возвращаемой ошибкой; `Observe` остаётся диагностическим. Hooks и offset loader
сериализованы адаптером. Отдельный admin client только читает source metadata,
второго consumer нет. Kafka commits отключены полностью.

P07 использовал cadence=1. P11 расширяет durable boundary до 1/3/6 complete ticks
без изменения Step/Flush/Snapshot. Partial tick и весь неподтверждённый batch
переигрываются от последнего PostgreSQL next_offset; см. P11 ниже.

### Регистрация и запуск

Нужны PostgreSQL 17 с применёнными V1–V5 и Kafka `telemetry.raw`: 16 partitions,
`cleanup.policy=delete`, PLAINTEXT. Для runtime роли применить
`db/scripts/grants.sql` от migration owner. SQL statement/lock/idle transaction
timeouts задаются CLI на соединениях. Lease TTL: 3–15 секунд, default 10 секунд.
Слишком медленная обработка/БД может остановить Worker по истёкшей lease;
он не пытается оживить старый epoch.

Пример PowerShell; подставить фактические immutable assets, UUID и hashes:

```powershell
$env:WORKER_DATABASE_URL = 'postgresql://USER:PASSWORD@HOST:5432/DB'
$workerArgs = @(
  '--assets', 'C:/assets/summit',
  '--profile-sha256', 'PROFILE_SHA256',
  '--input-sha256', 'PREPARED_INPUT_SHA256',
  '--execution-id', 'EXECUTION_UUID',
  '--consumer-group', 'cad-v2-execution',
  '--kafka-brokers', 'localhost:9092', '--delta', '30'
)
# Run once with administrative credentials, before starting replay:
# Explicitly choose actual offsets; zeros below are only for a fresh empty topic.
$offsets = @{}
0..15 | ForEach-Object { $offsets["$_"] = 0 }
go run ./cmd/worker @workerArgs --register-execution --start-offsets ($offsets | ConvertTo-Json -Compress)
# Switch WORKER_DATABASE_URL to the restricted runtime role.
go run ./cmd/worker @workerArgs
# Run the same command in another terminal for a second Worker.
```

`--start`/`--end` default to the frozen D2 window; custom aligned UTC windows are
allowed. Each process gets a random owner UUID. JSON logs report assignment,
restore/epoch/cursor, checkpoint/tick/events and errors. Ctrl+C/SIGTERM closes the
consumer and releases leases. DSN and telemetry payloads are not logged.

Регистрация проверяет frozen identity существующей строки run/execution и не
перезаписывает её. SQL JSON `start_offsets` содержит прежние числовые ключи 0..15
и immutable `_p07` metadata: version, topic ID, input binding. Это расширение
совместимо с существующим `seed_partitions`; V1–V5 не требуют изменений.
Исторические executions без `_p07` не усыновляются автоматически.

### Outbox

Envelope v1: `kind=rbi2_event`, `terminal=true`, `revision=1`. Stable `message_id`
использует предложенный V4 SHA256 canonical JSON массива
`["cad-output-v1",run_id,group_id,"rbi2_event",event_id,1]`. Epoch, wall clock,
checkpoint revision и attempt не влияют на identity. Payload — frozen event без
изменений. `Pending` читает durable backlog в порядке `(checkpoint_revision,
ordinal)` после любого restart/reassignment. Доставка/ACK подключаются общим dispatcher в разделе P09 ниже.

### Проверка на реальных сервисах

Worker integration tests требуют **отдельный тестовый Kafka broker**, поскольку
используют канонический `telemetry.raw` и в retention test удаляют тестовый prefix.
Параллельные suites на одном `telemetry.raw` не запускать. Каждому сценарию
создаются случайные execution/group и отдельная БД `p07_worker_*`; store suites
создают `p07_store_*`. Очистка ограничена созданными тестом БД/ролями.
Пользовательские volumes и Docker cleanup не используются.

```powershell
$env:P07_TEST_DATABASE_URL = 'postgres://TEST_ADMIN@127.0.0.1:PORT/postgres?sslmode=disable'
$env:P07_TEST_KAFKA_BROKERS = '127.0.0.1:KAFKA_PORT'
$env:P07_TEST_KAFKA_ISOLATED = '1'
$env:P07_WORKER_INTEGRATION = '1'
go test ./internal/app/worker ./internal/modules/detection/repository/postgres -count=1 -v -timeout 8m
go test -race ./internal/app/worker ./internal/modules/detection/repository/postgres ./internal/modules/detection/usecase -count=1 -timeout 8m
$env:P04_KAFKA = '1'
$env:P04_KAFKA_BROKERS = $env:P07_TEST_KAFKA_BROKERS
go test ./internal/modules/telemetry/delivery/kafkainput -run 'TestKafkaLifecycle|TestKafkaGroup' -count=1 -v -timeout 3m
```

Реальные сценарии: непрерывный Worker; 1→2→1 владельцев при rebalance (δ0/30);
kill отдельного `cmd/worker` с открытым Journal и partial tick; rollback до COMMIT;
потеря ACK после COMMIT; lease loss; stale epoch race; SHA/version/frozen identity;
topic identity mismatch; retention gap. Восстанавливаемые сценарии сравнивают
весь конечный математический snapshot и полные терминальные events с непрерывным
контрольным выполнением, включая EventID, VoteSegments и RBSubspans.
Потерянный tail не восстанавливается — ожидается явная остановка.
SQL-тест дополнительно проверяет атомарный rollback при ошибке последнего output,
runtime grants и приём terminal envelope существующим V5 `accept_result`.

Служебный Kafka test подтверждает assignment→Load→Handler, cleanup lifecycle и
проверку актуальных bounds после Load. Существующий P04 test дополнительно
подтверждает приоритет external offset над заранее записанным Kafka commit.

### Результат проверки 4 ноября 2026

Стенд: Go 1.27.2, PostgreSQL 17.5, Apache Kafka 4.1.2 / OpenJDK 21.0.1;
отдельные loopback listeners и случайные тестовые БД, без Docker cleanup.

| Проверка | Наблюдаемый результат |
|---|---|
| `go test ./... -count=1` | PASS; opt-in integration отдельно ниже |
| `go vet ./...`, `git diff --check` | PASS |
| Worker Kafka/PostgreSQL suite под `-race` | PASS, 82.961 s, все сценарии выше |
| PostgreSQL store suite под `-race` | PASS, 20.018 s |
| Processing core под `-race` | PASS, 2.622 s; δ0/10/30/60 |
| Kafka external offset priority/rebalance под `-race` | PASS; Kafka commit 8, durable 4 → `[4,5,6]` |
| Kafka lifecycle restore/fresh bounds под `-race` | PASS отдельно на свежем broker, 3.600 s |
| Frozen science/configs и SQL V1–V5 diff от `e4e16ce` | Пустой |

COMMIT fault injection оборачивает настоящий `pgx.Tx`: один режим выполняет
ROLLBACK вместо COMMIT, другой выполняет COMMIT и теряет только ACK. Это
детерминированная проверка двух исходов, а не заявление о сетевом chaos proxy.
Crash test действительно принудительно завершает отдельный `cmd/worker` процесс.

У native Kafka на этом Windows-стенде cleanup P04 topics вызывает
`AccessDeniedException` при rename partition directory и остановку broker.
Совместный запуск двух P04-тестов поэтому завершился инфраструктурным FAIL;
каждый из двух сценариев подтверждён отдельно (lifecycle повторён на свежем
broker). Полная P07 Worker/store race-suite не удаляет topics и прошла целиком
одним запуском. Docker/Linux broker в этой сессии не проверялся.

## P08: Master Core

`cmd/master` принимает общий envelope v1 через `POST /v1/results`, возвращает ACK
только после COMMIT `cad_v2.accept_result` и предоставляет пагинированный
`GET /v1/events?execution_id=UUID&group_id=a01&after=EVENT_ID&limit=100`.
`MASTER_DATABASE_URL` задаёт DSN; `--listen` по умолчанию `127.0.0.1:8080`.
Обычная runtime-роль Master использует существующий `db/scripts/grants.sql`.
`/healthz` проверяет HTTP lifecycle; startup проверяет PostgreSQL.

Общий generic `events/domain.Message[T]` сохраняет P07 wire representation;
RawMessage на приёме сохраняет оригинальный payload, типизированный RBI2Event
используется Worker. Проверяются duplicate JSON keys, schema/version, scientific
identity и execution/group/event binding; SQL сериализует конкурирующий приём,
дедупликацию и revision projection. Master не считает Temporal/Journal.

Слои: `events/domain`, `events/usecase`, `events/repository/postgres`,
`events/delivery/httpinput`, `app/master`, `cmd/master`. Полная документация
следующих этапов интегрируется сюда; новые Markdown-отчёты не создаются.

Проверка P08: реальные PostgreSQL tests под `-race` подтвердили exact retries,
конфликтующий payload/alternate message_id, terminal revision 3 до open revisions
1/2, сохранность payload и изоляцию executions. Проверка 40 scientific declarations
на LF checkout V1 `c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc` прошла.

## P09: Polling / HTTP Push / Kafka results

`--output-transport polling|http_push|kafka` фиксируется при регистрации execution;
обычный Worker обязан использовать тот же режим. `--listen 127.0.0.1:0` создаёт
собственный HTTP endpoint на свободном порту; для контейнеров задать `--listen`
и reachable `--advertise-url` явно. Push использует `--master-url`, Kafka —
`group.results` с 16 partitions, delete policy, group_id key и all-ISR ACK.
`--create-results-topic` — явная идемпотентная инициализация/проверка output topic.

Master: `--mode http_push` (default), либо `--mode polling --execution-id UUID
--poll-interval 100ms`, либо `--mode kafka --execution-id UUID --kafka-brokers ...
--results-group ...`. Для первого создания output topic добавить
`--create-results-topic`. Входная Kafka group и results group независимы.
Master и Worker используют доверенную внутреннюю сеть стенда; наружные порты
рекомендуется привязывать к loopback.

V6 добавляет только operational metadata: текущий endpoint с owner epoch,
`master_poll_cursors`, binding incarnation output topic и отдельные
`result_consumer_offsets`. Применить общий migration runner, затем обновлённый
`db/scripts/grants.sql`. Миграции V1–V5 не изменяются. Новые функции чтения outbox
и ACK используют существующий SQL `assert_owner`/`ack_outbox`.

Polling: Master discovery читает owner/lease/endpoint, затем GET `/v1/outbox` у
текущего Worker. Результаты идут через HTTP, не SQL. Cursor `(revision,ordinal)`
фиксируется в одной Master-транзакции с `accept_result`. Следующий HTTP запрос
передаёт durable cursor; Worker проверяет его в SQL и ACK-ает соответствующий
outbox prefix. После rebalance/restart cursor сохраняется, endpoint меняется.
Есть ограничение page count/bytes, HTTP deadlines и последовательное backpressure.

Push: общий dispatcher читает только durable pending rows, проверяет актуальный
локальный owner перед новой отправкой, повторяет сообщения с capped exponential
backoff и вызывает fenced SQL ACK только после durable Master receipt. Receipt
`accepted=false` означает exact duplicate и тоже подтверждает доставку.

Kafka: dispatcher ACK-ает outbox после all-ISR broker ACK. Это требует retention
output topic, покрывающей отставание Master, и надёжного broker storage; общей
Kafka+PostgreSQL exactly-once транзакции нет. Master атомарно сохраняет
`accept_result` и **отдельный output next_offset**, затем делает вспомогательный
Kafka commit. При restart/assignment SQL output offset заменяет Kafka commit;
потерянный retention tail или изменённый topic ID дают явную ошибку. Этот cursor
никогда не заменяет input offset математического Worker.

Проверка P09 на Kafka 4.1.2/PostgreSQL 17.5 под `-race`: все три режима дали
одинаковые 32 terminal parents и canonical payload hashes при одинаковом frozen
fixture. Проверены lost HTTP ACK, restart Master/results consumer, 1→2→1 Worker,
перенос обработки после rebalance и durable pending outbox. Master SQL-роль в
этом тесте не имеет SELECT на Worker outbox: прямой SQL polling технически
отклоняется. Отдельно прошли fresh/upgrade V6, исходные V1–V5 suites и SQL fencing.

```powershell
$env:P07_TEST_DATABASE_URL = 'postgres://TEST_ADMIN@127.0.0.1:PGPORT/postgres?sslmode=disable'
$env:P07_TEST_KAFKA_BROKERS = '127.0.0.1:KAFKAPORT'
$env:P07_TEST_KAFKA_ISOLATED = '1'
$env:P07_WORKER_INTEGRATION = '1'
go test -race ./internal/app/worker -run '^TestWorkerThreeTransportParityAndRestart$' -count=1 -v -timeout 5m
```

## P10: durable outside join и независимый golden

Обычная регистрация Worker включает `--diagnostics=true`. Этот флаг — immutable
execution metadata; старый terminal-only execution требует `--diagnostics=false`.
`--groups 4|8|16` задаёт научный universe и сохраняет физические 16 input partitions.
Невыбранные группы валидируются адаптером, но не создают математическое состояние.
Назначение выбранных групп по-прежнему выполняет Kafka Consumer Group.

V7 добавляет отдельные `worker_tick_fragments`, `master_tick_fragments` и маркер
завершённого join. Snapshot, authoritative input offset, terminal events и локальные
fragments фиксируются **одной** Worker-транзакцией. Fragment использует явно
отличимый envelope schema 2/kind `tick_fragment`; он не является финальным
`group_result` и сохраняет `outside_groups=null`. Все три транспорта переносят
одинаковые durable messages и используют один Master usecase.

Master сериализует короткий join по execution/timestamp, читает только 4/8/16
fragments одного timestamp и вызывает существующий `group.AnnotateOutside` на
копиях Raw. Заполняются Raw и Evidence.Raw; Temporal, scientific IDs и terminal
parents остаются исходными. Финальные schema 1 `group_result` и join marker
записываются в той же транзакции, что последний fragment. Повтор/перестановка и
restart безопасны; незавершённый timestamp остаётся durable и не выдаётся как
готовый результат. Полный D2 в памяти Master не хранится.

`GET /v1/results?execution_id=UUID&after_tick=RFC3339&after_group=a01&limit=100`
возвращает bounded pages финальных Raw/Temporal/Evidence. `GET /v1/summary` с
`execution_id` агрегирует принятые поля; Master не пересчитывает голоса.
`complete` в summary означает наличие всех group ticks; при демонстрации также
дождаться finalized checkpoints и опустевшего outbox для доставки Flush events.

Независимый источник: V1 pin
`c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc`. Текущий HEAD произвольного V1 не
используется. В `_worknotes/nir-final-kernel-selection` найдены сохранённые
baseline/research artifacts; provenance и SHA записаны в
`experiments/artifacts/golden-manifest.json`. P03 real acceptance подтвердила
40 scientific declarations, все δ и независимые исследовательские δ0 значения
46 080 / 1 284 / 750 / 473 / 285 / 277.

Свежие эталоны из отдельного чистого LF checkout pinned V1 для 4/8/16 × δ0/10/30/60
зарегистрированы в `experiments/artifacts/v1-generated-golden.json`. Большие
NDJSON и manifest остаются вне Git; manifest содержит исходные пути, команды, pin, SHA и
параметры. Воспроизведение создаёт **новую** директорию и не изменяет V1:

```powershell
pwsh -File experiments/generate-golden.ps1 -V1Repository $env:V1_REPOSITORY -Assets $env:ASSETS_ROOT -Output .cache/golden-new -Manifest .cache/golden-new-manifest.json
$env:V1_GOLDEN_MANIFEST = (Resolve-Path .cache/golden-new-manifest.json).Path
$env:SYSTEM_TEST_DATABASE_URL = 'postgres://TEST_ADMIN@127.0.0.1:PGPORT/postgres?sslmode=disable'
$env:SYSTEM_KAFKA_ISOLATED = '1'
$env:P07_TEST_KAFKA_BROKERS = '127.0.0.1:KAFKAPORT'
$env:V1_GOLDEN_ROOT = (Resolve-Path .cache/golden-new).Path
go run ./experiments/runner --config experiments/configs/p10-first-d2.json
```

В config также задать путь `golden_manifest` к новому manifest и новый `output`.
Runner собирает реальные процессы Replayer/Worker/Master, создаёт отдельную
случайную PostgreSQL database и Consumer Group, фиксирует input topic identity и
start offsets, запускает полный D2 и сравнивает каждое поле Raw, Temporal,
Evidence, parents, VoteSegments, RBSubspans и IDs с pinned V1. Comparator не
вызывает detector. Счётчики и canonical stream SHA сохраняются в JSON.
В конце удаляется только созданная этим запуском database; input topic и
пользовательские volumes не удаляются. Стенд должен быть изолирован от других
replays, поскольку input topic фиксирован контрактом.

Первые полные D2 результаты находятся в `experiments/artifacts/p10-first-d2/` и
`experiments/artifacts/p10-full-matrix/`. `elapsed_seconds` включает setup,
проверку reference, replay, drain и comparator; это **не** отдельная latency
обработки или транспорта. Для научного сравнения производительности использовать
специальную методику P11 с processing timestamps, а не исторический event_time.

Concurrent join использует единый порядок блокировок physical groups перед FK
INSERT. Это необходимо из-за существующего `V5.accept_result FOR UPDATE`: без
предварительного порядка два timestamp могли удерживать FK `KEY SHARE` и ждать
взаимного upgrade. Детерминированный SQL regression воспроизвёл три `40P01` без
исправления и прошёл под `-race` после него. Ownership rows этим Master-барьером
не блокируются. Первоначальный FAIL и результаты исправления сохраняются в
`experiments/artifacts/p10-join-lock-regression.json`; V1–V5 не изменены.
Порядок блокировок помещён внутрь V7 `accept_tick_fragment SECURITY DEFINER`;
Master не получает прямой UPDATE на execution metadata. Тот же concurrent тест
прошёл под ограниченной runtime-ролью, у которой прямой `SELECT ... FOR UPDATE`
отклоняется. `db.GrantRuntime` применяет тот же grants.sql из Go bootstrap/tests.

## P11: cadence, отказные сценарии и методика измерений

`--checkpoint-interval-ticks 1|3|6` задаётся при регистрации и на каждом Worker.
Несовпадение с immutable `executions.checkpoint_interval_ticks` — ошибка.
RunID/ConfigID/EventID от cadence не зависят. `NewPartitionWithCadence` вызывает
исходный Step на каждом marker и блокирует input до ACK только на границе batch.
`Transition.Results` содержит все завершённые ticks, `Result.Events` — все parents,
закрытые в любом tick batch. Последний tick окна выполняет Finish и фиксирует
короткий хвост тем же checkpoint/outbox барьером. Graceful shutdown не подтверждает
RAM-only хвост; следующий owner переигрывает его.

Unit race suite проверяет cadence × все δ, потерю complete batch tail + частичного
следующего tick, точные поля результатов, IDs, snapshot SHA и короткую finalization.
Реальные SQL tests проверяют batch atomicity, COMMIT rollback/lost ACK и immutable
cadence; `TestWorkerCadenceProcessCrashAndReplay` принудительно убивает отдельный
Worker и проверяет повторную сборку input, открытый EventID, все VoteSegments и
RBSubspans, отсутствие потерянных/дублированных fragments. Фактические статусы
интеграционных запусков сохраняются отдельно; наличие теста не означает PASS.

Экспериментальные configs:

- `p10-full-matrix.json`: точная математическая parity, затем kill Worker внутри
  durable open parent, rebalance и restart Master; cadence=1.
- `p11-batch-crash.json`: mid-event kill при cadence=3/6 с полным D2 comparator.
- `p11-measured.json`: scaling 1/2/4/8/16, cadence 1/3/6 и все transports при
  одинаковых frozen inputs, δ0, 16 групп, speed=0; polling 10/100/500 ms.

```powershell
go run ./experiments/runner --config experiments/configs/p10-full-matrix.json --resume
go run ./experiments/runner --config experiments/configs/p11-batch-crash.json --resume
go run ./experiments/runner --config experiments/configs/p11-measured.json --resume
python experiments/summarize.py --matrix experiments/artifacts/p11-measured/matrix.json --matrix experiments/artifacts/p11-batch-crash/matrix.json --output experiments/artifacts/summary-new
```

`--resume` пропускает сохранённый PASS с той же RunSpec. Неуспешные попытки и их
logs сохраняются перед новым запуском. Для нового независимого повторения изменить
`output`; для восстановления анализа без replay использовать
`--analyze-existing path/to/run.json` (нужны сохранённые logs и metrics samples).
Большие операционные `.log` и binary файлы остаются локальными; численные результаты,
конфигурации запусков хранятся в Git; provenance, численные результаты,
resource aggregates и samples — во внешнем архиве (раздел хранения ниже).

`--measure=true` на Worker/Master включает технические JSON traces без F18.
Измерения не входят в immutable scientific envelope. Определения:

| Показатель | Граница |
|---|---|
| compute | monotonic Step + Finish; без snapshot serialization/SQL |
| input queue | реальный Kafka CreateTime первой записи tick → первый приём Worker |
| durability wait | завершение вычисления tick → получение durable checkpoint ACK; включает ожидание batch и serialization |
| transport | начало отправки durable message → вход в Master intake; включает decode и очередь внутри polling page |
| Master commit | вход intake → получение ACK PostgreSQL; последний fragment включает весь outside join |
| E2E | реальный CreateTime первой записи tick → ACK последнего fragment всех групп timestamp, то есть финального join |
| throughput | complete group ticks / интервал earliest source CreateTime → last join ACK |

ACK — наблюдаемая верхняя граница durable commit, не точный server commit instant.
Durability и transport могут перекрываться: dispatcher видит committed outbox
раньше, чем input handler получил ACK. Поэтому эти компоненты нельзя механически
складывать. Исторический Summit timestamp используется только для корреляции.
Все процессы находятся на одном хосте; отрицательные/невалидные clock samples
вызывают ошибку анализа. Межхостовой стенд требует отдельной проверки clock skew.

Quantiles — exact nearest rank по traces, registry histograms на `/metrics` —
ограниченные buckets для наблюдения. Resource sampler раз в секунду читает native
process CPU seconds/RSS (Windows или Linux), heap, input lag и durable backlog.
Пики RSS/backlog/lag ограничены частотой sampling. Счётчики различают accepted,
duplicates, conflicts, output attempts/retries и payload bytes. Дополнительно учитываются фактические байты protocol streams: HTTP client
для Push/Polling, producer + consumer client для Kafka (обе broker legs).
Это включает HTTP/Kafka framing и control requests, но исключает TCP/IP headers,
PostgreSQL/input и внутренний broker traffic: последние — `NOT_MEASURED`.
Result compression явно выключена во всех режимах; input Snappy остаётся общим. CPU/RSS broker, PostgreSQL и Replayer отдельно не измеряются.

Каждый Worker имеет GOMAXPROCS=1, Master=2, Replayer=1. PostgreSQL fsync и
synchronous_commit включены; Kafka all-ISR в single-broker стенде не доказывает
переживание потери broker disk. Kafka auxiliary commit выполняется после durable PostgreSQL обработки всего
PollRecords batch (до 128); logical ACK/progress по-прежнему фиксируется на каждое
сообщение. Kafka output требует retention до durable intake
Master. Для нового Kafka execution output start регистрируется **до** первого
Worker publish, чтобы чужие старые executions не искажали transport benchmark;
restart использует сохранённый output cursor.

Один полный запуск на конфигурацию даёт предварительные сравнительные точки,
не confidence intervals. Shared database, централизованный diagnostic join,
внутренние CPU/I/O конкуренты и Windows scheduler ограничивают выводы о speedup.
Нельзя переносить такие числа на межхостовую production сеть без новых измерений.

## P12: Compose, наблюдаемость и демонстрация

```mermaid
flowchart LR
  R[Replayer: prepared F18] --> K[Kafka telemetry.raw: 16 partitions]
  K --> W[Dynamic Worker Consumer Group]
  W --> A[TickAssembler → frozen GroupProcessor]
  A --> C[PostgreSQL: snapshot + input offset + terminal outbox + fragments]
  C --> D[Current-owner dispatcher / HTTP outbox endpoint]
  D --> P[Polling / HTTP Push / group.results]
  P --> M[Master validation + SQL dedup]
  M --> J[Durable timestamp join: existing AnnotateOutside]
  J --> F[Final Raw / Temporal / Evidence]
  M --> E[Unchanged terminal RBI2 events]
```

`deploy/compose.yaml` содержит PostgreSQL, Kafka, административный bootstrap,
Master, масштабируемый Worker и opt-in Replayer. Assets монтируются read-only;
не копируются в image. Bootstrap применяет V1–V7, создаёт отдельные минимальные
runtime roles, применяет тот же `grants.sql`, проверяет topics и явно регистрирует
новый execution с initial offsets. Runtime не получает migration ownership.
Порты хоста привязаны к loopback; служебный plaintext HTTP/Kafka предназначен для
изолированной исследовательской сети. Пароли examples заменить; для DSN использовать
URL-safe значения или percent encoding.

Kafka configuration сверена с [официальным single-node example](https://github.com/apache/kafka/blob/4.1.2/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml).
Каждый Worker сообщает свой container IP, а не общий round-robin DNS alias.
Healthchecks ждут PostgreSQL, Kafka, завершения bootstrap и runtime `/readyz`.
`/healthz` — liveness; `/readyz` дополнительно проверяет SQL, а у Worker — восстановленное
назначение. `/metrics` возвращает JSON: execution/owner labels, counters, gauges,
bounded histograms и native CPU/RSS. Большие F18 payload в logs не выводятся.

```powershell
Copy-Item deploy/.env.example deploy/.env
# В deploy/.env задать настоящий ASSETS_ROOT, новые пароли и новый EXECUTION_ID.
docker compose --env-file deploy/.env -p cad-demo -f deploy/compose.yaml config --quiet
docker compose --env-file deploy/.env -p cad-demo -f deploy/compose.yaml up -d --build --wait --scale worker=4 master worker
docker compose --env-file deploy/.env -p cad-demo -f deploy/compose.yaml run --rm --no-deps replayer
# Обзор owner/epoch/recovery/checkpoint:
docker compose --env-file deploy/.env -p cad-demo -f deploy/compose.yaml logs --follow worker
```

Для смены transport требуется новый execution UUID с тем же scientific run.
Изменение режима существующего execution отклоняется. Для повторного replay тоже
использовать новый execution, а не дописывать второй D2 в finalized execution.

`deploy/demo.ps1` автоматизирует три режима на одном scientific δ30 run: запуск
нескольких Worker, ожидание durable open parent, сопоставление owner с конкретным
container через `/metrics`, kill, rebalance/Restore и проверку сохранённого EventID.
Итоговые summary и операционные параметры сохраняются в JSON, logs — локально.
Демонстрационный smoke не заменяет независимый full golden comparator.

```powershell
pwsh -File deploy/demo.ps1 -EnvironmentFile deploy/.env -Workers 4 -Output experiments/artifacts/demo-new
```

Скрипт не удаляет volumes и оставляет проект для осмотра. Для остановки достаточно
`docker compose ... stop`; не применять prune или down -v к пользовательским данным.
Первоначальная попытка Docker была NOT_RUN (историческая запись сохранена).
После запуска engine выполнен реальный Compose smoke с 4 Worker во всех трёх
режимах: 960 ticks и 12 terminal events на окно 08:00–08:10, одинаковые payload
hashes, kill/rebalance/Restore и сохранённые EventID. Итоги:
`experiments/artifacts/compose-demo/summary.json` и `results.json`.

Материалы для РПЗ: разделы README описывают immutable scientific contract,
архитектуру, границы транзакции/fencing, три транспорта и методику измерений;
`experiments/artifacts` содержит provenance, численные результаты и ограничения.
R/B/I2 показывают статистическую групповую аномальность. Они не устанавливают
аппаратный отказ, физическую причину, подтверждённый incident или диагностическую
точность без независимой разметки. Полная университетская РПЗ здесь не создаётся.
Для Kafka delivered outbox означает broker ACK; отставание Master после этого
видно в E2E/intake, а не в Worker backlog. Для Push/Polling backlog заканчивается
после durable Master ACK. Сопоставлять backlog без этой разницы нельзя.


### Фактическая приёмка P08–P12

P10 matrix **26/26 PASS**, отдельный первый D2 PASS, два полных mid-event crash D2
при cadence 3/6 PASS. Измерительная матрица **11/11 PASS**, каждый запуск также
прошёл независимый pinned V1 comparator. Итого 40 успешных полных распределённых
D2 запусков. Первый failing 16-Worker lock case сохранён отдельно; причина
устранена и покрыта deterministic concurrency regression.

δ0 контроль: 46 080 ticks / 1 284 positive / 750 parents / 473 new / 285 RBSubspans /
277 RB-supported parents. Сравнивались все поля, IDs и canonical stream hashes.
Provenance: `golden-manifest.json`, `v1-generated-golden.json`,
`p03-independent-acceptance.json` в `experiments/artifacts`.

Таблицы: `experiments/artifacts/final-summary/parity.csv` и `measurements.csv`;
графики scaling/resources/transport-latency/recovery находятся рядом. Original
run JSON — в `p11-measured/`; подробные resource samples вынесены в локальный архив.

| Worker (cadence=3, Push) | group ticks/s | speedup относительно 1 |
|---|---:|---:|
| 1 | 178.48 | 1.00 |
| 2 | 257.57 | 1.44 |
| 4 | 266.60 | 1.49 |
| 8 | 251.56 | 1.41 |
| 16 | 251.33 | 1.41 |

| Режим, 4 Worker/cadence=3 | ticks/s | transport p95 | E2E p95 |
|---|---:|---:|---:|
| HTTP Push | 266.60 | 1.608 ms | 133.417 s |
| Polling 10 ms | 211.35 | 442.103 ms | 175.107 s |
| Polling 100 ms | 217.21 | 472.679 ms | 170.823 s |
| Polling 500 ms | 196.11 | 528.640 ms | 194.994 s |
| Kafka | 171.21 | 184.105 s | 224.591 s |

Kafka transport interval включает broker queue до последовательного Master
intake. Высокое значение отражает backlog burst replay, а не TCP RTT. Polling
включает очередь элементов page. Push обслуживает параллельные HTTP requests,
Kafka intake здесь последователен; бизнес-логика и SQL транзакция на сообщение
общие, concurrency adapters различается. Это измерение реализованной системы,
не доказательство оптимальности транспорта. Docker engine включён пользователем
во время последнего Kafka-замера; контейнерные работы начались после него.
Этот background-load фактор и один запуск на configuration исключают строгие
статистические выводы.

Реальные отказы: Worker kill, partial tick/batch replay, COMMIT rollback/lost ACK,
pending outbox + Master restart, lease loss/stale epoch, rebalance, SHA/version/
frozen binding corruption, retention gap, PostgreSQL TCP outage и Kafka disconnect
(8 реальных sockets). Worker race suite PASS 214.028 s; отдельный Kafka network
race PASS 9.820 s. Machine-readable сводка — `experiments/artifacts/validation.json`.

Compose проверен на Docker 28.4.0/WSL2 Linux. `RUN_START`/`RUN_END` позволяют короткий
smoke; defaults — полное D2. Image собирает только bootstrap service, остальные
приложения используют тот же image. `CAD_IMAGE` задаёт отдельный test tag.
Пользовательские volumes не удалялись. Проект оставлен для осмотра; параметры без
паролей — `compose-demo/summary.json`.

Перед защитой: полная университетская РПЗ (вне этого задания), warmup/replications
на спокойном стенде и отдельная межхостовая сеть. Single-host таблицы и ограничения
clock/network следует сохранить в выводах. Статистическая аномальность не
устанавливает аппаратную причину; физическая диагностика здесь не заявляется.

## P13: независимый технический аудит

Аудит начат от опубликованного `de44b65850063782ff86493e8c9dede1b6a91573`.
P08–P12 ещё не были слиты в main, поэтому изменения аудита продолжают ту же
ветку. Frozen scientific packages, SQL V1–V5 и генеральное ТЗ не изменяются.
Результаты аудита сохраняются в `experiments/artifacts/p13-audit/`.

### Input и независимый golden

`summit-D1-pilot/D1.parquet` — имя утверждённого prepared **полного дня**:
2 488 320 records с 00:00:00 до 23:59:50 UTC. Научное окно D2 выбирается отдельно:
`2022-01-18T08:00:00Z <= timestamp < 2022-01-18T16:00:00Z`.
Независимый Arrow reader фактически прочитал файл с утверждённым SHA
`cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7`
и подтвердил 829 440 records, 46 080 group ticks, 51 840 records каждой группы,
первую/последнюю выбранные записи 08:00:00/15:59:50. На реальном D2 нет partial,
empty или invalid ticks; эти случаи проверяются отдельными synthetic integration
fixtures. Имя файла не означает повторное воспроизведение training window.

Повторный P03 real acceptance сравнил V2 с отдельным чистым scientific V1 pin
для всех δ0/10/30/60 по каждому полю и каноническим байтам. Отдельно проверены
оригинальные исследовательские δ0 значения и exact float bits входного pipeline.
Saved audit перечитал SHA всех 12 V1 golden-наборов (4/8/16 × четыре δ), заново
получил их canonical hashes и сопоставил с 40 сохранёнными SQL comparator PASS.
Это проверка provenance старых SQL comparisons, не повторное чтение удалённых
экспериментальных databases. У старых reports binary SHA есть, но исторический
build path не сохранялся и мог быть перезаписан; его повторная проверка отдельно
помечена `NOT_VERIFIED`, без подмены новым binary.

```powershell
# Python 3.12 / pyarrow 24.0.0 использованы при аудите входа.
python experiments/audit_input.py --assets $env:ASSETS_ROOT --output .cache/input-audit-new.json
go run ./experiments/runner --audit-saved experiments/artifacts --golden-root $env:V1_GOLDEN_ROOT --golden-manifest experiments/artifacts/v1-generated-golden.json --audit-output .cache/saved-audit-new.json
python experiments/audit_measurements.py --run experiments/artifacts/p11-measured/transport-w4-c3-kafka.json --output .cache/latency-audit-new.json
```

### Исправления качества измерений и ресурсов

В P11 sampler сравнивал 18-символьный prefix с 19-символьной строкой.
Из-за этого **все исторические input Kafka lag=0 не подтверждены**. Они не
означают отсутствие lag. P13 исправляет aggregation, проверяет nonzero gauges
regression-тестом и добавляет `input_kafka_lag_status`. Новые CSV оставляют старое
значение пустым с `NOT_VERIFIED_LEGACY_SAMPLER`; исходные JSON сохраняются как
историческое свидетельство. Timing analysis также отвергает неполные traces.

`--resume` теперь связывает PASS с фактическими input/profile SHA, общими
параметрами стенда и SHA golden manifest. Проверяются reference и сохранённые
binaries. Сборка запускается только для новых сценариев, в отдельную директорию
`bin/build-*`, не перезаписывая старые executable. Legacy reports без такого
binding следует проверять через `--audit-saved`, а не автоматически переигрывать.
Saved PASS всегда описывает первоначальный execution, не запуск текущего кода.

HTTP sender и Poller закрывают собственные idle connection pools при shutdown;
отдельный socket regression подтверждает закрытие. V5→V7 upgrade проверяется
на существующих execution/checkpoint/outbox/Master rows: все исходные строки
сохраняются точно, migrations повторно идемпотентны, outbox доступен после upgrade.

### Почему transport и E2E latency велики

`latency-decomposition.json` заново связывает реальные worker/master traces.
Для каждого tick сумма последовательных границ от свежего Kafka CreateTime до
финального join ACK точно восстанавливает E2E; исторический Summit event_time в
вычитаниях не участвует. Квантили отдельных компонентов складывать нельзя.

В старом Kafka замере 46 830 сообщений последовательно проходили Master intake.
Сумма intake→commit 256.424 s заняла 95.3% интервала 269.079 s; максимум 38 154
отправленных сообщения ещё не вошли в intake. Следовательно, 184.105 s p95
не являются broker/network RTT. Старые traces не отделяют broker/pre-fetch queue
от очереди внутри возвращённого PollRecords batch: эти компоненты `NOT_MEASURED`.
Новая instrumentation отдельно измеряет producer identity check, publish ACK,
CreateTime→PollRecords return, PollRecords return→intake, intake service и
вспомогательный Kafka commit. CreateTime→poll всё ещё включает producer transit,
broker и franz-go prefetch; это не чистое измерение broker queue.

При Push/4 Worker p95 durable ACK→send составляет 109.453 s, source→durable
27.922 s. Очередь durable outbox объясняет большой E2E при compute <1 ms.
Небольшое отрицательное ACK→send допустимо: outbox виден после server commit
раньше, чем input handler получил COMMIT ACK; это сохранённое перекрытие границ.

При росте Push с 4 до 16 Worker средний intake→commit вырос с 12.456 до
56.731 ms, sampled backlog — с 34 555 до 38 308, CPU Worker — с 122.375 до
210.906 s, throughput снизился. V7 действительно сериализует join по execution
порядком SQL locks. Это подтверждает наличие последовательной границы и рост
contention; точные исторические PostgreSQL lock-wait durations не измерялись.
P13 добавляет 1 Hz `pg_stat_activity` wait-event samples: counts — наблюдения
активных backend, а не точные секунды lock wait.

Для проверки транспортов с одинаковой последовательной intake concurrency
выполнены два trial на режим при одном Worker, 16 группах, δ0, cadence=3,
общих моделях/payload/resources и burst replay. Используется
`experiments/configs/p13-repeated.json`; каждый запуск проходит full-D2 comparator.
Это отдельный контроль, его throughput нельзя смешивать со старым 4-Worker
замером. Два trial не дают надёжного confidence interval и не заменяют повторные
scaling эксперименты на выделенном межхостовом стенде.

```powershell
# Для чистого checkout сначала задать env из P10 и сгенерировать pinned V1 golden.
# Сохранённые в архиве PASS не являются новым запуском на новом стенде.
$auditConfig = Get-Content experiments/configs/p13-repeated.json -Raw | ConvertFrom-Json
$auditConfig.output = '.cache/p13-runs-new'
$auditConfig.golden_manifest = '.cache/golden-new-manifest.json'
$auditConfig | ConvertTo-Json -Depth 20 | Set-Content .cache/p13-config-new.json -Encoding utf8NoBOM
go run ./experiments/runner --config .cache/p13-config-new.json
# Только для продолжения ЭТОГО output с сохранёнными локальными binaries:
go run ./experiments/runner --config .cache/p13-config-new.json --resume
```

Все шесть контрольных запусков дали точное full-D2 совпадение с pinned V1:
46 080 Raw/Temporal ticks, 1 284 positive/Evidence, 750 parents, неизменные IDs,
VoteSegments/RBSubspans и canonical hashes. Вместе с прежними сохранёнными PASS
подтверждены 46 полных distributed запусков. Это шесть новых измерительных
контролей, а не повторение всей прежней матрицы.

| Транспорт, 1 Worker/cadence=3, n=2 | Наблюдаемый throughput, ticks/s | Transport p95 | E2E p95 |
|---|---:|---:|---:|
| HTTP Push | 142.86–148.85 | 0.714 ms | 202.27–208.30 s |
| Polling 100 ms | 172.33–182.73 | 397.09–487.43 ms | 139.08–142.89 s |
| Kafka | 130.64–131.23 | 768.87–925.40 ms | 232.29–233.08 s |

JSON, точные значения каждого trial и график наблюдаемых диапазонов:
`p13-audit/repetitions.json` и `repetitions.png`. Источники —
`experiments/artifacts/p13-repeated/matrix.json`, individual reports и resource
samples из локального архива; большие logs/binaries остаются вне Git. Общие scientific IDs, hashes,
binary/configuration SHA и размеры payload проверяются перед сравнением:

```powershell
python experiments/summarize_repetitions.py --matrix experiments/artifacts/p13-repeated/matrix.json --output .cache/repetitions-new.json
```

Все 11 исторических measured reports независимо пересчитаны из traces и resource
samples: p50/p95/p99, throughput, CPU/RSS, backlog и counters совпали. Исправленный
CSV проверен против JSON, включая speedup/efficiency; четыре исходных графика
побайтно совпали с повторной генерацией. Доказательства:
`p13-audit/verified-all-historical-measurements.json` и `derived-artifacts.json`.
Воспроизведение 46 saved comparisons с 12 V1 наборами и rehash шести новых binaries
сохранено в `p13-audit/current-saved-parity.json`.

В Kafka controls средний publish ACK 1.584/1.654 ms, identity check 4.147/4.152 ms,
CreateTime→poll 214.39/193.29 ms, poll→intake 126.43/91.78 ms, intake service
5.448/5.472 ms. Вспомогательные Kafka commits выполнялись 11 593/11 830 раз
со средним 6.831/6.773 ms: при фактическом batch около четырёх records их overhead
заметен. Ограничение `PollRecords(...,128)` задаёт максимум, не гарантированный
размер batch. SQL output progress по-прежнему фиксируется на каждое сообщение.
Эти интервалы перекрываются; publish ACK нельзя добавлять к CreateTime→poll как
ещё один непересекающийся этап. Оптимизация consumer/SQL delivery в P13 не вводилась.

Новые sampled input lag максимумы ненулевые; PostgreSQL samples наблюдали
`Lock/transactionid` и I/O waits. Времена ожидания отдельных SQL locks по таким
samples точно восстановить нельзя. Первый Push control частично пересёкся с
короткой Python-проверкой исторических traces; builds/integration suites к началу
controls были завершены. Burst replay одинаково настроен `speed=0`, но реальная
скорость producer меняется с нагрузкой. Сравнение latency при фиксированной
offered rate, повторные scaling trials и межхостовая сеть остаются отдельными
экспериментальными ограничениями. Результаты не обосновывают универсальный рейтинг
транспортов или принципиальный предел Kafka.

Аудит также исправил timing retained assignment: после восстановления из SQL
отбрасываются timestamps и batch timings неподтверждённого RAM tail. Реальный
SQL regression до исправления давал четыре durability traces вместо трёх;
после него — ровно три, три fragments, `next_offset=56` и snapshot, побайтно
равный непрерывному контролю. Шесть healthy controls имели ровно 16 initial
restores каждый, без этого повторного restore; их timing результаты не затронуты.
Startup error теперь закрывает output sender ровно один раз, даже если dispatcher
ещё не начал Run. `GET /v1/events` отличает некорректный запрос (400) от
недоступной БД (503). Для всех исправлений добавлены targeted regression/race tests.

Итоговый аудит — `p13-audit/kr-matrix.json`, `findings.json`, `validation.json`,
`integrity.json` и SHA index `artifact-index.json`. Исторические приёмочные JSON
не переписываются; при расхождении оценки приоритет имеет P13. KR-16 остаётся
**PARTIAL** по статистической полноте и перечисленным measurement limitations;
KR-19 — **PARTIAL**, поскольку полная РПЗ вне задачи. Функциональные и
математические требования подтверждены, но полное закрытие КР/готовность всей
РПЗ к защите не объявляется. Frozen science, SQL V1–V5 и генеральное ТЗ сохранены.

### Хранение экспериментальных артефактов

Git содержит исходники, unit/integration tests, SQL-миграции, frozen configs,
Compose/Dockerfile, scripts и `experiments/configs/`. Все сгенерированные
результаты исключены правилом `/experiments/artifacts/`; локальные оригиналы
сохранены. История Git не переписывалась. Ранее записанные в README результаты
являются историческими измерениями; их доказательства находятся в архиве.

Полная внешняя резервная копия на commit `8e1e3bf`:
`C:/study/NIR_KP_6_SEM/NIR_KR_7_SEM/experiment-archives/cluster-anomaly-detection-v2/all-results-8e1e3bf-20261105/`.
Она содержит `experiment-results.zip`, `manifest.json` с SHA-256 каждого файла
и `SHA256SUMS`. Всего 618 файлов, 5 181 236 133 исходных байта;
ZIP занимает 1 072 456 101 байт. SHA-256 ZIP:
`291c8abace271250e967dec6521c7ec469cba436ae14268bdebe95ea8452cae5`;
manifest: `a5e6784066e7a565245a6b9f3f156dd215bc79595c8cad8d9dd3b5a223454d4d`.
 В неё включены все локальные `experiments/artifacts/`,
логи/измерения/графики, 12 независимо сгенерированных pinned V1 golden-наборов,
исторические V1 эталоны по исходному manifest и прежняя резервная копия samples.
Проверяются SHA всего ZIP, каждого распакованного файла и восстановление в новый
каталог. Архив может содержать локальные параметры тестового стенда; это данные
для отдельного хранения, а не часть Docker image или поставки приложения.

**Внешний локальный архив не включён в GitHub и должен быть сохранён отдельно**,
вместе с `manifest.json` и `SHA256SUMS`. Прежний архив
`metrics-cee8feb-20261105` в том же родительском каталоге не изменён.

```powershell
$archive = 'C:/study/NIR_KP_6_SEM/NIR_KR_7_SEM/experiment-archives/cluster-anomaly-detection-v2/all-results-8e1e3bf-20261105'
python experiments/archive_results.py verify --manifest "$archive/manifest.json"
# Только новая директория; существующие файлы никогда не перезаписываются.
python experiments/archive_results.py restore --manifest "$archive/manifest.json" --destination .cache/results-restored-new
```

Внутри восстановленного каталога `repo/` сохраняет исходную структуру
`experiments/artifacts/` и `.cache/`. `external/historical-golden/` содержит
исторические независимые эталоны с прежними относительными путями;
`previous-archives/` — неизменённую прежнюю резервную копию raw samples.
Manifest также сохраняет оригинальные абсолютные пути. Для анализа можно
использовать пути прямо из `repo/`, не копируя файлы обратно в checkout:

```powershell
$results = '.cache/results-restored-new/repo/experiments/artifacts'
python experiments/summarize.py --matrix "$results/p11-measured/matrix.json" --matrix "$results/p11-batch-crash/matrix.json" --matrix "$results/p10-full-matrix/matrix.json" --matrix "$results/p10-first-d2/matrix.json" --output .cache/plots-restored-new
python experiments/summarize_repetitions.py --matrix "$results/p13-repeated/matrix.json" --output .cache/repetitions-restored-new.json
```

CSV и графики строятся из matrix/run JSON с aggregates; сырые samples им не
нужны. `audit_measurements.py` и Go `--analyze-existing` дополнительно требуют
samples и trace logs: в локальной копии run JSON задать `log_directory` по новому
расположению. Для команд выше по README с исходным layout можно скопировать
`repo/experiments/artifacts` обратно только в отсутствующую директорию checkout.
Не перезаписывать локальные результаты. Пути provenance в старых manifests
исторические; для проверки сохранённых bytes использовать новый архивный manifest.

Полное независимое воспроизведение требует оригинальных Summit assets с SHA
из начала README и V1 repository с pin
`c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc`. Наличие архива не заменяет эти inputs.
`experiments/generate-golden.ps1` строит pinned V1 в отдельном checkout и создаёт
12 наборов для 4/8/16 групп и δ=0/10/30/60, фиксируя команды и SHA в новом manifest;
результаты V2 не используются как независимый baseline. Команды генерации и
запуска runner приведены в разделе P10. Сохранённые configs получают путь к
новому manifest из `$env:V1_GOLDEN_MANIFEST`; для возобновления архивного запуска
можно указать восстановленный manifest вместо генерации. Перед новым опытом
использовать новый output. Исторические PASS не требуют повторного D2-прогона.

Для нового полного backup до следующей очистки:

```powershell
python experiments/archive_results.py create --repo . --archive-dir C:/experiment-backups/new-unique-directory
```

Эта команда ожидает восстановленный исходный layout результатов и manifests;
у новых запусков обновить provenance перед созданием архива. Все источники
остаются на месте. Архивирование/восстановление не удаляет данные и не затрагивает
PostgreSQL databases или Docker volumes.

## P14: исследование производительности (в работе)

Ветка P14 создана от актуального `main`
`c9a81e637c18c628c708c59a1816708bce6e0847`: PR #12 слит, P13-CLEAN включён.
Локальный `main` не изменялся. Scientific V1 pin остаётся
`c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc`; исходные Summit assets проверены.
Go 1.27.2, Windows 11 10.0.26200, i7-12650H (10 cores/16 threads),
16 782 188 544 байт RAM, NVMe SSD; PostgreSQL 17.5 на Windows,
Kafka 4.1.2 в отдельном Docker/WSL2 стенде. Worker GOMAXPROCS=1,
Master=2, SQL fsync/synchronous_commit/full_page_writes включены.

P14-A **PRELIMINARY**: один новый full-D2 baseline Kafka/4 Worker/cadence=3/δ0,
без Go profiling, прошёл independent pinned V1 comparator: 46 080 Raw/Temporal,
1 284 Evidence, 750 parents, canonical hashes и все научные поля совпали.
Throughput pipeline — 123.382 ticks/s; setup и comparator в него не входят.
Это новый стенд: значения не объединяются с историческим P11 scaling (n=1)
и P13 controls (n=2) в одну оценку ускорения.

| Измерение нового baseline | Значение |
|---|---:|
| Compute p95 | 1.191 ms |
| Computed→durable confirmation p95 | 209.355 ms |
| Send→Master intake p95 (включает очередь) | 173.094 s |
| Intake→commit p95 | 29.412 ms |
| E2E до final join p95 | 200.132 s |
| Intake service / intake span | 361.161 / 373.298 s (96.75%) |
| Максимум send-before-intake queue | 32 384 messages |
| Durable ACK→send p95 | 17.078 s |

Независимый пересчёт boundaries дал нулевую ошибку телескопического разложения E2E.
Отрицательное signed ACK→send сохраняется при видимости outbox до client COMMIT ACK.
Quantiles стадий не складываются; SQL histograms, application stages и вложенные
pg_stat_statements перекрываются. Wait-event samples — наблюдения, не секунды.

Отдельный profiling run собрал CPU/heap/allocs/goroutine/mutex/block для Master
и четырёх Worker. Он остановился по lock_timeout и **не считается** успешным
benchmark/parity run. Последний полный снимок: 6 700 fragments, 55 028 строк
повторно прочитанных payloads, fetch/decode 11.128 s; AnnotateOutside — 307 вызовов,
1.765 ms суммарно. В первом allocation snapshot acceptFragmentTx включал 52.60%
alloc_space. SQL показал конфликты V7 FOR UPDATE с FK KEY SHARE Worker fragments.
Ослабление durability/fencing/timeouts не применялось.
H1/H2/H4 пока PARTIALLY_CONFIRMED, H3/H5/H6 — INCONCLUSIVE; causal improvements,
повторы и окончательные выводы ещё не получены. P14-D исследует readiness count
до чтения payloads при прежнем порядке блокировок и прежней атомарной транзакции.

Архив P14-A (старые P13 архивы не изменены):
`C:/study/NIR_KP_6_SEM/NIR_KR_7_SEM/experiment-archives/cluster-anomaly-detection-v2/p14-performance-20261105/p14-a-6b50489/`.
ZIP SHA-256: `ad6e4d9675ec21cb7e84e486aa026e7d1829a67c560b8c182b03620fc56e1f4b`.
Проверены все 169 файлов (685 392 058 исходных байт) и restore в новый каталог.
Архив содержит среду, original/failed runs, три профильных окна каждого процесса,
SQL statistics/plans, timing audit и machine-readable P14-A findings.
Legacy runner из source-only git archive обнаруживал HEAD родительского checkout;
`*.verified-source.json` исправляет только metadata в новом derived файле после
сверки каждого source byte с повторным `git archive c9a81e6`. Original JSON,
бинарные SHA, измерения и parity не переписываются.

```powershell
# Указать собственные isolated services, immutable assets и независимый V1 golden.
$env:SYSTEM_TEST_DATABASE_URL = 'postgres://TEST_ADMIN@127.0.0.1:PGPORT/postgres?sslmode=disable'
$env:SYSTEM_KAFKA_ISOLATED = '1'
$env:P07_TEST_KAFKA_BROKERS = '127.0.0.1:KAFKAPORT'
$env:ASSETS_ROOT = '/path/to/immutable/assets'
$env:V1_GOLDEN_ROOT = '/path/to/independent-golden'
$env:V1_GOLDEN_MANIFEST = '/path/to/v1-generated-golden.json'
# Profiling отдельно от окончательных throughput trials; output должен быть новым.
go run ./experiments/runner --config experiments/configs/p14-a-profile.json
# Все pprof endpoints выключены по умолчанию; CAD_PPROF=1 включает их только для
# loopback clients. Runner задаёт это явно лишь в profile=true experiment.
python experiments/archive_results.py verify --manifest /path/to/p14-archive/manifest.json
python experiments/archive_results.py restore --manifest /path/to/p14-archive/manifest.json --destination .cache/new-p14-restore
# Новый bounded-scope backup не требует копировать старые P13 results/golden.
python experiments/archive_results.py create --repo . --results-root experiments/artifacts --archive-dir /path/outside/repo/new-p14-backup
```
P14-D readiness-only (`e125d73`) **не принят как самостоятельный вариант**:
из трёх попыток две прошли parity (159.324/163.256 ticks/s), третья остановилась
по Worker SQL lock_timeout. Baseline n=3: 123.382/132.759/151.619 ticks/s;
медиана 132.759, sample stdev 14.381. Отказ не скрывается исключением из выборки.
Новый логический дефект readiness count не доказан: исходный profiling также
обнаружил V7 FK coupling. Требуется устранить сам конфликт и повторить сравнение.

Новая V8 (`b2af3fe`) сохраняет глобальный canonical guard Master и меняет
только identity-row locks `execution_partitions` на FK-compatible
`FOR NO KEY UPDATE`, включая final projection. Этот mode по-прежнему исключает
competing writers и key mutation/delete; V3 ownership fencing и блокировки
`master_events` остаются прежними. Основа — [матрица PostgreSQL 17 row locks](https://www.postgresql.org/docs/17/explicit-locking.html#LOCKING-ROWS).
Старые SQL V1–V7 не переписывались. Проверены red FK test на V7, green FK-reader /
writer exclusion на V8, rollback/retry, fresh/V5/V7 upgrade, snapshot/outbox/fragment
preservation, duplicate/reorder, concurrent timestamps, targeted race и vet.
Read-only Inspector review не выявил actionable findings; это не заменяет runtime
проверки. V8 остаётся кандидатом, приёмка не завершена.

### P14-D: границы текущей серии и стенд

D1 (`e125d73`) — только readiness check и сокращение повторного чтения/JSON decode.
D2 (`b2af3fe`, SQL V8) — отдельное изменение locking. Измерения совмещённого кода
обозначаются **D1+D2**; их эффект нельзя приписывать одному из этих изменений.

Ниже все попытки основной текущей native-Windows серии при одинаковых параметрах:
4 Worker, 16 групп, полный Summit D2, Kafka results, cadence=3, δ=0, burst replay=0.
Profiling, дополнительные SQL observers, предварительные проверки среды и отдельный
Linux pilot в эту таблицу throughput не входят; их исходные отчёты/логи сохраняются.

| Вариант | Попытки | PASS / FAIL | ticks/s успешных попыток | Independent golden | Решение |
|---|---:|---:|---|---|---|
| P13 baseline (`c9a81e6`) | 3 | 3 / 0 | 123.382; 132.759; 151.619 | PASS ×3 | Reference, n=3 |
| D1 (`e125d73`) | 3 | 2 / 1 | 159.324; 163.256 | PASS ×2; NOT_REACHED ×1 | REJECTED для самостоятельного применения |
| D1+D2 (`c30d2b3`) | 2 | 1 / 1 | 166.559 | PASS ×1; NOT_REACHED ×1 | INCONCLUSIVE, не принят |

В обоих неуспешных кандидатных запусках Worker завершился после SQL lock_timeout;
rebalance последовал за отказом. Третья запланированная попытка D1+D2 не запускалась
и не считается попыткой. NOT_REACHED означает остановку до comparator, а не
обнаруженное расхождение математики. Ускорение успешных попыток само по себе не
является основанием для приёмки менее устойчивого варианта.

Отдельный diagnostic run D1+D2 с наблюдением SQL прошёл full golden, но не входит
в чистое сравнение. В нём наблюдалась цепочка: INSERT в stage_tick_fragment ждёт
relation extension lock; владелец занят IO/DataFileExtend; другие операции ожидают
ownership row этой транзакции. PostgreSQL записал ожидание extension 1380.673 ms,
связанные transaction waits — до 1577 ms. Это отдельный механизм от FK KEY SHARE;
первопричина дисковой задержки и причины каждого предыдущего отказа ещё не доказаны.
Нельзя объяснять все отказы только ошибкой FK locking или только свойствами диска.
До приёмки V8 требуется закончить детерминированную проверку relation extension
и разбор оставшихся отказов; timeouts и durable/fencing contract не ослабляются.

Основа следующих benchmark — существующие P12 `deploy/compose.yaml`,
`deploy/Dockerfile` и bootstrap. Они уже развёртывают Worker, Master, Kafka и
PostgreSQL. Допустим отдельный override для одинаковых CPU/RAM limits, наблюдения
и изолированных данных; новая архитектура развёртывания или отдельный проект
переноса Windows → Linux не требуются. `deploy/demo.ps1` остаётся проверкой
Compose/recovery; performance harness дополнительно обеспечивает измерения и
независимый full golden. P12 demo сам по себе не является full golden benchmark.

Baseline P13, D1 и D1+D2 сравниваются внутри одного такого стенда с одинаковыми
assets/model SHA, ресурсами, GOMAXPROCS, transport, cadence, δ и replay intensity.
Предыдущий Linux pilot сохраняется как неуспешная диагностическая попытка и не
сравнивается с native-Windows throughput для доказательства оптимизации. Нужны
точные source SHA, applied migrations, image digests и настройки PostgreSQL/Kafka.
До решения по D/V8 новые оптимизации producer, Master concurrency и Dispatcher
не начинаются. Существующие P13 baseline и архивы остаются неизменными.
