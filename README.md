# cluster-anomaly-detection-v2

Go-реализация курсовой системы обнаружения групповых аномалий Summit.
Главный архитектурный и математический контракт —
[утверждённое ТЗ](docs/spec/FINAL_TZ_KR_Distributed_Group_Anomaly_Detection_V2.md).

Реализованы frozen offline R/B/I2, Parquet replayer, versioned input codec,
TickAssembler, Kafka Producer/Consumer, PostgreSQL foundation V1–V5,
полный математический Snapshot/Restore и [Worker runtime](#worker-runtime-checkpoint-и-recovery)
с Consumer Group, SQL ownership/fencing и atomic checkpoint/outbox. Archive Writer
поддерживает Summit bootstrap/F18 mapping и идемпотентную запись в
`project.telemetry`. Production archival Kafka Consumer и координация его progress
с архивом, межгрупповой outside join, production Master и три выходных transport
modes пока не реализованы. Worker сохраняет только терминальные RBI2 events;
полная distributed Raw/Evidence golden parity пока не заявляется.
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

### AnnotateOutside: проект следующего межгруппового join

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

Приёмка будущего join: для frozen windows/δ0/10/30/60 сравнить canonical
raw/temporal/evidence/events/summary с теми же V1 golden, затем перестановки
межгруппового прихода, exact/conflicting duplicates, crash на каждой join/outbox
границе и reassign Worker. P04 фиксирует проектное решение; сам join, archival
Kafka Consumer и Master не реализованы. Snapshot/Restore реализован в P06,
Worker checkpoint/outbox runtime — в P07 (пока terminal-only events).
Самостоятельный Archive Writer реализован в P04-DB.

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
и возвращает `Transition` только после complete marker. Результат каждого tick
доступен в `Transition.Result`, но Raw/Evidence не публикуются как окончательные
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

Cadence P07 фиксирована **1 completed tick**. Partial tick живёт только в RAM,
никакой prefetched cursor не становится durable. При падении он перечитывается
целиком с последнего complete boundary. Контракты `Transition`/`Ack`/SQL уже
отделяют вычисление от durable barrier: cadence 3/6 потребует накопить outputs и
сохранить snapshot последнего завершённого tick вместе с его cursor, не меняя
Step/Flush/Snapshot и не включая offset частичного следующего tick. Сам batching
и его измерения остаются следующей работой.

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
NDJSON остаются вне Git; manifest содержит исходные пути, команды, pin, SHA и
параметры. Воспроизведение создаёт **новую** директорию и не изменяет V1:

```powershell
pwsh -File experiments/generate-golden.ps1 -V1Repository $env:V1_REPOSITORY -Assets $env:ASSETS_ROOT -Output .cache/golden-new -Manifest .cache/golden-new-manifest.json
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
