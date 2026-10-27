# cluster-anomaly-detection-v2

Go-реализация курсовой системы обнаружения групповых аномалий Summit.
Главный архитектурный и математический контракт —
[утверждённое ТЗ](docs/spec/FINAL_TZ_KR_Distributed_Group_Anomaly_Detection_V2.md).

Реализованы frozen offline R/B/I2, Parquet replayer, versioned input codec,
TickAssembler и PostgreSQL foundation V1–V5. Kafka SDK, Worker/Master,
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
без success summary; автоматический retry отсутствует.

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
различаются. Durable recovery и exactly-once в этом входном срезе отсутствуют.

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
