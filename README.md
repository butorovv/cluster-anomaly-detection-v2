# cluster-anomaly-detection-v2

Первый функциональный срез курсовой: **prepared Parquet F18-v1 → типизированная
телеметрия → детерминированный Replay → NDJSON/stdout**.
Приложения, домен, будущее математическое ядро R/B/I2, Kafka, recovery,
Worker–Master и автоматизированные тесты реализуются **на Go**.
Python runtime, notebooks и исследовательские Python-инструменты V1 не входят в V2.
При обнаружении необходимой функции только на Python сначала нужен отчёт
о пробеле и предложение её реализации на Go.

Реализованы `cmd/replayer`, frozen R/B/I2 и `cmd/offline`, а также каноническая
PostgreSQL-схема V1 с транзакционным runner. Полная D2 golden parity пока
`NOT_RUN`: реальные данные и statistical/IF artifacts отсутствуют в Cloud.
Kafka, Worker, Master и recovery относятся к следующим этапам.

## Требования и запуск

Нужен Go **1.27.2** (как в закреплённом V1) и локальный prepared Parquet
с metadata `schema_version=F18-v1`, проверяемой схемой и UTC-сеткой 10 секунд
в пределах одного дня. Raw Summit Parquet не является prepared F18-v1.
Файл должен содержать возрастающие уникальные ключи
`(timestamp, group_id, hostname)` и узлы выбранного инвентаря `a01`–`a16`,
по 18 узлов в группе. Полнота группы для воспроизведения не требуется.
Данные не скачиваются автоматически и не хранятся в Git.

```sh
go mod download
go run ./cmd/replayer --help
go run ./cmd/replayer --input /path/to/prepared.parquet --speed 0 > replay.ndjson
go run ./cmd/replayer --input /path/to/D2.parquet --speed 10 \
  --start 2022-01-18T08:00:00Z --end 2022-01-18T08:01:00Z > minute.ndjson
```

`--speed 0` отключает ожидания; положительное значение масштабирует интервалы
по абсолютной временной линии V1, учитывая уже затраченное время отправки.
Границы окна полуоткрытые `[start,end)`; задаются вместе.
`REPLAYER_INPUT` и `REPLAYER_SPEED` поддерживаются; явные flags имеют приоритет.
Выход всегда stdout (`--output stdout` необязателен), help и диагностика идут
в stderr. Ошибка даёт ненулевой exit code; прерывание — 130.

Каждая строка NDJSON — отдельное сообщение `telemetry` или `timestamp_complete`.
`timestamp` — исходный event time. Порядок массива `f18` определяется
`domain.FeatureNames`; отсутствующие координаты представлены `null` и
`availability_mask`, внутри Go — `NaN`. Полная запись имеет `is_valid=true`.
Невалидные по полноте записи отправляются вместе с остальными.
Marker следует за последней доступной записью пары `(group,tick)` и содержит
`records` / `valid_records`. **Полностью отсутствующие пары markers не получают**,
как в V1 Replay; политика полного перечисления ticks нужна на этапе Kafka.
После успеха stderr содержит rows / timestamps / invalid / completions / groups / hosts.
Streaming-выход при ошибке может содержать уже отправленный префикс; автоматический
retry и transactional output в этом срезе не реализуются.

## Проверка на синтетике

Тесты генерируют маленькие Parquet в `t.TempDir`; Kafka/PostgreSQL/D2 не нужны.

```sh
go test ./...
go vet ./...
go build ./cmd/replayer
git diff --check
```

Для демонстрационного fixture в PowerShell:

```powershell
$env:REPLAYER_CONTRACT_DIR = Join-Path $PWD '.cache/demo'
go test ./internal/app/replayer -run '^TestCLIEndToEnd$' -count=1 -v
Remove-Item Env:REPLAYER_CONTRACT_DIR
go run ./cmd/replayer --input .cache/demo/synthetic.parquet --speed 0
```

Fixture содержит 4 строки: полный F18, частичный F18, строку с полностью
недоступным F18 и позднюю полную строку. Ожидаются 7 сообщений, включая 3 markers.
Для независимой сверки с чистым checkout V1 на закреплённом commit:

```powershell
$env:REPLAYER_V1_SOURCE = (Resolve-Path '../cluster-anomaly-detection').Path
go test ./internal/app/replayer -run '^TestV1Compatibility$' -count=1 -v
Remove-Item Env:REPLAYER_V1_SOURCE
```

Сам сравнительный тест написан на Go: копирует только нужные исходники V1
в временный модуль, запускает неизменённые reader/scheduler/sender и настоящий
CLI V2 на одном fixture. V1 не получает build/cache/generated files.
При отсутствии переменной эта дополнительная проверка явно пропускается.

## Происхождение и дальнейшие этапы

Источник: V1 commit `891e0ef71a0da0f9ad256f91d199fef38d1355a9`, tree
`8e7b9cf28c225a5f63f80b18ab2eb8f7b164f419`.
Матрица переноса, фактические расхождения, результаты и ограничения:
[docs/migration/01-telemetry-replayer.md](docs/migration/01-telemetry-replayer.md).
Полное согласованное ТЗ сохранено без изменений в
[docs/spec/FINAL_TZ_KR_Distributed_Group_Anomaly_Detection_V2.md](docs/spec/FINAL_TZ_KR_Distributed_Group_Anomaly_Detection_V2.md).

Срез P02 переносит frozen R/B/I2 из V1 `c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc`.
[Provenance, SHA и карта переноса](docs/migration/02-provenance.json) сохранены
в JSON. Реальная D2 golden parity остаётся обязательным условием перехода
к distributed runtime. Новые Markdown-отчёты не добавляются: инструкции
находятся в этом существующем README, главный контракт — ТЗ в `docs/spec`.

## Frozen offline R/B/I2

Нужен доверенный bundle с исходными логическими путями V1. В Git сохранены
неизменённые group/B1 configs; Parquet/manifest и statistical/IF JSON необходимо
предоставить отдельно. Bundle проверяет SHA до и после inference; CLI ничего
не обучает и не выбирает пороги.

```sh
go run ./cmd/offline --help
go run ./cmd/offline --assets /path/to/frozen-v1 \
  --profile-sha256 a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36 \
  --window D2 --groups 16 --delta 0 --output /path/to/new-output
```

Выход: `raw.ndjson`, `temporal.ndjson`, `evidence.ndjson`, `events.ndjson`,
`summary.json`; summary также идёт в stdout. Каталог выхода должен быть новым.
`summary.json` появляется только после успешного inference, закрытия файлов и
повторной проверки SHA. При ошибке каталог может содержать неполный префикс.
По умолчанию перечисляются все 46 080 D2 group-ticks, включая полностью пустые.
Поддерживаются δ=0/10/30/60, вложенные группы 4/8/16 и D1/D2;
`--start`/`--end` вместе задают необязательное подокно на UTC-сетке 10 секунд.
Пустые группы дают Unknown; поведение `replayer` в P02 сохранено.

Для независимой сверки с чистым V1 `c6de846` (Go-only, V1 read-only):

```sh
# TMPDIR должен иметь >5 GiB: это сохранённый disk guard оригинального V1.
mkdir -p /path/to/large-temp
TMPDIR=/path/to/large-temp OFFLINE_V1_SOURCE=/path/to/v1 \
  go test ./internal/app/offline -run 'TestV1' -count=1 -v
```

Тест сравнивает семантику всех записей и canonical output для четырёх δ;
синтетические модели используются только внутри тестов. Проверка declarations
40 scientific source/test files подтверждает сохранность математических операций.
Из адаптации Source исключён только исследовательский disk guard 5 GiB;
все content/hash/binding/grid проверки сохранены.

`REAL_D2_GOLDEN: NOT_RUN`: отсутствуют четыре pinned файла:
`data/prepared/summit-D1-pilot/D1.parquet`, `manifest.json` в том же каталоге,
`results/statistical-20220118-8h-v1.json`, `results/isolation-20220118-v1.json`.
Их точные SHA сохранены в provenance и group profile. Историческое имя
`D1.parquet` относится к исходнику, из которого выбирается окно D2; его нельзя
заменять новым D2-only файлом. Для независимого research comparator также нужны
pre-PR14 `raw.ndjson`, `journal-ticks.csv`, `event-tick-evidence.csv` и
`two-level-journal.ndjson`. Сохранённые `docs/migration/v1-reference` —
исторические R+B metadata, а не RBI2 golden.

```sh
TMPDIR=/path/to/large-temp OFFLINE_V1_SOURCE=/path/to/pinned-v1 \
  OFFLINE_REAL_ROOT=/path/to/frozen-v1 \
  go test ./internal/app/offline -run '^TestRealD2AllDeltaCanonicalParity$' -count=1 -v

GROUP_REAL_ROOT=/path/to/frozen-v1 \
  GROUP_REAL_REFERENCE=/path/to/pre-PR14-output \
  GROUP_RBI2_RESEARCH=/path/to/research-oracles \
  go test ./internal/modules/detection/group -run '^TestRealRBI2D2Parity$' -count=1 -v
```

## PostgreSQL: единая серия V1–V5

PostgreSQL 17, новый database и migration owner с правами создания объектов:

```sh
DATABASE_URL='postgresql://...' go run ./db/cmd/migrate
TEST_DATABASE_URL='postgresql://.../postgres' go test ./db -count=1 -v
```

| Версия | Назначение | Источник |
|---|---|---|
| V1__core_schema.sql | каноническая `project` schema и 5 metric types | V1 c6de846, побайтно |
| V2__p01_schema.sql | `cad_v2` tables/domain/identity | P01 100_schema.up.sql |
| V3__p01_ownership.sql | ownership/lease/fencing functions | P01 101_ownership.up.sql |
| V4__p01_checkpoint_outbox.sql | atomic checkpoint/outbox/envelope API | P01 102_checkpoint_outbox.up.sql |
| V5__p01_master.sql | idempotent Master acceptance | P01 103_master.up.sql |

P01 source: `f6ae695ee8620387cdcbc60c11c42987b55936c4`, ветка
`p01/postgresql-foundation`. Интеграция выполнена в нашей PR-ветке; исходная
ветка P01 и main не изменяются. В up-файлах добавлен только integration comment;
весь исходный SQL, включая исторические комментарии P01, сохранён после него.
Исторический текст о range 100–103 описывает исходный standalone комплект.
Текущие версии — V2–V5. Проверяемая карта/исходные и итоговые SHA есть в JSON.

Runner `db.Apply` в одной транзакции применяет pending DDL и записывает
`public.schema_migrations(version,name,sha256,applied_at)`, удерживая advisory
lock. Единственная executable серия — `db/migrations/V<number>__*.sql`.
Повторный запуск применяет 0 миграций. Изменённые имена/checksums, неизвестные
применённые версии, дубликаты и пропуски номеров — ошибки. Search_path
соединения восстанавливается. Поддержано обновление tracked V1 → V5 с сохранением
core telemetry. `project.event` — эксплуатационный факт, `project.telemetry` —
long-format history; `cad_v2.master_events` и checkpoints — отдельные сущности.

Существующая неучтённая `project` или `cad_v2` не принимается автоматически.
Если P01 100–103 уже применялись к persistent/shared DB, нужен отдельный план
adoption, а не переименование файлов или подмена ledger. Runner не делает
reset, DROP volumes и down. Destructive down-файлы и standalone runner P01
не импортированы; backup/recovery с изменением tracked history требует отдельного
рассмотрения. `db/demo` не входит в runner; production DML содержит только
обязательные metric types. Optional demo — только отдельная учебная база:

```sh
psql "$DEMO_DATABASE_URL" -v ON_ERROR_STOP=1 -1 -f db/demo/V1__demo_data.sql
```

`db/scripts/grants.sql` — optional выдача прав уже существующим runtime ролям,
не миграция. Runtime не должен владеть таблицами, быть superuser или наследовать
migration owner. Запускать как admin/migration owner с psql variables:

```sh
psql "$DATABASE_URL" -v worker_role=cad_worker -v master_role=cad_master \
  -f db/scripts/grants.sql
```

Проверки на PostgreSQL 17.11 создают/удаляют только новые случайные `p02_test_*`
БД, требуют CREATEDB, для privilege suite — прав создания/переключения test role,
для crash test — завершения своего тестового backend. Проверяются fresh V1→V5,
upgrade с V1, history/repeat/checksum, rollback всей серии при сбое V5,
сохранность core telemetry, исходные пять P01 SQL suites и Go concurrent
fencing/crash/Master-dedup. PostgreSQL optional: без TEST_DATABASE_URL явный SKIP.
Python test/runtime dependency не добавляется.

P01 SQL — foundation. Snapshot DTO/codec validation, GroupProcessor integration,
ownership discovery и transport result envelope ещё требуют реализации и
continuation parity; SQL fixture bytes не являются snapshot. Будущий processor
владеет bundle и journal одной группы, обрабатывает Tick и flush exclusive end.
Snapshot должен сохранять temporal positive/unknown/barriers, last/end/split,
open parent, witnesses/evidence/maxima/VoteSegments, legacy Engine/RBSubspans,
assembler и accepted-input state, с проверкой identity/hosts/SHA и round-trip.
Worker/Master/Kafka/HTTP runtime этим PR не реализуется.
