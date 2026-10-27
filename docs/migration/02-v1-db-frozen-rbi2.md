# P02 — PostgreSQL V1 и frozen R/B/I2 offline

Статус на 27.10.2026: baseline и Go offline реализованы и проверены;
**REAL_D2_GOLDEN: NOT_RUN**. Это не завершение распределённой системы и не
подтверждение D2-приёмки. Главный контракт —
[генеральное ТЗ V2](../spec/FINAL_TZ_KR_Distributed_Group_Anomaly_Detection_V2.md);
[ТЗ P02](../spec/CODEX_TZ_P02_V1_DB_AND_RBI2_PORT.md) задаёт границы этого переноса.

## Зафиксированные исходники и изоляция

| Репозиторий | Локальная исходная ветка | HEAD / опубликованный main |
|---|---|---|
| `butorovv/cluster-anomaly-detection` (read-only) | `work` | `c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc` |
| `butorovv/cluster-anomaly-detection-v2` (base) | `work` | `3fa11eb5249040a64c515f13d10d7f73ae1a0926` |

Оба main проверены через GitHub и `git ls-remote`. Работа идёт в отдельной копии
V2 `/workspace/cluster-anomaly-detection-v2`, ветка
`feature/v1-core-schema-and-frozen-rbi2`, строго от указанного base SHA.
V1 `/workspace/cluster-anomaly-detection` остаётся чистым; его код для comparator
копируется во временный модуль, builds/cache/output не пишутся в исходник.
Ни main, ни ветка/рабочий каталог P01 не изменялись; merge/rebase/force push не
выполнялись. При первоначальной проверке GitHub показывал только V2 `main`:
**P01 not visible**. Повторная проверка по просьбе пользователя отмечается ниже.

Storage commit: `574a31a04849bc52d1adf4abeece72911422f998`.
Detection commit: `848af2d260ba4c8f4a495dacba230ae739d9986a`.
Документация и read-only inventory P01 оформлены отдельным коммитом;
точный SHA находится в `git log` этой ветки и итоговом отчёте/PR.

## Матрица переноса и provenance

Полная пофайловая карта с SHA-256 исходников и перенесённых файлов:
[02-provenance.json](02-provenance.json). В ней также сохранены binding,
immutable input keys/SHA, feature schema, версии зависимостей и недостающие
артефакты. Source commit относится ко всем исходным файлам, включая SQL и tests.

| V1 | V2 | Изменение |
|---|---|---|
| `db/migrations/V1__core_schema.sql` | тот же путь | Побайтно, включая пять metric_type INSERT |
| `db/source.json`, `db/queries/f18.sql`, `db/demo/V1__demo_data.sql` | те же пути | Побайтно; demo не исполняется runner |
| `db/README.md`, PostgreSQL adapter/test | изучены; новый `db/README.md` | Runtime adapter/importer не переносится в P02 |
| `internal/group/*.go` и tests | `internal/modules/detection/group/` | Импорты, перемещённый parser, относительные пути tests |
| `internal/local/*.go` и tests | `internal/modules/detection/local/` | Импорты и `topology.ParseHostname` |
| `internal/analysis/{window,statistical,isolation}.go` и tests | `internal/modules/detection/analysis/` | Импорты |
| `internal/experiment/source.go` | `internal/modules/detection/experiment/` | Используются V2 Parquet, WindowReader и topology parser |
| `internal/experiment/{statistical,isolation}.go` | там же | Сохранены envelopes и frozen validators; исключены StageA/StageC/reporting commands |
| `cmd/rbi2/main.go` и tests | `internal/app/offline/` + тонкий `cmd/offline` | CLI `offline`, exported Run; необязательное grid-подокно |
| пять `configs/*` frozen/protocol JSON | те же пути | Побайтно, без изменения logical keys |
| legacy R+B validation/SHA/run-summary metadata | `docs/migration/v1-reference/` | Побайтно; это исторический R+B provenance, не RBI2 golden |

Принятое инфраструктурное отклонение: V1 `Source` вызывает disk guard с резервом
5 GiB + 64 MiB для исследовательских выходов. V2 read-only inference этот guard
не вызывает и не копирует его platform package. Проверки содержимого, topology,
F18, UTC grid, порядка, checksum и frozen binding сохранены. Неизменённый V1
comparator по-прежнему требует filesystem с >5 GiB; для него используется TMPDIR
на `/workspace`, а не небольшой `/tmp`.

`TestV1ScientificDeclarationsUnchanged` сравнил AST declarations всех 40
перенесённых scientific source/test files. После нормализации импортов/parser/
относительных asset paths declarations идентичны. StageA/StageC и physical
T/P regression в runtime V2 не переносились. Существующие Fit-функции scientific
библиотеки сохранены для исходных Go unit tests; production CLI их не вызывает.
Новых моделей, calibration или D2 training нет.

## Единственный baseline и версия SQL

Реальный inventory исходной V1: одна миграция
`db/migrations/V1__core_schema.sql`. В исходном V2 base миграций/runner не было.
В этой ветке единственный исполняемый baseline — тот же файл версии **1**.
`db/demo/V1__demo_data.sql` находится вне executable migration series;
дубликата `000001`/up/down нет. P01-файлы не импортированы и не перенумерованы.

Временное решение P02: Go `db.Apply` + `go run ./db/cmd/migrate`, один ledger
`public.schema_migrations(version,name,sha256,applied_at)`. Runner проверяет
строгую последовательность V1→V2+, сериализуется advisory transaction lock,
атомарно фиксирует DDL и metadata. SQL использует исходный `SET search_path`;
runner восстанавливает search_path вызывающего соединения. Нет auto-reset,
DROP volumes, смены checksum/history или automatic down. У V1 отсутствует
безопасная lossless обратная миграция. Повторный штатный запуск применяет 0
миграций, повторный прямой неучтённый SQL завершается ошибкой/rollback.
Untracked существующая `project` требует отдельного плана adoption.

Проверено на PostgreSQL **17.11 (Debian 17.11-1.pgdg13+2)** в локальном
подготовленном контейнере. Каждый integration test создаёт отдельную случайную
`p02_test_*` БД через template0 и удаляет только её. Пользовательская БД и
подготовленные schema/volumes не очищались.

Проверки: 12 исходных таблиц, 12 FK с RESTRICT, 2 trigger functions, 3 user
triggers; точные code/kind/quantity/unit пяти metric types. Проверены CHECK,
FK, UNIQUE, совместимость вида metric_source/component, неизменность
классификации, сохранение synthetic numeric telemetry после повторного runner,
откат неудачного DDL вместе с ledger, upgrade V1→synthetic V2 и отказ неизвестной
версии/изменённого checksum. Synthetic V2 существует только в test MapFS,
не добавляется в migration directory и не резервирует номер P01.

## Замороженная математика и идентификаторы

Сохранены IF/B1 scoring и индивидуальные Q99, пять R channels, median/MAD,
второй distinct host одного канала/знака и строгий `>`; 10D B и squared
Mahalanobis; I2 18/18 known, Positive при count≥2. Incomplete/missing → Unknown.
Сохранены ternary 2-of-3, causal δ=0/10/30/60, Unknown barriers, gap/flush,
RBI2Journal, `[detected,closed)`, closure/censor, witnesses, VoteSegments,
RBSubspans и canonical identity. Диагностика legacy Y остаётся в Raw для
совместимости оригинала, но не становится голосом R/B/I2.

Доверенный group profile SHA:
`a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36`.
Dataset ID:
`prepared-sha256:cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7`.
F18 order, все hosts, timezone provenance (`UTC`, verified=false) сохранены.
Тест без telemetry подтвердил production δ0 ConfigID
`b44f95bee95d00b908b30de77d89d8ae56c2a6a8a40d9d8fc8a508413b17da87`, RunID
`845af471d9ec5b8a503f09bb1395486cbe1658d8cb9786d94f1d6b4e6e5688ae` и legacy RunID
`70bd4e3d0d926bc49d6d36aacc9c3d66d1ba9a7817d4bb7fa908225aaf63853f`.
Это проверка identity, а не выполнение отсутствующего production bundle.

Runtime остаётся единым Go 1.27.2 module. Mathematical dependencies: stdlib
float64/math/sort/rand, Gonum v0.17.0. Parquet v0.32.0 сохранён от bootstrap;
pgx/v5 v5.11.0 используется только SQL runner/tests. Domain kernel не импортирует
PostgreSQL, Kafka или HTTP. Нового TelemetryRecord/Topology в runtime нет.

## Проверки и фактические результаты

Команды ниже выполнены из V2 (переменные подключения подставлены средой,
секреты не выводились):

```sh
source /workspace/.cloud-setup/activate.sh
mkdir -p /workspace/p02-test-tmp
TMPDIR=/workspace/p02-test-tmp OFFLINE_V1_SOURCE=/workspace/cluster-anomaly-detection \
  go test ./... -count=1
go vet ./...
go build ./cmd/offline
go build ./cmd/replayer
git diff --check
```

Все команды завершились **PASS / exit 0**, включая SQL integration tests.
Отдельно `go test ./db -v` вывел фактическую версию PostgreSQL 17.11.
Без TEST_DATABASE_URL integration tests явно SKIP. Без OFFLINE_V1_SOURCE
независимый source comparator явно SKIP; обычные synthetic/unit tests работают
автономно. Штатная сборка не требует Python.

Независимая V1→V2 проверка использует исходники ровно V1 `c6de846`, copied
без изменений в временный module, и настоящий binary `cmd/offline` V2.
Сопоставляются типизированные raw/temporal/evidence/event/summary записи, затем
SHA canonical bytes, потоково с ограниченной памятью. Сравнение точное, без
нового float tolerance. Исходный research comparator V1 для отдельных CSV
чисел сохраняет прежний `1e-8 + 1e-8*abs(reference)`.

| Synthetic input: 8 ticks × 16 groups | δ0 | δ10 | δ30 | δ60 |
|---|---:|---:|---:|---:|
| Group ticks | 128 | 128 | 128 | 128 |
| Parent events | 45 | 32 | 32 | 32 |
| V1/V2 semantics + canonical bytes | PASS | PASS | PASS | PASS |

Fixture содержит полностью отсутствующую группу, полностью пустые ticks,
пропущенный host, partial F18, known Negative, Positive и Unknown. Проверены
повторяемость, сохранность frozen bundle при переносе физических путей,
checksum/binding corruption, invalid/duplicate/out-of-order rejection,
TTL/Unknown/gap/flush/event bounds исходными tests. Дополнительный test с
полным D2 окном и synthetic source подтвердил **46 080** raw и temporal rows,
включая полностью отсутствующий хвост. Эти synthetic counts не являются D2
golden результатом Summit.

## Запуск offline и недостающие реальные артефакты

CLI принимает `--assets ROOT`, доверенный `--profile-sha256`, новый `--output`;
`--window D2` / `--groups 16` / `--delta 0` — defaults. Поддерживаются D1/D2,
4/8/16 групп, δ0/10/30/60; `--start`+`--end` задают grid-подокно внутри split.
Имена физических файлов могут быть перенесены целым ROOT; logical keys и SHA
не меняются. `D1.parquet` — историческое имя pinned prepared source V1;
D2 выбирается временным окном внутри этого источника, его нельзя заменять
новым D2-only Parquet с другим hash.

```sh
go run ./cmd/offline --assets /path/to/frozen-v1 \
  --profile-sha256 a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36 \
  --window D2 --groups 16 --delta 0 --output /path/to/new-output
```

Четыре NDJSON streams и summary сохраняют формат V1; summary пишется только
после успешного закрытия потоков и повторной проверки immutable SHA. Ошибка
оставляет диагностируемый неполный output без success summary. Выходной каталог
не должен существовать. Replayer не менялся: его markers по-прежнему относятся
только к встреченным group/tick; offline перечисляет всю сетку.

**REAL_D2_GOLDEN: NOT_RUN**. В Cloud отсутствуют следующие pinned files:

| Logical key | Ожидаемый SHA-256 |
|---|---|
| `data/prepared/summit-D1-pilot/D1.parquet` | `cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7` |
| `data/prepared/summit-D1-pilot/manifest.json` | `6d603a5445d1a76be24408eed84933e96a4a10af6c3bfd32471d93ca7dd9f291` |
| `results/statistical-20220118-8h-v1.json` | `36da27a4a4f485baa4b1b1b899c26a36efc15401327fd85abc3d63ea3c1ade80` |
| `results/isolation-20220118-v1.json` | `6aa1ca4f2f76b56418aafaa1ea25e6812a3b9fdcc0d309aeed44dc1db03f2da3` |

Также отсутствуют complete pre-PR14 raw.ndjson и независимые RBI2 research
oracles `journal-ticks.csv`, `event-tick-evidence.csv`, `two-level-journal.ndjson`.
Legacy R+B metadata доступны отдельно и не подменяют эти oracles.
При предоставлении данных выполнить обязательную приёмку **до distributed**:

```sh
TMPDIR=/path/to/large-temp OFFLINE_V1_SOURCE=/path/to/pinned-v1 \
  OFFLINE_REAL_ROOT=/path/to/frozen-v1 \
  go test ./internal/app/offline -run '^TestRealD2AllDeltaCanonicalParity$' -count=1 -v

GROUP_REAL_ROOT=/path/to/frozen-v1 \
  GROUP_REAL_REFERENCE=/path/to/pre-PR14-output \
  GROUP_RBI2_RESEARCH=/path/to/independent-rbi2-research \
  go test ./internal/modules/detection/group -run '^TestRealRBI2D2Parity$' -count=1 -v
```

Первый test проверяет все четыре δ поэлементно против неизменённого V1,
production identities/canonical bytes, 46 080 ticks, 1 408 raw I2 Positive и
агрегаты генерального ТЗ: δ0 `1284/750/473/285/277`, δ10
`2049/711/419/292/292`, δ30 `3446/654/359/295/295`, δ60
`5344/596/319/287/277`. Второй — сохранённый V1 comparator независимого δ0
research oracle. При наличии env, но отсутствии файлов tests FAIL, а не SKIP.

## Разделение БД и будущего runtime

`project.event` — эксплуатационный факт узла с severity/event_type, не RBI2
parent и не `master_events`. `project.telemetry` — numeric long-format history,
не Kafka checkpoint store и не accepted-input ledger. Immutable detection
topology берётся из canonical manifest; DB node_group остаётся самостоятельной
предметной моделью и не получает выдуманную аппаратную семантику.

Будущий GroupProcessor должен владеть одним group inventory, immutable bundle,
RBI2Journal и последовательностью принятого Tick. Нужны операции обработки tick
с raw/temporal/evidence/closed parents и flush на exclusive end. Существующие
EvaluateGroup, journal.Step/Flush — реальная научная основа; пустой интерфейс
с неподтверждённым контрактом сюда не добавлен.

Для Snapshot/Restore позже требуется перечислить фактические поля Engine
(positive/unknown history, split/last/end), occurrence evidence, открытый parent,
VoteSegments, witnesses, вложенный legacy Engine/RB subspans, assembler и
accepted input state. Restore должен проверять version, frozen SHA, run/config,
host order, logical boundary и round-trip до продвижения Kafka offset.
Конкретная snapshot schema/serialization и atomic checkpoint/outbox остаются
за следующим этапом и P01; P02 не реализует worker/master/transports/fencing.

## Интеграция P01: порядок и открытые ADR

1. Зафиксировать SHA обеих веток и актуальный inventory миграций. V1 резервирует
   ровно этот baseline; P01 series после него — **V2, V3, ...**.
2. Владелец интеграции согласует/перенумеровывает только ещё не применённые P01
   миграции, сохраняя dependency order. P02 чужие номера не меняет.
3. Выбрать **один** runner и **одну** history table. Текущий `db.Apply` provisional:
   либо адаптировать P01 к нему, либо заменить его P01 runner и перенести tests.
   Не запускать два ledger параллельно и не добавлять второй V1/000001 baseline.
4. Проверить `project`/`public` schemas, таблицы, типы, функции, trigger/index/
   constraint names, FK и search_path. Совпадение версии не означает DDL
   совместимость. Эксплуатационный event нельзя переименованием превратить в
   RBI2/master result. P01 не должен переопределять core telemetry/node модель.
5. Если любой вариант P01 уже был применён к persistent/shared DB, запретить
   простое переименование history и подготовить отдельный контролируемый план.
   Временные тестовые БД можно создавать заново; volumes пользователя — нет.
6. Прогнать fresh V1→P01 и upgrade tracked-V1→P01; проверить сохранность core
   telemetry/reference rows, checksum/history и повторный запуск.
7. Перед distributed runtime получить отсутствующие pinned assets/oracles и
   пройти REAL_D2_GOLDEN для всех δ. Synthetic parity это условие не снимает.

Открытые ADR: общий runner/ledger; handling ранее применённых P01 версий;
разделение namespaces core/runtime; ownership/fencing, snapshot и envelope
schemas. При первоначальном inventory P01 DDL не было видно, поэтому точные
коллизии его объектов тогда не могли быть установлены.

Сообщение для P01 Codex (готово к передаче пользователем):

> P02 резервирует V1 / 000001 под побайтный canonical V1 core (`project.*`).
> Новые P01 миграции должны идти с версии 2, в исходном порядке зависимостей.
> Финальную перенумерацию и выбор единого runner/ledger выполнит владелец
> интеграции; применённые persistent DB версии нельзя просто переименовать.
> У P02 ledger public.schema_migrations; проверить collisions и search_path.
> project.event/telemetry — core domain/history, не master_events/checkpoints.
> Snapshot/fencing/result-envelope ADR остаются открытыми. Автоmerge не включён.

## Повторная проверка P01

27.10.2026 в **15:22:30 МСК / 12:22:30 UTC**, через 3 минуты после просьбы,
`git ls-remote` обнаружил ветку `p01/postgresql-foundation`, HEAD
`f6ae695ee8620387cdcbc60c11c42987b55936c4`. Её Git objects загружены только для
`git show` в нашей копии; checkout, правок и коммитов в P01 не выполнялось.

Реальный список P01:

- `db/migrations/100_schema.up.sql` / `100_schema.down.sql`;
- `db/migrations/101_ownership.up.sql` / `101_ownership.down.sql`;
- `db/migrations/102_checkpoint_outbox.up.sql` / `102_checkpoint_outbox.down.sql`;
- `db/migrations/103_master.up.sql` / `103_master.down.sql`.

P01 явно резервирует V1 в комментарии 100_schema.up.sql и использует отдельную
schema **cad_v2**, domain sha256_hex, runs/executions/execution_partitions,
partition_ownership/group_checkpoints/outbox_messages/master_inbox/
master_results/master_events и SQL functions. Прямых определений `project.*`
или `public.schema_migrations` в этой серии не найдено; явного DDL object-name
конфликта с core не обнаружено. Точные SHA этих файлов записаны в provenance.
Это inspection, а не комбинированный SQL integration test.

Конкретная несовместимость — migration discovery/history: P02 принимает только
`V<number>__description.sql` и непрерывные версии; файлы P01 `100_*.up/down.sql`
он отвергнет. В P01 нет version ledger/штатного runner: `db/scripts/test.sh`
выполняет *.up.sql через psql -1, тестирует down/up только на специальной БД.
Этот test harness не должен стать вторым production runner и не учитывает V1.
Down-файлы P01 содержат DROP и не должны попадать в embedded up-only series.

Предложение владельцу интеграции, **без переименования файлов в P02/P01**:
согласовать mapping `100_schema.up → V2__p01_schema.sql`,
`101_ownership.up → V3__p01_ownership.sql`,
`102_checkpoint_outbox.up → V4__p01_checkpoint_outbox.sql`,
`103_master.up → V5__p01_master.sql`; оставить down только как отдельный
явно запускаемый recovery material. Это план, не выполненный patch/merge.
Если уже есть persistent DB, этот mapping нельзя применять простым rename.

Дополнение к сообщению P01:

> Вижу p01/postgresql-foundation @ f6ae695. cad_v2 изолирована от project;
> прямого DDL collision не найдено. 100–103 up/down несовместимы с discovery
> P02 V<number>__description.sql; нужен единый up-only series/ledger. Предлагаемый
> mapping 100→V2, 101→V3, 102→V4, 103→V5 утверждает владелец интеграции.
> Чужие файлы не изменены, combined integration и merge не выполнялись.
