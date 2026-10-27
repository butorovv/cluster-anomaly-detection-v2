# Параллельное ТЗ № P02 — перенос канонической PostgreSQL V1 и frozen R/B/I2 в V2

**Адресат:** отдельная задача Codex Cloud. **Целевой репозиторий:** `butorovv/cluster-anomaly-detection-v2`. **Источник только для чтения:** `butorovv/cluster-anomaly-detection`. **Главный нормативный документ:** приложенное `FINAL_TZ_KR_Distributed_Group_Anomaly_Detection_V2(1).md` (математический контракт `group-rbi2-v1`).

## 0. Контекст параллельной разработки и границы ответственности

На момент подготовки задания (27.10.2026) опубликованная V2 содержит результат ТЗ № 01: Go-модуль, topology/telemetry, Parquet reader и NDJSON `cmd/replayer`; frozen scientific kernel и database migrations в опубликованной V2 пока не перенесены. **Перед работой самостоятельно перепроверь текущее состояние Git/веток:** другие агенты работают параллельно, состояние могло измениться.

Другой Codex Cloud **сейчас** выполняет P01 (PostgreSQL Foundation): проектирует таблицы run/ownership/checkpoints/outbox/master results, SQL-миграции и тесты. P02 отвечает за **унаследованную БД V1 + математическое ядро**, но **НЕ** разрабатывает заново объекты P01. Нельзя одновременным агентам писать в `main`, одну Git-ветку или одну рабочую директорию.

Создай изолированную ветку `feature/v1-core-schema-and-frozen-rbi2` от зафиксированного V2 `main` HEAD, запиши исходный commit SHA. Работай только в этой ветке. V1 — полностью read-only. Не делай merge/rebase/force push, не изменяй `main` и не объединяй результаты P01 самостоятельно. Если механизм Cloud самостоятельно изолирует Git-копию, используй её, но явно проверь репозиторий, ветку и статус перед изменениями.

## 1. Основная задача — сохранить БД V1 как первую миграцию V2

1. Найди и изучи **реальные** исходники V1: `db/migrations/V1__core_schema.sql`, `db/README.md`, `db/source.json` (если есть), `db/queries/f18.sql`, `db/demo/V1__demo_data.sql`, Go-адаптеры PostgreSQL и их тесты. Зафиксируй provenance: источник/commit SHA/SHA-256 исходных файлов и принятые отклонения.
2. `db/migrations/V1__core_schema.sql` V1 создаёт `project` schema и предметные сущности `node_group`, `node`, `component`, `metric_type`, `metric_source`, `telemetry`, `event`, `engineer`, `maintenance`, `resource`, `maintenance_resource`, `document`, функции/триггеры и канонический справочник пяти типов метрик. Это **V1 core**, не новая worker-state схема. Сохрани существенную DDL-семантику, включая ограничения, порядок создания и служебные объекты.
3. Перенеси core в целевую систему миграций V2 как **версию № 1**. Предпочтительно сохранить оригинал `V1__core_schema.sql` и стиль версионирования `V<number>__description.sql`, если в V2 ещё не утверждён другой migration runner. **Не создавай одновременно два независимо применяемых `V1`/`000001` файла:** должен быть ровно один исполняемый baseline на чистой БД.
4. Не переноси демонстрационные фиктивные данные в производственную миграцию: `db/demo` — отдельно и только явно по запросу/в отдельной demo БД. Справочные данные `metric_type`, которые V1 считает обязательными, остаются частью core. Не меняй имена/значения только ради эстетики.
5. Учти, что V1 core применяется один раз к **чистой** БД. Создай воспроизводимый SQL-migration runner или Go-интеграцию с существующим runner только если это не конфликтует с другими агентами; применяй транзакционно, с учётом выполненных версий. Не делай авто-reset, DROP существующих volumes и подмену migration history. Для `down`: исходный V1 не имеет обратной миграции; если используется пара `up/down`, чётко задокументируй безопасную политику, без автоматического удаления данных в штатном сценарии.
6. Проверки: fresh PostgreSQL 17 (либо документированная совместимая версия), применение baseline один раз, проверка схемы/constraints/triggers/reference rows, отказ повторного неучтённого применения и сохранность данных. Данные Summit недоступны в Cloud по умолчанию; разрешены синтетические SQL fixtures, но не выдавай их за D2 golden.

## 2. **Обязательный протокол нумерации относительно параллельной P01**

**Жёсткий принцип:** после объединения итоговый линейный порядок должен быть `V1 — V1 core` → `V2, V3, ... — новые DDL из P01` (или функционально эквивалентные `000001` → `000002+` в единственной выбранной системе). Номера внутри *разных репозиториев* могут совпадать; проблема только в единой истории V2. Номер сам по себе не гарантирует совместимость DDL.

- P02 **владеет baseline V1**, не редактирует созданные P01 миграции, не пытается заранее угадать номера, количество и названия P01. Добавь понятную **интеграционную инструкцию**, что P01 migration series должна быть перенумерована/согласована при merge **после baseline**; сохранять исходный порядок зависимостей, проверять дубли `project.*`, `public.*`, имена типов/индексов, FK, `search_path`.
- Если в V2 уже появились миграции от другой задачи, **не перезаписывай и не переименовывай их молча**: задокументируй конфликт, подготовь интеграционный план/patch, но оставь разрешение владельцу интеграции.
- Если миграции применялись к persistent/shared DB, переименование версии после применения запрещено без отдельного контролируемого migration plan. Для чистой ephemeral тестовой БД можно пересоздавать тестовую БД, но не пользовательские volumes.
- P01 может иметь собственный каталог миграций и свой runner: не добавляй второй competing migration ledger. В merge нужно выбрать **один runner и одно history table**, атомарный порядок V1→V2..., тестировать fresh database и upgrade-path. Не предполагай, что P01 уже закончил работу.
- В конце выдай **готовое сообщение для P01 Codex** с конкретной просьбой трактовать `V1` как зарезервированную версию core, новые миграции стартовать после неё, а финальную перенумерацию выполнять при интеграции. Укажи непереносимые столкновения и открытые ADR.

## 3. Перенос научного ядра V1 в V2 — весь runtime на Go

Перенеси **существующие Go-реализации** frozen `group-rbi2-v1` (вариант C) без новой математики, переобучения, подгонки порогов и изменения идентификаторов. Используй реальные V1 пути, в частности:

- `internal/local/*` — IF/B1, frozen model loading/calibration/decisions;
- `internal/group/{model,evaluate,i2,bundle,rbi2_temporal,rbi2_event,rbi2_identity,rbi2_offline,offline,identity,...}.go` и связанные тесты;
- необходимые `internal/analysis/*`, frozen artifacts/configs/manifest и золотые результаты V1; исключай unrelated research tooling;
- существующие V2 `internal/modules/{telemetry,topology}` и `internal/platform/parquet`, не создавай второй тип TelemetryRecord/Topology и не ломай `cmd/replayer`.

Нужен рабочий `cmd/offline` с тонким `main`, корректная композиция в `internal/app/offline` и предметное ядро в `internal/modules/detection`, деление на `domain/usecase/repository` **только при необходимости**. Не копируй V1 целиком; минимальная адаптация импортов с сохранением порядка операций/численной семантики. Границы domain не должны зависеть от PostgreSQL, Kafka или HTTP.

### Математические инварианты

- Данные: F18 (18 признаков), 16 групп `a01`–`a16` по 18 hosts, UTC grid 10 s, D1 frozen training, D2 validation.
- IF/B1: frozen 100-tree Isolation Forest, reservoir 256, seed 42, индивидуальные D1 Q99 B1 thresholds; никогда не обучай на D2 и не пересчитывай пороги в V2.
- R: те же пять каналов, робастные медиана/MAD и **минимум два разных host, один канал и знак**; frozen Q99, строгий `>`; неполная группа → `Unknown`. **Не** физическая регрессия.
- B: 10D (mean+std пяти каналов), squared Mahalanobis и frozen Q99; неполная группа → `Unknown`.
- I2: 18/18 известных локальных IF/B1 решений; `Positive` при ≥2 Positive, независимо от канала/знака; при любом неизвестном host → `Unknown`.
- Ternary 2-of-3 и причинный temporal engine δ=0/10/30/60, barriers `Unknown`, gap/flush, RBI2Journal с parent событиями и вложенными RB subspans.
- Сохрани original EventID/RunID/ConfigID canonical identity, evidence, vote segments, bounds `[detected,closed)`, closure reasons, missing tick semantics и детерминизм.
- Offline перечисляет **все** 46 080 group-ticks D2, включая полностью пропущенные group/tick, в отличие от текущего replayer, который даёт completion только для встреченных. Не меняй поведение replayer в этой задаче.
- Математический код и tests — **Go**. Python V1 допустим лишь для изучения/опционального offline отчёта, **никакого Python runtime dependency V2**.

### Golden parity

Эталон D2, δ=0: `46 080 group-ticks`, `1 408 raw I2 Positive`, `1 284 RBI2 Positive ticks`, `750 parent events`, `473 new parent events`, `285 legacy R+B subspans` в `277 parents`. При δ10/30/60 — значения из основного ТЗ. Нужна **семантическая поэлементная сверка**, а не только агрегаты: сырые R/B/I2, Unknown/missing, temporal, timestamps, Run/Config/Event IDs, `[detected,closed)`, closure/censor, witnesses, evidence, VoteSegments, RBSubspans, canonical output; float tolerance только заранее известный V1 comparator. Используй frozen artifacts с SHA проверками.

Если реальные D2 Parquet/frozen artifact отсутствуют в Cloud, не подделывай parity. Сделай работающее Go offline с synthetic fixtures, точными unit/roundtrip/invariant tests, reproducible instructions и чётко отметь `REAL_D2_GOLDEN: NOT_RUN` и список недостающих файлов; когда данные доступны, полный golden — обязательная приёмка до распределённого runtime. Не переобучай модели для «демонстрации».

## 4. Дополнительный независимый полезный результат

Подготовь небольшой **contract/provenance manifest** и матрицу миграции V1→V2:

- source commit SHA, file maps, immutable model/dataset identifiers, SHA-256 там, где доступны;
- перечень math dependencies, проверенные версии go.mod и offline-run instructions;
- отчёт по отличию БД V1 от P01 worker-state schema: V1 `project.event` — эксплуатационные события, **не** RBI2 `master_events`; V1 `telemetry` — исторический long-format, **не** Kafka checkpoint store; фиксированная topology — immutable manifest, а БД V1 — независимая предметная модель;
- риски пересечений `project` schema, constraints/search_path и миграций;
- необходимые интерфейсы для будущего GroupProcessor/Snapshot/Restore — **описать**, но пока не реализовывать пустые сервисы или неподтверждённые snapshot schemas.

## 5. Что запрещено

Не менять V1, `main`, PR/ветку P01, чужие migration files. Не выполнять merge в main. Не реализовывать Worker, Master, Kafka producer/consumer, checkpoint snapshots, outbox repositories, fencing runtime, HTTP transports или физическую T/P-регрессию. Не вводить новый IF/R/B/I2 и не заменять frozen bundle синтетической моделью в production. Не перетирать существующий replayer, topology, telemetry. Не использовать mass copy не относящихся к срезу научных скриптов. Не удалять действующие БД/volumes, не редактировать файлы генерации другого агента.

## 6. Проверка и критерии принятия

1. Перед кодом: зафиксированы V1/V2 source SHA и inventory затрагиваемых файлов/миграций, отчёт о пересечениях с P01.
2. V2 `go.mod` остаётся одним модулем и весь runtime Go; `go test ./...`, `go vet ./...`, `go build ./cmd/offline`, `go build ./cmd/replayer`, `git diff --check` проходят (если нет — отдельный точный blocker).
3. На чистой PostgreSQL создаётся исходная core schema V1 единственным применением baseline, валидированы базовые типы/FK/триггеры/reference data, тестируется metadata version; runner не повторно применяет V1 при штатном запуске.
4. SQL / Go tests подтверждают отсутствие изменения семантики V1. Demo fixtures не загрязняют основную БД.
5. Offline kernel — frozen и подтверждён тестами V1-V2 comparator/synthetic fixtures; с реальным D2 — поэлементная parity, без него явно `NOT_RUN`.
6. P01 остаётся независимым: новые миграции не интегрированы/не перенумерованы в чужой ветке; есть конкретный безопасный merge protocol V1→V2+.
7. Напиши `docs/migration/02-v1-db-frozen-rbi2.md` с provenance, тестами, неизменёнными и адаптированными контрактами, отсутствующими артефактами, интеграционным checklist.
8. Зафиксируй логически разделённые коммиты, предпочтительно `feat(storage): import canonical v1 database baseline` и `feat(detection): port frozen rbi2 offline kernel`; при инфраструктурном ограничении Cloud оставь их на ветке/представь diff. Подготовь **Draft PR** только если это поддерживает выбранный workflow, без автоматического merge. Никаких пушей в `main`.

## 7. Обязательный итоговый отчёт

Выведи: исходные HEAD/ветки обоих репо; затронутые файлы; список реальных миграций V1/V2/P01 (только если доступны, иначе `P01 not visible`); SQL versioning strategy; SHA/provenance; команды и фактические результаты тестов; D2 parity status; URL ветки/PR или локальные commit SHAs; известные риски; **отдельное короткое сообщение для другого агента P01** о зарезервированном baseline V1 и продолжении миграций с версии 2. Не заявляй, что работа P01 или интеграция в main завершена.
