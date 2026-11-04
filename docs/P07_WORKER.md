# P07: Kafka / PostgreSQL Worker

Реализованы P07-A–F: один процесс обслуживает все назначенные ему Kafka
partitions, несколько процессов используют общий `group.id`. Partition `0..15`
соответствует `a01..a16`; конкретный Worker выбирает Kafka Consumer Group.
Математика, P06 Snapshot/Restore, frozen assets и SQL-миграции V1–V5 не менялись.

## Архитектура и API

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
V5 `accept_result` вызывается только тестом совместимости; Master не запускается.

## Assignment, progress и recovery

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

## Регистрация и запуск

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

## Outbox

Envelope v1: `kind=rbi2_event`, `terminal=true`, `revision=1`. Stable `message_id`
использует предложенный V4 SHA256 canonical JSON массива
`["cad-output-v1",run_id,group_id,"rbi2_event",event_id,1]`. Epoch, wall clock,
checkpoint revision и attempt не влияют на identity. Payload — frozen event без
изменений. `Pending` читает durable backlog в порядке `(checkpoint_revision,
ordinal)` после любого restart/reassignment. Доставка/ack/dispatcher не реализованы.

## Проверка на реальных сервисах

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

## Результат проверки 4 ноября 2026

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

## Оставшиеся требования генерального ТЗ

Межгрупповой AnnotateOutside и final Raw/Evidence/group_result, Master и три
Worker→Master транспорта, outbox dispatcher/ack, cadence 3/6 и сравнительные
performance/scaling эксперименты ещё не реализованы. P07 использует synthetic
frozen fixtures с реальными Kafka/PostgreSQL; полного distributed D2 golden
равенства Raw/Evidence до outside join не заявляет.
