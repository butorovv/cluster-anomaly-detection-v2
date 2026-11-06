# P14 Integration — предварительная совместимость

Подготовлена единая версия P13 + PostgreSQL D1/V8 + Kafka B1/B2 в отдельной
`feature/p14-integration` от `c9a81e637c18c628c708c59a1816708bce6e0847`.
Исходные экспериментальные ветки и main не изменялись. Источники и сохранённые
шесть snapshots перечислены в `SNAPSHOTS.json`; шаги подготовки — в `PLAN.md`.

Production diff ограничен 8 файлами: D1 `join.go`, migration V8, Kafka sender/
async sender, Dispatcher/async Dispatcher, Worker CLI/output. SQL profiling,
старые runner/benchmark/README изменения исключены. Все Kafka bytes и V8 совпадают
с заданными исходными SHA; из SQL join исключены только profiling hooks.
`go.mod/go.sum`, V1–V7, scientific kernel, Master и Replayer не изменены.

| Проверка | A | B | C | D |
|---|---|---|---|---|
| `go test ./...` (без external opt-ins) | PASS | PASS | PASS | PASS |
| `go vet ./...` | PASS | PASS | PASS | PASS |
| Real PostgreSQL suites (4 packages) | PASS | PASS | PASS | PASS |
| Real Kafka sender/Worker recovery | PASS | PASS | PASS | PASS |
| Targeted in-process race | — | — | PASS | PASS |

Для D: unit 584 PASS/128 SKIP test/subtest events; SQL 93/1; Kafka/Worker 98/11;
race 40/8. Это события, включая subtests, а не число независимых сценариев;
наборы пересекаются, их нельзя складывать как новую статистическую выборку.
Неожиданных Go test FAIL=0. Guards не сработали: minimum cgroup available
2.77 GiB, minimum disk free 2.13 GiB, maximum sample gap 2.04 s; paging counters
не изменились. Собственные контейнеры удалены, существующий postgres не тронут.

SQL D1-only и V8-only также PASS. Проверены fresh/V5/V7 upgrade, transactional
V8 rollback/retry, открытая V7 tx после upgrade, FK KEY SHARE с сохранением writer
exclusion, fencing/stale epoch, checkpoint/outbox/offset atomicity, join rollback,
duplicate/reorder/restart-state и concurrent last fragment/read budget.
В SQL server raw log 6 ожидаемых lock_timeout: по одному V7-open-tx и одному
Worker/FK negative control на B/D/V8-only. Retry проверен тестами; deadlock=0.
Это сохранённые SQL failures, не неожиданные Go test failures и не исторический
physical DataFileExtend stall ноутбука.

Kafka scope: acks=all, unknown/lost ACK, replay/dedup, bounded admission,
callback ordering/ownership fencing, SQL Ack retries, crash/rebalance/lease loss,
network disconnect/reconnect Kafka/PostgreSQL, Master/Worker restart и сохранение
MessageID/checkpoint/scientific payload в синтетических fixtures 4/8/16 групп.
Race scope — Dispatcher/sender in-process state; live network faults не гонялись
повторно под race. Отдельный lower-level P04 opt-in suite, owned Compose outage,
RealD2 и independent full golden не запускались; все SKIP перечислены в архиве.
`TestKafkaAsyncActualRecreationRejectsOldPin` также SKIP: требует test-only
ResultsTopic overlay/owned-topic binding. B1 real topic recreation test прошёл;
это не выдаётся за выполненный async recreation test.
Нагрузочная проверка 4 Worker × полный D2 и причины старого I/O stall остаются
локальной задачей. Облачные tests не доказывают отсутствие contention под ней.

Один setup attempt остановлен из-за неверного Kafka readiness endpoint внутри
контейнера; исправлены внутренний listener и bounded readiness, а также cleanup
обвязки. Production timeout не менялся; новых production исправлений не потребовалось.
Оригинальный source этой обвязки и запись прерывания сохранены вместе с raw logs.

Стенд: Go 1.27.2, Linux amd64, cgroup 2 CPU / 8 GiB; Docker PostgreSQL 17.11
на tmpfs с durability=on, Kafka 4.1.2, последовательные packages/GOMAXPROCS=2.
Это не native Windows PG17.5 стенд. Image digests/settings, 2 s resource samples,
команды/exit codes, полные JSON logs, source/file SHA и cleanup receipts — в
внешнем архиве `p14-integration-20261107.zip` с manifest/SHA256SUMS.
Исходные Windows P14 архивы недоступны и здесь не верифицировались.

Performance, latency, lag/backlog series и ускорение: **NOT_MEASURED**.
Full Summit D2 / independent golden: **NOT_RUN**. Приёмка — только предварительная
совместимость; системный статус комбинации **INCONCLUSIVE**. Исторические статусы
D1 standalone REJECT, V8 INCONCLUSIVE и B1+B2 SYSTEM INCONCLUSIVE сохраняются.
Следующий этап описан в `HANDOFF.md` и `local-windows.json`. После публикации —
остановка, без PR/merge, новых оптимизаций и автоматического D2.
