# P14 Integration: подготовка и safety gate

## Актуальный объём после уточнения пользователя

Подготовленные snapshots сохраняются без пересоздания. Завершить уже начатые
correctness tests, исправить только воспроизводимые дефекты, затем один отдельный
commit и push feature/p14-integration. Главный результат — объединённая D-версия
и короткий REPORT/HANDOFF для локального Windows Summit D2 с independent golden.
Облачная performance-матрица и полный D2 не запускаются. Предыдущий план ниже
сохранён как запись подготовки, его отложенная серия сейчас не исполняется.
Облачный PASS означает только предварительную совместимость. После push остановка.

Дата: 2026-11-07 Europe/Moscow. Продолжение ограничено уточнением пользователя:
облачные performance-эксперименты запрещены до подтверждения сопоставимости.
Windows-архивы недоступны; их содержимое и SHA в этом окружении НЕ верифицированы.
Из предыдущих чатов прочитаны сохранённые выдержки HANDOFF/итогов, это вторичный
источник, не замена исходным архивам. Исторические FAIL сохраняют свой статус.

## Источники и изоляция

- A: c9a81e637c18c628c708c59a1816708bce6e0847, V1–V7.
- SQL источник: 9da066efb9c42947f683afd14dc26620c43156b1.
- C: 64445776dd240b7e441f0babbdd6a68df6ecbc28, B1+B2, V1–V7.
- B: A + readiness count D1 + неизменённая V8 + необходимые regression tests.
- D: B + точный Kafka diff C/A. Только новая feature/p14-integration от A.
- Диагностические D1-only и V8-only: отдельные detached worktrees/commits.
- Не переносить profiling, старые runner/analyzer/SQL benchmark/README изменения.
  Из SQL join.go исключить только profiling calls и import; алгоритм D1 сохранить.
- Не менять main, исходный work checkout и экспериментальные ветки; без PR/merge.

## Матрица targeted checks

| Check | A | B | C | D | Диагностика / критерий |
|---|---|---|---|---|---|
| Git tree/file SHA, deps, migrations | да | да | да | да | V1–V7 неизменны; V8 точный source SHA; frozen kernel одинаков |
| Все доступные unit tests / vet | да | да | да | да | Raw JSON logs; каждый SKIP отдельно |
| Fresh/V5 upgrade, atomic rollback, SQL suites | да | да | да | да | Точные migration ledger/checksums через Apply tests |
| V7→V8, rollback/retry, open tx upgrade | — | да | — | да | V8-only тоже; незавершённая V7 tx сохраняет старый lock |
| Duplicate/reorder/restart-state, final join atomicity | да | да | да | да | Сохранение MessageID/checkpoint/outbox/offsets |
| D1 payload read budget, concurrent last duplicate | — | да | — | да | D1-only тоже; это correctness/read count, не benchmark |
| FK KEY SHARE vs Master identity lock | V7 control | да | V7 control | да | V8-only; ожидаемый 55P03, rollback и retry; не скрывать FAIL SQL |
| Kafka callback early/delayed, unknown ACK, bounded admission | — | — | да | да | Unit faults + real Kafka sender checks |
| Kafka ACK→SQL ACK, SQL retry, stale callback/fencing | legacy | legacy | да | да | Async integration; one unfinished delivery/group |
| Worker crash/restart/rebalance/lease loss | да | да | да | да | Существующие real Kafka/PostgreSQL fixtures |
| Kafka/PG disconnect/reconnect, replay/dedup | да | да | да | да | Изолированные сервисы, network proxy faults |
| Targeted race | — | — | да | да | Dispatcher/sender + SQL concurrency |
| Исторический physical DataFileExtend stall | NOT_REPRODUCED | NOT_REPRODUCED | NOT_REPRODUCED | NOT_REPRODUCED | Облачный tmpfs не проверяет ноутбучный диск; нужен blocker graph |
| Independent full scientific golden | BLOCKED | BLOCKED | BLOCKED | BLOCKED | Нужны frozen inputs/models/golden; синтетика его не заменяет |

Любой неожиданный correctness FAIL останавливает соответствующую конфигурацию.
Ожидаемый negative-control 55P03 остаётся SQL failure с проверенным recovery.
Таймауты production не увеличивать. SQL extension observation может дать
NOT_OBSERVED; отсутствие witness не закрывает историческую причину отказа.

## Исполнение доступных tests

Новые одноразовые PostgreSQL 17 и Kafka 4.1.2, отдельные имена/порты, без доступа
к существующим volumes. Последовательные Go packages (-p=1, GOMAXPROCS=2) в лимите
2 CPU / 8 GiB. PostgreSQL tmpfs используется только для correctness, не throughput.
До тестов сохранить image digests/settings, cgroup/RAM/disk. Watchdog: каждые 2 s;
остановка при cgroup available <600 MiB, host MemAvailable <600 MiB, disk <2 GiB
или отсутствии свежего sample >10 s. Cleanup собственных контейнеров в finally;
raw FAIL/logs/command exit codes не перезаписывать. Не запускать RealD2 opt-ins.

## Отложенная ограниченная серия (НЕ ЗАПУСКАТЬ В ОБЛАКЕ)

После получения архивов, equality checks и safety gate: 4 Worker, 16 групп,
cadence=3, delta=0, одинаковые topics/partitions/input/model hashes, Master/Replayer,
CPU/RAM/GOMAXPROCS и PG settings. Kafka C/D N=4, linger=0, прежние bounds
(4 MiB+256 admitted bytes, страницы 64 records/4 MiB); A/B sync control.
Начальный scope 240 timestamps (>60), без полного D2; увеличивать scope только
при недостаточном steady interval и после resource gate. Три независимых блока:
A,B,C,D | D,C,B,A | C,A,D,B. Не заменять failed attempts успешными.
Новая DB/execution/topics на attempt с одинаковой topology. Входы идентичны по SHA;
не использовать scientific timestamps для wall-clock E2E. Profiling отдельно.

Для каждого attempt: group-ticks/s и pipeline interval; E2E p50/p95/p99;
Master throughput/intake; SQL Page/Ack/checkpoint/commit/pools/query count;
lock waits/55P03/deadlocks; sender records/s/ACK/retries/duplicates/errors/inflight/
batch size; input/results lag; раздельные backlog/oldest age outbox_messages и
worker_tick_fragments; end-input→полный drain; Worker/Master CPU/RSS/goroutines;
RAM/paging и freshness. Сохранять 1 Hz time series, lag/backlog/speed/E2E plots.
Отсутствующие измерения: NOT_MEASURED. Не объединять разные migration sets/linger
или sender records/s с group-ticks/s. Вычислять B/A,C/A,D/A,D/B,D/C по сравнимым
повторам, показывать диапазон/каждый FAIL и взаимодействие, не предполагать сложение.

До full D2 golden и окончательной приёмки — остановка. Текущие системные решения:
D1 standalone REJECT; V8 INCONCLUSIVE; B1+B2 INCONCLUSIVE; D INCONCLUSIVE.
