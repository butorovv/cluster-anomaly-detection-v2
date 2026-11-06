# HANDOFF — интеграция для локального Windows Summit D2

Объединённая версия: ветка `feature/p14-integration`. Точный опубликованный SHA
указан в финальном сообщении и publication receipt архива. Единственный parent
интеграционного commit — P13 `c9a81e637c18c628c708c59a1816708bce6e0847`.
SQL-источник: `9da066efb9c42947f683afd14dc26620c43156b1`; Kafka-источник:
`64445776dd240b7e441f0babbdd6a68df6ecbc28`. D1 перенесён без profiling calls,
V8 и все Kafka files — точные исходные bytes. Frozen scientific код не изменён.
Подготовленные A/B/C/D, D1-only и V8-only сохранены; пересоздавать их не требуется.

Облачные tests дают только предварительную совместимость. D1 standalone сохраняет
REJECTED_STANDALONE_UNSTABLE; V8, B1+B2 и общая комбинация системно INCONCLUSIVE.
Historical SQL lock_timeout и недостаток fault evidence не закрываются новым PASS.
Новая облачная performance-серия, полный D2, PR и merge не выполнялись.

## Что требуется на ноутбуке

1. Новый checkout/worktree опубликованного SHA; Go 1.27.2; проверка чистоты Git.
2. Immutable `ASSETS_ROOT` со следующими исходными файлами, их manifest и SHA:
   `data/prepared/summit-D1-pilot/D1.parquet`, `manifest.json` рядом с ним;
   `configs/group-profile.summit-v1.json`, `local-b1.summit-20220118.json`,
   `summit-hardware-reference.json`, `group-detection.summit-v1.json`;
   `results/statistical-20220118-8h-v1.json`, `isolation-20220118-v1.json`.
   Имя D1.parquet историческое: полный Summit D2 выбирается из prepared дня.
   Ожидаемые profile/input SHA находятся в `local-windows.json`; не менять их
   ради запуска на другом или синтетическом входе. Bundle проверяет модели.
3. Frozen independent V1 golden из pin
   `c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc`: manifest
   `v1-generated-golden.json` и `g16-d0/{raw,temporal,evidence,events}.ndjson`,
   `g16-d0/summary.json`. SHA каждого файла должен совпасть с manifest.
   Не генерировать golden интеграционной V2. Исторический V1 executable отсутствовал;
   его provenance не считать восстановленным. Имеющиеся frozen bytes сохранять.
4. Native PostgreSQL 17.5 и отдельный Kafka 4.1.2 Docker/WSL стенд; maintenance DSN
   с правом CREATE DATABASE и создания test roles, без публикации пароля в логах.
   Исторические endpoints 55415/29414 — только справка, проверить фактические.
   Новый execution/DB, input/results topics с прежними 16 partitions и bounds.
   Не использовать пользовательские topics с посторонними producers/consumers.
   Не удалять существующие volumes; не менять Windows/WSL/Docker settings.
5. Оригинальные локальные P14-1/P14-2 archive manifests, HANDOFF, raw FAIL и
   Windows resource watcher/paging/freshness/cleanup wrapper. Эти архивы облако
   не читало и не верифицировало. Их нельзя заменять извлечённым chat summary.

## Перед локальным запуском

- Сопоставить source SHA, binary hashes, scientific assets, Kafka settings и PG
  durability с выбранным reference. Применить V1–V8 через существующий Apply;
  V1–V7 не переписаны. Сохранить реальный schema_migrations ledger и settings
  до удаления test DB. V8 — не online исправление уже удерживаемого V7 lock:
  старые Master tx должны завершиться до измерения.
- `go test ./...` и `go vet ./...`; затем имеющиеся SQL/Worker/Kafka targeted
  suites с явными isolated opt-ins. Не включать реальные opt-ins против чужих
  сервисов. Повторить upgrade, FK KEY SHARE/NO KEY UPDATE, checkpoint fencing,
  Kafka ACK→SQL ACK/restart/replay и сетевые recovery проверки локально.
- Параметры кандидата: 4 Worker / 16 групп / delta=0 / cadence=3 / Kafka /
  replay_speed=0; Worker/Master/Replayer GOMAXPROCS=1/2/1. Kafka async=true,
  N=4, linger=0, admitted bytes=4194560, page<=64 records и <=4 MiB,
  drain=5s. Одна незавершённая попытка на группу через Kafka ACK и SQL ACK.
  Production PG lock_timeout=2000ms, statement_timeout=5000ms сохранены.
- Восстановить прежние guards RAM/paging/disk, freshness samples и finally cleanup.
  До этого полный прогон не запускать. Новый output, без `--resume` чужих PASS.

## Подготовленная команда (в этом чате НЕ выполнялась)

`local-windows.json` содержит один full D2 run и использует прежний P13 runner.
Этот runner намеренно не заменён profiling/benchmark инфраструктурой P14-1.
В PowerShell установить локальные реальные значения, не выводя DSN с паролем:

```powershell
git fetch origin feature/p14-integration
git worktree add --detach ../cluster-anomaly-detection-v2-p14-integration FETCH_HEAD
Set-Location ../cluster-anomaly-detection-v2-p14-integration
git rev-parse HEAD
go version
go test ./...
go vet ./...
# ASSETS_ROOT, V1_GOLDEN_ROOT, V1_GOLDEN_MANIFEST: реальные frozen каталоги/manifest.
# SYSTEM_TEST_DATABASE_URL: отдельный maintenance DSN.
# P07_TEST_KAFKA_BROKERS: отдельный broker, SYSTEM_KAFKA_ISOLATED='1'.
# P14_INTEGRATION_OUTPUT: новый, ещё не существующий каталог результатов.
# Запускать только после локальной проверки ресурсов, под прежним watcher/finally:
go run ./experiments/runner --config experiments/p14integration/local-windows.json
```

Runner создаёт disposable DB/новый execution, запускает реальные Worker/Master/
Replayer, сверяет независимый golden и offsets/checkpoints, сохраняет raw logs и
binary SHA. Он удаляет свою DB в defer: необходимые SQL diagnostics нужно снять
до cleanup через существующую локальную обвязку. Polling/HTTP матрица в этом
config отсутствует; Kafka CLI defaults дают N=4/linger=0.

Ожидаемые справочные δ0 cardinalities: 46080 group-ticks, 1284 Evidence,
750 parent events, 473 new events, 285 RBSubspans. Поля/IDs/summary и canonical
hashes должны совпасть полностью; одних cardinalities недостаточно.

При lock_timeout сохранить transaction/backend IDs, SQLSTATE, statements,
pg_blocking_pids/pg_locks/pg_stat_activity и PostgreSQL logs в общей временной шкале.
Различать V7 FK conflict и relation-extension/DataFileExtend→ownership цепь;
не объявлять старый root cause установленным по новому здоровому прогону.
Таймауты не увеличивать. При новой ошибке — диагноз и минимальное исправление,
не замена failed attempt успешным. Системное ускорение потребует отдельного
сопоставимого P13 контроля; одиночный полный D2 подтверждает только свой scope.
