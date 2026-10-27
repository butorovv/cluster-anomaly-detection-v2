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

Срез P02: [перенос БД и R/B/I2](docs/migration/02-v1-db-frozen-rbi2.md),
[provenance и SHA](docs/migration/02-provenance.json),
[миграции PostgreSQL](db/README.md). Реальная D2 golden parity остаётся
обязательным условием перехода к distributed runtime.

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
синтетические модели используются только внутри тестов. Команды реальной D2
приёмки и список недостающих файлов приведены в отчёте P02.
