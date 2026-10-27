# Миграция № 01: telemetry/replayer

Статус: функциональный срез реализован и проверен; дата проверки 27.10.2026.

Обязательный язык всей V2 — Go, включая будущее ядро R/B/I2, Kafka,
recovery, Worker–Master и тесты. Python runtime и экспериментальные инструменты
V1 не переносятся. Нужный этому срезу код уже существует на Go.

Источник V1: `../cluster-anomaly-detection`, commit
`891e0ef71a0da0f9ad256f91d199fef38d1355a9`, tree
`8e7b9cf28c225a5f63f80b18ab2eb8f7b164f419`. Worktree исходно чистый.
Ожидавшийся в задании tree `c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc`
в доступном object database отсутствует: сравнение с ним невозможно.
Источником служит явно закреплённый доступный commit; совпадение деревьев не заявляется.

## Матрица до переноса

| V1 | V2 | Сохраняемый контракт | Адаптация / расхождение |
|---|---|---|---|
| internal/modules/telemetry/domain/node.go | internal/modules/topology/domain/node.go | Node, общий ParseHostname | Удалить неиспользуемые DB-модели; отдельный строгий inventory a01–a16 |
| internal/experiment/source.go, source_test.go | internal/modules/topology/domain/inventory.go | 16×18 уникальных позиций | Канонический порядок и выбранные имена проверять явно; manifest/checksum не переносить |
| internal/modules/telemetry/domain/record.go | internal/modules/telemetry/domain/record.go | F18, NaN, mask, Valid, identity | Импорт topology вместо дублированного Node |
| internal/replayer/reader.go | internal/platform/parquet/reader.go | F18-v1, nullable coordinates, CPU cores, UTC day/grid | Конкретный IO изолирован от домена; ядро проверок сохранить |
| internal/replayer/scheduler.go | internal/modules/telemetry/usecase/replay.go | Строгий ключ, pacing, встретившиеся completions | TimestampComplete перемещён в domain |
| internal/replayer/window.go | internal/modules/telemetry/usecase/window.go | [start,end), проверка пропущенного префикса | Пакет и импорты |
| internal/replayer/sender.go | internal/modules/telemetry/delivery/ndjson/sender.go | NDJSON, null, mask, порядок сообщений | Только JSON sender; логовый sink не нужен |
| internal/app/replayer.go, internal/platform/config/config.go, cmd/replayer/main.go | internal/app/replayer, cmd/replayer/main.go | Composition root, flags, context, stderr | stdout по умолчанию, без PostgreSQL config |
| internal/analysis/window.go | usecase/window.go + CLI flags | 10s и полуоткрытые окна | Математический пакет целиком не переносить |
| internal/group/offline.go | документация | Offline выдаёт полностью отсутствующие ticks | Replay V1 их не выдаёт: open item следующего протокола |

`internal/platform/parquet` выбран для конкретного файлового IO: domain/usecase
зависят от значений и реально потребляемых интерфейсов, а app связывает адаптеры.

Полное ТЗ сохранено в `../spec/FINAL_TZ_KR_Distributed_Group_Anomaly_Detection_V2.md`.
Задание № 01 выделяет bootstrap/replayer перед переносом научного ядра;
весь roadmap финального ТЗ в этом коммите не реализуется.

## Фактический результат

V2 создан как самостоятельный Git-репозиторий `cluster-anomaly-detection-v2`
рядом с V1, branch `main`, origin
`https://github.com/butorovv/cluster-anomaly-detection-v2.git`.
До создания локального репозитория `git ls-remote` успешно подтвердил отсутствие
remote refs. История V1 не копировалась; вложенного V1 checkout/.git в V2 нет.

Реальный путь: `cmd/replayer → app/replayer → platform/parquet →
telemetry/usecase.Replay → telemetry/delivery/ndjson`.
`TelemetryRecord` и `TimestampComplete` не зависят от Parquet; общий hostname
parser и единственный Node живут в topology. Пустых будущих модулей/entrypoints нет.

Сохранено:

- Точное расположение всех F18 координат, NaN/availability mask и правило Valid.
  Числовой ноль остаётся числом, а missing становится NDJSON `null`.
- Generic reader V1 с буфером 256 строк, F18-v1 metadata, физическими и logical
  типами, nullable признаками, UTC-днём и 10-секундной сеткой, EOF/Close/context.
  `cpu*_valid_cores` читаются и проверяются как в V1: `-1` — неизвестное число
  ядер для предоставленного среднего, прочие значения согласуются с наличием
  CPU температуры. Эти метаданные не добавлены в TelemetryRecord: V1 использует
  их при чтении, но не несёт дальше как отдельные F18 координаты.
- Схема сопоставляется по именам/типам/nullability, как в V1. Физический порядок
  колонок файла не используется для определения порядка F18: mapping явный.
- Строгий порядок/отсутствие дублей проверяет Replay; WindowReader проверяет
  также потреблённый исключённый префикс и останавливается на exclusive end,
  не проверяя весь непотреблённый суффикс. Сам Parquet reader не сортирует данные.
- Pacing и порядок completion до ожидания следующего timestamp сохранены.
  Каждая доступная строка, включая частично/полностью недоступный F18,
  отправляется. При read/send/cancel error незавершённая группа не завершается.
- Completion выдаётся только для встретившихся пар, counters не требуют 18 строк.
  Полностью missing tick не синтезируется. Event time не заменяется wall-clock.

Адаптации:

- Новый module path, пакеты и импорты; TimestampComplete вынесен из scheduler
  в domain с отдельной проверкой identity/неотрицательных счётчиков.
- Из старого node.go не перенесены неиспользуемые DB Component/MetricSource/NodeGroup.
  Group теперь содержит canonical логические узлы. Никаких утверждений о питании,
  стойках или cooling failure domains нет.
- ParseHostname сохранён широким (`a1n1`, `h99n18` допустимы). Отдельный
  ValidateSelectedNode требует canonical a01–a16/n01–n18; CLI применяет его
  к каждой потреблённой строке. Это намеренно более строгая граница выбранной
  выборки, а не изменение общего парсера или TelemetryRecord.Validate.
- CLI выводит NDJSON по умолчанию; удалены log sink, database config, log-level
  и verbose. Вход надо указать явно или через REPLAYER_INPUT. Явные flags
  перекрывают env; добавлены --start/--end для существующего WindowReader.
- Минимальный go.mod: только parquet-go v0.32.0 напрямую и реально необходимые
  transitive dependencies тех же версий, что V1. PGX/Gonum/Kafka отсутствуют.
- Все новые проверки — Go tests. Сравнительный Go test запускает неизменённые
  V1 исходники в отдельном временном модуле с сокращённым графом dependencies.
  V1 go.mod/go.sum не меняются, сборки и generated files в V1 не создаются.

Полное финальное ТЗ скопировано побайтно без изменений. SHA-256:
`d179b4e454a5f427fcd722cef77d1575786cddcf3a9005ac5118e62ca5502091`.
`.gitattributes` сохраняет байты этого внешнего документа и его исходные
Markdown hard breaks (два пробела в конце строк). Проверка whitespace отключена
только для архивной копии ТЗ; для кода и остальной документации остаётся активной.
Расхождения относительно недоступного исходного tree из постановки определить
нельзя: соответствующий Git object отсутствует. Это ограничение provenance,
а не доказательство эквивалентности тому tree.

## Фактическая проверка

Окружение: Windows amd64, `go version go1.27.2 windows/amd64`.
В PATH находится Go 1.25.8; для всех итоговых Go-команд использован уже
установленный toolchain:
`C:\Users\Георгий\go\pkg\mod\golang.org\toolchain@v0.0.1-go1.27.2.windows-amd64\bin\go.exe`.
`GOTOOLCHAIN=local`, GOCACHE направлен в игнорируемый `.cache/go-build` V2.

Выполнено из корня V2:

```powershell
$go = 'C:\Users\Георгий\go\pkg\mod\golang.org\toolchain@v0.0.1-go1.27.2.windows-amd64\bin\go.exe'
$env:GOTOOLCHAIN = 'local'
$env:GOCACHE = Join-Path $PWD '.cache/go-build'
& (Join-Path (Split-Path $go) 'gofmt.exe') -w (rg --files -g '*.go')
& $go mod tidy
$env:REPLAYER_V1_SOURCE = (Resolve-Path '../cluster-anomaly-detection').Path
& $go test ./... -count=1
& $go vet ./...
& $go build ./cmd/replayer
.\replayer.exe --help
git diff --check
git diff --cached --check
```

Итог: перечисленные команды успешно выполнены. Отдельно выполнен
`go test ./internal/app/replayer -run TestV1Compatibility -count=1 -v`.
Сверка дала **побайтно одинаковый** NDJSON от неизменённых V1 Go
reader/scheduler/JSON sender и собранного V2 CLI:

- rows=4, invalid=2, timestamps=2, completions=3, groups=2, hosts=3;
- 7 сообщений в точном порядке, original timestamps и все 18 координат;
- полная, частичная, полностью missing строка; целиком отсутствующий промежуточный
  tick не получает marker; частичная группа имеет 2 records / 1 valid_record;
- одинаковые JSON null/mask/Valid; технических различий порядка JSON ключей нет;
- speed=0: V1 clock запрещает Sleep panic-ом, V2 отсутствие ожиданий дополнительно
  проверено unit tests с управляемыми часами;
- SHA-256 NDJSON: `4adf1fc12d4653119922904ea88a53dc68fea5dcc4e4352e95d4acfe0846ceda`.

Unit/integration охватывают topology 16×18 и порядок/дубли/идентичность,
F18 feature order/NaN/Inf/mask/Valid, cancellation, EOF, ошибки reader/sender/
completer, speed=0/1/10/100, absolute pacing, half-open windows, schema/type/
nullability mismatch, UTC day/grid, повреждённые/пустые файлы, NDJSON и CLI.
Ранние ошибки переноса тестов (неквалифицированный TimestampComplete и unkeyed
literals после переноса типа в domain) исправлены; итоговые test/vet успешны.
Первый offline `go mod tidy` не смог получить отсутствовавшие test dependencies
самой Parquet-библиотеки; повтор с доступом к Go proxy завершился успешно.
Это не добавило runtime-сервисов и не обновило версии dependencies V1.

Real-data smoke: **выполнен успешно** на уже имевшемся V1
`data/prepared/summit-D1-pilot/D1.parquet` (93 075 717 bytes).
Первичная проверка только верхнего уровня prepared пропустила вложенный pilot;
перед финальным отчётом выполнен рекурсивный поиск и запуск реального CLI.
Manifest подтверждён по F18-v1, точному feature_order, 16 группам и 288 hosts
в canonical порядке. SHA-256 файла совпал с `prepared_file_hashes.D1.parquet`:
`cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7`.
Исходный manifest содержит `assumed_timezone=UTC`, `timezone_verified=false`:
smoke не доказывает внешнюю верификацию исходного часового пояса или D2 parity.

```powershell
.\replayer.exe --input ../cluster-anomaly-detection/data/prepared/summit-D1-pilot/D1.parquet --speed 0 --start 2022-01-18T00:00:00Z --end 2022-01-18T00:01:00Z 1> .cache/real-smoke.ndjson 2> .cache/real-smoke.stderr.log
```

Exit 0: rows=1728, timestamps=6, groups=16, hosts=288, invalid=0,
timestamp_completions=96. Каждая из 1824 stdout-строк разобрана как JSON;
в telemetry проверен размер F18=18, в marker — records=valid_records=18.
Выход и логи только в игнорируемой `.cache` V2. Pilot содержит 8640 ticks за
сутки несмотря на историческое имя D1; он используется здесь только как
проверенный реальный вход первой минуты, а не как frozen D1 training window.
Данные не скачивались/не менялись, golden D2 не генерировался.

`git status --short --untracked-files=all` V1 до и после работы пустой;
HEAD/tree остались указанными выше.

## Открытые пункты и следующий срез

1. **Полностью отсутствующие ticks:** отдельный Kafka-протокол должен перечислять
   каждый `(group,tick)` выбранного окна, в том числе `records=0`, либо задавать
   эквивалентную детерминированную политику. Сейчас этого нет. V1 offline WalkTicks
   выдаёт пустые ticks, но Replay V1/V2 markers для отсутствующих пар не выдаёт.
2. **Задание № 02:** frozen model bundle + Go R/B/I2 + offline golden parity,
   с собственными SHA и проверкой реальных D2/δ=0,10,30,60. На этом этапе
   математический код, models, IF trees, profiles и golden не перенесены.
3. Прямой Kafka producer — следующий самостоятельный шаг после согласованного
   порядка реализации; ни ingestion, ни Kafka broker/Compose не добавлены.
4. Worker, Master, recovery/checkpoints, DB/outbox и три выходных транспорта —
   последующие этапы полного ТЗ. Весь implementation/test/runtime код — Go.
5. При появлении необходимой Python-only функции сначала оформить отчёт о ней
   и план реализации на Go. В текущем срезе такой зависимости не обнаружено;
   аудит математического ядра следующего среза здесь не заявляется.
6. Manifest/checksum и фиксированные D1/D2/D3 окна изучены, но experiment/source
   целиком не перенесён. CLI не удостоверяет исследовательский manifest или
   dataset SHA: в этом срезе он проверяет F18-v1 и заданный поток/окно.
7. Для проверки относительно tree `c6de846...` нужен доступный Git object/commit.
   Текущая подтверждённая сверка относится только к commit `891e0ef...`.

Работали orchestrator (gpt-6.1-sol/high, configured), inspector
(gpt-6-luna/low, configured, explore) и implementer (gpt-6-luna/medium,
configured, topology). Настройки сверены с `.codex/config.toml` и role TOML
родительского workspace; фактические runtime model/effort метаданными не
подтверждены. Финальное чтение production diff, интеграция и проверки выполнены
основным агентом. Техническая read-only изоляция inspector не заявляется.
