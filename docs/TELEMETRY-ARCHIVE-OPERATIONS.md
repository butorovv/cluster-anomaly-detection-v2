# Архив Summit и REST физической телеметрии

Архивная БД и Go-сервис чтения работают отдельно от отдельных демонстрационных
запусков. Диагностический Worker, Master и runner не требуют архива для работы
в исходной конфигурации. В Qt отсутствует вызов Parquet-читателя. Путь
`--summit-assets` нужен Replayer при запуске из GUI.

## Компоненты

- `telemetry-archive import-summit --input PATH` использует `ARCHIVE_DATABASE_URL`
  владельца архива, проверяет SHA256, применяет отдельно V1 и служебные миграции
  архива. Один V1 архив соответствует одному источнику.
- `telemetry-archive serve --review-root PATH --runs-root PATH` читает через
  `ARCHIVE_READ_URL`, при включённом режиме stream через
  `ARCHIVE_STREAM_READ_URL`. У роли есть только SELECT необходимых таблиц.
  Сервис принимает HTTP на loopback. По умолчанию порт 18087.
- `telemetry-archive init-stream` подготавливает пустую БД физического источника.
  `telemetry-archive consume` запускает независимую группу `cad-archive-<execution>`
  с параметрами `--execution`, `--source`, `--profile`, `--from`, `--to`,
  `--brokers`. У штатного демонстрационного запуска новый топик и начальные
  offsets 0. При недопустимой позиции consumer завершает работу с ошибкой.
- `telemetry-archive stream-progress --execution UUID --to UTC` проверяет
  конечные позиции всех 16 партиций. Диагностическое завершение и архивный
  drain подтверждаются отдельно.

На ВМ постоянный контейнер `cad-gui-archive-pg` использует том
`cad-gui-archive-data` и слушает `127.0.0.1:15487`. Базы для импорта и стрима
разделены для проверки первого архивирования и изоляции. Параметры доступа
хранятся в закрытом каталоге `~/cad-gui-qt-4f4b7ad/archive-storage` вне Git.
Исходный D2 и прежние окружения остаются в собственных каталогах.

```bash
cad="$HOME/cad-gui-qt-4f4b7ad"
work="$cad/archive-release"
assets="$HOME/p16-d2-c3-matrix.bd4c292002/assets"
go="$HOME/p16-recovery-diag-9YovoW/go/bin/go"
cd "$work"
cmake -S gui -B build/qt -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build/qt --parallel 7
ctest --test-dir build/qt --output-on-failure
mkdir -p build/archive
GOCACHE="$cad/go-cache" GOMODCACHE="$cad/go-mod" \
  "$go" build -o build/archive/telemetry-archive ./cmd/telemetry-archive
```

Однократная проверяемая загрузка готового источника либо проверка существующего
манифеста. Имя файла D1 унаследовано от подготовки набора. Проверка D2 опирается на SHA256, топологию и восьмичасовое окно.

```bash
storage="$cad/archive-storage"
ARCHIVE_DATABASE_URL="$(cat "$storage/owner.dsn")" \
  build/archive/telemetry-archive import-summit \
  --input "$assets/data/prepared/summit-D1-pilot/D1.parquet"
```

Для запуска сервиса и GUI из XFCE:

```bash
umask 077
runs="$cad/archive-replay-runs"
ARCHIVE_READ_URL="$(cat "$storage/reader.dsn")" \
ARCHIVE_STREAM_READ_URL="$(cat "$storage/stream-reader.dsn")" \
  build/archive/telemetry-archive serve --review-root "$cad" --runs-root "$runs"
```

В другом терминале XFCE:

```bash
PATH="$cad/docker-bin:$PATH" \
XDG_CONFIG_HOME="$cad/archive-qt-profile" \
CAD_GUI_ARCHIVE_BINARY="$work/build/archive/telemetry-archive" \
CAD_GUI_STREAM_DATABASE_URL="$(cat "$storage/stream-owner.dsn")" \
  "$work/build/qt/cluster-monitor" --review-root "$cad" --summit-assets "$assets" \
  --archive-api http://127.0.0.1:18087 --managed-root "$runs" --demo-pwsh "$cad/pwsh/pwsh"
```

Для запуска из SSH используйте тот же сеанс `DISPLAY=:11` с
`XAUTHORITY="$HOME/.Xauthority"`. TigerVNC доступен через прежний
SSH-туннелем. В обычном GUI полное покрытие из манифеста включает reuse.
`CAD_GUI_FORCE_ARCHIVE_STREAM=1` предназначен для изолированных испытаний
с отдельной пустой БД, не для повторной загрузки уже импортированного D2.

У каждого управляемого запуска свой Compose project. Supervisor сохраняет
диагностическую историю и проверяет все 16 конечных позиций архива перед
штатной остановкой. Закрытие окна не останавливает supervisor. Если
архиватор завершился аварийно, supervisor один раз запускает его заново
в пределах окна финализации. Он продолжает с PostgreSQL next_offset.

Если покрытие не подтверждено, состояние становится `incomplete`. Supervisor
сохраняет доступную диагностику и оставляет свой Compose работающим,
включая Kafka. Срок удержания сообщений Kafka ограничен конфигурацией
тестового брокера. Возобновляйте обработку до истечения retention.

После восстановления доступа к архивной БД запустите тот же supervisor
с исходным RunId и `-ResumeArchive`. Токен привязки, окно и источники
берутся из исходных метаданных. Replayer и Worker повторно не запускаются.

```bash
PATH="$cad/docker-bin:$PATH" \
CAD_GUI_ARCHIVE_BINARY="$work/build/archive/telemetry-archive" \
CAD_GUI_STREAM_DATABASE_URL="$(cat "$storage/stream-owner.dsn")" \
  "$cad/pwsh/pwsh" -NoProfile -File "$work/gui/managed-run.ps1" \
  -Catalog "$runs" -RunId "$execution_id" -AssetsRoot "$assets" -ResumeArchive
```

Ручной `gui/demo.ps1 -Action Stop` используйте только для каталога
принадлежащего вам запуска после подтверждения архива. Для просмотра
истории достаточно REST и постоянного PostgreSQL. Исходные нагрузочные
сценарии воспроизводятся прежним runner с выключенным архиватором и без
REST-трафика.

## Регрессия и приёмка

Из checkout на ВМ выполните базовую Go-регрессию, проверку зависимостей
и Qt-тесты.

```bash
export GOCACHE="$cad/go-cache" GOMODCACHE="$cad/go-mod"
"$go" test ./...
"$go" vet ./...
QT_QPA_PLATFORM=offscreen ctest --test-dir build/qt --output-on-failure
ARCHIVE_INTEGRATION_TEST_URL="$(cat "$storage/test-owner.dsn")" \
  TEST_DATABASE_URL="$(cat "$storage/test-owner.dsn")" \
  "$go" test -count=1 ./internal/app/telemetryarchive ./internal/modules/telemetry/archive/postgres
```

Тестовая БД отделена от постоянного импорта и штатного стрима. Не передавайте
туда DSN хранилища старых экспериментов. Полный интеграционный результат,
ограничения и парный замер описаны в [матрице приёмки](TELEMETRY-ARCHIVE-ACCEPTANCE.md).

## Контракт REST

`GET /v1/telemetry?execution_id=…&group_id=a14&host_id=a14n04&from=2022-01-18T15:52:20Z&to=2022-01-18T15:57:30Z`

Диапазон полуоткрытый в UTC, параметры на сетке 10 секунд. Максимум 610 с и
61 отметка. Ответ содержит идентичность исполнения, источника и профиля,
`provenance` (`verified_import` или `stream_archive`), 18 каналов в каноническом
порядке с единицами W/°C и отсчёты по времени. Статусы отдельных элементов:

```json
{"time":"2022-01-18T15:57:20Z","status":"available","values":[424.2,464.8,0,null,66,62.9,63.7,62.8,62.6,67.1,30,29.6,33,33.4,34.4,34,34.2,35.9]}
{"time":"2022-01-18T15:57:30Z","status":"missing_observation"}
{"time":"2022-01-18T15:57:40Z","status":"pending_archive"}
{"time":"2022-01-18T15:57:50Z","status":"not_published"}
```

Это примеры структуры ответа. Реальные значения возвращаются из PostgreSQL.
`400` означает неверный интервал или связку узел–группа, `404` — неизвестный
запуск или группу, `409` — неподтверждённую идентичность, `503` — временную
недоступность. Ошибки и ожидающие отметки не становятся постоянными
пропусками в кэше Qt. Групповой барьер получен из точных опубликованных тиков
Master или проверенных страниц истории. Маски в stream относятся только к
конкретному исполнению. Пустой маркер и запись узла с маской 0 различаются.

Ограничение: потребитель использует внешнюю авторитетную позицию PostgreSQL
и не полагается на Kafka commits. Запись, маски и позиция подтверждаются
одним COMMIT. Новый владелец партиции повышает epoch, старый не проходит
compare-and-swap. После неоднозначной ошибки повтор начинает с позиции,
прочитанной заново из PostgreSQL. На уровне доставки используется at-least-once
с идемпотентной записью. Исходные байты Kafka и причины пропуска датчика в V1
не сохраняются.
