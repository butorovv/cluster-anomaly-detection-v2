# Рабочее место оператора диагностики

## Запуск V3.2 на ВМ

В VNC доступны сохранённый Summit D2 и управляемая имитация онлайн-мониторинга.
[Сборка на ВМ, запуск Qt и архива, команды SSH/VNC и безопасная остановка](../docs/TELEMETRY-ARCHIVE-OPERATIONS.md).
[Матрица приёмки A01–A20](../docs/TELEMETRY-ARCHIVE-ACCEPTANCE.md) и [сценарии воспроизведения](../docs/TELEMETRY-ARCHIVE-OPERATIONS.md#воспроизводимые-проверки).

Для проверки истории выберите Summit D2, установите 15:57:20 UTC, откройте a14
и узел a14n04. B доступен на уровне группы, IF — локальное свидетельство.
Графики узла получают 18 каналов единым асинхронным REST-запросом. Шкала
активного запуска останавливается на последнем непрерывном подтверждённом
срезе. Опубликованный тик может ожидать архивной записи, а отсутствующий
канал отображается разрывом.

Для проверки управления выберите «Новый запуск» или «Запустить имитацию».
Короткий интервал с известными событиями — 18.01.2022 08:02:20–08:03:20 UTC.
Для имитации применяется ×1. Для архива доступны ×1, ×5, ×10 и MAX.
Supervisor продолжает обработку после закрытия GUI, подтверждает drain и
checkpoints и сохраняет диагностические страницы. Импортированный Summit
используется повторно. В потоковом режиме архиватор подтверждает свою
позицию и покрытие отдельно. После остановки Compose физические графики
по-прежнему читает единственный постоянный REST-сервис.

V3.2 распознаёт проверенные копии прежних демонстраций в
`<review-root>/v32-review-snapshots/<имя>`. Исходные окружения хранятся отдельно.
Master управляемого запуска публикуется на loopback. Kafka доступна на
loopback только в потоковом режиме. Диагностическая PostgreSQL остаётся в
сети Compose. [Архитектура и ограничения](../docs/TELEMETRY-ARCHIVE-DESIGN.md).

Экспорт доступен из журнала и каталога. В диалоге выберите все доступные или отфильтрованные события. Чтобы получить CSV на Windows, скачайте указанный приложением файл командой `scp -i $key 'ubuntu@VM:/абсолютный/путь/events.csv' .` через существующий SSH. [Аудит V3.2](UX-V32-AUDIT.md).

Настольный клиент показывает срезы кластера, диагностику R/B/I2 и IF, журнал завершённых событий и состояние Master. Клиент получает данные по HTTP; расчёты выполняет Go Backend. [Контракт и правила сверки](API.md).

## Сборка

Нужны CMake 3.21+, компилятор C++20 и Qt 6.4+ с Widgets, Network и Test. Qt Test нужен только при включённом BUILD_TESTING. Библиотека и компилятор должны быть из совместимых комплектов. Пример для Qt 6.11.0 и MinGW на Windows, из корня репозитория.

```powershell
$env:Path = 'C:\Qt\6.11.0\mingw_64\bin;C:\Qt\Tools\mingw1310_64\bin;C:\Qt\Tools\Ninja;C:\Qt\Tools\CMake_64\bin;' + $env:Path
cmake -S gui -B build/qt-gui -G Ninja -DCMAKE_PREFIX_PATH=C:/Qt/6.11.0/mingw_64 -DCMAKE_BUILD_TYPE=Release
cmake --build build/qt-gui --parallel
ctest --test-dir build/qt-gui --output-on-failure
.\build\qt-gui\cluster-monitor.exe
```

В Linux укажите каталог установленного комплекта Qt в CMAKE_PREFIX_PATH. Имя файла запуска — `build/qt-gui/cluster-monitor`. Go Backend по-прежнему собирается отдельно командой `go build ./cmd/...`.

В окне настроек укажите адрес Master API, Execution ID и интервал опроса. По умолчанию опрос выполняется каждые 1000 мс. Адрес и UUID можно передать как `--master URL --execution UUID`. UUID меняется и в верхней панели. При потере связи клиент сохраняет данные текущего запуска, помечает их как неактуальные и продолжает попытки подключения.

При тайм-ауте агрегированной сводки опрос потоков результатов и событий продолжается. Обзор показывает прогресс по уже скачанным уникальным результатам, а после восстановления сводки клиент проверяет полноту и догружает пропуски.

## Доступ к GUI на Linux с Windows

Приложение, XFCE и TigerVNC запускаются на ВМ под обычным пользователем. VNC слушает loopback ВМ, а Windows открывает loopback-туннель по существующему SSH-подключению. Для просмотра подходит [TigerVNC Viewer для Windows x64](https://sourceforge.net/projects/tigervnc/files/stable/1.16.2/vncviewer64-1.16.2.exe/download).

```powershell
pwsh -File gui/connect-vnc.ps1 -SshTarget 'ubuntu@VM' -IdentityFile $key -Viewer $viewer -SetVncPassword
```

Флаг `-SetVncPassword` запускает интерактивное задание пароля в SSH. Для повторного входа опустите флаг. При ручном подключении оставьте туннель открытым и введите `127.0.0.1::15911` в TigerVNC Viewer.

```powershell
ssh -N -T -o StrictHostKeyChecking=yes -o ExitOnForwardFailure=yes -i $key -L 127.0.0.1:15911:127.0.0.1:5911 ubuntu@VM
```

Закрытие Viewer завершает только локальный туннель. Повторный запуск подключения открывает ту же удалённую сессию.

На развёрнутой ВМ исходники и артефакты приёмки лежат в `~/cad-gui-qt-4f4b7ad`. Сборка и ручной запуск из терминала XFCE.

```bash
cad="$HOME/cad-gui-qt-4f4b7ad"
cd "$cad/repo"
cmake -S gui -B build/linux-qt -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build/linux-qt --parallel 8
ctest --test-dir build/linux-qt --output-on-failure
run="$cad/d2-max"
build/linux-qt/cluster-monitor --master "$(jq -r .master "$run/demo.json")" \
  --execution "$(jq -r .execution "$run/demo.json")" --summit-speed "$(jq -r .speed "$run/demo.json")"
```

Для запуска из SSH используются те же аргументы приложения и окружение `DISPLAY=:11 XAUTHORITY="$HOME/.Xauthority"`. Если графическую сессию штатно остановили, возобновите её под пользователем ВМ.

```bash
cad="$HOME/cad-gui-qt-4f4b7ad"
tigervncserver :11 -localhost yes -rfbport 5911 -SecurityTypes VncAuth \
  -PasswordFile "$HOME/.config/cad-gui-vnc/passwd" -geometry 1920x1080 -depth 24 \
  -nolisten tcp -xstartup "$cad/xfce-start"
```

Остановить только этот рабочий стол можно командой `tigervncserver -kill :11`. Контейнеры от этого продолжают работать.

Для нового запуска на ВМ используйте новый каталог. Режим `-PrivateDataPorts` исключает публикацию PostgreSQL и Kafka на хосте; Master по-прежнему публикуется только на loopback. На Linux контейнеры Bootstrap, Worker и Replayer работают с UID и GID владельца закрытых входных файлов; каталог монтируется только для чтения.

```bash
cad="$HOME/cad-gui-qt-4f4b7ad"
export PATH="$cad/docker-bin:$cad/pwsh:$PATH"
cd "$cad/repo"
run="$cad/d2-video"
assets="$HOME/p16-d2-c3-matrix.bd4c292002/assets"
pwsh -NoProfile -File gui/demo.ps1 -Action Init -RunDirectory "$run" \
  -AssetsRoot "$assets" -MasterPort 18086 -Speed 0 -Workers 4 -PrivateDataPorts
pwsh -NoProfile -File gui/demo.ps1 -Action Replay -RunDirectory "$run"
# После просмотра остановить только выбранный проект. Тома сохраняются.
pwsh -NoProfile -File gui/demo.ps1 -Action Stop -RunDirectory "$run"
```

Короткие функциональные прогоны задаются `-WindowStart` и `-WindowEnd` при Init, на десятисекундной сетке UTC. Для повторной проверки остановки или запуска используйте каталог именно своей демонстрации.

Отчёты развёрнутого D2 лежат в `~/cad-gui-qt-4f4b7ad/d2-max`. В `verification.json` записан независимый обход REST, в `sql-final.json` — число объединений, финализированных checkpoints и сообщений в ожидании, в `golden-comparison.json` — сверка с эталоном. В `durable-timing.json` длительность отсчитана от начала Replayer до последнего подтверждения доставки, поскольку первый вызов агрегированной сводки во время слива получил тайм-аут. Повторная сверка выполнена на том же execution после обновления read-only API.

## Режимы просмотра

| Режим | Действие оператора | Настройка Replayer |
| --- | --- | --- |
| LIVE | Показ последнего полученного среза по мере обработки | 1, целевой интервал 10 с |
| FAST REPLAY | Потоковый просмотр, прогресс и журнал | 5 или 10, целевые интервалы 2 с и 1 с; 0 для MAX |
| HISTORY | Задайте интервал и срез в UTC. Кнопки ±10 с перемещают срез по сетке. Щелчок по графику выбирает момент; из журнала открывается интервал выбранного события | Фоновая обработка и опрос продолжаются |

Скорость задаётся при запуске Replayer. Переключатель GUI меняет режим просмотра, текущий темп всегда подписан отдельно, если запуск объявлен через `--summit-speed`. В исходном обзоре результат на выбранный тик отображается независимо для каждой группы. Четыре цвета различают норму, аномалию, неопределённое решение и отсутствие результата. Журнал показывает завершённые события и в HISTORY отбирает события, пересекающие выбранный интервал.

График построен на QWidget/QPainter. Прототип на Qt 6.11.0 отрисовал две серии по 2880 точек в окне 1050×248 в среднем за 7,82 мс на кадр. Qt Charts в том же локальном прототипе занял 23,03 мс и имеет статус deprecated с Qt 6.10. Qt Graphs для 2D опирается на Qt Quick; подходящего установленного комплекта не было. Qwt добавил бы отдельную зависимость для сборки под Windows. Выбранный компонент оставляет разрывы на местах отсутствующих значений и ограничивает перерисовки потока до пяти в секунду. [Qt Charts](https://doc.qt.io/qt-6/qtcharts-overview.html) · [Qt Graphs for Widgets](https://doc.qt.io/qt-6/qtgraphswidgets-module.html) · [Qwt](https://qwt.sourceforge.io/).

## Полный прогон Summit D2

Нужны работающий Docker Desktop/Engine с Compose, несколько гигабайт свободного места под Docker, PowerShell 7 и frozen assets с путями из [README Backend](../README.md). На диске должен быть исходный подготовленный `data/prepared/summit-D1-pilot/D1.parquet` со всем днём. Replayer выбирает восьмичасовое окно 08:00–16:00 UTC из этого файла и проверяет его SHA256.

```powershell
$runPath = 'build/gui-demo-max'
pwsh -NoProfile -File gui/demo.ps1 -Action Init -RunDirectory $runPath -AssetsRoot 'C:/path/to/Summit-assets' -Speed 0

$demo = Get-Content "$runPath/demo.json" -Raw | ConvertFrom-Json
.\build\qt-gui\cluster-monitor.exe --master $demo.master --execution $demo.execution --summit-speed $demo.speed
```

После подключения запустите Replayer во втором терминале.

```powershell
pwsh -NoProfile -File gui/demo.ps1 -Action Replay -RunDirectory build/gui-demo-max
```

Init создаёт новый UUID и отдельный Compose-проект с собственными томами. Для естественного темпа используйте `-Speed 1`, для ускоренного `-Speed 5` или `-Speed 10`, каждый раз с новым каталогом запуска. При коллизии портов укажите `-MasterPort`, `-PostgresPort` и `-KafkaPort`. Запуск с прежним UUID после частичного воспроизведения запрещён сценарием, чтобы исключить повторную подачу телеметрии в то же исполнение.

Сценарий ждёт завершения Worker и доставки, затем независимо обходит страницы REST. Проверяется точная сетка из 46 080 групповых тиков и совпадение числа уникальных завершённых событий со сводкой. Если проверка REST прервалась после успешного Replayer, повторите только `-Action Verify`. Master и PostgreSQL продолжают работать после команды Replay. Откройте HISTORY для просмотра итогов. Когда демонстрация завершена, остановите сервисы с сохранением томов.

```powershell
pwsh -NoProfile -File gui/demo.ps1 -Action Stop -RunDirectory build/gui-demo-max
```

В `replay.json` измеряется длительность команды Replayer. `pipeline.json` фиксирует время от запуска команды до подтверждённой обработки и доставки. `verification.json` содержит эти измерения и результат проверки. GUI отдельно показывает метки телеметрии, системное UTC и фактический интервал приёма записей Master по данным PostgreSQL.

### Запись фрагмента для защиты

1. Откройте GUI перед запуском Replayer. Зафиксируйте подключение, UUID, FAST REPLAY/MAX и серые карточки до первого результата.
2. Покажите рост прогресса и появление красной группы. Откройте её карточку или строку текущего аномального состояния и покажите исходные и временные критерии, голоса и таблицу IF.
3. Вернитесь в потоковый режим, откройте журнал и покажите добавление завершённых событий.
4. Дождитесь завершения обработки и сверки. Покажите системный экран, итоговый счётчик и длительность из отчёта.
5. Выберите HISTORY, откройте событие и перейдите к его диагностическому интервалу.

Для видео на 60–90 секунд используйте фрагменты реального MAX-прогона. Если сокращаете запись, укажите фактическое время из отчёта отдельно от длительности видео.

### Интеграционная проверка клиента

Запустите тест против своего работающего Master перед Replayer. На уже завершённом запуске опустите CAD_GUI_TEST_REQUIRE_LIVE.

```powershell
$demo = Get-Content 'build/gui-demo-max/demo.json' -Raw | ConvertFrom-Json
$env:CAD_GUI_TEST_MASTER = $demo.master
$env:CAD_GUI_TEST_EXECUTION = $demo.execution
$env:CAD_GUI_TEST_REQUIRE_LIVE = '1'
$env:QTEST_FUNCTION_TIMEOUT = '960000'
.\build\qt-gui\cluster-client-test.exe realMaster
```

Интеграционный тест проверяет сетку каждой группы, число положительных и неопределённых тиков и число завершённых событий по сводке Master. С CAD_GUI_TEST_REQUIRE_LIVE он также проверяет положительное решение до конца прогона. Обычный CTest моделирует поздние публикации по обоим курсорам, разрыв HTTP и смену UUID, затем проверяет навигацию, историю и отображение диагностических состояний.
