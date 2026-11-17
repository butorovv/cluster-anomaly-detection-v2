# Проверка штатного experiment runner

Итоговый `PASS` сохраняется после сверки golden, проверки завершения процессов
и очистки. Replayer должен завершиться самостоятельно с кодом 0. Worker и
Master останавливаются по запросу runner с общим лимитом 20 секунд и отдельным
лимитом 5 секунд на ожидание принудительно остановленных процессов. Для
ненулевого кода штатной отмены runner сверяет роль, код, время запроса и весь
текст завершающей ошибки. В `kill_worker` и `restart_master` принудительный
выход учитывается только после успешной отправки kill и подтверждения exit.
На Windows для штатной доставки `CTRL_BREAK_EVENT` нужен общий консольный
сеанс runner и его дочерних процессов. [Microsoft Learn](https://learn.microsoft.com/en-us/windows/console/console-process-groups)

## Локальные регрессии

Команды работают без Kafka, PostgreSQL, ВМ и входных данных Summit.

```sh
go test -count=1 ./internal/app/experiment -run '^TestRunner' -v
go vet ./internal/app/experiment ./experiments/runner
git diff --check 64f1f84..HEAD
```

## Сборка с проверяемым манифестом

Из checkout с новым runner соберите bundle из `git archive` выбранного полного
коммита. Архив задаёт `source_commit` через глобальный PAX-заголовок. SHA-256
архива фиксирует его содержимое. Сборка выполняется внутри распакованного
снимка с `-buildvcs=false` и `GOWORK=off`. [Документация git archive](https://git-scm.com/docs/git-archive)

```powershell
$revision = (git rev-parse --verify HEAD).Trim()
$buildRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('cad-runner-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $buildRoot | Out-Null
$sourceArchive = Join-Path $buildRoot 'source.tar'
$bundle = Join-Path $buildRoot 'bundle'
git archive --format=tar --output=$sourceArchive $revision
if ($LASTEXITCODE -ne 0) { throw 'git archive failed' }
$sourceSHA = (Get-FileHash -Algorithm SHA256 -LiteralPath $sourceArchive).Hash.ToLowerInvariant()
$buildSHA = go run ./experiments/runner --build-source $sourceArchive --source-sha256 $sourceSHA --build-output $bundle
if ($LASTEXITCODE -ne 0) { throw 'runner build failed' }
$runner = Join-Path $bundle $(if ($IsWindows) { 'bin/runner.exe' } else { 'bin/runner' })
$runnerBuild = @('--build-manifest', (Join-Path $bundle 'build-manifest.json'), '--build-manifest-sha256', $buildSHA)
Get-Content -LiteralPath $runnerBuild[1]
```

При необходимости передайте сборщику `--source-overlay <каталог>` с обычными
файлами по путям относительно исходного дерева. В манифесте SHA-256 overlay
считается по отсортированному JSON-отображению `путь → SHA-256 содержимого`.
Сохраните bundle и напечатанный `$buildSHA`. При запуске runner повторно
сверяет манифест, архив, overlay и SHA Runner/Replayer/Worker/Master. Перед
каждым стартом дочернего процесса SHA его файла проверяется ещё раз.
Перед итоговым PASS runner повторно проверяет весь bundle. В отчёте
сохраняются идентификаторы сборки и хеши бинарников.

Для последующего прогона на изолированном стенде укажите новый `output` в
конфиге и вызовите `& $runner @runnerBuild --config <config.json>` в PowerShell.
`--resume` требует тот же проверенный bundle и подтверждённую остановку для
повторного принятия сохранённого PASS. Для исторических результатов используйте
`--audit-saved` или отдельный новый `output` для свежего прогона.
