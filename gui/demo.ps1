# Requires PowerShell 7 and Docker Compose.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateSet('Init','Replay','Verify','Stop')][string]$Action,
    [Parameter(Mandatory)][string]$RunDirectory,
    [string]$AssetsRoot,
    [guid]$ExecutionId = [guid]::Empty,
    [ValidateSet('1','5','10','0')][string]$Speed = '0',
    [ValidateRange(1,65535)][int]$MasterPort = 18080,
    [ValidateRange(1,65535)][int]$PostgresPort = 55482,
    [ValidateRange(1,65535)][int]$KafkaPort = 29182,
    [ValidateRange(1,16)][int]$Workers = 4,
    [ValidateRange(1,7200)][int]$DrainTimeoutSeconds = 900,
    [string]$WindowStart = '2022-01-18T08:00:00Z',
    [string]$WindowEnd = '2022-01-18T16:00:00Z',
    [switch]$PrivateDataPorts,
    [switch]$SaveSnapshot
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$runPath = [IO.Path]::GetFullPath($RunDirectory)
$metadata = Join-Path $runPath 'demo.json'
$envPath = Join-Path $runPath 'compose.env'
$composePath = Join-Path $root 'deploy/compose.yaml'

function Compose([string[]]$Arguments) {
    $files = @('-f', $composePath)
    $override = Join-Path $runPath 'private-data-ports.yaml'
    if (Test-Path -LiteralPath $override) { $files += @('-f', $override) }
    $restore = @{}
    foreach ($line in Get-Content -LiteralPath $envPath) {
        if ($line -match '^([A-Z_][A-Z0-9_]*)=') {
            $name = $Matches[1]
            $restore[$name] = [Environment]::GetEnvironmentVariable($name)
            [Environment]::SetEnvironmentVariable($name, $null)
        }
    }
    try {
        & docker compose --env-file $envPath --project-name $run.project @files @Arguments
        if ($LASTEXITCODE -ne 0) { throw "Docker Compose завершился с кодом $LASTEXITCODE" }
    } finally {
        foreach ($entry in $restore.GetEnumerator()) {
            [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value)
        }
    }
}
function Summary {
    Invoke-RestMethod "$($run.master)/v1/summary?execution_id=$($run.execution)" -TimeoutSec 16
}
function SaveJson($Value, [string]$Name) {
    $Value | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $runPath $Name) -Encoding utf8NoBOM
}

if ($Action -eq 'Init') {
    if (Test-Path -LiteralPath $runPath) { throw 'Укажите новый каталог демонстрации.' }
    if (!$AssetsRoot) { throw 'Укажите -AssetsRoot с исходным набором Summit.' }
    $start = [datetimeoffset]::ParseExact($WindowStart, "yyyy-MM-dd'T'HH:mm:ss'Z'", [cultureinfo]::InvariantCulture, 'AssumeUniversal')
    $end = [datetimeoffset]::ParseExact($WindowEnd, "yyyy-MM-dd'T'HH:mm:ss'Z'", [cultureinfo]::InvariantCulture, 'AssumeUniversal')
    if ($start -lt [datetimeoffset]'2022-01-18T08:00:00Z' -or $end -gt [datetimeoffset]'2022-01-18T16:00:00Z' -or
        $start -ge $end -or $start.Second % 10 -ne 0 -or $end.Second % 10 -ne 0) {
        throw 'Выберите непустое окно на 10-секундной сетке Summit D2.'
    }
    $assets = (Resolve-Path -LiteralPath $AssetsRoot).Path.Replace('\','/')
    if ($assets -match "['\r\n]") { throw 'Путь к assets содержит неподдерживаемый символ.' }
    $file = Join-Path $assets 'data/prepared/summit-D1-pilot/D1.parquet'
    $expected = 'cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7'
    if ((Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash -ne $expected) {
        throw 'SHA256 исходного Summit Parquet не совпадает с зарегистрированным.'
    }
    $execution = if ($ExecutionId -eq [guid]::Empty) { [guid]::NewGuid().ToString() } else { $ExecutionId.ToString() }
    $project = "cad-gui-$($execution.Replace('-',''))"
    $run = [ordered]@{
        project = $project; execution = $execution; speed = $Speed
        master = "http://127.0.0.1:$MasterPort"
        start = $WindowStart; end = $WindowEnd; expectedTicks = [int](($end - $start).TotalSeconds / 10 * 16)
        workers = $Workers
    }
    $null = New-Item -ItemType Directory -Path $runPath
    SaveJson $run 'demo.json'
    $lines = @(
        "ASSETS_ROOT='$assets'", "EXECUTION_ID=$execution", "CAD_IMAGE=$($project):local",
        "POSTGRES_PASSWORD=$([guid]::NewGuid().ToString('N'))",
        "WORKER_DB_PASSWORD=$([guid]::NewGuid().ToString('N'))",
        "MASTER_DB_PASSWORD=$([guid]::NewGuid().ToString('N'))",
        "MASTER_PORT=$MasterPort", "PG_PORT=$PostgresPort", "KAFKA_PORT=$KafkaPort",
        "RUN_START=$($run.start)", "RUN_END=$($run.end)", "REPLAY_SPEED=$Speed",
        "INPUT_SHA256=$expected", 'PROFILE_SHA256=a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36',
        'OUTPUT_TRANSPORT=http_push', 'GROUPS_COUNT=16', 'DELTA_SECONDS=0',
        'CHECKPOINT_INTERVAL_TICKS=3', 'MEASURE=false'
    )
    Set-Content -LiteralPath $envPath -Value $lines -Encoding utf8NoBOM
    if ($PrivateDataPorts) {
        $networkOverride = @'
services:
  postgres:
    ports: !reset []
  kafka:
    ports: !reset []
'@
        if ($IsLinux) {
            $uid = & id -u
            $gid = & id -g
            if ($uid -notmatch '^[1-9][0-9]*$' -or $gid -notmatch '^[1-9][0-9]*$') {
                throw 'Запускайте закрытую Linux-демонстрацию под обычным владельцем входных файлов.'
            }
            # Match the owner of private bind-mounted assets without changing their permissions.
            foreach ($service in 'bootstrap','worker','replayer') {
                $networkOverride += "`n  ${service}:`n    user: '${uid}:${gid}'"
            }
        }
        $networkOverride | Set-Content -LiteralPath (Join-Path $runPath 'private-data-ports.yaml') -Encoding utf8NoBOM
    }
    Compose @('build','bootstrap')
    Compose @('up','-d','--no-build','--wait','--scale',"worker=$Workers",'master','worker')
    Write-Host "Master $($run.master)"
    Write-Host "Execution ID $execution"
    Write-Host "Аргументы GUI --master $($run.master) --execution $execution --summit-speed $Speed"
    return
}

$run = Get-Content -LiteralPath $metadata -Raw | ConvertFrom-Json
$execution = [guid]::ParseExact($run.execution, 'D').ToString()
if ($run.project -ne "cad-gui-$($execution.Replace('-',''))") {
    throw 'Метаданные не соответствуют выделенному проекту демонстрации.'
}
if ($Action -eq 'Stop') {
    Compose @('stop')
    return
}
if ($Action -eq 'Replay') {
    if ((Summary).group_ticks -ne 0) { throw 'Выберите новый execution_id через Init перед повторным Replay.' }
    $statusFile = Join-Path $runPath 'replay-start.json'
    $lock = [IO.File]::Open($statusFile, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write)
    $lock.Dispose()
    $started = [datetimeoffset]::UtcNow
    SaveJson @{ startedUtc = $started } 'replay-start.json'
    $elapsed = [Diagnostics.Stopwatch]::StartNew()
    Compose @('run','--rm','--no-deps','replayer')
    SaveJson @{
        startedUtc = $started; finishedUtc = [datetimeoffset]::UtcNow
        replaySeconds = $elapsed.Elapsed.TotalSeconds
    } 'replay.json'
}
if (!(Test-Path -LiteralPath (Join-Path $runPath 'replay.json'))) {
    throw 'Завершение Replayer не зафиксировано в этом каталоге.'
}

# Read-only drain check for the dedicated demo DB. The application uses HTTP exclusively.
$query = @"
SELECT (SELECT count(*) FROM cad_v2.group_checkpoints
        WHERE execution_id='$($run.execution)'::uuid AND finalized),
       (SELECT count(*) FROM cad_v2.outbox_messages WHERE execution_id='$($run.execution)'::uuid AND delivered_at IS NULL)
       + (SELECT count(*) FROM cad_v2.worker_tick_fragments WHERE execution_id='$($run.execution)'::uuid AND delivered_at IS NULL);
"@
$drain = [Diagnostics.Stopwatch]::StartNew()
$summary = $null
do {
    $pending = Compose @('exec','-T','postgres','psql','-U','cad_admin','-d','cad','-At','-v','ON_ERROR_STOP=1','-c',$query)
    if ($pending -eq '16|0') {
        try {
            $summary = Summary
            if ($summary.complete) { break }
        } catch {
            Write-Warning 'Временный сбой получения сводки Master. Сверка будет повторена.'
        }
    }
    Start-Sleep -Seconds 1
} while ($drain.Elapsed.TotalSeconds -lt $DrainTimeoutSeconds)
if (!$summary.complete -or $pending -ne '16|0') { throw 'Master или Worker не завершили обработку.' }
$completion = Join-Path $runPath 'pipeline.json'
if ($Action -eq 'Replay') {
    SaveJson @{
        drainedUtc = [datetimeoffset]::UtcNow
        wallSeconds = $elapsed.Elapsed.TotalSeconds
    } 'pipeline.json'
}

# After drain, compare an independent full API traversal with the fixed Summit grid.
$snapshot = Join-Path $runPath 'snapshot'
$resultPages = 0
$eventPages = 0
if ($SaveSnapshot) {
    $null = New-Item -ItemType Directory -Path $snapshot
}
$tick = [datetimeoffset]::Parse($run.start)
$group = 1
$results = 0
$after = ''
while ($true) {
    $uri = "$($run.master)/v1/results?execution_id=$($run.execution)&limit=128$after"
    if ($SaveSnapshot) {
        $response = (Invoke-WebRequest $uri -TimeoutSec 20).Content
        $page = ConvertFrom-Json -InputObject $response
    } else {
        $page = Invoke-RestMethod $uri -TimeoutSec 20
    }
    if ($page.items.Count -eq 0) { break }
    foreach ($item in $page.items) {
        if ($item.raw.group -ne ('a{0:00}' -f $group) -or [datetimeoffset]$item.raw.timestamp -ne $tick) {
            throw 'Нарушена непрерывность сетки выбранного окна Summit D2.'
        }
        $results++
        if ($group -eq 16) { $group = 1; $tick = $tick.AddSeconds(10) } else { $group++ }
    }
    if ($SaveSnapshot) {
        Set-Content -Value $response -LiteralPath (Join-Path $snapshot "results-$resultPages.json") -Encoding utf8NoBOM
        $resultPages++
    }
    $next = '&after_tick=' + [uri]::EscapeDataString(([datetimeoffset]$page.next.tick).UtcDateTime.ToString('yyyy-MM-ddTHH:mm:ssZ')) + '&after_group=' + [uri]::EscapeDataString($page.next.group_id)
    if ($next -eq $after) { throw 'Курсор результатов не продвинулся.' }
    $after = $next
}
$events = [Collections.Generic.HashSet[string]]::new()
$after = ''
while ($true) {
    $uri = "$($run.master)/v1/events?execution_id=$($run.execution)&limit=100&after=$after"
    if ($SaveSnapshot) {
        $response = (Invoke-WebRequest $uri -TimeoutSec 20).Content
        $page = ConvertFrom-Json -InputObject $response -NoEnumerate
    } else {
        $page = Invoke-RestMethod $uri -TimeoutSec 20
    }
    if ($page.Count -eq 0) { break }
    foreach ($event in $page) {
        if ([string]::CompareOrdinal($event.id, $after) -le 0 -or !$events.Add($event.id)) {
            throw 'Некорректный порядок ID или повтор события.'
        }
        $after = $event.id
    }
    if ($SaveSnapshot) {
        Set-Content -Value $response -LiteralPath (Join-Path $snapshot "events-$eventPages.json") -Encoding utf8NoBOM
        $eventPages++
    }
}
$summary = Summary
if ($results -ne $run.expectedTicks -or $results -ne $summary.group_ticks -or $events.Count -ne $summary.parent_events) {
    throw 'Содержимое REST-страниц расходится со сводкой.'
}
$report = [ordered]@{
    observedAtUtc = [datetimeoffset]::UtcNow; groupTicks = $results; terminalEvents = $events.Count
    replay = (Get-Content -LiteralPath (Join-Path $runPath 'replay.json') -Raw | ConvertFrom-Json)
    summary = $summary
}
if (Test-Path -LiteralPath $completion) {
    $report.pipeline = Get-Content -LiteralPath $completion -Raw | ConvertFrom-Json
}
if ($SaveSnapshot) {
    $report.snapshot = @{ resultPages = $resultPages; eventPages = $eventPages }
}
SaveJson $report 'verification.json'
$report | ConvertTo-Json -Depth 12
