# Runs one owned Summit demonstration and retains its verified REST pages.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Catalog,
    [Parameter(Mandatory)][string]$RunId,
    [Parameter(Mandatory)][string]$AssetsRoot,
    [ValidateRange(1024,65535)][int]$MasterPort = 18086,
    [switch]$ResumeArchive
)
$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath($Catalog)
$id = [guid]::ParseExact($RunId, 'D').ToString()
$entry = Join-Path $root $id
$metadata = Join-Path $entry 'request.json'
$status = Join-Path $entry 'state.json'
$work = Join-Path $entry 'environment'
$driver = Join-Path $PSScriptRoot 'demo.ps1'

function WriteState([string]$Phase, [string]$Message = '') {
    $payload = @{phase = $Phase; pid = $PID; message = $Message; updatedUtc = [datetimeoffset]::UtcNow}
    $tmp = "$status.tmp"
    [IO.File]::WriteAllText($tmp, ($payload | ConvertTo-Json -Compress), [Text.UTF8Encoding]::new($false))
    [IO.File]::Move($tmp, $status, $true)
}

$lock = $null
$archiver = $null
$archiveMode = 'reuse'
$retained = $false
function NewArchiveProcess([string[]]$Arguments, [bool]$Capture = $false) {
    $info = [Diagnostics.ProcessStartInfo]::new($env:CAD_GUI_ARCHIVE_BINARY)
    $info.UseShellExecute = $false
    $info.RedirectStandardOutput = $Capture
    $info.Environment['ARCHIVE_DATABASE_URL'] = $env:CAD_GUI_STREAM_DATABASE_URL
    foreach ($arg in $Arguments) { $info.ArgumentList.Add($arg) }
    [Diagnostics.Process]::Start($info)
}
try {
    $job = Get-Content -LiteralPath $metadata -Raw | ConvertFrom-Json
    if ($job.id -cne $id -or $job.name -isnot [string] -or $job.name.Trim().Length -notin 1..120 -or
        $job.kind -notin @('online','archive') -or $job.speed -notin @('0','1','5','10') -or
        ($job.kind -eq 'online' -and $job.speed -ne '1')) {
        throw 'Некорректный запрос управляемого запуска.'
    }
    $from = ([datetimeoffset]$job.start).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    $to = ([datetimeoffset]$job.end).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    # The kernel releases this lock when the supervisor exits, even if the GUI is closed.
    try {
        $lock = [IO.File]::Open((Join-Path $root 'active.lock'), 'OpenOrCreate', 'ReadWrite', 'None')
    } catch {
        if (!$ResumeArchive) { WriteState 'error' 'Уже выполняется другой управляемый запуск.' }
        exit 1
    }
    if ($ResumeArchive) {
        $previous = Get-Content -LiteralPath $status -Raw | ConvertFrom-Json
        $existing = Get-Content -LiteralPath (Join-Path $work 'demo.json') -Raw | ConvertFrom-Json
        if ($previous.phase -ne 'incomplete' -or $existing.archiveMode -ne 'stream' -or
            $existing.execution -ne $id -or $existing.start -ne $from -or $existing.end -ne $to) {
            throw 'Возобновление доступно только для сохранённого неполного потокового запуска.'
        }
        $retained = $true
        $archiveMode = 'stream'
    } else {
        WriteState 'preparing'
        $archiveUrl = if ($env:CAD_GUI_ARCHIVE_API) { $env:CAD_GUI_ARCHIVE_API } else { 'http://127.0.0.1:18087' }
        $archiveUri = [uri]$archiveUrl
        if (!$archiveUri.IsLoopback -or $archiveUri.Scheme -ne 'http' -or $archiveUri.AbsolutePath -ne '/') {
            throw 'Недопустимый локальный адрес сервиса архива.'
        }
        $coverage = Invoke-RestMethod -TimeoutSec 5 -Uri (
            $archiveUrl.TrimEnd('/') + '/v1/telemetry/availability?from=' +
            [uri]::EscapeDataString($from) + '&to=' + [uri]::EscapeDataString($to))
        if ($coverage.mode -notin @('reuse','stream')) { throw 'Не удалось подтвердить режим архива.' }
        $archiveMode = if ($env:CAD_GUI_FORCE_ARCHIVE_STREAM -eq '1') { 'stream' } else { $coverage.mode }
    }
    if ($archiveMode -eq 'stream' -and (
        !$env:CAD_GUI_ARCHIVE_BINARY -or !$env:CAD_GUI_STREAM_DATABASE_URL -or
        ![IO.Path]::IsPathFullyQualified($env:CAD_GUI_ARCHIVE_BINARY) -or
        [IO.Path]::GetFileName($env:CAD_GUI_ARCHIVE_BINARY) -ne 'telemetry-archive' -or
        !(Test-Path -LiteralPath $env:CAD_GUI_ARCHIVE_BINARY -PathType Leaf))) {
        throw 'Потоковый архив не настроен.'
    }
    if (!$ResumeArchive) {
        & $driver -Action Init -ExecutionId $id -RunDirectory $work -AssetsRoot $AssetsRoot `
            -MasterPort $MasterPort -KafkaPort 29186 -PrivateDataPorts -ArchivePublication -ArchiveMode $archiveMode `
            -Speed $job.speed -WindowStart $from -WindowEnd $to
    }
    if ($archiveMode -eq 'stream') {
        $retained = $true
        $source = Get-Content -LiteralPath (Join-Path $work 'demo.json') -Raw | ConvertFrom-Json
        $consume = @('consume','--execution',$id,'--source',$source.inputSha256,
            '--profile',$source.profileSha256,'--from',$from,'--to',$to)
        $archiver = NewArchiveProcess $consume
    }
    if (!$ResumeArchive) {
        WriteState 'processing'
        & $driver -Action Replay -RunDirectory $work -SaveSnapshot
    }
    if ($archiveMode -eq 'stream') {
        WriteState 'archiving'
        $deadline = [Diagnostics.Stopwatch]::StartNew()
        $finished = $archiver.WaitForExit(90000)
        if ($finished -and $archiver.ExitCode -ne 0 -and $deadline.ElapsedMilliseconds -lt 88000) {
            Write-Warning 'Архиватор завершился с ошибкой. Повтор с позиции PostgreSQL.'
            $archiver = NewArchiveProcess $consume
            $finished = $archiver.WaitForExit([int][math]::Max(0, 90000 - $deadline.ElapsedMilliseconds))
        }
        if (!$finished -or $archiver.ExitCode -ne 0) {
            throw 'Диагностика сохранена, но архив не подтвердил завершение.'
        }
        $probe = NewArchiveProcess @('stream-progress','--execution',$id,'--to',$to) $true
        $progress = $probe.StandardOutput.ReadToEnd() | ConvertFrom-Json
        $probe.WaitForExit()
        if ($probe.ExitCode -ne 0 -or !$progress.complete) { throw 'Архивное покрытие не подтверждено.' }
        if ($ResumeArchive) {
            $report = Get-Content -LiteralPath (Join-Path $work 'verification.json') -Raw | ConvertFrom-Json
            if (!$report.summary.complete -or $report.snapshot.partial) { & $driver -Action Verify -RunDirectory $work -SaveSnapshot }
        }
    }
    $retained = $false
    WriteState 'stopping'
    & $driver -Action Stop -RunDirectory $work
    WriteState 'complete'
} catch {
    if ($retained) {
        WriteState 'incomplete' 'Архив неполон. Окружение сохранено для возобновления.'
    } elseif (!$ResumeArchive) {
        WriteState 'error' 'Сбой запуска. Подробности доступны в журнале запуска на ВМ.'
    }
    Write-Error -ErrorAction Continue $_
    if ($null -ne $archiver -and !$archiver.HasExited) { $archiver.Kill(); $archiver.WaitForExit() }
    if ($null -ne $lock -and (!$ResumeArchive -or $retained) -and (Test-Path -LiteralPath (Join-Path $work 'demo.json'))) {
        try {
            & $driver -Action CapturePartial -RunDirectory $work -SaveSnapshot
        } catch {
            Write-Warning 'Чтение неполного диагностического снимка недоступно.'
        }
        if (!$retained) {
            try {
                & $driver -Action Stop -RunDirectory $work
            } catch {
                Write-Warning 'Не удалось остановить принадлежащее запуску окружение.'
            }
        }
    }
    exit 1
} finally {
    if ($null -ne $lock) { $lock.Dispose() }
}
