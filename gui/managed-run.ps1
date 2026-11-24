# Runs one owned Summit demonstration and retains its verified REST pages.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Catalog,
    [Parameter(Mandatory)][string]$RunId,
    [Parameter(Mandatory)][string]$AssetsRoot,
    [ValidateRange(1024,65535)][int]$MasterPort = 18086
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
        WriteState 'error' 'Уже выполняется другой управляемый запуск.'
        exit 1
    }
    WriteState 'preparing'
    & $driver -Action Init -ExecutionId $id -RunDirectory $work -AssetsRoot $AssetsRoot `
        -MasterPort $MasterPort -PrivateDataPorts -ArchivePublication -Speed $job.speed -WindowStart $from -WindowEnd $to
    WriteState 'processing'
    & $driver -Action Replay -RunDirectory $work -SaveSnapshot
    WriteState 'stopping'
    & $driver -Action Stop -RunDirectory $work
    WriteState 'complete'
} catch {
    WriteState 'error' 'Сбой запуска. Подробности доступны в журнале запуска на ВМ.'
    Write-Error -ErrorAction Continue $_
    if ($null -ne $lock -and (Test-Path -LiteralPath (Join-Path $work 'demo.json'))) {
        try {
            & $driver -Action Stop -RunDirectory $work
        } catch {
            Write-Warning 'Не удалось остановить принадлежащее запуску окружение.'
        }
    }
    exit 1
} finally {
    if ($null -ne $lock) { $lock.Dispose() }
}
