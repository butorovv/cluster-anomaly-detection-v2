#requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidatePattern('^(?:[A-Za-z0-9_.-]+@)?[A-Za-z0-9][A-Za-z0-9_.-]*$')][string]$SshTarget,
    [string]$IdentityFile,
    [string]$Viewer,
    [ValidateRange(1,65535)][int]$SshPort = 22,
    [ValidateRange(1024,65535)][int]$LocalPort = 15911,
    [ValidateRange(1024,65535)][int]$RemotePort = 5911,
    [switch]$SetVncPassword
)
$ErrorActionPreference = 'Stop'
$sshPath = (Get-Command ssh.exe -CommandType Application).Source
if (!$Viewer) {
    $Viewer = (Get-Command vncviewer.exe -CommandType Application -ErrorAction SilentlyContinue).Source
}
if (!$Viewer) {
    $installed = Join-Path $env:ProgramFiles 'TigerVNC/vncviewer.exe'
    if (Test-Path -LiteralPath $installed -PathType Leaf) { $Viewer = $installed }
}
if (!$Viewer) {
    throw 'Укажите -Viewer с путём к TigerVNC Viewer: https://tigervnc.org/'
}
$Viewer = (Resolve-Path -LiteralPath $Viewer).ProviderPath
$sshOptions = @('-p', "$SshPort", '-o', 'StrictHostKeyChecking=yes',
    '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3',
    '-o', 'GatewayPorts=no', '-o', 'ForwardAgent=no', '-o', 'ForwardX11=no')
if ($IdentityFile) {
    $sshOptions += @('-o','IdentitiesOnly=yes','-i',(Resolve-Path -LiteralPath $IdentityFile).ProviderPath)
}
if ($SetVncPassword) {
    $setPassword = 'exec "$HOME/.config/cad-gui-vnc/set-password"'
    & $sshPath @sshOptions -tt $SshTarget $setPassword
    if ($LASTEXITCODE -ne 0) { throw 'Не удалось установить пароль VNC.' }
}

$options = [Diagnostics.ProcessStartInfo]::new($sshPath)
foreach ($arg in ($sshOptions + @('-o','BatchMode=yes','-o','ExitOnForwardFailure=yes',
        '-o','ConnectTimeout=15','-N','-T','-L',"127.0.0.1:${LocalPort}:127.0.0.1:$RemotePort",$SshTarget))) {
    $options.ArgumentList.Add($arg)
}
$tunnel = [Diagnostics.Process]::Start($options)
try {
    $timeout = [Diagnostics.Stopwatch]::StartNew()
    do {
        if ($tunnel.HasExited) { throw "SSH-туннель завершился с кодом $($tunnel.ExitCode)." }
        $listener = Get-NetTCPConnection -State Listen -LocalAddress '127.0.0.1' -LocalPort $LocalPort -OwningProcess $tunnel.Id -ErrorAction SilentlyContinue
        if ($listener) { break }
        Start-Sleep -Milliseconds 250
    } while ($timeout.Elapsed.TotalSeconds -lt 20)
    if (!$listener) { throw "SSH не открыл локальный порт $LocalPort." }

    $clientOptions = [Diagnostics.ProcessStartInfo]::new($Viewer)
    $clientOptions.ArgumentList.Add("127.0.0.1::$LocalPort")
    $client = [Diagnostics.Process]::Start($clientOptions)
    try { $client.WaitForExit() } finally { $client.Dispose() }
} finally {
    if (!$tunnel.HasExited) { $tunnel.Kill(); $tunnel.WaitForExit() }
    $tunnel.Dispose()
}
