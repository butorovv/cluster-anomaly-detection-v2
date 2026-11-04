param(
 [string]$EnvironmentFile='deploy/.env',
 [string]$Project='cad-demo',
 [int]$Workers=2,
 [string[]]$Modes=@('polling','http_push','kafka'),
 [int]$MasterPort=8080,
 [string]$Output='experiments/artifacts/demo'
)
$ErrorActionPreference='Stop'
if($Project -notmatch '^cad-demo[a-z0-9_-]*$' -or $Workers -lt 2 -or $Workers -gt 16){throw 'Use an isolated cad-demo* project and 2..16 Workers'}
if(Test-Path -LiteralPath $Output){throw 'New demo output directory required'}
New-Item -ItemType Directory -Path $Output -Force | Out-Null
$Output=(Resolve-Path -LiteralPath $Output).Path
& docker version --format '{{.Server.Version}}' *> (Join-Path $Output 'docker-version.log')
if($LASTEXITCODE -ne 0){@{status='NOT_RUN';reason='Docker engine unavailable';command='pwsh -File deploy/demo.ps1 -EnvironmentFile deploy/.env'}|ConvertTo-Json|Set-Content (Join-Path $Output 'status.json') -Encoding utf8NoBOM;Write-Output 'NOT_RUN: Docker engine unavailable';exit 0}
$base=Get-Content -LiteralPath $EnvironmentFile -Raw
function Invoke-Compose([string[]]$Arguments){
 & docker compose --env-file $script:envPath -p $Project -f deploy/compose.yaml @Arguments
 if($LASTEXITCODE -ne 0){throw "Compose operation failed: $($Arguments[0])"}
}
function Query([string]$SQL){$value=Invoke-Compose @('exec','-T','postgres','psql','-U','cad_admin','-d','cad','-At','-c',$SQL);return ($value -join "`n").Trim()}
$results=@()
foreach($mode in $Modes){
 if($mode -notin @('polling','http_push','kafka')){throw 'Invalid transport mode'}
 $execution=[guid]::NewGuid().ToString();$envPath=Join-Path $Output "$mode.env"
 # Runtime secrets remain in ignored local *.env files, never experimental JSON.
 $text=$base
 foreach($entry in @{EXECUTION_ID=$execution;OUTPUT_TRANSPORT=$mode;DELTA_SECONDS='30'}.GetEnumerator()){
  $text=[regex]::Replace($text,"(?m)^$($entry.Key)=.*$",'');$text+="`n$($entry.Key)=$($entry.Value)`n"
 }
 Set-Content -LiteralPath $envPath -Value $text -Encoding utf8NoBOM
 Invoke-Compose @('config','--quiet')
 Invoke-Compose @('stop','worker','master')
 Invoke-Compose @('up','-d','--build','--wait','--scale',"worker=$Workers",'master','worker')
 $cli=@('compose','--env-file',$envPath,'-p',$Project,'-f','deploy/compose.yaml','run','--rm','--no-deps','replayer')
 $start=@{FilePath='docker';ArgumentList=@($cli|ForEach-Object{if($_ -match '\s'){'"'+$_+'"'}else{$_}});PassThru=$true;RedirectStandardOutput=(Join-Path $Output "$mode-replayer.log");RedirectStandardError=(Join-Path $Output "$mode-replayer-errors.log")};if($IsWindows){$start.WindowStyle='Hidden'}
 $replay=Start-Process @start
 $deadline=[datetime]::UtcNow.AddMinutes(3);$open=''
 while([datetime]::UtcNow -lt $deadline){
  $open=Query "SELECT o.owner_id::text || '|' || c.group_id || '|' || (convert_from(c.snapshot,'UTF8')::jsonb#>>'{journal,open,id}') || '|' || o.epoch::text FROM cad_v2.group_checkpoints c JOIN cad_v2.partition_ownership o USING(execution_id,group_id) WHERE c.execution_id='$execution'::uuid AND convert_from(c.snapshot,'UTF8')::jsonb#>>'{journal,open,id}' IS NOT NULL LIMIT 1"
  if($open){break};if($replay.HasExited){throw 'Replay exited before an open event was observed'};Start-Sleep -Milliseconds 100
 }
 if(!$open){throw 'No durable open parent observed'}
 $owner,$group,$event,$epoch=$open.Split('|');$victim=''
 foreach($container in @(Invoke-Compose @('ps','-q','worker'))){
  $raw=& docker exec $container wget -q -O - http://127.0.0.1:8081/metrics
  if($LASTEXITCODE -eq 0 -and ($raw|ConvertFrom-Json).labels.owner_id -eq $owner){$victim=$container;break}
 }
 if(!$victim){throw 'Open parent owner container not found'}
 $before=Query "SELECT group_id || ':' || owner_id::text || ':' || epoch::text FROM cad_v2.partition_ownership WHERE execution_id='$execution'::uuid AND owner_id IS NOT NULL ORDER BY group_id"
 $killed=[datetime]::UtcNow;& docker kill $victim;if($LASTEXITCODE -ne 0){throw 'Worker kill failed'}
 Invoke-Compose @('up','-d','--no-deps','--scale',"worker=$Workers",'worker')
 $resumed='';$deadline=[datetime]::UtcNow.AddMinutes(3)
 while([datetime]::UtcNow -lt $deadline){$resumed=Query "SELECT owner_id::text || '|' || epoch::text FROM cad_v2.partition_ownership WHERE execution_id='$execution'::uuid AND group_id='$group' AND owner_id <> '$owner'::uuid AND epoch>$epoch";if($resumed){break};Start-Sleep -Milliseconds 200}
 if(!$resumed){throw 'Reassignment/restore not observed'}
 $recoverySeconds=([datetime]::UtcNow-$killed).TotalSeconds
 if(!$replay.WaitForExit(1200000)){throw 'Replay timeout; inspect logs before retrying'}
 if($replay.ExitCode -ne 0){throw 'Replayer failed'}
 $deadline=[datetime]::UtcNow.AddMinutes(5);$summary=$null
 while([datetime]::UtcNow -lt $deadline){
  $summary=Invoke-RestMethod "http://127.0.0.1:$MasterPort/v1/summary?execution_id=$execution"
  $remaining=Query "SELECT (SELECT count(*) FROM cad_v2.outbox_messages WHERE execution_id='$execution'::uuid AND delivered_at IS NULL)+(SELECT count(*) FROM cad_v2.worker_tick_fragments WHERE execution_id='$execution'::uuid AND delivered_at IS NULL)"
  if($summary.complete -and $remaining -eq '0'){break};Start-Sleep -Milliseconds 500
 }
 $preserved=Query "SELECT count(*) FROM cad_v2.master_events WHERE execution_id='$execution'::uuid AND event_id='$event' AND terminal"
 if(!$summary.complete -or $remaining -ne '0' -or $preserved -ne '1'){throw 'Incomplete durable result or lost EventID'}
 $terminalJSON=Query "SELECT coalesce(jsonb_agg(payload ORDER BY event_id),'[]'::jsonb)::text FROM cad_v2.master_events WHERE execution_id='$execution'::uuid AND terminal"
 $terminalSHA=[Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($terminalJSON))).ToLowerInvariant()
 if($results.Count -gt 0 -and $results[0].terminal_payload_sha256 -ne $terminalSHA){throw 'Cross-transport terminal payload/ID parity failed'}
 $results+=@{terminal_payload_sha256=$terminalSHA;status='PASS';execution_id=$execution;mode=$mode;workers=$Workers;captured_event_id=$event;group=$group;old_owner=$owner;old_epoch=$epoch;new_owner_epoch=$resumed;kill_to_new_owner_seconds=$recoverySeconds;initial_assignment=$before;summary=$summary;event_id_preserved=$true}
 $results|ConvertTo-Json -Depth 8|Set-Content (Join-Path $Output 'results.json') -Encoding utf8NoBOM
 Invoke-Compose @('logs','--no-color')|Set-Content (Join-Path $Output "$mode-system.log") -Encoding utf8NoBOM
}
# Keep the isolated services and volumes for inspection. No down -v / prune.
Write-Output (Join-Path $Output 'results.json')
