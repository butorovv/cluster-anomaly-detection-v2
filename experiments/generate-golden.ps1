param(
 [Parameter(Mandatory=$true)][string]$V1Repository,
 [Parameter(Mandatory=$true)][string]$Assets,
 [Parameter(Mandatory=$true)][string]$Output,
 [Parameter(Mandatory=$true)][string]$Manifest
)
$ErrorActionPreference='Stop'
$pin='c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc'
$profile='a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36'
$prepared='cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7'
$Assets=(Resolve-Path -LiteralPath $Assets).Path
$V1Repository=(Resolve-Path -LiteralPath $V1Repository).Path
$Output=[IO.Path]::GetFullPath($Output)
$Manifest=[IO.Path]::GetFullPath($Manifest)
if((Test-Path -LiteralPath $Output) -or (Test-Path -LiteralPath $Manifest)){throw 'New output directory and manifest required; existing artifacts are never overwritten'}
if((Get-FileHash -LiteralPath (Join-Path $Assets 'data/prepared/summit-D1-pilot/D1.parquet') -Algorithm SHA256).Hash.ToLowerInvariant() -ne $prepared){throw 'Prepared input SHA mismatch'}
if((Get-FileHash -LiteralPath (Join-Path $Assets 'configs/group-profile.summit-v1.json') -Algorithm SHA256).Hash.ToLowerInvariant() -ne $profile){throw 'Frozen profile SHA mismatch'}
New-Item -ItemType Directory -Path $Output | Out-Null
$checkout=Join-Path $Output 'source'
& git -c core.autocrlf=false clone --shared --no-checkout -- $V1Repository $checkout
if($LASTEXITCODE -ne 0){throw 'V1 clone failed'}
& git -C $checkout -c core.autocrlf=false checkout --detach $pin
if($LASTEXITCODE -ne 0){throw 'Exact V1 pin unavailable'}
$suffix=if($IsWindows){'.exe'}else{''}
$binary=Join-Path $Output ('v1-rbi2'+$suffix)
Push-Location -LiteralPath $checkout
try {
 & go build -mod=readonly -o $binary ./cmd/rbi2
 if($LASTEXITCODE -ne 0){throw 'Pinned V1 build failed'}
} finally { Pop-Location }
$binarySHA=(Get-FileHash -LiteralPath $binary -Algorithm SHA256).Hash.ToLowerInvariant()
$results=@()
foreach($groups in @(4,8,16)){
 foreach($delta in @(0,10,30,60)){
  $target=Join-Path $Output "g$groups-d$delta"
  $arguments=@('--assets',$Assets,'--profile-sha256',$profile,'--window','D2','--groups',"$groups",'--delta',"$delta",'--output',$target)
  $timer=[Diagnostics.Stopwatch]::StartNew()
  & $binary @arguments
  if($LASTEXITCODE -ne 0){throw "Pinned V1 generation failed g$groups-d$delta"}
  $timer.Stop()
  $files=@(Get-ChildItem -LiteralPath $target -File | Sort-Object Name | ForEach-Object { @{path=$_.FullName;bytes=$_.Length;sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()} })
  $results+=@{groups=$groups;delta=$delta;source_commit=$pin;command='pinned V1 cmd/rbi2; exact arguments recorded in args';args=$arguments;elapsed_seconds=$timer.Elapsed.TotalSeconds;files=$files;binary=$binary;binary_sha256=$binarySHA;build_workdir=$checkout}
  ConvertTo-Json -InputObject $results -Depth 10 | Set-Content -LiteralPath $Manifest -Encoding utf8NoBOM
 }
}
$dirty=& git -C $checkout status --porcelain
if($LASTEXITCODE -ne 0 -or $dirty){throw 'Pinned V1 checkout was modified during generation'}
