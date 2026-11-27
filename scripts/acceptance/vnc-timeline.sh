#!/usr/bin/env bash
set -euo pipefail
umask 077
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
: "${CAD_ACCEPTANCE_OUTPUT:?Set a new output directory}"
: "${CAD_ACCEPTANCE_CATALOG:?Set the catalog served by the archive REST service}"
: "${CAD_ACCEPTANCE_ASSETS:?Set the Summit assets directory}"
: "${CAD_ACCEPTANCE_VNC_PROBE:?Set the path to cluster-archive-vnc-probe}"
: "${DISPLAY:?Use an available X11 or VNC display}"
s=$(realpath -m -- "$CAD_ACCEPTANCE_OUTPUT")
root=$(realpath -- "$CAD_ACCEPTANCE_CATALOG")
a=$(realpath -- "$CAD_ACCEPTANCE_ASSETS")
probe=$(realpath -- "$CAD_ACCEPTANCE_VNC_PROBE")
p=${CAD_ACCEPTANCE_PWSH:-pwsh}
url=${CAD_GUI_ARCHIVE_API:-http://127.0.0.1:18087}
mkdir -- "$s"
export XDG_CONFIG_HOME="$s/profile"
cd "$repo"
for speed in 5 10 0 1; do
 out="$s/picker-final-$speed"
 mkdir "$out"
 id=$(cat /proc/sys/kernel/random/uuid)
 r="$root/$id"
 mkdir "$r"
 end=08:15:30
 if [ "$speed" = 0 ]; then end=08:28:00; fi
 if [ "$speed" = 1 ]; then end=08:14:20; fi
 jq -n --arg id "$id" --arg speed "$speed" --arg to "2022-01-18T${end}Z" --arg created "$(date -u +%FT%TZ)" '{id:$id,name:("Timeline "+$speed),kind:"archive",speed:$speed,start:"2022-01-18T08:13:00Z",end:$to,created:$created}' > "$r/request.json"
 echo "$id" > "$out/execution.txt"
 CAD_GUI_FORCE_ARCHIVE_STREAM=0 "$p" -NoProfile -File gui/managed-run.ps1 -Catalog "$root" -RunId "$id" -AssetsRoot "$a" > "$out/runner.log" 2>&1 &
 play=$!
 phase=''
 for i in $(seq 1 600); do
  if [ -s "$r/state.json" ]; then phase=$(jq -r .phase "$r/state.json"); fi
  [ "$phase" != processing ] || break
  if [ "$phase" = error ] || ! kill -0 "$play" 2>/dev/null; then tail -24 "$out/runner.log"; exit 1; fi
  sleep 0.25
 done
 test "$phase" = processing
 set +e
 "$probe" "$root" "$id" timeline "$p" "$out" "$url" > "$out/qt-live.log" 2>&1
 live=$?
 set -e
 wait "$play"
 set +e
 "$probe" "$root" "$id" history "$p" "$out" "$url" > "$out/qt-history.log" 2>&1
 history=$?
 set -e
 printf 'speed=%s live=%s history=%s\n' "$speed" "$live" "$history" | tee "$out/outcome.txt"
 cat "$out/qt-live.log" "$out/qt-history.log"
 test "$live" = 0 && test "$history" = 0
 envdir="$r/environment"
 proj=$(jq -er .project "$envdir/demo.json")
 test "$proj" = "cad-gui-${id//-/}"
 test -z "$(docker ps -q --filter "label=com.docker.compose.project=$proj")"
 test -s "$envdir/verification.json"
 compose=(compose --env-file "$envdir/compose.env" -p "$proj" -f deploy/compose.yaml -f "$envdir/private-data-ports.yaml")
 docker "${compose[@]}" logs --no-color > "$out/retained-container.log" 2>&1
done
