#!/usr/bin/env bash
set -euo pipefail
umask 077
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
: "${CAD_ACCEPTANCE_OUTPUT:?Set a new output directory}"
: "${CAD_ACCEPTANCE_ASSETS:?Set the Summit assets directory}"
: "${CAD_ACCEPTANCE_ARCHIVE_BINARY:?Set the telemetry-archive executable path}"
: "${CAD_ACCEPTANCE_ARCHIVE_DSN_FILE:?Set a private DSN file for an isolated test PostgreSQL}"
: "${CAD_ACCEPTANCE_POSTGRES_CONTAINER:?Set the isolated archive PostgreSQL container name}"
: "${CAD_ACCEPTANCE_DISK:?Set the block device name in /proc/diskstats}"
out=$(realpath -m -- "$CAD_ACCEPTANCE_OUTPUT")
a=$(realpath -- "$CAD_ACCEPTANCE_ASSETS")
bin=$(realpath -- "$CAD_ACCEPTANCE_ARCHIVE_BINARY")
p=${CAD_ACCEPTANCE_PWSH:-pwsh}
pg_container=$CAD_ACCEPTANCE_POSTGRES_CONTAINER
pg_user=${CAD_ACCEPTANCE_POSTGRES_USER:-archive_owner}
master_port=${CAD_ACCEPTANCE_MASTER_PORT:-18238}
kafka_port=${CAD_ACCEPTANCE_KAFKA_PORT:-29238}
for value in "$master_port" "$kafka_port"; do
 if ! [[ "$value" =~ ^[0-9]{4,5}$ ]] || (( 10#$value < 1024 || 10#$value > 65535 )); then
  echo 'The diagnostic ports must be between 1024 and 65535.' >&2
  exit 2
 fi
done
[[ "$pg_user" =~ ^[a-z_][a-z0-9_]*$ ]]
mkdir -- "$out"
cd "$repo"
prefix="cad_pair_$(tr -d '-' </proc/sys/kernel/random/uuid | cut -c1-16)"
pg() { docker exec "$pg_container" psql -X -v ON_ERROR_STOP=1 -U "$pg_user" -d "$1" -Atc "$2"; }
snapshot() { pg "$1" "SELECT xact_commit,xact_rollback,blks_read,blks_hit,tup_inserted,temp_bytes FROM pg_stat_database WHERE datname=current_database(); SELECT wal_bytes FROM pg_stat_wal;"; cat /proc/diskstats; }
for entry in 1:off 1:on 2:on 2:off 3:off 3:on; do
 test -z "$(ss -H -ltn "( sport = :${master_port} or sport = :${kafka_port} )")"
 pair=${entry%:*}
 mode=${entry#*:}
 dir="$out/$pair-$mode"
 mkdir "$dir"
 db="${prefix}_$pair"
 if [ "$mode" = on ]; then
  pg postgres "CREATE DATABASE $db OWNER $pg_user"
  dsn=$(python3 -c 'import sys,urllib.parse; x=urllib.parse.urlsplit(open(sys.argv[1]).read().strip()); print(urllib.parse.urlunsplit(x._replace(path="/"+sys.argv[2])))' "$CAD_ACCEPTANCE_ARCHIVE_DSN_FILE" "$db")
  ARCHIVE_DATABASE_URL="$dsn" "$bin" init-stream
  test "$(pg "$db" 'SELECT count(*) FROM project.telemetry')" = 0
 fi
 id=$(cat /proc/sys/kernel/random/uuid)
 run="$dir/env"
 proj="cad-gui-${id//-/}"
 "$p" -NoProfile -File gui/demo.ps1 -Action Init -RunDirectory "$run" -AssetsRoot "$a" -ExecutionId "$id" -MasterPort "$master_port" -KafkaPort "$kafka_port" -ArchiveMode stream -PrivateDataPorts -WindowStart 2022-01-18T08:02:20Z -WindowEnd 2022-01-18T08:03:20Z > "$dir/run.log" 2>&1
 active_db=$db
 [ "$mode" = on ] || active_db=postgres
 snapshot "$active_db" > "$dir/before.txt"
 if [ "$mode" = on ]; then
  profile=$(jq -r .profileSha256 "$run/demo.json")
  ARCHIVE_DATABASE_URL="$dsn" /usr/bin/time -f '%e %U %S %M' -o "$dir/archive-time.txt" "$bin" consume --brokers "127.0.0.1:$kafka_port" --execution "$id" --profile "$profile" --from 2022-01-18T08:02:20Z --to 2022-01-18T08:03:20Z > "$dir/archive.log" 2>&1 & archive=$!
 fi
 date -u +%FT%T.%NZ > "$dir/begin.txt"
 "$p" -NoProfile -File gui/demo.ps1 -Action Replay -RunDirectory "$run" -SaveSnapshot >> "$dir/run.log" 2>&1 & replay=$!
 while kill -0 "$replay" 2>/dev/null; do
  stamp=$(date -u +%FT%T.%NZ)
  docker stats --no-stream --format '{{json .}}' $(docker ps -q --filter "label=com.docker.compose.project=$proj") "$pg_container" | jq -c --arg t "$stamp" '.+{sample:$t}' >> "$dir/docker-stats.jsonl" || true
 done
 wait "$replay"
 if [ "$mode" = on ]; then wait "$archive"; fi
 date -u +%FT%T.%NZ > "$dir/finalized.txt"
 snapshot "$active_db" > "$dir/after.txt"
 if [ "$mode" = on ]; then
  pg "$db" "COPY (SELECT to_char(tick AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'), partition_id+1, extract(epoch from committed_at), inserted,duplicates FROM cad_archive.coverage WHERE execution_id='$id' ORDER BY 1,2) TO STDOUT" > "$dir/archive.tsv"
  docker compose --env-file "$run/compose.env" -p "$proj" -f deploy/compose.yaml -f "$run/private-data-ports.yaml" exec -T postgres psql -X -U cad_admin -d cad -Atc "COPY (SELECT to_char(tick AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'), right(r.group_id,2)::int,extract(epoch from received_at) FROM cad_v2.master_results r JOIN cad_v2.master_inbox USING (execution_id,message_id) WHERE r.execution_id='$id' ORDER BY 1,2) TO STDOUT" > "$dir/diagnostics.tsv"
 fi
 if ! timeout 35 "$p" -NoProfile -File gui/demo.ps1 -Action Stop -RunDirectory "$run" >> "$dir/run.log" 2>&1; then
   test -z "$(docker ps -q --filter "label=com.docker.compose.project=$proj")"
 fi
 printf '%s %s ' "$pair" "$mode" >> "$out/pairs-log.txt"
 jq -c . "$run/pipeline.json" >> "$out/pairs-log.txt"
done
python3 scripts/acceptance/paired-archive-summary.py "$out" "$CAD_ACCEPTANCE_DISK" "$pg_container" > "$out/summary.jsonl"
cat "$out/pairs-log.txt"
