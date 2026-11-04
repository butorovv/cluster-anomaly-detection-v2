#!/bin/sh
set -eu
case "${1:-}" in
 bootstrap) exec /app/bootstrap ;;
 worker)
  # Each replica advertises its own container IP; a shared DNS alias would send
  # polling to an arbitrary Worker after rebalance.
  ip=$(hostname -i | awk '{print $1}')
  exec /app/worker --assets /assets --profile-sha256 "$PROFILE_SHA256" --input-sha256 "$INPUT_SHA256" --execution-id "$EXECUTION_ID" --consumer-group "cad-input-$EXECUTION_ID" --kafka-brokers "$KAFKA_BROKERS" --groups "$GROUPS_COUNT" --delta "$DELTA_SECONDS" --checkpoint-interval-ticks "$CHECKPOINT_INTERVAL_TICKS" --output-transport "$OUTPUT_TRANSPORT" --master-url http://master:8080 --listen 0.0.0.0:8081 --advertise-url "http://$ip:8081" --lease-ttl 15s --measure="$MEASURE" ;;
 master)
  exec /app/master --listen 0.0.0.0:8080 --mode "$OUTPUT_TRANSPORT" --execution-id "$EXECUTION_ID" --kafka-brokers "$KAFKA_BROKERS" --results-group "cad-results-$EXECUTION_ID" --poll-interval "${POLL_INTERVAL_MS}ms" --measure="$MEASURE" ;;
 replayer)
  exec /app/replayer --input /assets/data/prepared/summit-D1-pilot/D1.parquet --speed "$REPLAY_SPEED" --start 2022-01-18T08:00:00Z --end 2022-01-18T16:00:00Z --completion-mode full-grid --wire-format v1 --execution-id "$EXECUTION_ID" --input-sha256 "$INPUT_SHA256" --profile-sha256 "$PROFILE_SHA256" --output kafka --kafka-brokers "$KAFKA_BROKERS" ;;
 *) exec "$@" ;;
esac
