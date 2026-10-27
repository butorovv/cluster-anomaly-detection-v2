#!/usr/bin/env bash
set -euo pipefail
exec env -u DOCKER_HOST -u DOCKER_CONTEXT -u DOCKER_TLS -u DOCKER_TLS_VERIFY -u DOCKER_CERT_PATH \
  docker --host=unix:///var/run/docker.sock exec -i "${P01_CONTAINER:-p01-postgres-test}" \
  psql -U postgres -d "${PGDATABASE:-p01_test}" "$@"
