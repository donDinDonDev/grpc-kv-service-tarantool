#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

KV_TARANTOOL_PASSWORD="${KV_TARANTOOL_PASSWORD:-}"
KV_HTTP_PORT="${KV_HTTP_PORT:-8080}"
KV_GRPC_PORT="${KV_GRPC_PORT:-9090}"
KV_STARTUP_TIMEOUT_SECONDS="${KV_STARTUP_TIMEOUT_SECONDS:-60}"

if [[ -z "${KV_TARANTOOL_PASSWORD}" ]]; then
  if command -v openssl >/dev/null 2>&1; then
    KV_TARANTOOL_PASSWORD="$(openssl rand -hex 16)"
  elif command -v uuidgen >/dev/null 2>&1; then
    KV_TARANTOOL_PASSWORD="$(uuidgen | tr '[:upper:]' '[:lower:]' | tr -d '-')"
  else
    echo "KV_TARANTOOL_PASSWORD must be set because neither openssl nor uuidgen is available" >&2
    exit 1
  fi
fi

export KV_TARANTOOL_PASSWORD
export KV_HTTP_PORT
export KV_GRPC_PORT

docker compose up --build -d

if ! command -v curl >/dev/null 2>&1; then
  exit 0
fi

readiness_url="http://127.0.0.1:${KV_HTTP_PORT}/actuator/health/readiness"
deadline=$((SECONDS + KV_STARTUP_TIMEOUT_SECONDS))

until curl --silent --show-error --fail "${readiness_url}" >/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "readiness endpoint did not become healthy within ${KV_STARTUP_TIMEOUT_SECONDS}s" >&2
    docker compose ps >&2
    exit 1
  fi
  sleep 1
done
