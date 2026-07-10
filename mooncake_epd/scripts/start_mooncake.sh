#!/bin/bash
# 启动真实 Mooncake metadata + master + store REST 服务

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_ROOT="${PROJECT_DIR}/../venv_mooncake"

MASTER_PORT="${MASTER_PORT:-50061}"
METADATA_PORT="${METADATA_PORT:-8090}"
STORE_PORT="${STORE_PORT:-8089}"
GLOBAL_SEGMENT_SIZE="${GLOBAL_SEGMENT_SIZE:-1073741824}"   # 1 GiB
LOCAL_BUFFER_SIZE="${LOCAL_BUFFER_SIZE:-268435456}"        # 256 MiB
LOCAL_HOSTNAME="${LOCAL_HOSTNAME:-127.0.0.1}"
PROTOCOL="${PROTOCOL:-tcp}"

unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
export NO_PROXY=127.0.0.1,localhost

source "${VENV_ROOT}/bin/activate"

echo "======================================"
echo "Starting Mooncake runtime"
echo "metadata: http://${LOCAL_HOSTNAME}:${METADATA_PORT}/metadata"
echo "master:   ${LOCAL_HOSTNAME}:${MASTER_PORT}"
echo "store:    http://${LOCAL_HOSTNAME}:${STORE_PORT}"
echo "protocol: ${PROTOCOL}"
echo "======================================"

python -m mooncake.http_metadata_server \
  --host "${LOCAL_HOSTNAME}" \
  --port "${METADATA_PORT}" &
METADATA_PID=$!
sleep 1

mooncake_master \
  --rpc_port="${MASTER_PORT}" &
MASTER_PID=$!
sleep 2

export MOONCAKE_MASTER="${LOCAL_HOSTNAME}:${MASTER_PORT}"
export MOONCAKE_TE_META_DATA_SERVER="http://${LOCAL_HOSTNAME}:${METADATA_PORT}/metadata"
export MOONCAKE_PROTOCOL="${PROTOCOL}"
export MOONCAKE_GLOBAL_SEGMENT_SIZE="${GLOBAL_SEGMENT_SIZE}"
export MOONCAKE_LOCAL_BUFFER_SIZE="${LOCAL_BUFFER_SIZE}"
export MOONCAKE_LOCAL_HOSTNAME="${LOCAL_HOSTNAME}"

python -m mooncake.mooncake_store_service \
  --port="${STORE_PORT}" \
  --max-wait-time=30 &
STORE_PID=$!

echo "Mooncake services started."
echo "  export MOONCAKE_STORE_URL=http://${LOCAL_HOSTNAME}:${STORE_PORT}"
echo "Press Ctrl+C to stop."

cleanup() {
  echo "Stopping Mooncake services..."
  kill "${STORE_PID}" 2>/dev/null || true
  kill "${MASTER_PID}" 2>/dev/null || true
  kill "${METADATA_PID}" 2>/dev/null || true
}

trap cleanup SIGINT SIGTERM EXIT
wait
