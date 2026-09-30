#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
# store-rs is a subdirectory when it lives inside the Mooncake monorepo,
# where the git toplevel is the enclosing repository rather than this tree.
[ -f "${REPO_ROOT}/Cargo.toml" ] || REPO_ROOT="${REPO_ROOT}/mooncake-store/store-rs"
# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/lib/common.sh"
REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-6380}"
MODE="${1:-all}"
TRANSPORT_BACKEND="${MC_STORE_RS_TRANSPORT_BACKEND:-classic-te}"
ROUTE_CONTROL="${MC_STORE_RS_ROUTE_CONTROL:-embedded-wrh}"
ROUTE_TOPK="${MC_STORE_RS_ROUTE_TOPK:-2}"
PROTOCOL="${MC_STORE_RS_TEST_PROTOCOL:-tcp}"

usage() {
  cat <<'EOF'
Usage: scripts/tests/client/test-client-rw-cli.sh [all|real|dummy]

Build and execute the standalone mooncake-store-client binary, then verify both
repository-standard read/write validators:

- `scripts/clients/dummy_client_rw.py` against the daemon dummy API
- `scripts/clients/real_client_rw.py` against the real routed store runtime

Environment:
  MC_STORE_RS_REDIS_PORT      Redis port for the temporary metadata backend
  MC_STORE_RS_KEEP_TEMP       Keep temp logs on failure when set to 1
  MC_STORE_RS_TRANSPORT_BACKEND
                              Transport backend passed into the runtime
                              (`classic-te` or `tent`, default: `classic-te`)
  MC_STORE_RS_ROUTE_CONTROL   Route control mode (`embedded-wrh` by default)
  MC_STORE_RS_ROUTE_TOPK      Embedded WRH top-k authority count (default: 2)
  MC_STORE_RS_TEST_PROTOCOL   Transport protocol (`tcp` by default)
  MOONCAKE_UPSTREAM_DIR       Mooncake upstream submodule path
  MOONCAKE_UPSTREAM_BUILD_DIR Explicit upstream build directory override
  MC_STORE_RS_CLIENT_RW_BIN   Optional explicit standalone client binary path.
EOF
}

if [[ "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${MODE}" != "all" && "${MODE}" != "real" && "${MODE}" != "dummy" ]]; then
  echo "unsupported mode: ${MODE}" >&2
  usage >&2
  exit 1
fi

wait_for_healthz() {
  local metrics_addr=$1
  python3 - "${metrics_addr}" <<'PY'
import sys
import time
import urllib.request

metrics_addr = sys.argv[1]
deadline = time.time() + 15
last_error = None
while time.time() < deadline:
    try:
        with urllib.request.urlopen(f"http://{metrics_addr}/healthz", timeout=2) as response:
            if response.read().decode() == "ok\n":
                raise SystemExit(0)
    except Exception as error:
        last_error = error
        time.sleep(0.1)
raise SystemExit(f"metrics endpoint http://{metrics_addr}/healthz did not become ready: {last_error!r}")
PY
}

allocate_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

TEMP_DIR=$(mktemp -d)
PIDS=()
REDIS_STARTED=0

cleanup() {
  local status=$?
  local pid

  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -TERM "${pid}" >/dev/null 2>&1 || true
    fi
  done
  sleep 0.2
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -KILL "${pid}" >/dev/null 2>&1 || true
    fi
  done

  if [[ "${REDIS_STARTED}" == "1" ]]; then
    redis-cli -p "${REDIS_PORT}" shutdown nosave >/dev/null 2>&1 || true
  fi
  if [[ "${status}" != "0" && "${MC_STORE_RS_KEEP_TEMP:-0}" == "1" ]]; then
    echo "preserving temp dir: ${TEMP_DIR}" >&2
  else
    rm -rf "${TEMP_DIR}"
  fi
  exit "${status}"
}
trap cleanup EXIT

mc_scripts_require_command cargo
mc_scripts_require_command python3
mc_scripts_require_command redis-cli
mc_scripts_require_command redis-server

UPSTREAM_BUILD_DIR=$(mc_scripts_resolve_upstream_build_dir "${REPO_ROOT}")
mc_scripts_setup_upstream_runtime_env "${REPO_ROOT}" python "${UPSTREAM_BUILD_DIR}"
export PYTHONDONTWRITEBYTECODE=1

mc_scripts_start_local_redis_if_needed "${REDIS_PORT}" REDIS_STARTED

cd "${REPO_ROOT}"

if [[ -n "${MC_STORE_RS_CLIENT_RW_BIN:-}" ]]; then
  echo "==> reusing prebuilt standalone mooncake-store-client binary"
  BIN="${MC_STORE_RS_CLIENT_RW_BIN}"
else
  echo "==> building standalone mooncake-store-client binary"
  cargo build -p mooncake-store-py
  BIN="${REPO_ROOT}/target/debug/mooncake-store-client"
fi

if [[ ! -x "${BIN}" ]]; then
  echo "expected binary was not produced at ${BIN}" >&2
  exit 1
fi

RUN_ID=$(date +%s%N)
KEYSPACE="mc/store-rs/test-client-rw-cli/${RUN_ID}"
REDIS_URL="redis://127.0.0.1:${REDIS_PORT}/0"
DAEMON_A_TRANSPORT_PORT=$(allocate_port)
DAEMON_B_TRANSPORT_PORT=$(allocate_port)
DUMMY_RPC_PORT=$(allocate_port)
METRICS_A_PORT=$(allocate_port)
METRICS_B_PORT=$(allocate_port)

start_daemon() {
  local stable_id=$1
  local segment_name=$2
  local transport_port=$3
  local metrics_port=$4
  local log_file=$5
  local client_server_addr=${6:-}

  local args=(
    "${BIN}"
    --local-hostname 127.0.0.1
    --metadata-url "${REDIS_URL}"
    --storage-bytes $((64 * 1024 * 1024))
    --scratch-bytes $((16 * 1024 * 1024))
    --protocol "${PROTOCOL}"
    --transport-rpc-port "${transport_port}"
    --transport-backend "${TRANSPORT_BACKEND}"
    --stable-id "${stable_id}"
    --keyspace "${KEYSPACE}"
    --local-segment-name "${segment_name}"
    --route-control "${ROUTE_CONTROL}"
    --route-topk "${ROUTE_TOPK}"
    --metrics-addr "127.0.0.1:${metrics_port}"
    --label pool=pool-a
    --label storage=true
  )
  if [[ -n "${client_server_addr}" ]]; then
    args+=(--client-server-address "${client_server_addr}")
  fi
  "${args[@]}" >"${log_file}" 2>&1 &
  PIDS+=($!)
}

echo "==> starting storage daemons"
DAEMON_A_LOG="${TEMP_DIR}/daemon-a.log"
DAEMON_B_LOG="${TEMP_DIR}/daemon-b.log"
start_daemon \
  "rw-daemon-a-${RUN_ID}" \
  "rw-daemon-a-segment-${RUN_ID}" \
  "${DAEMON_A_TRANSPORT_PORT}" \
  "${METRICS_A_PORT}" \
  "${DAEMON_A_LOG}" \
  "127.0.0.1:${DUMMY_RPC_PORT}"
start_daemon \
  "rw-daemon-b-${RUN_ID}" \
  "rw-daemon-b-segment-${RUN_ID}" \
  "${DAEMON_B_TRANSPORT_PORT}" \
  "${METRICS_B_PORT}" \
  "${DAEMON_B_LOG}"
wait_for_healthz "127.0.0.1:${METRICS_A_PORT}"
wait_for_healthz "127.0.0.1:${METRICS_B_PORT}"
sleep 1

if [[ "${MODE}" == "all" || "${MODE}" == "dummy" ]]; then
  echo "==> validating dummy single-item path"
  python3 ./scripts/clients/dummy_client_rw.py \
    --daemon_addr "127.0.0.1:${DUMMY_RPC_PORT}" \
    --tenant default \
    --keyspace "${KEYSPACE}" \
    --key_prefix "dummy-single-${RUN_ID}" \
    --num_kv 12 \
    --value_size 1024 \
    --batch_size 1 \
    --mode both \
    --delete

  echo "==> validating dummy shm batch path"
  python3 ./scripts/clients/dummy_client_rw.py \
    --daemon_addr "127.0.0.1:${DUMMY_RPC_PORT}" \
    --tenant default \
    --keyspace "${KEYSPACE}" \
    --key_prefix "dummy-batch-${RUN_ID}" \
    --num_kv 12 \
    --value_size 1024 \
    --batch_size 4 \
    --batch_api shm \
    --mode both \
    --delete

  echo "==> validating dummy multi-buffer shm path"
  python3 ./scripts/clients/dummy_client_rw.py \
    --daemon_addr "127.0.0.1:${DUMMY_RPC_PORT}" \
    --tenant default \
    --keyspace "${KEYSPACE}" \
    --key_prefix "dummy-multi-${RUN_ID}" \
    --num_kv 8 \
    --value_size 1536 \
    --batch_size 4 \
    --batch_api multi_buffer \
    --mode both \
    --delete
fi

if [[ "${MODE}" == "all" || "${MODE}" == "real" ]]; then
  echo "==> validating real routed single-item path"
  python3 ./scripts/clients/real_client_rw.py \
    --local_host "127.0.0.1:$(allocate_port)" \
    --metadata_url "${REDIS_URL}" \
    --storage-bytes 0 \
    --scratch-bytes $((16 * 1024 * 1024)) \
    --protocol "${PROTOCOL}" \
    --transport-backend "${TRANSPORT_BACKEND}" \
    --route-control "${ROUTE_CONTROL}" \
    --route-topk "${ROUTE_TOPK}" \
    --keyspace "${KEYSPACE}" \
    --routed-writes \
    --mode write \
    --num_kv 12 \
    --value_size 1024 \
    --batch_size 1 \
    --replica_num 2 \
    --key_prefix "real-single-${RUN_ID}"

  python3 ./scripts/clients/real_client_rw.py \
    --local_host "127.0.0.1:$(allocate_port)" \
    --metadata_url "${REDIS_URL}" \
    --storage-bytes 0 \
    --scratch-bytes $((16 * 1024 * 1024)) \
    --protocol "${PROTOCOL}" \
    --transport-backend "${TRANSPORT_BACKEND}" \
    --route-control "${ROUTE_CONTROL}" \
    --route-topk "${ROUTE_TOPK}" \
    --keyspace "${KEYSPACE}" \
    --routed-writes \
    --mode read \
    --num_kv 12 \
    --value_size 1024 \
    --batch_size 1 \
    --replica_num 1 \
    --key_prefix "real-single-${RUN_ID}"

  echo "==> validating real routed batch path"
  python3 ./scripts/clients/real_client_rw.py \
    --local_host "127.0.0.1:$(allocate_port)" \
    --metadata_url "${REDIS_URL}" \
    --storage-bytes 0 \
    --scratch-bytes $((16 * 1024 * 1024)) \
    --protocol "${PROTOCOL}" \
    --transport-backend "${TRANSPORT_BACKEND}" \
    --route-control "${ROUTE_CONTROL}" \
    --route-topk "${ROUTE_TOPK}" \
    --keyspace "${KEYSPACE}" \
    --routed-writes \
    --mode write \
    --num_kv 12 \
    --value_size 1024 \
    --batch_size 4 \
    --replica_num 2 \
    --key_prefix "real-batch-${RUN_ID}"

  python3 ./scripts/clients/real_client_rw.py \
    --local_host "127.0.0.1:$(allocate_port)" \
    --metadata_url "${REDIS_URL}" \
    --storage-bytes 0 \
    --scratch-bytes $((16 * 1024 * 1024)) \
    --protocol "${PROTOCOL}" \
    --transport-backend "${TRANSPORT_BACKEND}" \
    --route-control "${ROUTE_CONTROL}" \
    --route-topk "${ROUTE_TOPK}" \
    --keyspace "${KEYSPACE}" \
    --routed-writes \
    --mode read \
    --num_kv 12 \
    --value_size 1024 \
    --batch_size 4 \
    --replica_num 1 \
    --key_prefix "real-batch-${RUN_ID}" \
    --delete
fi

echo "client rw validation OK"
