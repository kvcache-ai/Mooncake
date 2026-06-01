#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/lib/common.sh"

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

UPSTREAM_BUILD_DIR=$(mc_scripts_resolve_upstream_build_dir "${REPO_ROOT}")
mc_scripts_setup_upstream_runtime_env "${REPO_ROOT}" python "${UPSTREAM_BUILD_DIR}"
export PYTHONDONTWRITEBYTECODE=1

cd "${REPO_ROOT}"

if [[ -n "${MC_STORE_RS_CLIENT_RW_BIN:-}" ]]; then
  echo "==> reusing prebuilt standalone mooncake-store-client binary"
  BIN="${MC_STORE_RS_CLIENT_RW_BIN}"
else
  echo "==> building mooncake-store-py"
  cargo build -p mooncake-store-py
  BIN="${REPO_ROOT}/target/debug/mooncake-store-client"
fi

if [[ ! -x "${BIN}" ]]; then
  echo "expected binary was not produced at ${BIN}" >&2
  exit 1
fi

RUN_ID=$(date +%s%N)
KEYSPACE="mc/store-rs/test-unified-parallel-tensor/${RUN_ID}"
DUMMY_RPC_PORT=$(allocate_port)
METRICS_PORT=$(allocate_port)
TRANSPORT_PORT=$(allocate_port)
PROTOCOL="${MC_STORE_RS_TEST_PROTOCOL:-tcp}"
TRANSPORT_BACKEND="${MC_STORE_RS_TRANSPORT_BACKEND:-tent}"
TRANSPORT_METADATA_URL="${MC_STORE_RS_TRANSPORT_METADATA_URL:-redis://127.0.0.1:6379/1}"

wait_for_healthz() {
  local metrics_addr=$1
  python3 - "${metrics_addr}" <<'PY'
import sys, time, urllib.request
metrics_addr = sys.argv[1]
deadline = time.time() + 15
last_error = None
while time.time() < deadline:
    try:
        with urllib.request.urlopen(f"http://{metrics_addr}/healthz", timeout=2) as r:
            if r.read().decode() == "ok\n":
                raise SystemExit(0)
    except Exception as e:
        last_error = e
        time.sleep(0.1)
raise SystemExit(f"healthz not ready: {last_error!r}")
PY
}

echo "==> starting dummy daemon"
DAEMON_LOG="${TEMP_DIR}/daemon.log"
DAEMON_ARGS=(
  "${BIN}" run
  --local-hostname 127.0.0.1
  --metadata-url "redis://127.0.0.1:6379/0"
  --storage-bytes $((64 * 1024 * 1024))
  --scratch-bytes $((16 * 1024 * 1024))
  --protocol "${PROTOCOL}"
  --transport-rpc-port "${TRANSPORT_PORT}"
  --transport-backend "${TRANSPORT_BACKEND}"
  --stable-id "upt-daemon-${RUN_ID}"
  --keyspace "${KEYSPACE}"
  --client-server-address "127.0.0.1:${DUMMY_RPC_PORT}"
  --metrics-addr "127.0.0.1:${METRICS_PORT}"
)
if [[ "${TRANSPORT_BACKEND}" == "tent" ]]; then
  DAEMON_ARGS+=(--transport-metadata-url "${TRANSPORT_METADATA_URL}")
fi
"${DAEMON_ARGS[@]}" >"${DAEMON_LOG}" 2>&1 &
PIDS+=($!)
wait_for_healthz "127.0.0.1:${METRICS_PORT}"

echo "==> running unified parallel tensor E2E tests"
python3 ./scripts/tests/client/test_unified_parallel_tensor.py \
  --daemon_addr "127.0.0.1:${DUMMY_RPC_PORT}" \
  --tenant default \
  --keyspace "${KEYSPACE}"

echo "unified parallel tensor E2E validation OK"
