#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)
REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-6380}"

usage() {
  cat <<'EOF'
Usage: scripts/test-client-eviction-cli.sh

Build and directly execute the standalone mooncake-store-client binary, then
verify background storage-owner eviction through a real routed writer, Prometheus
metrics, and tracing logs.

Environment:
  MC_STORE_RS_REDIS_PORT      Redis port for the temporary metadata backend
  MC_STORE_RS_KEEP_TEMP       Keep temp logs on failure when set to 1
  MOONCAKE_UPSTREAM_DIR       Mooncake upstream submodule path
  MOONCAKE_UPSTREAM_BUILD_DIR Explicit upstream build directory override
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

require_command() {
  local command_name=$1
  local cargo_env

  if command -v "${command_name}" >/dev/null 2>&1; then
    return 0
  fi

  if [[ "${command_name}" == "cargo" ]]; then
    cargo_env="${CARGO_HOME:-${HOME}/.cargo}/env"
    if [[ -f "${cargo_env}" ]]; then
      # shellcheck disable=SC1090
      source "${cargo_env}"
    fi
  fi

  if command -v "${command_name}" >/dev/null 2>&1; then
    return 0
  fi

  echo "${command_name} is required for CLI eviction verification" >&2
  exit 1
}

list_upstream_dirs() {
  local primary_worktree

  if [[ -n "${MOONCAKE_UPSTREAM_DIR:-}" ]]; then
    printf '%s\n' "${MOONCAKE_UPSTREAM_DIR}"
  fi
  printf '%s\n' "${REPO_ROOT}/third_party/Mooncake"

  if primary_worktree=$(git -C "${REPO_ROOT}" worktree list --porcelain 2>/dev/null | awk '/^worktree / { print substr($0, 10); exit }'); then
    if [[ -n "${primary_worktree}" && "${primary_worktree}" != "${REPO_ROOT}" ]]; then
      printf '%s\n' "${primary_worktree}/third_party/Mooncake"
    fi
  fi
}

resolve_upstream_build_dir() {
  local candidates=()
  local candidate
  local upstream_dir

  if [[ -n "${MOONCAKE_UPSTREAM_BUILD_DIR:-}" ]]; then
    candidates+=("${MOONCAKE_UPSTREAM_BUILD_DIR}")
  fi
  while IFS= read -r upstream_dir; do
    [[ -z "${upstream_dir}" ]] && continue
    candidates+=(
      "${upstream_dir}/build-rust"
      "${upstream_dir}/build-wheel-compat"
    )
  done < <(list_upstream_dirs)

  for candidate in "${candidates[@]}"; do
    if [[ -f "${candidate}/mooncake-transfer-engine/src/libtransfer_engine.so" ]] \
      && [[ -f "${candidate}/mooncake-transfer-engine/tent/src/libtent_shared.so" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  echo "unable to find Mooncake runtime libraries under any known Mooncake upstream tree" >&2
  echo "checked candidates:" >&2
  printf '  %s\n' "${candidates[@]}" >&2
  echo "set MOONCAKE_UPSTREAM_BUILD_DIR to a built upstream directory" >&2
  exit 1
}

wait_for_log() {
  local log_file=$1
  local needle=$2
  local timeout_seconds=${3:-20}
  local attempts=$((timeout_seconds * 10))
  local attempt

  for ((attempt = 0; attempt < attempts; attempt += 1)); do
    if [[ -f "${log_file}" ]] && grep -Fq "${needle}" "${log_file}"; then
      return 0
    fi
    sleep 0.1
  done

  echo "timed out waiting for log line: ${needle}" >&2
  echo "--- ${log_file} ---" >&2
  cat "${log_file}" >&2
  return 1
}

wait_for_healthz() {
  local metrics_addr=$1
  python3 - "${metrics_addr}" <<'PY'
import sys
import time
import urllib.request

metrics_addr = sys.argv[1]
deadline = time.time() + 10
while time.time() < deadline:
    try:
        with urllib.request.urlopen(f"http://{metrics_addr}/healthz", timeout=2) as response:
            if response.read().decode() == "ok\n":
                raise SystemExit(0)
    except Exception:
        time.sleep(0.1)
raise SystemExit(f"metrics endpoint http://{metrics_addr}/healthz did not become ready")
PY
}

wait_for_metrics() {
  local metrics_addr=$1
  python3 - "${metrics_addr}" <<'PY'
import re
import sys
import time
import urllib.request

metrics_addr = sys.argv[1]
deadline = time.time() + 20
last_text = ""

def metric_value(text: str, metric: str, operation: str) -> int:
    pattern = rf'^{re.escape(metric)}\{{operation="{re.escape(operation)}",status="ok"\}} (\d+)$'
    match = re.search(pattern, text, re.MULTILINE)
    return int(match.group(1)) if match else 0

while time.time() < deadline:
    with urllib.request.urlopen(f"http://{metrics_addr}/metrics", timeout=2) as response:
        last_text = response.read().decode()

    background_calls = metric_value(
        last_text,
        "mooncake_store_client_operation_total",
        "storage_owner_background_eviction",
    )
    background_evicted = metric_value(
        last_text,
        "mooncake_store_client_operation_bytes_out_total",
        "storage_owner_background_eviction",
    )
    evict_one_calls = metric_value(
        last_text,
        "mooncake_store_client_operation_total",
        "storage_owner_evict_one",
    )

    if background_calls >= 1 and background_evicted >= 1 and evict_one_calls >= 1:
        print(
            f"metrics ok: background_calls={background_calls} "
            f"background_evicted={background_evicted} evict_one_calls={evict_one_calls}"
        )
        raise SystemExit(0)
    time.sleep(0.2)

print(last_text, file=sys.stderr)
raise SystemExit("eviction metrics did not reach the expected values")
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

require_command cargo
require_command python3
require_command redis-cli
require_command redis-server

UPSTREAM_BUILD_DIR=$(resolve_upstream_build_dir)
UPSTREAM_DIR=$(cd -- "${UPSTREAM_BUILD_DIR}/.." && pwd)
export MOONCAKE_UPSTREAM_DIR="${UPSTREAM_DIR}"
export MOONCAKE_UPSTREAM_BUILD_DIR="${UPSTREAM_BUILD_DIR}"
export LD_LIBRARY_PATH="${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/src:${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/tent/src:${LD_LIBRARY_PATH:-}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="${REPO_ROOT}/python"

if ! redis-cli -p "${REDIS_PORT}" ping >/dev/null 2>&1; then
  redis-server \
    --port "${REDIS_PORT}" \
    --bind 127.0.0.1 \
    --daemonize yes \
    --save '' \
    --appendonly no
  REDIS_STARTED=1
fi

cd "${REPO_ROOT}"

echo "==> building standalone mooncake-store-client binary"
cargo build -p mooncake-store-py

BIN="${REPO_ROOT}/target/debug/mooncake-store-client"
if [[ ! -x "${BIN}" ]]; then
  echo "expected binary was not produced at ${BIN}" >&2
  exit 1
fi

RUN_ID=$(date +%s%N)
KEYSPACE="mc/store-rs/test-client-eviction-cli/${RUN_ID}"
REDIS_URL="redis://127.0.0.1:${REDIS_PORT}/0"
STABLE_ID="cli-evict-${RUN_ID}"
DRIVER_ID="cli-evict-driver-${RUN_ID}"
READER_ID="cli-evict-reader-${RUN_ID}"
STORAGE_LOG="${TEMP_DIR}/storage.log"
SEGMENT_NAME="cli-evict-segment-${RUN_ID}"

BASE_ARGS=(
  --local-hostname 127.0.0.1
  --metadata-url "${REDIS_URL}"
  --storage-bytes 1048576
  --scratch-bytes 1048576
  --protocol tcp
  --route-control metadata-only
  --keyspace "${KEYSPACE}"
  --lease-ttl-ms 4000
  --heartbeat-interval-ms 500
  --label pool=pool-a
  --label storage=true
  --label route=false
  --trace-filter debug
  --metrics-addr 127.0.0.1:0
)

echo "==> starting standalone storage client"
"${BIN}" \
  "${BASE_ARGS[@]}" \
  --stable-id "${STABLE_ID}" \
  --epoch 1 \
  --initial-state active \
  --local-segment-name "${SEGMENT_NAME}" \
  >"${STORAGE_LOG}" 2>&1 &
STORAGE_PID=$!
PIDS+=("${STORAGE_PID}")
wait_for_log "${STORAGE_LOG}" "mooncake-store-client started stable_id=${STABLE_ID} epoch=1 initial_state=active segment=${SEGMENT_NAME}"

METRICS_ADDR=$(sed -n 's/.*metrics_addr=\([^ ]*\).*/\1/p' "${STORAGE_LOG}" | tail -n 1)
if [[ -z "${METRICS_ADDR}" || "${METRICS_ADDR}" == "disabled" ]]; then
  echo "failed to resolve metrics address from ${STORAGE_LOG}" >&2
  cat "${STORAGE_LOG}" >&2
  exit 1
fi
wait_for_healthz "${METRICS_ADDR}"

echo "==> driving routed writes and waiting for background eviction"
REDIS_URL="${REDIS_URL}" \
KEYSPACE="${KEYSPACE}" \
DRIVER_ID="${DRIVER_ID}" \
READER_ID="${READER_ID}" \
STABLE_ID="${STABLE_ID}" \
python3 - <<'PY'
import os
import time

from mooncake.store import MooncakeDistributedStore, ReplicateConfig

writer = MooncakeDistributedStore()
assert writer.setup(
    "127.0.0.1",
    os.environ["REDIS_URL"],
    4 * 1024 * 1024,
    1 * 1024 * 1024,
    "tcp",
    "",
    "",
    stable_id=os.environ["DRIVER_ID"],
    keyspace=os.environ["KEYSPACE"],
    labels={"pool": "pool-a", "storage": "false", "route": "false"},
    routed_writes=True,
    replica_count=1,
    route_control="metadata_only",
) == 0

reader = MooncakeDistributedStore()
assert reader.setup(
    "127.0.0.1",
    os.environ["REDIS_URL"],
    4 * 1024 * 1024,
    1 * 1024 * 1024,
    "tcp",
    "",
    "",
    stable_id=os.environ["READER_ID"],
    keyspace=os.environ["KEYSPACE"],
    labels={"pool": "pool-a", "storage": "false", "route": "false"},
    route_control="metadata_only",
) == 0

owner = f"{os.environ['STABLE_ID']}:1"
policy = ReplicateConfig(
    replica_num=1,
    preferred_storage_owners=[owner],
    prefer_local=False,
)

hot_key = "evict-hot"
cold_key = "evict-cold"
fresh_key = "evict-fresh"
payload_size = 320 * 1024
hot = b"H" * payload_size
cold = b"C" * payload_size
fresh = b"F" * payload_size

assert writer.put(hot_key, hot, config=policy) == 0
assert writer.put(cold_key, cold, config=policy) == 0

warm_deadline = time.time() + 10
while time.time() < warm_deadline:
    try:
        if reader.get(hot_key) == hot:
            break
    except Exception:
        pass
    time.sleep(0.1)
else:
    raise AssertionError("reader did not observe the hot key before eviction")

assert writer.put(fresh_key, fresh, config=policy) == 0

deadline = time.time() + 20
while time.time() < deadline:
    if reader.query_route(cold_key) is not None:
        time.sleep(0.2)
        continue
    assert reader.get(hot_key) == hot
    assert reader.get(fresh_key) == fresh
    assert reader.batch_get([hot_key, fresh_key]) == [hot, fresh]
    try:
        reader.get(cold_key)
    except Exception:
        writer.close()
        reader.close()
        raise SystemExit(0)
    time.sleep(0.2)

writer.close()
reader.close()
raise AssertionError("cold key was not evicted within the deadline")
PY

wait_for_log "${STORAGE_LOG}" "background storage-owner eviction completed"
wait_for_log "${STORAGE_LOG}" "storage-owner evicted replica via route-owner cas"

echo "==> validating eviction metrics"
wait_for_metrics "${METRICS_ADDR}"

echo "CLI eviction binary test passed"
