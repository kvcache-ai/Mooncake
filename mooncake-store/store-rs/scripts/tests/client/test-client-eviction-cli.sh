#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/lib/common.sh"
REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-6380}"
STARTUP_TIMEOUT_SECONDS="${MC_STORE_RS_EVICTION_STARTUP_TIMEOUT_SECONDS:-30}"
EVICTION_TIMEOUT_SECONDS="${MC_STORE_RS_EVICTION_TIMEOUT_SECONDS:-60}"
METRICS_TIMEOUT_SECONDS="${MC_STORE_RS_EVICTION_METRICS_TIMEOUT_SECONDS:-45}"

usage() {
  cat <<'EOF'
Usage: scripts/tests/client/test-client-eviction-cli.sh

Build and directly execute the standalone mooncake-store-client binary, then
verify background storage-owner eviction through a real routed writer, JSON
stats, and tracing logs.

Environment:
  MC_STORE_RS_REDIS_PORT      Redis port for the temporary metadata backend
  MC_STORE_RS_KEEP_TEMP       Keep temp logs on failure when set to 1
  MC_STORE_RS_EVICTION_STARTUP_TIMEOUT_SECONDS
                              Time to wait for the storage CLI startup banner
                              (default: 30)
  MC_STORE_RS_EVICTION_TIMEOUT_SECONDS
                              Time to wait for routed writes to trigger eviction
                              (default: 60)
  MC_STORE_RS_EVICTION_METRICS_TIMEOUT_SECONDS
                              Time to wait for eviction stats after the driver
                              observes eviction (default: 45)
  MOONCAKE_UPSTREAM_DIR       Mooncake upstream submodule path
  MOONCAKE_UPSTREAM_BUILD_DIR Explicit upstream build directory override
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

wait_for_log() {
  local log_file=$1
  local needle=$2
  local timeout_seconds=${3:-20}
  local pid=${4:-}
  local attempts=$((timeout_seconds * 10))
  local attempt

  for ((attempt = 0; attempt < attempts; attempt += 1)); do
    if [[ -f "${log_file}" ]] && grep -Fq "${needle}" "${log_file}"; then
      return 0
    fi
    if [[ -n "${pid}" ]] && ! kill -0 "${pid}" >/dev/null 2>&1; then
      echo "process ${pid} exited while waiting for log line: ${needle}" >&2
      echo "--- ${log_file} ---" >&2
      cat "${log_file}" >&2
      return 1
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
  local timeout_seconds=${2:-45}
  python3 - "${metrics_addr}" "${timeout_seconds}" <<'PY'
import re
import sys
import time
import urllib.request

metrics_addr = sys.argv[1]
deadline = time.time() + float(sys.argv[2])
last_text = ""

def metric_value(text: str, metric: str, operation: str) -> int:
    pattern = rf'^{re.escape(metric)}\{{operation="{re.escape(operation)}",status="ok"\}} (\d+)$'
    match = re.search(pattern, text, re.MULTILINE)
    return int(match.group(1)) if match else 0

while time.time() < deadline:
    try:
        with urllib.request.urlopen(f"http://{metrics_addr}/metrics", timeout=2) as response:
            last_text = response.read().decode()
    except Exception as exc:
        last_text = f"# failed to fetch metrics from {metrics_addr}: {exc}"
        time.sleep(0.2)
        continue

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

wait_for_stats() {
  local bin=$1
  local server_addr=$2
  local timeout_seconds=${3:-45}
  python3 - "${bin}" "${server_addr}" "${timeout_seconds}" <<'PY'
import json
import subprocess
import sys
import time

binary = sys.argv[1]
server_addr = sys.argv[2]
deadline = time.time() + float(sys.argv[3])
last_text = ""

def operation_value(document: dict, operation: str, status: str, field: str) -> int:
    for entry in document.get("operations", []):
        if entry.get("operation") == operation and entry.get("status") == status:
            return int(entry.get(field, 0))
    return 0

while time.time() < deadline:
    try:
        last_text = subprocess.check_output(
            [binary, "stats", "--server", server_addr, "--json"],
            text=True,
            stderr=subprocess.STDOUT,
        )
        document = json.loads(last_text)
    except Exception as exc:
        last_text = f"# failed to fetch stats from {server_addr}: {exc}"
        time.sleep(0.2)
        continue

    background_calls = operation_value(
        document, "storage_owner_background_eviction", "ok", "calls_total"
    )
    background_evicted = operation_value(
        document, "storage_owner_background_eviction", "ok", "bytes_out_total"
    )
    evict_one_calls = operation_value(
        document, "storage_owner_evict_one", "ok", "calls_total"
    )

    if background_calls >= 1 and background_evicted >= 1 and evict_one_calls >= 1:
        print(
            f"stats ok: background_calls={background_calls} "
            f"background_evicted={background_evicted} evict_one_calls={evict_one_calls}"
        )
        raise SystemExit(0)
    time.sleep(0.2)

print(last_text, file=sys.stderr)
raise SystemExit("eviction stats did not reach the expected values")
PY
}

capture_metrics_snapshot() {
  local metrics_addr=${METRICS_ADDR:-}
  local output_file=${METRICS_SNAPSHOT:-}
  if [[ -z "${metrics_addr}" || -z "${output_file}" || "${metrics_addr}" == "disabled" ]]; then
    return 0
  fi
  python3 - "${metrics_addr}" "${output_file}" <<'PY' || true
import sys
import urllib.request

metrics_addr = sys.argv[1]
output_file = sys.argv[2]
try:
    with urllib.request.urlopen(f"http://{metrics_addr}/metrics", timeout=2) as response:
        text = response.read().decode()
except Exception as exc:
    text = f"# failed to fetch metrics from {metrics_addr}: {exc}\n"
with open(output_file, "w", encoding="utf-8") as fout:
    fout.write(text)
PY
}

capture_stats_snapshot() {
  local bin=${BIN:-}
  local metrics_addr=${METRICS_ADDR:-}
  local output_file=${STATS_SNAPSHOT:-}
  if [[ -z "${bin}" || -z "${metrics_addr}" || -z "${output_file}" || "${metrics_addr}" == "disabled" ]]; then
    return 0
  fi
  "${bin}" stats --server "${metrics_addr}" --json >"${output_file}" 2>/dev/null || true
}

print_tail_if_exists() {
  local label=$1
  local path=$2
  local lines=${3:-120}
  if [[ -f "${path}" ]]; then
    echo "--- ${label}: ${path} (tail -${lines}) ---" >&2
    tail -n "${lines}" "${path}" >&2
  else
    echo "--- ${label}: ${path} missing ---" >&2
  fi
}

print_failure_context() {
  echo "==> failure diagnostics" >&2
  print_tail_if_exists "driver log" "${DRIVER_LOG:-}" 160
  print_tail_if_exists "storage log" "${STORAGE_LOG:-}" 200
  if [[ -f "${METRICS_SNAPSHOT:-}" ]]; then
    echo "--- metrics snapshot: ${METRICS_SNAPSHOT} ---" >&2
    grep -E "mooncake_store_client_operation|mooncake_store_heartbeat|mooncake_store_runtime_lease|storage_owner" \
      "${METRICS_SNAPSHOT}" >&2 || cat "${METRICS_SNAPSHOT}" >&2
  fi
  if [[ -f "${STATS_SNAPSHOT:-}" ]]; then
    echo "--- stats snapshot: ${STATS_SNAPSHOT} ---" >&2
    cat "${STATS_SNAPSHOT}" >&2
  fi
}

TEMP_DIR=$(mktemp -d)
PIDS=()
REDIS_STARTED=0
METRICS_ADDR=""
METRICS_SNAPSHOT="${TEMP_DIR}/metrics.final"
STATS_SNAPSHOT="${TEMP_DIR}/stats.final.json"

cleanup() {
  local status=$?
  local pid
  local attempt

  if [[ "${status}" != "0" ]]; then
    capture_metrics_snapshot
    capture_stats_snapshot
  fi

  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -TERM "${pid}" >/dev/null 2>&1 || true
    fi
  done
  for ((attempt = 0; attempt < 20; attempt += 1)); do
    local any_alive=0
    for pid in "${PIDS[@]:-}"; do
      if kill -0 "${pid}" >/dev/null 2>&1; then
        any_alive=1
        break
      fi
    done
    [[ "${any_alive}" == "0" ]] && break
    sleep 0.1
  done
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -KILL "${pid}" >/dev/null 2>&1 || true
    fi
  done
  for pid in "${PIDS[@]:-}"; do
    wait "${pid}" >/dev/null 2>&1 || true
  done

  if [[ "${REDIS_STARTED}" == "1" ]]; then
    redis-cli -p "${REDIS_PORT}" shutdown nosave >/dev/null 2>&1 || true
  fi
  if [[ "${status}" != "0" ]]; then
    print_failure_context
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
DRIVER_LOG="${TEMP_DIR}/driver.log"
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
wait_for_log \
  "${STORAGE_LOG}" \
  "mooncake-store-client started stable_id=${STABLE_ID} epoch=1 initial_state=active segment=${SEGMENT_NAME}" \
  "${STARTUP_TIMEOUT_SECONDS}" \
  "${STORAGE_PID}"

METRICS_ADDR=$(sed -n 's/.*metrics_addr=\([^ ]*\).*/\1/p' "${STORAGE_LOG}" | tail -n 1)
if [[ -z "${METRICS_ADDR}" || "${METRICS_ADDR}" == "disabled" ]]; then
  echo "failed to resolve metrics address from ${STORAGE_LOG}" >&2
  cat "${STORAGE_LOG}" >&2
  exit 1
fi
wait_for_healthz "${METRICS_ADDR}"

echo "==> driving routed writes and waiting for background eviction"
if ! REDIS_URL="${REDIS_URL}" \
  KEYSPACE="${KEYSPACE}" \
  DRIVER_ID="${DRIVER_ID}" \
  READER_ID="${READER_ID}" \
  STABLE_ID="${STABLE_ID}" \
  EVICTION_TIMEOUT_SECONDS="${EVICTION_TIMEOUT_SECONDS}" \
  PYTHONUNBUFFERED=1 \
  python3 - >"${DRIVER_LOG}" 2>&1 <<'PY'
import os
import time
import traceback

from mooncake.store import MooncakeDistributedStore, ReplicateConfig

writer = MooncakeDistributedStore()
reader = MooncakeDistributedStore()

try:
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
    eviction_timeout = float(os.environ["EVICTION_TIMEOUT_SECONDS"])

    print(f"driver: writing hot/cold payloads payload_size={payload_size}")
    assert writer.put(hot_key, hot, config=policy) == 0
    assert writer.put(cold_key, cold, config=policy) == 0

    warm_deadline = time.time() + min(15.0, eviction_timeout)
    last_error = None
    while time.time() < warm_deadline:
        try:
            if reader.get(hot_key) == hot:
                print("driver: reader observed hot key before eviction")
                break
        except Exception as exc:
            last_error = repr(exc)
        time.sleep(0.1)
    else:
        raise AssertionError(
            f"reader did not observe the hot key before eviction; last_error={last_error}"
        )

    print("driver: writing fresh payload to cross high watermark")
    assert writer.put(fresh_key, fresh, config=policy) == 0

    deadline = time.time() + eviction_timeout
    last_route = None
    last_error = None
    attempts = 0
    while time.time() < deadline:
        attempts += 1
        try:
            last_route = reader.query_route(cold_key)
        except Exception as exc:
            last_error = f"query_route: {exc!r}"
            last_route = "<query failed>"
        if last_route is not None:
            time.sleep(0.2)
            continue
        assert reader.get(hot_key) == hot
        assert reader.get(fresh_key) == fresh
        assert reader.batch_get([hot_key, fresh_key]) == [hot, fresh]
        try:
            reader.get(cold_key)
        except Exception as exc:
            print(
                "driver: eviction observed "
                f"attempts={attempts} cold_get_error={exc!r}"
            )
            raise SystemExit(0)
        last_error = "cold key route disappeared but cold get still succeeded"
        time.sleep(0.2)

    raise AssertionError(
        "cold key was not evicted within the deadline; "
        f"timeout={eviction_timeout}s attempts={attempts} "
        f"last_route={last_route!r} last_error={last_error}"
    )
except Exception:
    traceback.print_exc()
    raise
finally:
    try:
        writer.close()
    finally:
        reader.close()
PY
then
  echo "eviction driver failed; see ${DRIVER_LOG}" >&2
  exit 1
fi

echo "==> validating eviction stats"
wait_for_stats "${BIN}" "${METRICS_ADDR}" "${METRICS_TIMEOUT_SECONDS}"

echo "CLI eviction binary test passed"
