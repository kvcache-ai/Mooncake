#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/lib/common.sh"
REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-6380}"

usage() {
  cat <<'EOF'
Usage: scripts/tests/client/test-client-hot-upgrade-cli.sh

Build and directly execute the standalone mooncake-store-client binary, then
verify hot-upgrade startup flags, SIGTERM-triggered promotion, and payload
preservation with real processes.

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

wait_for_exit() {
  local pid=$1
  local name=$2
  local timeout_seconds=${3:-20}
  local attempts=$((timeout_seconds * 10))
  local attempt

  for ((attempt = 0; attempt < attempts; attempt += 1)); do
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done

  echo "${name} pid=${pid} did not exit within ${timeout_seconds}s" >&2
  return 1
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

echo "==> building standalone mooncake-store-client binary"
cargo build -p mooncake-store-py

BIN="${REPO_ROOT}/target/debug/mooncake-store-client"
if [[ ! -x "${BIN}" ]]; then
  echo "expected binary was not produced at ${BIN}" >&2
  exit 1
fi

RUN_ID=$(date +%s%N)
KEYSPACE="mc/store-rs/test-client-hot-upgrade-cli/${RUN_ID}"
REDIS_URL="redis://127.0.0.1:${REDIS_PORT}/0"
STABLE_ID="cli-hot-upgrade-${RUN_ID}"
DRIVER_ID="cli-hot-driver-${RUN_ID}"
TEST_KEY="cli-upgrade-key"
TEST_VALUE="payload-before-upgrade-${RUN_ID}"
PREDECESSOR_LOG="${TEMP_DIR}/predecessor.log"
SUCCESSOR_LOG="${TEMP_DIR}/successor.log"
PREDECESSOR_SEGMENT="cli-predecessor-${RUN_ID}"
SUCCESSOR_SEGMENT="cli-successor-${RUN_ID}"
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
  --drain-on-exit
)

echo "==> starting active predecessor binary"
"${BIN}" \
  "${BASE_ARGS[@]}" \
  --stable-id "${STABLE_ID}" \
  --epoch 1 \
  --initial-state active \
  --local-segment-name "${PREDECESSOR_SEGMENT}" \
  >"${PREDECESSOR_LOG}" 2>&1 &
PREDECESSOR_PID=$!
PIDS+=("${PREDECESSOR_PID}")
wait_for_log "${PREDECESSOR_LOG}" "mooncake-store-client started stable_id=${STABLE_ID} epoch=1 initial_state=active segment=${PREDECESSOR_SEGMENT}"

echo "==> starting standby successor binary"
"${BIN}" \
  "${BASE_ARGS[@]}" \
  --stable-id "${STABLE_ID}" \
  --epoch 2 \
  --initial-state standby \
  --local-segment-name "${SUCCESSOR_SEGMENT}" \
  >"${SUCCESSOR_LOG}" 2>&1 &
SUCCESSOR_PID=$!
PIDS+=("${SUCCESSOR_PID}")
wait_for_log "${SUCCESSOR_LOG}" "mooncake-store-client started stable_id=${STABLE_ID} epoch=2 initial_state=standby segment=${SUCCESSOR_SEGMENT}"

echo "==> writing payload through an external routed client and pinning it to predecessor"
REDIS_URL="${REDIS_URL}" \
KEYSPACE="${KEYSPACE}" \
DRIVER_ID="${DRIVER_ID}" \
STABLE_ID="${STABLE_ID}" \
TEST_KEY="${TEST_KEY}" \
TEST_VALUE="${TEST_VALUE}" \
python3 - <<'PY'
import os

from mooncake.store import MooncakeDistributedStore, ReplicateConfig

store = MooncakeDistributedStore()
assert store.setup(
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

policy = ReplicateConfig(
    replica_num=1,
    preferred_storage_owners=[f"{os.environ['STABLE_ID']}:1"],
    prefer_local=False,
)
expected = os.environ["TEST_VALUE"].encode()
assert store.put(os.environ["TEST_KEY"], expected, config=policy) == 0
route = store.query_route(os.environ["TEST_KEY"])
assert route is not None
owners = [replica["owner"] for replica in route["replicas"]]
assert owners == [f"{os.environ['STABLE_ID']}:1"], owners
assert store.get(os.environ["TEST_KEY"]) == expected
store.close()
PY

echo "==> sending SIGTERM to predecessor and verifying graceful hot-upgrade"
kill -TERM "${PREDECESSOR_PID}"
wait_for_log "${PREDECESSOR_LOG}" "mooncake-store-client handoff stable_id=${STABLE_ID} from_epoch=1 to_runtime=${STABLE_ID}:2"
wait_for_log "${PREDECESSOR_LOG}" "mooncake-store-client upgraded stable_id=${STABLE_ID} from_epoch=1 to_epoch=2"
wait_for_log "${SUCCESSOR_LOG}" "mooncake-store-client promoted stable_id=${STABLE_ID} epoch=2 from_epoch=1 kind=HotUpgrade"
wait_for_exit "${PREDECESSOR_PID}" "predecessor"

echo "==> verifying payload is still intact on successor after hot-upgrade"
REDIS_URL="${REDIS_URL}" \
KEYSPACE="${KEYSPACE}" \
DRIVER_ID="${DRIVER_ID}-reader" \
STABLE_ID="${STABLE_ID}" \
TEST_KEY="${TEST_KEY}" \
TEST_VALUE="${TEST_VALUE}" \
python3 - <<'PY'
import os
import time

from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
assert store.setup(
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
    route_control="metadata_only",
) == 0

expected = os.environ["TEST_VALUE"].encode()
successor_owner = f"{os.environ['STABLE_ID']}:2"

for _ in range(100):
    route = store.query_route(os.environ["TEST_KEY"])
    if route is None:
        time.sleep(0.1)
        continue
    owners = [replica["owner"] for replica in route["replicas"]]
    if owners != [successor_owner]:
        time.sleep(0.1)
        continue
    payload = store.get(os.environ["TEST_KEY"])
    if payload == expected:
        break
    time.sleep(0.1)
else:
    raise AssertionError("payload was not preserved on the promoted successor")

assert payload == expected
store.close()
PY

echo "CLI hot-upgrade binary test passed"
