#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)
REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-6380}"

usage() {
  cat <<'EOF'
Usage: scripts/test-client-hot-upgrade-cli.sh

Build and directly execute the standalone mooncake-store-client binary, then
verify hot-upgrade startup flags and SIGTERM-triggered graceful handoff with
real processes.

Environment:
  MC_STORE_RS_REDIS_PORT      Redis port for the temporary metadata backend
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

  echo "${command_name} is required for CLI hot-upgrade verification" >&2
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

print_log() {
  local log_file=$1
  if [[ -f "${log_file}" ]]; then
    echo "--- ${log_file} ---" >&2
    cat "${log_file}" >&2
  fi
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
  rm -rf "${TEMP_DIR}"
  exit "${status}"
}
trap cleanup EXIT

require_command cargo
require_command redis-cli
require_command redis-server

UPSTREAM_BUILD_DIR=$(resolve_upstream_build_dir)
UPSTREAM_DIR=$(cd -- "${UPSTREAM_BUILD_DIR}/.." && pwd)
export MOONCAKE_UPSTREAM_DIR="${UPSTREAM_DIR}"
export MOONCAKE_UPSTREAM_BUILD_DIR="${UPSTREAM_BUILD_DIR}"
export LD_LIBRARY_PATH="${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/src:${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/tent/src:${LD_LIBRARY_PATH:-}"

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
cargo build -p mooncake-store-py --bin mooncake-store-client

BIN="${REPO_ROOT}/target/debug/mooncake-store-client"
if [[ ! -x "${BIN}" ]]; then
  echo "expected binary was not produced at ${BIN}" >&2
  exit 1
fi

RUN_ID=$(date +%s%N)
KEYSPACE="mc/store-rs/test-client-hot-upgrade-cli/${RUN_ID}"
REDIS_URL="redis://127.0.0.1:${REDIS_PORT}/0"
STABLE_ID="cli-hot-upgrade-${RUN_ID}"
PREDECESSOR_LOG="${TEMP_DIR}/predecessor.log"
SUCCESSOR_LOG="${TEMP_DIR}/successor.log"
PREDECESSOR_SEGMENT="cli-predecessor-${RUN_ID}"
SUCCESSOR_SEGMENT="cli-successor-${RUN_ID}"
COMMON_ARGS=(
  --local-hostname 127.0.0.1
  --metadata-url "${REDIS_URL}"
  --storage-bytes 1048576
  --scratch-bytes 1048576
  --protocol tcp
  --keyspace "${KEYSPACE}"
  --lease-ttl-ms 4000
  --heartbeat-interval-ms 500
  --label pool=pool-a
  --label storage=true
  --drain-on-exit
)

echo "==> starting standby successor binary"
"${BIN}" \
  "${COMMON_ARGS[@]}" \
  --stable-id "${STABLE_ID}" \
  --epoch 2 \
  --initial-state standby \
  --local-segment-name "${SUCCESSOR_SEGMENT}" \
  >"${SUCCESSOR_LOG}" 2>&1 &
SUCCESSOR_PID=$!
PIDS+=("${SUCCESSOR_PID}")
wait_for_log "${SUCCESSOR_LOG}" "mooncake-store-client started stable_id=${STABLE_ID} epoch=2 initial_state=standby segment=${SUCCESSOR_SEGMENT}"

echo "==> starting active predecessor binary"
"${BIN}" \
  "${COMMON_ARGS[@]}" \
  --stable-id "${STABLE_ID}" \
  --epoch 1 \
  --initial-state active \
  --local-segment-name "${PREDECESSOR_SEGMENT}" \
  >"${PREDECESSOR_LOG}" 2>&1 &
PREDECESSOR_PID=$!
PIDS+=("${PREDECESSOR_PID}")
wait_for_log "${PREDECESSOR_LOG}" "mooncake-store-client started stable_id=${STABLE_ID} epoch=1 initial_state=active segment=${PREDECESSOR_SEGMENT}"

echo "==> sending SIGTERM to predecessor and verifying graceful hot-upgrade"
kill -TERM "${PREDECESSOR_PID}"
wait_for_log "${PREDECESSOR_LOG}" "mooncake-store-client handoff stable_id=${STABLE_ID} from_epoch=1 to_runtime=${STABLE_ID}:2"
wait_for_log "${PREDECESSOR_LOG}" "mooncake-store-client upgraded stable_id=${STABLE_ID} from_epoch=1 to_epoch=2"
wait_for_log "${SUCCESSOR_LOG}" "mooncake-store-client promoted stable_id=${STABLE_ID} epoch=2 from_epoch=1 kind=HotUpgrade"
wait_for_exit "${PREDECESSOR_PID}" "predecessor"

echo "==> sending SIGTERM to promoted successor and verifying clean shutdown"
kill -TERM "${SUCCESSOR_PID}"
wait_for_log "${SUCCESSOR_LOG}" "mooncake-store-client drained stable_id=${STABLE_ID} evacuated_routes=0"
wait_for_log "${SUCCESSOR_LOG}" "mooncake-store-client stopped stable_id=${STABLE_ID}"
wait_for_exit "${SUCCESSOR_PID}" "successor"

PIDS=()

echo "CLI hot-upgrade binary test passed"
