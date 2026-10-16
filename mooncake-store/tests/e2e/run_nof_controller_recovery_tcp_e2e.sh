#!/usr/bin/env bash
# Two workers / namespaces recover through a real TCP malloc-bdev target restart.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
BUILD_DIR=${BUILD_DIR:-"$REPO_ROOT/build"}
SPDK_DIR=${SPDK_DIR:-"$REPO_ROOT/extern/spdk"}
LOG_DIR=${LOG_DIR:-$(mktemp -d /tmp/mooncake_nof_recovery_e2e.XXXXXX)}
TIMEOUT_SEC=${TIMEOUT_SEC:-30}
TARGET_HOST=127.0.0.1
TARGET_PORT=${TARGET_PORT:-$(python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    print(sock.getsockname()[1])
PY
)}
TARGET_NQN=${TARGET_NQN:-nqn.2016-06.io.spdk:mooncake-recovery-$$}
TARGET_CORE_MASK=${TARGET_CORE_MASK:-0x1}
TARGET_BIN="$SPDK_DIR/build/bin/nvmf_tgt"
CLIENT_BIN="$BUILD_DIR/mooncake-store/tests/e2e/nof_controller_recovery_e2e"
TARGET_PID=""
CLIENT_PID=""
source "$SCRIPT_DIR/nof_target.sh"

cleanup() {
  local rc=$?
  trap - EXIT
  if [[ -n "$CLIENT_PID" ]]; then
    # timeout creates a process group containing the client.
    kill -KILL -- "-$CLIENT_PID" 2>/dev/null || true
    wait "$CLIENT_PID" 2>/dev/null || true
  fi
  nof_target_stop
  if (( rc != 0 )); then
    echo "FAILED: NoF controller recovery (logs: $LOG_DIR)" >&2
    tail -n 60 "$LOG_DIR/client.log" "$LOG_DIR/target-1.log" \
      "$LOG_DIR/target-2.log" 2>/dev/null >&2 || true
  fi
  exit "$rc"
}

[[ "$TIMEOUT_SEC" =~ ^[1-9][0-9]*$ ]] || { echo "Invalid TIMEOUT_SEC" >&2; exit 2; }
[[ -x "$CLIENT_BIN" && -x "$TARGET_BIN" ]] || {
  echo "Build nof_controller_recovery_e2e (USE_NOF=ON) and SPDK nvmf_tgt first" >&2
  exit 2
}
# Keep previous results; never remove an existing run's directory.
mkdir -p "$LOG_DIR"
[[ -z "$(ls -A "$LOG_DIR")" ]] || { echo "LOG_DIR must be empty" >&2; exit 2; }
LOG_DIR=$(cd -- "$LOG_DIR" && pwd)
RPC_SOCKET="$LOG_DIR/spdk.sock"
[[ ${#RPC_SOCKET} -lt 104 ]] || { echo "LOG_DIR path is too long for an RPC socket" >&2; exit 2; }
trap cleanup EXIT
export LD_LIBRARY_PATH="$BUILD_DIR/mooncake-common${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

start_target() {
  nof_target_start "$LOG_DIR/target-$1.log" || return
  nof_target_add_malloc_ns 1 32 512 || return
  nof_target_add_malloc_ns 2 32 4096 || return
  nof_target_start_listener
}

wait_for_pattern() {
  local pattern=$1 deadline=$((SECONDS + TIMEOUT_SEC))
  until grep -q "$pattern" "$LOG_DIR/client.log"; do
    kill -0 "$CLIENT_PID" 2>/dev/null && (( SECONDS < deadline )) || return 1
    sleep 0.1
  done
}

start_target 1
ENDPOINT="trtype:TCP adrfam:IPv4 traddr:$TARGET_HOST trsvcid:$TARGET_PORT subnqn:$TARGET_NQN"
MC_NVME_FABRICS_CONNECT_TIMEOUT_US=500000 MC_NOF_SUBMIT_CHUNK_BYTES=65536 \
MC_NOF_INFLIGHT_BYTES_LIMIT=131072 \
  timeout --kill-after=5 "$((TIMEOUT_SEC * 6))" "$CLIENT_BIN" \
  --endpoint="$ENDPOINT" --control_dir="$LOG_DIR" --timeout_sec="$TIMEOUT_SEC" \
  >"$LOG_DIR/client.log" 2>&1 &
CLIENT_PID=$!
wait_for_pattern '^PASS initial_io$'

kill -STOP "$TARGET_PID"
deadline=$((SECONDS + TIMEOUT_SEC))
until grep -q '^State:.*T' "/proc/$TARGET_PID/status"; do
  (( SECONDS < deadline )) || exit 1
  sleep 0.01
done
touch "$LOG_DIR/target-paused"
wait_for_pattern '^READY fault_io_pending$'
nof_target_stop
wait_for_pattern '^PASS failed_io_drained$'

start_target 2
touch "$LOG_DIR/target-restarted"
wait_for_pattern '^PASS controller_recovery$'
wait "$CLIENT_PID"
CLIENT_PID=""
echo "PASSED: two-worker NoF controller recovery (logs: $LOG_DIR)"
