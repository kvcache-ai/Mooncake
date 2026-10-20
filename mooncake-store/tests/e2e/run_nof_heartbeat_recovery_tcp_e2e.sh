#!/usr/bin/env bash
# Real Master heartbeats recover two namespaces without registering them again.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
BUILD_DIR=${BUILD_DIR:-"$REPO_ROOT/build"}
SPDK_DIR=${SPDK_DIR:-"$REPO_ROOT/extern/spdk"}
LOG_DIR=${LOG_DIR:-$(mktemp -d /tmp/mooncake_heartbeat_recovery.XXXXXX)}
TIMEOUT_SEC=${TIMEOUT_SEC:-45}
TARGET_HOST=127.0.0.1
read -r target_port master_port metrics_port < <(python3 - <<'PY'
import socket
sockets = [socket.socket() for _ in range(3)]
try:
    for sock in sockets:
        sock.bind(('127.0.0.1', 0))
    print(*(sock.getsockname()[1] for sock in sockets))
finally:
    for sock in sockets:
        sock.close()
PY
)
TARGET_PORT=${TARGET_PORT:-$target_port}
MASTER_RPC=${MASTER_RPC:-127.0.0.1:$master_port}
MASTER_HOST=${MASTER_RPC%:*}
MASTER_PORT=${MASTER_RPC##*:}
METRICS_PORT=${METRICS_PORT:-$metrics_port}
TARGET_NQN=${TARGET_NQN:-nqn.2016-06.io.spdk:mooncake-heartbeat-$$}
TEST_BIN="$BUILD_DIR/mooncake-store/tests/e2e/nof_heartbeat_recovery_e2e"
MASTER_BIN="$BUILD_DIR/mooncake-store/src/mooncake_master"
TARGET_PID=""
MASTER_PID=""
TEST_PID=""
source "$SCRIPT_DIR/nof_target.sh"

stop_master() {
  local deadline=$((SECONDS + $1)) rc=0
  kill -TERM "$MASTER_PID" || return 1
  while kill -0 "$MASTER_PID" 2>/dev/null; do
    (( SECONDS < deadline )) || return 1
    sleep 0.1
  done
  wait "$MASTER_PID" || rc=$?
  MASTER_PID=""
  return "$rc"
}

cleanup() {
  local rc=$?
  trap - EXIT
  if [[ -n "$TEST_PID" ]]; then
    # GNU timeout owns a process group containing the test.
    kill -KILL -- "-$TEST_PID" 2>/dev/null || true
    wait "$TEST_PID" 2>/dev/null || true
  fi
  if [[ -n "$MASTER_PID" ]]; then
    if ! stop_master 5; then
      if [[ -n "$MASTER_PID" ]]; then
        kill -KILL "$MASTER_PID" 2>/dev/null || true
        wait "$MASTER_PID" 2>/dev/null || true
      fi
    fi
  fi
  nof_target_stop
  if (( rc != 0 )); then
    echo "FAILED: NoF heartbeat recovery (logs: $LOG_DIR)" >&2
    tail -n 60 "$LOG_DIR/client.log" "$LOG_DIR/master.log" "$LOG_DIR/target-1.log" \
      "$LOG_DIR/target-2.log" 2>/dev/null >&2 || true
  fi
  exit "$rc"
}

[[ "$TIMEOUT_SEC" =~ ^[1-9][0-9]*$ ]] && (( TIMEOUT_SEC > 30 )) || {
  echo "TIMEOUT_SEC must be greater than 30" >&2; exit 2
}
[[ -x "$MASTER_BIN" && -x "$TEST_BIN" && -x "$SPDK_DIR/build/bin/nvmf_tgt" ]] || {
  echo "Build mooncake_master, nof_heartbeat_recovery_e2e (USE_NOF=ON) and SPDK nvmf_tgt first" >&2
  exit 2
}
mkdir -p "$LOG_DIR"
[[ -z "$(ls -A "$LOG_DIR")" ]] || { echo "LOG_DIR must be empty" >&2; exit 2; }
LOG_DIR=$(cd -- "$LOG_DIR" && pwd)
RPC_SOCKET="$LOG_DIR/spdk.sock"
[[ ${#RPC_SOCKET} -lt 104 ]] || {
  echo "LOG_DIR path is too long for an RPC socket" >&2; exit 2
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
export LD_LIBRARY_PATH="$BUILD_DIR/mooncake-common${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export MC_RPC_PROTOCOL=tcp
export MC_RPC_TIMEOUT_MS=2000
export MC_RPC_CONNECT_TIMEOUT_MS=2000

start_target() {
  nof_target_start "$LOG_DIR/target-$1.log" || return
  nof_target_add_malloc_ns 1 32 4096 || return
  nof_target_add_malloc_ns 2 32 4096 || return
  nof_target_start_listener
}

wait_for_pattern() {
  local pattern=$1 deadline=$((SECONDS + TIMEOUT_SEC))
  until grep -q "$pattern" "$LOG_DIR/client.log"; do
    kill -0 "$TEST_PID" 2>/dev/null &&
      kill -0 "$MASTER_PID" 2>/dev/null && (( SECONDS < deadline )) || return 1
    sleep 0.1
  done
}

start_target 1
ENDPOINT="trtype:TCP adrfam:IPv4 traddr:$TARGET_HOST trsvcid:$TARGET_PORT subnqn:$TARGET_NQN"
MC_NVME_FABRICS_CONNECT_TIMEOUT_US=500000 \
  "$MASTER_BIN" --rpc_address="$MASTER_HOST" --rpc_port="$MASTER_PORT" \
  --metrics_port="$METRICS_PORT" --enable_ha=false \
  --enable_http_metadata_server=false --logtostderr=true --v=1 \
  --nof_heartbeat_interval_sec=10 --nof_heartbeat_probe_timeout_ms=1000 \
  --nof_heartbeat_failures_threshold=3 >"$LOG_DIR/master.log" 2>&1 &
MASTER_PID=$!

# The helper only registers/queries through RPC and observes Master logs.
# It never creates MasterService or drives SPDK recovery.
timeout --kill-after=5 "$((TIMEOUT_SEC * 6))" "$TEST_BIN" \
  --master_server="$MASTER_RPC" --endpoint="$ENDPOINT" \
  --control_dir="$LOG_DIR" --timeout_sec="$TIMEOUT_SEC" \
  >"$LOG_DIR/client.log" 2>&1 &
TEST_PID=$!
wait_for_pattern '^PASS initial_heartbeats$'

kill -STOP "$TARGET_PID"
deadline=$((SECONDS + TIMEOUT_SEC))
until grep -q '^State:.*T' "/proc/$TARGET_PID/status"; do
  (( SECONDS < deadline )) || exit 1
  sleep 0.01
done
touch "$LOG_DIR/target-paused"
wait_for_pattern '^PASS probe_timeout$'
nof_target_stop

# Recreate the target with the same endpoint and namespace configuration.
start_target 2
touch "$LOG_DIR/target-restarted"
wait_for_pattern '^PASS heartbeats_recovered$'

nof_target_stop
touch "$LOG_DIR/target-down"
wait_for_pattern '^PASS heartbeat_checks$'
wait "$TEST_PID"
TEST_PID=""
stop_master "$TIMEOUT_SEC"
echo "PASS master_shutdown"
echo "PASSED: Master NoF heartbeat recovery (logs: $LOG_DIR)"
