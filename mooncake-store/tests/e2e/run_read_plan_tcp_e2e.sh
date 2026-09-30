#!/usr/bin/env bash
# Real TCP ReadPlan tests: sequential and pipeline.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
BUILD_DIR=${BUILD_DIR:-"$REPO_ROOT/build"}
PYTHON=${PYTHON:-python3}
LOG_DIR=${LOG_DIR:-$(mktemp -d /tmp/mooncake_read_plan_tcp_e2e.XXXXXX)}
MASTER_BIN="$BUILD_DIR/mooncake-store/src/mooncake_master"
MASTER_PID=""
PACKAGE_DIR=""

cleanup() {
  if [[ -n "$MASTER_PID" ]]; then
    kill "$MASTER_PID" >/dev/null 2>&1 || true
    for ((i = 0; i < 50; i++)); do
      kill -0 "$MASTER_PID" >/dev/null 2>&1 || break
      sleep 0.1
    done
    kill -KILL "$MASTER_PID" >/dev/null 2>&1 || true
    wait "$MASTER_PID" 2>/dev/null || true
  fi
  [[ -z "$PACKAGE_DIR" ]] || rm -rf -- "$PACKAGE_DIR"
}
trap cleanup EXIT

free_port() {
  "$PYTHON" - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

shopt -s nullglob
STORE_MODULES=("$BUILD_DIR"/mooncake-integration/store*.so)
if [[ ! -x "$MASTER_BIN" || ${#STORE_MODULES[@]} -eq 0 ]]; then
  echo "build mooncake_master and store under $BUILD_DIR before running this test" >&2
  exit 2
fi
mkdir -p "$LOG_DIR"
PACKAGE_DIR=$(mktemp -d /tmp/mooncake_read_plan_python.XXXXXX)
mkdir "$PACKAGE_DIR/mooncake"
touch "$PACKAGE_DIR/mooncake/__init__.py"
for module in "${STORE_MODULES[@]}"; do
  ln -s "$(realpath "$module")" "$PACKAGE_DIR/mooncake/$(basename "$module")"
done

export PYTHONPATH="$PACKAGE_DIR"
export LD_LIBRARY_PATH="$BUILD_DIR/mooncake-store/src:$BUILD_DIR/mooncake-common/etcd:$BUILD_DIR/mooncake-common/src:$BUILD_DIR/mooncake-common:$BUILD_DIR/mooncake-transfer-engine/src${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export MOONCAKE_MASTER="127.0.0.1:$(free_port)"
export MOONCAKE_PROTOCOL=tcp
export MOONCAKE_DEVICE=
export MOONCAKE_TE_META_DATA_SERVER=P2PHANDSHAKE
export MC_FORCE_TCP=1
export MC_STORE_MEMCPY=0
export E2E_NUM_LAYERS=4 E2E_NUM_KEYS=3 E2E_PAGE_SIZE=65536

"$MASTER_BIN" \
  --rpc_address=127.0.0.1 \
  --rpc_port="${MOONCAKE_MASTER##*:}" \
  --metrics_port="$(free_port)" \
  --enable_metric_reporting=false \
  --default_kv_lease_ttl=60000 \
  --logtostderr=true >"$LOG_DIR/master.log" 2>&1 &
MASTER_PID=$!

if ! "$PYTHON" - "$MOONCAKE_MASTER" "$MASTER_PID" <<'PY'
import os
import socket
import sys
import time
host, port = sys.argv[1].rsplit(":", 1)
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    os.kill(int(sys.argv[2]), 0)
    try:
        with socket.create_connection((host, int(port)), 0.2):
            break
    except OSError:
        time.sleep(0.1)
else:
    raise TimeoutError("master did not become ready")
PY
then
  cat "$LOG_DIR/master.log" >&2
  exit 3
fi

for mode in sequential pipeline; do
  export MOONCAKE_READ_PLAN_PIPELINE=0
  [[ "$mode" == sequential ]] || export MOONCAKE_READ_PLAN_PIPELINE=1
  export MOONCAKE_LOCAL_HOSTNAME="127.0.0.1:$(free_port)"
  echo "ReadPlan TCP configuration: $mode"
  if ! timeout 120 "$PYTHON" "$SCRIPT_DIR/read_plan_tcp_e2e.py" 2>&1 | tee "$LOG_DIR/$mode.log"; then
    echo "FAILED: ReadPlan TCP $mode (logs: $LOG_DIR)" >&2
    tail -n 80 "$LOG_DIR/master.log" >&2
    exit 1
  fi
done
echo "PASSED: ReadPlan TCP integration tests (logs: $LOG_DIR)"
