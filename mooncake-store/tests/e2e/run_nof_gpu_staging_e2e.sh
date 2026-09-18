#!/usr/bin/env bash
# Verify GPU multi-buffer PUT/GET using a private SPDK TCP malloc target.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
BUILD_DIR=${BUILD_DIR:-"$REPO_ROOT/build"}
SPDK_DIR=${SPDK_DIR:-"$REPO_ROOT/extern/spdk"}
NOF_TEST_PYTHON=${NOF_TEST_PYTHON:-"$REPO_ROOT/.venv/bin/python"}

TARGET_PID=""
MASTER_PID=""
CLIENT_PID=""
LOG_PID=""

die() {
    echo "ERROR: $*" >&2
    exit 1
}

check_environment() {
    [[ -x "$NOF_TEST_PYTHON" ]] ||
        die "Set NOF_TEST_PYTHON to a virtualenv Python"
    [[ -d "$BUILD_DIR" && -d "$SPDK_DIR" ]] ||
        die "Set BUILD_DIR and SPDK_DIR to existing builds"

    BUILD_DIR=$(cd -- "$BUILD_DIR" && pwd)
    SPDK_DIR=$(cd -- "$SPDK_DIR" && pwd)
    MASTER_BIN="$BUILD_DIR/mooncake-store/src/mooncake_master"
    TARGET_BIN="$SPDK_DIR/build/bin/nvmf_tgt"
    [[ -x "$MASTER_BIN" && -x "$TARGET_BIN" ]] ||
        die "Build mooncake_master and nvmf_tgt first"
    [[ -f "$SPDK_DIR/scripts/rpc.py" ]] || die "Missing SPDK scripts/rpc.py"

    local option
    for option in USE_NOF USE_CUDA; do
        grep -Eq "^$option:BOOL=(ON|TRUE|YES|1)$" "$BUILD_DIR/CMakeCache.txt" ||
            die "Configure the Mooncake build with -D$option=ON"
    done
    [[ -w /dev/hugepages ]] ||
        die "Prepare writable hugepages for this user before running"

    export PYTHONPATH="$BUILD_DIR/mooncake-integration${PYTHONPATH:+:$PYTHONPATH}"
    local library_dirs="$BUILD_DIR/mooncake-store/src:$BUILD_DIR/mooncake-common"
    library_dirs+=":$BUILD_DIR/mooncake-transfer-engine/src"
    export LD_LIBRARY_PATH="$library_dirs${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    export PYTHONDONTWRITEBYTECODE=1
    export MC_FORCE_TCP=1 MC_RPC_PROTOCOL=tcp MC_NOF_TRTYPE=TCP
    export MC_USE_TENT=0 MC_MS_AUTO_DISC=0
    unset MC_STORE_USE_HUGEPAGE

    "$NOF_TEST_PYTHON" - "$BUILD_DIR/mooncake-integration" <<'PY'
import pathlib
import sys

import store
import torch

extension_dir = pathlib.Path(sys.argv[1]).resolve()
if pathlib.Path(store.__file__).resolve().parent != extension_dir:
    sys.exit("Expected the store extension from BUILD_DIR/mooncake-integration")
if not torch.cuda.is_available():
    sys.exit("A CUDA GPU and CUDA-enabled PyTorch are required")
print(f"Using store extension: {store.__file__}", flush=True)
PY
}

cleanup() {
    local rc=$?
    local pid log
    trap - EXIT
    for pid in "$CLIENT_PID" "$LOG_PID" "$MASTER_PID" "$TARGET_PID"; do
        if [[ -n "$pid" ]]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    echo "Logs: $RUN_DIR"
    if (( rc != 0 )); then
        for log in "$RUN_DIR"/*.log; do
            [[ ! -f "$log" ]] || tail -n 20 "$log" >&2
        done
    fi
    exit "$rc"
}

prepare_run() {
    RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/mc-nof-staging.XXXXXX")
    RPC_SOCKET="$RUN_DIR/target.sock"
    trap cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    echo "[e2e] Logs: $RUN_DIR"

    local settings default_cpumask
    settings=$("$NOF_TEST_PYTHON" - <<'PY'
import os
import socket

sockets = [socket.socket() for _ in range(3)]
for sock in sockets:
    sock.bind(("127.0.0.1", 0))
print(*(sock.getsockname()[1] for sock in sockets),
      hex(1 << min(os.sched_getaffinity(0))))
for sock in sockets:
    sock.close()
PY
    )
    read -r MASTER_PORT METRICS_PORT TARGET_PORT default_cpumask <<< "$settings"
    MASTER_RPC="127.0.0.1:$MASTER_PORT"
    TARGET_NQN="nqn.2026-09.io.mooncake:staging-$$"
    SPDK_CPUMASK=${SPDK_CPUMASK:-"$default_cpumask"}
}

target_rpc() {
    "$NOF_TEST_PYTHON" "$SPDK_DIR/scripts/rpc.py" \
        -s "$RPC_SOCKET" -t 30 "$@"
}

wait_for_target_rpc() {
    local attempt
    for ((attempt = 0; attempt < 100; attempt++)); do
        kill -0 "$TARGET_PID" 2>/dev/null || die "SPDK target exited"
        [[ ! -S "$RPC_SOCKET" ]] || break
        sleep 0.1
    done
    [[ -S "$RPC_SOCKET" ]] || die "SPDK target RPC socket did not become ready"
}

start_target() {
    echo "[e2e] Starting SPDK TCP target"
    (cd "$RUN_DIR" && exec "$TARGET_BIN" -m "$SPDK_CPUMASK" --no-pci -s 256 \
        --iova-mode=va --wait-for-rpc -r "$RPC_SOCKET") >"$RUN_DIR/target.log" 2>&1 &
    TARGET_PID=$!
    wait_for_target_rpc
    {
        target_rpc framework_start_init
        target_rpc framework_wait_init
        target_rpc bdev_malloc_create -b StagingMemory 64 4096
        target_rpc nvmf_create_transport -t TCP
        target_rpc nvmf_create_subsystem "$TARGET_NQN" -a -s NOFSTAGING0000001
        target_rpc nvmf_subsystem_add_ns "$TARGET_NQN" StagingMemory
        target_rpc nvmf_subsystem_add_listener "$TARGET_NQN" \
            -t tcp -a 127.0.0.1 -s "$TARGET_PORT"
    } >"$RUN_DIR/target-setup.log" 2>&1
    echo "[e2e] SPDK target ready: 127.0.0.1:$TARGET_PORT"
}

wait_for_master() {
    "$NOF_TEST_PYTHON" - "$MASTER_PID" "$MASTER_PORT" <<'PY'
import os
import socket
import sys
import time

master_pid = int(sys.argv[1])
master_port = int(sys.argv[2])
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    try:
        os.kill(master_pid, 0)
    except ProcessLookupError:
        sys.exit("Master exited before becoming ready")
    try:
        with socket.create_connection(("127.0.0.1", master_port), timeout=0.2):
            break
    except OSError:
        time.sleep(0.1)
else:
    sys.exit("Master RPC port did not become ready")
PY
}

start_master() {
    echo "[e2e] Starting master: $MASTER_RPC"
    (cd "$RUN_DIR" && exec "$MASTER_BIN" --rpc_address=127.0.0.1 \
        --rpc_port="$MASTER_PORT" \
        --metrics_host=127.0.0.1 --metrics_port="$METRICS_PORT" \
        --enable_ha=false \
        --enable_http_metadata_server=false --logtostderr=true) >"$RUN_DIR/master.log" 2>&1 &
    MASTER_PID=$!
    wait_for_master
    echo "[e2e] Master ready"
}

register_nof() {
    echo "[e2e] Registering NOF segment"
    "$NOF_TEST_PYTHON" - "$TARGET_NQN" "$TARGET_PORT" "$MASTER_RPC" \
        >"$RUN_DIR/register.log" 2>&1 <<'PY'
import sys

import store

target_nqn, target_port, master_rpc = sys.argv[1:]
rc = store.MooncakeDistributedNoFRegister().real_register(
    target_nqn, 1, "127.0.0.1", int(target_port), 0, 64 * 1024 * 1024, master_rpc
)
print(f"register_ret {rc}", flush=True)
sys.exit(0 if rc == 0 else 1)
PY
    echo "[e2e] NOF segment registered"
}

run_test() {
    local rc
    export MOONCAKE_NOF_STAGING_TEST_HOST=127.0.0.1
    export MOONCAKE_NOF_STAGING_TEST_MASTER="$MASTER_RPC"
    echo "[e2e] Running GPU -> NOF -> GPU test (master=$MASTER_RPC)"
    : > "$RUN_DIR/client.log"
    timeout --kill-after=5s 60s "$NOF_TEST_PYTHON" -u \
        "$SCRIPT_DIR/test_nof_gpu_staging.py" -v >"$RUN_DIR/client.log" 2>&1 &
    CLIENT_PID=$!
    tail --pid="$CLIENT_PID" --sleep-interval=0.1 -n +1 -f \
        "$RUN_DIR/client.log" &
    LOG_PID=$!
    if wait "$CLIENT_PID"; then rc=0; else rc=$?; fi
    CLIENT_PID=""
    wait "$LOG_PID" || true
    LOG_PID=""
    echo "[e2e] Client exited: rc=$rc"
    return "$rc"
}

main() {
    echo "[e2e] Checking environment"
    check_environment
    echo "[e2e] Environment ready"
    prepare_run
    start_target
    start_master
    register_nof
    run_test
}

main "$@"
