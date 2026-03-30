#!/usr/bin/env bash
#
# reproduce_crash.sh — Reproduce mooncake-store hang/crash when master + client
#                      are killed simultaneously
#
# Usage:
#   chmod +x reproduce_crash.sh
#   ./reproduce_crash.sh [--rounds N] [--strategy STRATEGY] [--timeout SECONDS]
#
# Strategies (--strategy):
#   kill9_master_then_term_client  — kill -9 master, then SIGTERM client after 100ms
#   term_both                      — SIGTERM both master and client simultaneously
#   kill9_both                     — kill -9 both master and client simultaneously
#   term_master_then_term_client   — SIGTERM master, then SIGTERM client after 100ms
#   stop_master_then_term_client   — SIGSTOP master (freeze), then SIGTERM client (best for hang reproduction)
#
# Examples:
#   ./reproduce_crash.sh --rounds 5 --strategy kill9_master_then_term_client --timeout 10
#

set -uo pipefail

# ======================== Configuration ========================
MOONCAKE_BUILD="/home/ljh/develop/Mooncake/build"
MASTER_BIN="${MOONCAKE_BUILD}/mooncake-store/src/mooncake_master"
CLIENT_BIN="${MOONCAKE_BUILD}/mooncake-store/src/mooncake_client"
PYTHON_PATH="${MOONCAKE_BUILD}/mooncake-integration"

RPC_PORT=30051
HTTP_META_PORT=8090
METADATA_SERVER="http://127.0.0.1:${HTTP_META_PORT}/metadata"
MASTER_ADDR="127.0.0.1:${RPC_PORT}"
LOCAL_HOST="127.0.0.1"

LOG_DIR="/home/ljh/bug_reproduce/logs"
ROUND_TOTAL=3
STRATEGY="stop_master_then_term_client"
EXIT_TIMEOUT=10   # Timeout in seconds for client process to exit

# ======================== Argument Parsing ========================
while [[ $# -gt 0 ]]; do
    case "$1" in
        --rounds)    ROUND_TOTAL="$2"; shift 2 ;;
        --strategy)  STRATEGY="$2";    shift 2 ;;
        --timeout)   EXIT_TIMEOUT="$2"; shift 2 ;;
        --help|-h)
            head -19 "$0" | tail -17
            exit 0
            ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

mkdir -p "${LOG_DIR}"

# ======================== Utility Functions ========================

log() { >&2 echo "[$(date '+%H:%M:%S')] $*"; }

cleanup_procs() {
    # Resume any frozen processes first, then force kill
    pkill -CONT -f "mooncake_master.*--rpc_port=${RPC_PORT}" 2>/dev/null || true
    pkill -9 -f "mooncake_master.*--rpc_port=${RPC_PORT}" 2>/dev/null || true
    pkill -9 -f "mooncake_client.*--master_server_address=${MASTER_ADDR}" 2>/dev/null || true
    pkill -9 -f "reproduce_client_worker.py" 2>/dev/null || true
    sleep 0.5
}

wait_for_port() {
    local port=$1 max_wait=$2
    for ((i=0; i<max_wait; i++)); do
        if ss -tlnp 2>/dev/null | grep -q ":${port} " || \
           netstat -tlnp 2>/dev/null | grep -q ":${port} "; then
            return 0
        fi
        sleep 1
    done
    return 1
}

start_master() {
    local logfile="${LOG_DIR}/master_round${1}.log"
    log "Starting master (rpc_port=${RPC_PORT}, http_meta_port=${HTTP_META_PORT})"
    "${MASTER_BIN}" \
        --rpc_port="${RPC_PORT}" \
        --enable_http_metadata_server=true \
        --http_metadata_server_port="${HTTP_META_PORT}" \
        --rpc_conn_timeout_seconds=0 \
        --logtostderr=true \
        > "${logfile}" 2>&1 &
    MASTER_PID=$!
    log "master PID=${MASTER_PID}"

    if ! wait_for_port "${RPC_PORT}" 10; then
        log "ERROR: master failed to start within 10s"
        return 1
    fi
    log "master is ready"
}

start_client() {
    local logfile="${LOG_DIR}/client_round${1}.log"
    log "Starting client (master_addr=${MASTER_ADDR})"
    "${CLIENT_BIN}" \
        --master_server_address="${MASTER_ADDR}" \
        --metadata_server="${METADATA_SERVER}" \
        --host="${LOCAL_HOST}" \
        --protocol="tcp" \
        --global_segment_size="256 MB" \
        --logtostderr=true \
        > "${logfile}" 2>&1 &
    CLIENT_PID=$!
    log "client PID=${CLIENT_PID}"
    sleep 3  # Wait for client to complete setup, register segments, start heartbeat
    if ! kill -0 "${CLIENT_PID}" 2>/dev/null; then
        log "ERROR: client process has already exited"
        return 1
    fi
    log "client is ready"
}

# Write test data via Python to increase mounted segment count
write_test_data() {
    local round=$1
    log "Writing test data via Python client..."
    PYTHONPATH="${PYTHON_PATH}" python3 << PYEOF > "${LOG_DIR}/python_round${round}.log" 2>&1 || true
import store
import os, sys, time

try:
    s = store.MooncakeDistributedStore()
    rc = s.setup(
        local_hostname="127.0.0.1",
        metadata_server="${METADATA_SERVER}",
        global_segment_size=256 * 1024 * 1024,
        local_buffer_size=64 * 1024 * 1024,
        protocol="tcp",
        rdma_devices="",
        master_server_addr="${MASTER_ADDR}"
    )
    if rc != 0:
        print(f"setup failed: {rc}")
        sys.exit(1)

    # Write multiple keys to increase mounted segments and active state
    for i in range(10):
        key = f"test_crash_key_{i}"
        data = b"x" * (1024 * 1024)  # 1MB per key
        rc = s.put(key, data)
        print(f"put({key}): {rc}")

    print("Data written, waiting 2s for heartbeat to stabilize...")
    time.sleep(2)

    s.close()
    print("Python client done")
except Exception as e:
    print(f"Python client error: {e}")
    sys.exit(1)
PYEOF
    log "Python client data write completed"
}

# Execute kill strategy
execute_kill_strategy() {
    local strategy=$1
    log "Executing kill strategy: ${strategy}"

    case "${strategy}" in
        kill9_master_then_term_client)
            # Easiest to trigger: master dies instantly, client attempts graceful exit
            log "  kill -9 master (PID=${MASTER_PID})"
            kill -9 "${MASTER_PID}" 2>/dev/null || true
            sleep 0.1
            log "  kill -TERM client (PID=${CLIENT_PID})"
            kill -TERM "${CLIENT_PID}" 2>/dev/null || true
            ;;
        term_both)
            log "  kill -TERM master+client simultaneously"
            kill -TERM "${MASTER_PID}" "${CLIENT_PID}" 2>/dev/null || true
            ;;
        kill9_both)
            log "  kill -9 master+client simultaneously"
            kill -9 "${MASTER_PID}" "${CLIENT_PID}" 2>/dev/null || true
            ;;
        term_master_then_term_client)
            log "  kill -TERM master"
            kill -TERM "${MASTER_PID}" 2>/dev/null || true
            sleep 0.1
            log "  kill -TERM client"
            kill -TERM "${CLIENT_PID}" 2>/dev/null || true
            ;;
        stop_master_then_term_client)
            # SIGSTOP freezes master: process alive but unresponsive to RPC
            # TCP connections stay open, syncAwait never gets a reply — perfect hang simulation
            log "  kill -STOP master (PID=${MASTER_PID}) — frozen, not killed"
            kill -STOP "${MASTER_PID}" 2>/dev/null || true
            sleep 0.1
            log "  kill -TERM client (PID=${CLIENT_PID})"
            kill -TERM "${CLIENT_PID}" 2>/dev/null || true
            ;;
        *)
            log "ERROR: unknown strategy ${strategy}"
            return 1
            ;;
    esac
}

# Monitor client exit behavior
monitor_client_exit() {
    local timeout=$1 round=$2
    local elapsed=0
    local strace_pid=""

    log "Waiting for client (PID=${CLIENT_PID}) to exit, timeout=${timeout}s..."

    # Attempt to attach strace (may fail due to ptrace permissions, non-fatal)
    if command -v strace &>/dev/null; then
        strace -p "${CLIENT_PID}" -f -tt -e trace=futex,connect,sendto,recvfrom \
            -o "${LOG_DIR}/strace_round${round}.log" 2>/dev/null &
        strace_pid=$!
        if kill -0 "${strace_pid}" 2>/dev/null; then
            log "Attached strace (PID=${strace_pid}) to client"
        else
            strace_pid=""
            log "strace attach failed (insufficient permissions), skipping syscall tracing"
        fi
    fi

    while (( elapsed < timeout )); do
        if ! kill -0 "${CLIENT_PID}" 2>/dev/null; then
            wait "${CLIENT_PID}" 2>/dev/null
            local exit_code=$?
            log "client exited within ${elapsed}s (exit_code=${exit_code})"
            if [[ -n "${strace_pid}" ]]; then kill "${strace_pid}" 2>/dev/null || true; fi
            echo "OK:${elapsed}s:exit${exit_code}"
            return 0
        fi
        sleep 1
        ((elapsed++))
    done

    # Timeout — bug triggered
    log "!!! CLIENT HANG DETECTED !!! Process still alive after ${timeout}s"

    # Collect diagnostic information
    log "Collecting diagnostics..."
    {
        echo "=== /proc/${CLIENT_PID}/status ==="
        cat "/proc/${CLIENT_PID}/status" 2>/dev/null || echo "(unable to read)"
        echo ""
        echo "=== /proc/${CLIENT_PID}/stack (kernel stack) ==="
        cat "/proc/${CLIENT_PID}/stack" 2>/dev/null || echo "(requires root privileges)"
        echo ""
        echo "=== /proc/${CLIENT_PID}/task/*/status (all threads) ==="
        for tid_dir in /proc/${CLIENT_PID}/task/*/; do
            tid=$(basename "$tid_dir")
            echo "--- Thread ${tid} ---"
            cat "${tid_dir}/status" 2>/dev/null | grep -E "^(Name|State|voluntary)" || true
        done
        echo ""
        echo "=== pstack/gdb backtrace ==="
        if command -v gdb &>/dev/null; then
            gdb -batch -ex "thread apply all bt" -p "${CLIENT_PID}" 2>/dev/null || echo "(gdb attach failed)"
        elif command -v pstack &>/dev/null; then
            pstack "${CLIENT_PID}" 2>/dev/null || echo "(pstack failed)"
        else
            echo "(gdb/pstack not available)"
        fi
    } > "${LOG_DIR}/diag_round${round}.log" 2>&1

    if [[ -n "${strace_pid}" ]]; then kill "${strace_pid}" 2>/dev/null || true; fi

    # Force kill client
    log "Force killing client (kill -9)"
    kill -9 "${CLIENT_PID}" 2>/dev/null || true
    wait "${CLIENT_PID}" 2>/dev/null || true

    echo "HANG:${timeout}s"
    return 1
}

# ======================== Main Flow ========================

log "======================================================"
log "Mooncake Store Crash Reproduction Script"
log "Strategy: ${STRATEGY}"
log "Rounds: ${ROUND_TOTAL}"
log "Timeout: ${EXIT_TIMEOUT}s"
log "======================================================"

# Ensure clean environment
cleanup_procs

hang_count=0
crash_count=0
ok_count=0

for ((round=1; round<=ROUND_TOTAL; round++)); do
    log ""
    log "==================== Round ${round}/${ROUND_TOTAL} ===================="

    # 1. Start master
    if ! start_master "${round}"; then
        log "master failed to start, skipping this round"
        cleanup_procs
        continue
    fi

    # 2. Start client
    if ! start_client "${round}"; then
        log "client failed to start, skipping this round"
        cleanup_procs
        continue
    fi

    # 3. Write test data (increases segment registration count)
    write_test_data "${round}"

    # 4. Wait for heartbeat to stabilize
    log "Waiting 2s for heartbeat to stabilize..."
    sleep 2

    # 5. Verify processes are still alive
    if ! kill -0 "${MASTER_PID}" 2>/dev/null; then
        log "WARN: master has already exited"
        cleanup_procs
        continue
    fi
    if ! kill -0 "${CLIENT_PID}" 2>/dev/null; then
        log "WARN: client has already exited"
        cleanup_procs
        continue
    fi

    # 6. Execute kill strategy
    execute_kill_strategy "${STRATEGY}"

    # 7. Monitor client exit
    result=$(monitor_client_exit "${EXIT_TIMEOUT}" "${round}" || true)

    if [[ "${result}" == HANG* ]]; then
        ((hang_count++))
        log "Round ${round} result: HANG (bug triggered)"
    elif [[ "${result}" == OK* ]]; then
        exit_time=$(echo "${result}" | cut -d: -f2)
        exit_code=$(echo "${result}" | cut -d: -f3)
        if [[ "${exit_code}" != "exit0" ]]; then
            ((crash_count++))
            log "Round ${round} result: CRASH (${exit_code}, took ${exit_time})"
        else
            ((ok_count++))
            log "Round ${round} result: OK (clean exit, took ${exit_time})"
        fi
    else
        ((crash_count++))
        log "Round ${round} result: UNKNOWN"
    fi

    # Clean up residual processes for next round
    cleanup_procs
    sleep 1
done

# ======================== Summary ========================
log ""
log "======================================================"
log "Test completed — ${ROUND_TOTAL} rounds total"
log "  HANG (bug triggered):  ${hang_count}"
log "  CRASH (abnormal exit): ${crash_count}"
log "  OK (clean exit):       ${ok_count}"
log "======================================================"

if (( hang_count > 0 )); then
    log ""
    log "Bug reproduced! See log directory: ${LOG_DIR}"
    log "  - strace_round*.log:  syscall trace (stuck on futex/connect)"
    log "  - diag_round*.log:    process diagnostics (thread states, backtrace)"
    log "  - client_round*.log:  client stdout/stderr logs"
    log "  - master_round*.log:  master stdout/stderr logs"
    exit 1
fi

exit 0
