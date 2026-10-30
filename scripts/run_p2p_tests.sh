#!/bin/bash
# Script to run the P2P wheel's basic integration tests.
# Usage: ./scripts/run_p2p_tests.sh

set -e  # Exit immediately if a command exits with a non-zero status
set -o pipefail

# Ensure LD_LIBRARY_PATH includes /usr/local/lib.
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:/usr/local/lib"
LOG_ROOT="${MOONCAKE_TEST_LOG_DIR:-/tmp/mooncake-tests}"
mkdir -p "$LOG_ROOT"
LOG_ROOT=$(cd "$LOG_ROOT" && pwd)
LOG_DIR=$(mktemp -d "$LOG_ROOT/p2p.XXXXXX")
echo "P2P test logs: $LOG_DIR"
export TMPDIR="$LOG_DIR" GLOG_logtostderr=1
unset MOONCAKE_CONFIG_PATH MC_RUNTIME_CONFIG

METADATA_SERVER_PID=""
TARGET_PID=""
MASTER_PID=""
CLIENT_PID=""
TEST_PID=""
DUMMY_TEST_PID_1=""
DUMMY_TEST_PID_2=""
RUN_TESTS_METADATA_SERVER_MODE="${RUN_TESTS_METADATA_SERVER_MODE:-managed}"

# Stop only this script's process groups, including on test failure.
stop_process() {
    local pid="$1" attempt code=0 expected=0 forced=0
    [[ -n "$pid" ]] || return 0
    if kill -0 "$pid" 2>/dev/null; then
        expected=143
    fi
    if ! kill -TERM -- "-$pid" 2>/dev/null; then
        if kill -0 "$pid" 2>/dev/null; then
            echo "ERROR: Cannot stop process group $pid" >&2
            return 1
        fi
    fi
    for attempt in {1..150}; do
        kill -0 "$pid" 2>/dev/null || break
        if [[ "$attempt" == 100 ]]; then
            echo "ERROR: Process $pid did not stop in 10 seconds; sending SIGKILL" >&2
            kill -KILL -- "-$pid" || return 1
            forced=1
        fi
        sleep 0.1
    done
    if kill -0 "$pid" 2>/dev/null; then
        echo "ERROR: Process $pid is still alive after cleanup" >&2
        return 1
    fi
    wait "$pid" || code=$?
    if (( forced || (code != 0 && code != expected) )); then
        echo "ERROR: Process $pid exited with status $code" >&2
        return 1
    fi
}

wait_ready() {
    local pid="$1" port="$2" ipc="${3:-}" attempt
    for attempt in {1..300}; do
        if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
            echo "ERROR: Service $pid exited before port $port was ready; logs: $LOG_DIR" >&2
            return 1
        fi
        if ss -H -ltn "sport = :$port" | grep -q .; then
            if [[ -z "$ipc" ]] || ss -H -lx | grep -F "@@mooncake_client_$port.sock" >/dev/null; then
                return 0
            fi
        fi
        sleep 0.1
    done
    echo "ERROR: Service on port $port was not ready within 30 seconds; logs: $LOG_DIR" >&2
    return 1
}

cleanup() {
    local status=$? pid
    trap - EXIT
    for pid in "$TEST_PID" "$DUMMY_TEST_PID_2" "$DUMMY_TEST_PID_1" "$CLIENT_PID" "$MASTER_PID" "$TARGET_PID" "$METADATA_SERVER_PID"; do
        if ! stop_process "$pid"; then
            status=1
        fi
    done
    if [[ "$status" != 0 ]]; then
        echo "ERROR: P2P tests failed; logs: $LOG_DIR" >&2
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for command in mooncake_master mooncake_client; do
    version=$(timeout 10s "$command" --version)
    if ! grep -qx "architecture=p2p" <<<"$version"; then
        echo "ERROR: $command is not the P2P binary: $version" >&2
        exit 1
    fi
done

case "$RUN_TESTS_METADATA_SERVER_MODE" in
    managed)
        echo "Starting standalone HTTP metadata server..."
        setsid mooncake_http_metadata_server --port 8080 >"$LOG_DIR/metadata.log" 2>&1 &
        METADATA_SERVER_PID=$!
        ;;
    external)
        echo "Using caller-managed HTTP metadata server on port 8080"
        ;;
    *)
        echo "ERROR: RUN_TESTS_METADATA_SERVER_MODE must be managed or external" >&2
        exit 1
        ;;
esac
wait_ready "$METADATA_SERVER_PID" 8080

export MC_METADATA_SERVER=http://127.0.0.1:8080/metadata
export LOCAL_HOSTNAME=127.0.0.1 PROTOCOL=tcp DEVICE_NAME=""
export MC_FORCE_TCP=true DEFAULT_KV_LEASE_TTL=500
cd mooncake-wheel/tests

echo "Running transfer_engine tests..."
setsid python transfer_engine_target.py >"$LOG_DIR/transfer-engine-target.log" 2>&1 &
TARGET_PID=$!
setsid python transfer_engine_initiator_test.py >"$LOG_DIR/transfer-engine.log" 2>&1 &
TEST_PID=$!
wait "$TEST_PID"
TEST_PID=""
stop_process "$TARGET_PID"
TARGET_PID=""

echo "Running P2P master tests..."
export MASTER_SERVER=127.0.0.1:50051
export MOONCAKE_TIERED_BACKEND_CONFIG="$LOG_DIR/tiered-backend.json"
cat >"$MOONCAKE_TIERED_BACKEND_CONFIG" <<'JSON'
{"tiers": [{"type": "DRAM", "capacity": 536870912, "priority": 100, "allocator_type": "OFFSET"}]}
JSON
setsid mooncake_master --rpc_address=127.0.0.1 --rpc_port=50051 \
    --metrics_port=0 --enable_metric_reporting=false --enable_ha=false \
    >"$LOG_DIR/master.log" 2>&1 &
MASTER_PID=$!
wait_ready "$MASTER_PID" 50051

# Cross-process I/O must run before other Real clients join this master.
echo "Running P2P cross-process I/O tests..."
timeout --verbose --kill-after=5s 600s python test_p2p_cross_process_io.py \
    >"$LOG_DIR/p2p-cross-process-io.log" 2>&1 &
TEST_PID=$!
wait "$TEST_PID"
TEST_PID=""

echo "Running P2P distributed object store tests..."
timeout --verbose --kill-after=5s 300s python test_p2p_distributed_object_store.py \
    >"$LOG_DIR/p2p-distributed-object-store.log" 2>&1 &
TEST_PID=$!
wait "$TEST_PID"
TEST_PID=""

# Run Tensor tests before stopping a Client that may leave stale segments.
pip install torch numpy
echo "Running Tensor and metadata tests..."
timeout --verbose --kill-after=5s 180s python test_put_get_tensor.py -f \
    >"$LOG_DIR/put-get-tensor.log" 2>&1 &
TEST_PID=$!
wait "$TEST_PID"
TEST_PID=""

export MOONCAKE_REAL_CLIENT_ADDRESS=127.0.0.1:50052
setsid mooncake_client --host=127.0.0.1 --metadata_server="$MC_METADATA_SERVER" \
    --protocol=tcp --client_rpc_port=0 --master_server_address="$MASTER_SERVER" \
    --port=50052 --tiered_backend_config="$MOONCAKE_TIERED_BACKEND_CONFIG" \
    --enable_http_server=false --enable_client_metric_collection=false \
    --async_sender_thread_count=0 --te_async_poll_worker_num=2 >"$LOG_DIR/real.log" 2>&1 &
CLIENT_PID=$!
wait_ready "$CLIENT_PID" 50052 ipc

echo "Running Dummy client tests..."
timeout --verbose --kill-after=5s 180s python test_dummy_client.py \
    >"$LOG_DIR/dummy.log" 2>&1 &
TEST_PID=$!
wait "$TEST_PID"
TEST_PID=""

echo "Running multi-Dummy client tests..."
timeout --verbose --kill-after=5s 180s python test_multi_dummy_clients.py --client-id client1 \
    >"$LOG_DIR/multi-dummy-1.log" 2>&1 &
DUMMY_TEST_PID_1=$!
timeout --verbose --kill-after=5s 180s python test_multi_dummy_clients.py --client-id client2 \
    >"$LOG_DIR/multi-dummy-2.log" 2>&1 &
DUMMY_TEST_PID_2=$!
wait "$DUMMY_TEST_PID_1"
DUMMY_TEST_PID_1=""
wait "$DUMMY_TEST_PID_2"
DUMMY_TEST_PID_2=""
stop_process "$CLIENT_PID"
CLIENT_PID=""
stop_process "$MASTER_PID"
MASTER_PID=""

echo "Running CLI entry point tests..."
setsid python test_cli.py >"$LOG_DIR/cli.log" 2>&1 &
TEST_PID=$!
wait "$TEST_PID"
TEST_PID=""
stop_process "$METADATA_SERVER_PID"
METADATA_SERVER_PID=""

echo "All P2P basic tests completed successfully; logs: $LOG_DIR"
cd ../..
