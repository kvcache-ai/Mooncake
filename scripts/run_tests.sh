#!/bin/bash
# Script to run all tests for the mooncake package
# Usage: ./scripts/run_tests.sh

set -e  # Exit immediately if a command exits with a non-zero status

# Ensure LD_LIBRARY_PATH includes /usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

METADATA_SERVER_PID=""
RUN_TESTS_METADATA_SERVER_MODE="${RUN_TESTS_METADATA_SERVER_MODE:-managed}"

metadata_port_is_listening() {
    ss -H -ltn 'sport = :8080' | grep -q .
}

start_metadata_server() {
    local attempt

    echo "Starting standalone HTTP metadata server..."
    mooncake_http_metadata_server --port 8080 &
    METADATA_SERVER_PID=$!

    for attempt in {1..50}; do
        if ! kill -0 "$METADATA_SERVER_PID" 2>/dev/null; then
            echo "ERROR: metadata server exited before listening on port 8080"
            wait "$METADATA_SERVER_PID" 2>/dev/null || true
            METADATA_SERVER_PID=""
            return 1
        fi

        if metadata_port_is_listening; then
            echo "Standalone HTTP metadata server is ready (pid=$METADATA_SERVER_PID)"
            return 0
        fi
        sleep 0.1
    done

    echo "ERROR: metadata server did not listen on port 8080 within 5 seconds"
    ss -ltnp | grep ':8080' || true
    return 1
}

use_external_metadata_server() {
    local attempt

    echo "Using caller-managed HTTP metadata server on port 8080..."
    for attempt in {1..50}; do
        if metadata_port_is_listening; then
            echo "Caller-managed HTTP metadata server is ready"
            return 0
        fi
        sleep 0.1
    done

    echo "ERROR: caller-managed metadata server is not listening on port 8080"
    return 1
}

stop_metadata_server() {
    local attempt
    local pid="${METADATA_SERVER_PID:-}"

    if [ -z "$pid" ]; then
        return 0
    fi

    echo "Stopping standalone HTTP metadata server (pid=$pid)..."
    kill -TERM "$pid" 2>/dev/null || true

    for attempt in {1..50}; do
        if ! metadata_port_is_listening; then
            wait "$pid" 2>/dev/null || true
            METADATA_SERVER_PID=""
            echo "Standalone HTTP metadata server stopped"
            return 0
        fi
        sleep 0.1
    done

    echo "WARNING: metadata server did not release port 8080 after SIGTERM; sending SIGKILL"
    ps -fp "$pid" || true
    ss -ltnp | grep ':8080' || true
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    METADATA_SERVER_PID=""

    for attempt in {1..50}; do
        if ! metadata_port_is_listening; then
            echo "Standalone HTTP metadata server was force-stopped"
            return 0
        fi
        sleep 0.1
    done

    echo "ERROR: port 8080 is still occupied after stopping metadata server"
    ps -ef | grep '[m]ooncake' || true
    ss -ltnp | grep ':8080' || true
    return 1
}

cleanup_metadata_server() {
    stop_metadata_server || true
}

case "$RUN_TESTS_METADATA_SERVER_MODE" in
    managed)
        trap cleanup_metadata_server EXIT
        start_metadata_server
        ;;
    external)
        use_external_metadata_server
        ;;
    *)
        echo "ERROR: RUN_TESTS_METADATA_SERVER_MODE must be 'managed' or 'external'"
        exit 1
        ;;
esac

echo "Running transfer_engine tests..."
cd mooncake-wheel/tests
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata MC_FORCE_TCP=true python transfer_engine_target.py &
TARGET_PID=$!
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata MC_FORCE_TCP=true python transfer_engine_initiator_test.py
kill $TARGET_PID || true

echo "Running master tests..."

which mooncake_master 2>/dev/null | grep -q '/usr/local/bin/mooncake_master' && \
  { echo "ERROR: mooncake_master found in /usr/local/bin, not installed by python"; exit 1; } || \
  echo "mooncake_master not found in /usr/local/bin, installed by python"

echo "mooncake_master found, running tests..."
# Set a small kv lease ttl to make the test faster.
# Must be consistent with the client test parameters.
mooncake_master --default_kv_lease_ttl=500 &
MASTER_PID=$!
sleep 1
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_distributed_object_store.py
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_replicated_distributed_object_store.py

# Run Tensor tests before stopping a Client that may leave stale segments.
pip install torch numpy
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_put_get_tensor.py
sleep 1
mooncake_client &
CLIENT_PID=$!
sleep 1
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_dummy_client.py
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_multi_dummy_clients.py --client-id client1 &
DUMMY_TEST_PID_1=$!
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_multi_dummy_clients.py --client-id client2 &
DUMMY_TEST_PID_2=$!
wait $DUMMY_TEST_PID_1
wait $DUMMY_TEST_PID_2
kill $CLIENT_PID || true
kill $MASTER_PID || true
wait $MASTER_PID 2>/dev/null || true


# Check if MOONCAKE_STORAGE_ROOT_DIR is set and not empty
if [ -n "$TEST_SSD_OFFLOAD_IN_EVICT" ]; then
    TEST_ROOT_DIR="/tmp/mooncake_test_ssd"
    mkdir -p $TEST_ROOT_DIR
    echo "MOONCAKE_STORAGE_ROOT_DIR is set to: $TEST_ROOT_DIR"
    echo "Running with ssd offload in evict tests..."
    # Set a small kv lease ttl to make the test faster.
    # Must be consistent with the client test parameters.
    mooncake_master --default_kv_lease_ttl=500 --root_fs_dir=$TEST_ROOT_DIR &
    MASTER_PID=$!
    sleep 1
    MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_ssd_offload_in_evict.py
    kill $MASTER_PID || true
    wait $MASTER_PID 2>/dev/null || true
    rm -rf $TEST_ROOT_DIR
else
    echo "Skipping test: MOONCAKE_STORAGE_ROOT_DIR environment variable is not set"
fi

echo "Running CLI entry point tests..."
python test_cli.py

ENABLE_EMBEDDED_METADATA_SERVER=true
if [ "$RUN_TESTS_METADATA_SERVER_MODE" = "managed" ]; then
    stop_metadata_server
else
    ENABLE_EMBEDDED_METADATA_SERVER=false
    echo "Leaving caller-managed HTTP metadata server running"
fi
killall mooncake_master || true
killall mooncake_client || true
mooncake_master \
    --default_kv_lease_ttl=500 \
    --enable_http_metadata_server="$ENABLE_EMBEDDED_METADATA_SERVER" &
MASTER_PID=$!
sleep 1
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_distributed_object_store.py
sleep 1
kill $MASTER_PID || true
wait $MASTER_PID 2>/dev/null || true


echo "All tests completed successfully!"
cd ../..
