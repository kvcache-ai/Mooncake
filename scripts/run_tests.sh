#!/bin/bash
# Script to run all tests for the mooncake package
# Usage: ./scripts/run_tests.sh

set -e  # Exit immediately if a command exits with a non-zero status

# Ensure LD_LIBRARY_PATH includes /usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib


DEFAULT_TEST_PORT_BASE=${MOONCAKE_DEFAULT_TEST_PORT_BASE:-61000}
TEST_PORTS_PER_RUN=${MOONCAKE_TEST_PORTS_PER_RUN:-100}

allocate_port_block() {
    if [ -n "${MOONCAKE_TEST_PORT_BASE:-}" ]; then
        TEST_PORT_BASE=$MOONCAKE_TEST_PORT_BASE
        return
    fi

    local state_file=${MOONCAKE_TEST_PORT_STATE_FILE:-/tmp/mooncake_run_tests_next_port}
    local lock_file=${MOONCAKE_TEST_PORT_LOCK_FILE:-/tmp/mooncake_run_tests_port.lock}
    local next_base

    exec 9>"$lock_file"
    flock 9

    if [ -f "$state_file" ]; then
        next_base=$(cat "$state_file")
    else
        next_base=$DEFAULT_TEST_PORT_BASE
    fi

    if ! [[ "$next_base" =~ ^[0-9]+$ ]] || [ "$next_base" -lt "$DEFAULT_TEST_PORT_BASE" ] || [ $((next_base + TEST_PORTS_PER_RUN)) -gt 65535 ]; then
        next_base=$DEFAULT_TEST_PORT_BASE
    fi

    TEST_PORT_BASE=$next_base
    echo $((TEST_PORT_BASE + TEST_PORTS_PER_RUN)) > "$state_file"

    flock -u 9
}

allocate_test_ports() {
    allocate_port_block
    NEXT_TEST_PORT=$TEST_PORT_BASE

    allocate_named_port METADATA_PORT
    allocate_named_port MAIN_MASTER_PORT
    allocate_named_port MAIN_MASTER_METRICS_PORT
    allocate_named_port DUMMY_CLIENT_PORT
    allocate_named_port MULTI_DUMMY_CLIENT_PORT
    allocate_named_port SSD_MASTER_PORT
    allocate_named_port SSD_MASTER_METRICS_PORT
    allocate_named_port PROMOTION_MASTER_PORT
    allocate_named_port PROMOTION_MASTER_METRICS_PORT
    allocate_named_port CXL_MASTER_PORT
    allocate_named_port CXL_MASTER_METRICS_PORT
    allocate_named_port CLI_MASTER_PORT
    allocate_named_port CLI_CLIENT_PORT
    allocate_named_port CLI_HTTP_METADATA_PORT
    allocate_named_port CLI_METRICS_PORT
    allocate_named_port EMBEDDED_HTTP_METADATA_PORT
    allocate_named_port EMBEDDED_MASTER_PORT
    allocate_named_port EMBEDDED_MASTER_METRICS_PORT
}

allocate_port() {
    ALLOCATED_PORT=$NEXT_TEST_PORT
    NEXT_TEST_PORT=$((NEXT_TEST_PORT + 1))
}

allocate_named_port() {
    allocate_port
    printf -v "$1" '%s' "$ALLOCATED_PORT"
}

start_metadata() {
    METADATA_SERVER_URL="http://127.0.0.1:${METADATA_PORT}/metadata"
    mooncake_http_metadata_server --port ${METADATA_PORT} &
    METADATA_PID=$!
    sleep 1
}

stop_metadata() {
    kill $METADATA_PID || true
    wait $METADATA_PID 2>/dev/null || true
}

start_master() {
    MASTER_PORT=$1
    MASTER_METRICS_PORT=$2
    shift 2
    mooncake_master --port=${MASTER_PORT} --metrics_port=${MASTER_METRICS_PORT} "$@" &
    MASTER_PID=$!
    MASTER_SERVER_ADDR="127.0.0.1:${MASTER_PORT}"
    sleep 1
}

stop_master() {
    kill $MASTER_PID || true
    wait $MASTER_PID 2>/dev/null || true
}

start_client() {
    CLIENT_PORT=$1
    shift
    mooncake_client --master_server_address=${MASTER_SERVER_ADDR} --metadata_server=${METADATA_SERVER_URL} --port=${CLIENT_PORT} "$@" &
    CLIENT_PID=$!
    sleep 1
}

stop_client() {
    kill $CLIENT_PID || true
    wait $CLIENT_PID 2>/dev/null || true
}

allocate_test_ports
echo "Using Mooncake test port block ${TEST_PORT_BASE}-$((TEST_PORT_BASE + TEST_PORTS_PER_RUN - 1))"

echo "Running transfer_engine tests..."
cd mooncake-wheel/tests
start_metadata
MC_METADATA_SERVER=${METADATA_SERVER_URL} MC_FORCE_TCP=true python transfer_engine_target.py &
TARGET_PID=$!
MC_METADATA_SERVER=${METADATA_SERVER_URL} MC_FORCE_TCP=true python transfer_engine_initiator_test.py
kill $TARGET_PID || true

echo "Running master tests..."

which mooncake_master 2>/dev/null | grep -q '/usr/local/bin/mooncake_master' && \
  { echo "ERROR: mooncake_master found in /usr/local/bin, not installed by python"; exit 1; } || \
  echo "mooncake_master not found in /usr/local/bin, installed by python"

echo "mooncake_master found, running tests..."
# Set a small kv lease ttl to make the test faster.
# Must be consistent with the client test parameters.
start_master ${MAIN_MASTER_PORT} ${MAIN_MASTER_METRICS_PORT} --default_kv_lease_ttl=500
MC_METADATA_SERVER=${METADATA_SERVER_URL} MASTER_SERVER=${MASTER_SERVER_ADDR} DEFAULT_KV_LEASE_TTL=500 python test_distributed_object_store.py
MC_METADATA_SERVER=${METADATA_SERVER_URL} MASTER_SERVER=${MASTER_SERVER_ADDR} DEFAULT_KV_LEASE_TTL=500 python test_replicated_distributed_object_store.py

start_client ${DUMMY_CLIENT_PORT}
MC_METADATA_SERVER=${METADATA_SERVER_URL} MASTER_SERVER=${MASTER_SERVER_ADDR} DEFAULT_KV_LEASE_TTL=500 MOONCAKE_CLIENT_PORT=${DUMMY_CLIENT_PORT} python test_dummy_client.py
stop_client

start_client ${MULTI_DUMMY_CLIENT_PORT}
MC_METADATA_SERVER=${METADATA_SERVER_URL} MASTER_SERVER=${MASTER_SERVER_ADDR} DEFAULT_KV_LEASE_TTL=500 MOONCAKE_CLIENT_PORT=${MULTI_DUMMY_CLIENT_PORT} python test_multi_dummy_clients.py --client-id client1 &
DUMMY_TEST_PID_1=$!
MC_METADATA_SERVER=${METADATA_SERVER_URL} MASTER_SERVER=${MASTER_SERVER_ADDR} DEFAULT_KV_LEASE_TTL=500 MOONCAKE_CLIENT_PORT=${MULTI_DUMMY_CLIENT_PORT} python test_multi_dummy_clients.py --client-id client2 &
DUMMY_TEST_PID_2=$!
wait $DUMMY_TEST_PID_1 $DUMMY_TEST_PID_2
stop_client

pip install numpy safetensors packaging
# Keep the test torch aligned with the EP/PG variants packaged into the CI wheel.
pip install "${MOONCAKE_TEST_TORCH_SPEC:-torch==2.11.0+cu128}" \
    --index-url "${MOONCAKE_TEST_TORCH_INDEX_URL:-https://download.pytorch.org/whl/cu128}" \
    --extra-index-url https://pypi.org/simple
MC_METADATA_SERVER=${METADATA_SERVER_URL} MASTER_SERVER=${MASTER_SERVER_ADDR} DEFAULT_KV_LEASE_TTL=500 python test_put_get_tensor.py
MC_METADATA_SERVER=${METADATA_SERVER_URL} MASTER_SERVER=${MASTER_SERVER_ADDR} DEFAULT_KV_LEASE_TTL=500 python test_safetensor_functions.py
stop_master


# Check if MOONCAKE_STORAGE_ROOT_DIR is set and not empty
if [ -n "$TEST_SSD_OFFLOAD_IN_EVICT" ]; then
    TEST_ROOT_DIR="/tmp/mooncake_test_ssd"
    mkdir -p $TEST_ROOT_DIR
    echo "MOONCAKE_STORAGE_ROOT_DIR is set to: $TEST_ROOT_DIR"
    echo "Running with ssd offload in evict tests..."
    # Set a small kv lease ttl to make the test faster.
    # Must be consistent with the client test parameters.
    start_master ${SSD_MASTER_PORT} ${SSD_MASTER_METRICS_PORT} --default_kv_lease_ttl=500 --root_fs_dir=$TEST_ROOT_DIR
    MC_METADATA_SERVER=${METADATA_SERVER_URL} MASTER_SERVER=${MASTER_SERVER_ADDR} DEFAULT_KV_LEASE_TTL=500 python test_ssd_offload_in_evict.py
    stop_master
    rm -rf $TEST_ROOT_DIR
else
    echo "Skipping test: MOONCAKE_STORAGE_ROOT_DIR environment variable is not set"
fi

if [ -n "$TEST_PROMOTION_ON_HIT" ]; then
    TEST_ROOT_DIR="/tmp/mooncake_test_promotion"
    mkdir -p $TEST_ROOT_DIR
    echo "Running L2->L1 promotion-on-hit e2e test..."
    # offload_on_evict drives the prerequisite SSD-only state; promotion_on_hit
    # turns the read path into a promotion trigger; threshold=1 makes the test
    # deterministic. --root_fs_dir is required so the master returns a non-
    # empty fsdir from GetStorageConfig, which is the trigger that initializes
    # the client's FileStorage (and therefore the offload heartbeat).
    start_master ${PROMOTION_MASTER_PORT} ${PROMOTION_MASTER_METRICS_PORT} \
        --default_kv_lease_ttl=500 \
        --root_fs_dir=$TEST_ROOT_DIR \
        --enable_offload=true \
        --offload_on_evict=true \
        --promotion_on_hit=true \
        --promotion_admission_threshold=1
    # Lower bucket-flush thresholds so the test workload (~64 MB) actually
    # writes to disk rather than sitting in the bucket backend's ungrouped
    # pool until the default 500-key / 256-MB bucket fills.
    MC_METADATA_SERVER=${METADATA_SERVER_URL} \
        MASTER_SERVER=${MASTER_SERVER_ADDR} \
        DEFAULT_KV_LEASE_TTL=500 \
        MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=$TEST_ROOT_DIR \
        MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT=10 \
        MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760 \
        python test_promotion_on_hit.py
    stop_master
    rm -rf $TEST_ROOT_DIR
else
    echo "Skipping test: TEST_PROMOTION_ON_HIT environment variable is not set"
fi

echo "Running CXL protocol test (test_distributed_object_store_cxl.py)..."
killall mooncake_master || true
sleep 2

echo "Starting Mooncake Master with CXL enabled (--enable_cxl=true)..."
start_master ${CXL_MASTER_PORT} ${CXL_MASTER_METRICS_PORT} \
  --default_kv_lease_ttl=500 \
  --enable_cxl=true
sleep 2
MC_METADATA_SERVER=${METADATA_SERVER_URL} MASTER_SERVER=${MASTER_SERVER_ADDR} DEFAULT_KV_LEASE_TTL=500 python test_distributed_object_store_cxl.py
stop_master
sleep 2
echo "CXL protocol test completed successfully!"
stop_metadata

echo "Running CLI entry point tests..."
MOONCAKE_CLI_MASTER_PORT=${CLI_MASTER_PORT} \
    MOONCAKE_CLI_CLIENT_PORT=${CLI_CLIENT_PORT} \
    MOONCAKE_CLI_HTTP_METADATA_PORT=${CLI_HTTP_METADATA_PORT} \
    MOONCAKE_CLI_METRICS_PORT=${CLI_METRICS_PORT} \
    python test_cli.py

killall mooncake_http_metadata_server || true
killall mooncake_master || true
killall mooncake_client || true
start_master ${EMBEDDED_MASTER_PORT} ${EMBEDDED_MASTER_METRICS_PORT} --default_kv_lease_ttl=500 \
    --enable_http_metadata_server=true \
    --http_metadata_server_port=${EMBEDDED_HTTP_METADATA_PORT}
MC_METADATA_SERVER=http://127.0.0.1:${EMBEDDED_HTTP_METADATA_PORT}/metadata MASTER_SERVER=${MASTER_SERVER_ADDR} DEFAULT_KV_LEASE_TTL=500 python test_distributed_object_store.py
sleep 1
stop_master


echo "All tests completed successfully!"
cd ../..
