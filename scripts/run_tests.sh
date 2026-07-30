#!/bin/bash
# Script to run all tests for the mooncake package
# Usage: ./scripts/run_tests.sh

set -e  # Exit immediately if a command exits with a non-zero status

# Ensure LD_LIBRARY_PATH includes /usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

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
sleep 1
mooncake_client &
CLIENT_PID=$!
sleep 1
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_dummy_client.py
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_multi_dummy_clients.py --client-id client1 &
DUMMY_TEST_PID_1=$!
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_multi_dummy_clients.py --client-id client2 &
DUMMY_TEST_PID_2=$!
wait $DUMMY_TEST_PID_1 $DUMMY_TEST_PID_2
kill $CLIENT_PID || true

pip install numpy safetensors packaging
# Keep the test torch aligned with the EP/PG variants packaged into the CI wheel.
pip install "${MOONCAKE_TEST_TORCH_SPEC:-torch==2.11.0+cu128}" \
    --index-url "${MOONCAKE_TEST_TORCH_INDEX_URL:-https://download.pytorch.org/whl/cu128}" \
    --extra-index-url https://pypi.org/simple
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_put_get_tensor.py
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_safetensor_functions.py
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

if [ -n "$TEST_PROMOTION_ON_HIT" ]; then
    TEST_ROOT_DIR="/tmp/mooncake_test_promotion"
    mkdir -p $TEST_ROOT_DIR
    echo "Running L2->L1 promotion-on-hit e2e test..."
    # offload_on_evict drives the prerequisite SSD-only state; promotion_on_hit
    # turns the read path into a promotion trigger; threshold=1 makes the test
    # deterministic. --root_fs_dir is required so the master returns a non-
    # empty fsdir from GetStorageConfig, which is the trigger that initializes
    # the client's FileStorage (and therefore the offload heartbeat).
    mooncake_master \
        --default_kv_lease_ttl=500 \
        --root_fs_dir=$TEST_ROOT_DIR \
        --enable_offload=true \
        --offload_on_evict=true \
        --promotion_on_hit=true \
        --promotion_admission_threshold=1 &
    MASTER_PID=$!
    sleep 1
    # Lower bucket-flush thresholds so the test workload (~64 MB) actually
    # writes to disk rather than sitting in the bucket backend's ungrouped
    # pool until the default 500-key / 256-MB bucket fills.
    MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
        DEFAULT_KV_LEASE_TTL=500 \
        MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=$TEST_ROOT_DIR \
        MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT=10 \
        MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760 \
        python test_promotion_on_hit.py
    kill $MASTER_PID || true
    wait $MASTER_PID 2>/dev/null || true
    rm -rf $TEST_ROOT_DIR
else
    echo "Skipping test: TEST_PROMOTION_ON_HIT environment variable is not set"
fi

echo "Running CXL protocol test (test_distributed_object_store_cxl.py)..."
killall mooncake_master || true
sleep 2

echo "Starting Mooncake Master with CXL enabled (--enable_cxl=true)..."
mooncake_master \
  --default_kv_lease_ttl=500 \
  --enable_cxl=true \
  &
CXL_MASTER_PID=$!
sleep 3
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_distributed_object_store_cxl.py
kill $CXL_MASTER_PID || true
wait $CXL_MASTER_PID 2>/dev/null || true
sleep 2
echo "CXL protocol test completed successfully!"

echo "Running CLI entry point tests..."
python test_cli.py

killall mooncake_http_metadata_server || true
killall mooncake_master || true
killall mooncake_client || true
mooncake_master --default_kv_lease_ttl=500 --enable_http_metadata_server=true &
MASTER_PID=$!
sleep 1
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_distributed_object_store.py
sleep 1
kill $MASTER_PID || true
wait $MASTER_PID 2>/dev/null || true


echo "All tests completed successfully!"
cd ../..
