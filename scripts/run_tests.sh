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

pip install torch numpy
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 python test_put_get_tensor.py
kill $MASTER_PID || true


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
    rm -rf $TEST_ROOT_DIR
else
    echo "Skipping test: MOONCAKE_STORAGE_ROOT_DIR environment variable is not set"
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


echo "All tests completed successfully!"
cd ../..
