#!/bin/bash
# Script to run all tests for the mooncake package
# Usage: ./scripts/run_tests.sh

set -e  # Exit immediately if a command exits with a non-zero status

# Ensure LD_LIBRARY_PATH includes /usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib


echo "Running transfer_engine tests..."
cd mooncake-wheel/tests
MC_METADATA_SERVER=http://127.0.0.1:8090/metadata python transfer_engine_target.py &
TARGET_PID=$!
MC_METADATA_SERVER=http://127.0.0.1:8090/metadata python transfer_engine_initiator_test.py
kill $TARGET_PID || true

echo "Running master tests..."
echo "Checking if mooncake_master is installed..."
if [ ! -f ~/.local/bin/mooncake_master ]; then
    echo "mooncake_master not found"
    exit 1
fi
echo "mooncake_master found, running tests..."
mooncake_master &
MASTER_PID=$!
sleep 1
MC_METADATA_SERVER=http://127.0.0.1:8090/metadata python test_distributed_object_store.py
kill $MASTER_PID || true

echo "All tests completed successfully!"
cd ../..
