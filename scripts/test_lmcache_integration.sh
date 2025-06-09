#!/bin/bash
# LMCache Integration Test Script
# Tests the integration between Mooncake and LMCache notification system
# Usage: ./scripts/test_lmcache_integration.sh

set -e  # Exit immediately if a command exits with a non-zero status

echo "Starting LMCache integration test..."

# Install dependencies
echo "Installing dependencies..."
pip install pyzmq

# Function to cleanup processes on exit
cleanup() {
    echo "Cleaning up processes..."
    kill $MASTER_PID 2>/dev/null || true
    pkill mooncake_master 2>/dev/null || true
    pkill -f "test_lmcache_server.py" 2>/dev/null || true
}

# Set trap to cleanup on script exit
trap cleanup EXIT

# Kill any existing master processes
echo "Cleaning up existing processes..."
pkill mooncake_master 2>/dev/null || true
pkill -f "test_lmcache_server.py" 2>/dev/null || true
sleep 1

# Start mooncake_master with LMCache integration
echo "Starting mooncake_master with LMCache integration..."
mooncake_master --lmcache_controller_url=tcp://127.0.0.1:9001 &
MASTER_PID=$!
sleep 3

# Start LMCache test server with timeout and run test operations
echo "Running LMCache integration test..."
timeout 35 bash -c '
    # Start test server in background with 30 second timeout
    python3 scripts/test_lmcache_server.py 30 &
    TEST_SERVER_PID=$!
    sleep 3

    # Run test operations to generate notifications
    echo "Performing test operations to generate LMCache notifications..."
    cd mooncake-wheel/tests
    MC_METADATA_SERVER=http://127.0.0.1:8080/metadata python3 -c "
from mooncake.store import MooncakeDistributedStore
import time

print(\"Setting up MooncakeDistributedStore...\")
store = MooncakeDistributedStore()
store.setup_lmcache(\"localhost\", \"http://127.0.0.1:8080/metadata\", 512*1024*1024, 512*1024*1024, \"tcp\", \"\", \"127.0.0.1:50051\", \"lmcache_instance_id\", \"1\")

print(\"Performing test operations...\")
store.put(\"lmcache_test_key\", b\"test_data\")
time.sleep(2)

print(\"LMCache test operations completed successfully\")
"
    cd ../..

    # Wait for test server to complete
    wait $TEST_SERVER_PID
    TEST_RESULT=$?

    if [ $TEST_RESULT -eq 0 ]; then
        echo "LMCache integration test PASSED"
        exit 0
    else
        echo "LMCache integration test FAILED"
        exit 1
    fi
'

# Check the result of the timeout command
if [ $? -eq 0 ]; then
    echo "SUCCESS: LMCache integration test completed successfully"
    exit 0
else
    echo "FAILED: LMCache integration test failed or timed out"
    exit 1
fi
