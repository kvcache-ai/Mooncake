#!/bin/bash
# LMCache Integration Test Script
# Tests the integration between Mooncake and LMCache notification system
# Usage: ./scripts/test_lmcache_integration.sh

set -e  # Exit immediately if a command exits with a non-zero status

echo "Starting LMCache integration test..."

# Start LMCache test server in background
echo "Starting LMCache test server..."
python3 scripts/test_lmcache_server.py &
LMCACHE_SERVER_PID=$!
sleep 2

# Function to cleanup processes on exit
cleanup() {
    echo "Cleaning up processes..."
    kill $MASTER_PID 2>/dev/null || true
    kill $LMCACHE_SERVER_PID 2>/dev/null || true
    pkill mooncake_master 2>/dev/null || true
}

# Set trap to cleanup on script exit
trap cleanup EXIT

# Check if LMCache server started successfully
if curl -s http://127.0.0.1:9090/status > /dev/null; then
    echo "LMCache server started successfully"

    # Kill any existing master processes and restart with LMCache URL
    echo "Starting mooncake_master with LMCache integration..."
    pkill mooncake_master || true
    sleep 1
    mooncake_master --lmcache_controller_url=http://127.0.0.1:9090/api/kv_events &
    MASTER_PID=$!
    sleep 2

    # Run integration test to generate notifications
    echo "Running LMCache integration test operations..."
    cd mooncake-wheel/tests
    MC_METADATA_SERVER=http://127.0.0.1:8080/metadata python3 -c "
from mooncake.store import MooncakeDistributedStore
import time

print('Setting up MooncakeDistributedStore...')
store = MooncakeDistributedStore()
store.setup_lmcache('localhost', 'http://127.0.0.1:8080/metadata', 512*1024*1024, 512*1024*1024, 'tcp', '', '127.0.0.1:50051','lmcache_instance_id','lmcache_worker_id')

print('Performing test operations...')
store.put('lmcache_test_key', b'test_data')
time.sleep(1)

print('LMCache test operations completed successfully')
"
    cd ../..

    # Check for notifications received by LMCache server
    echo "Checking LMCache notifications..."
    sleep 2
    curl -s http://127.0.0.1:9090/status
    NOTIFICATIONS=$(curl -s http://127.0.0.1:9090/status | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('notifications_received', 0))")
    echo "LMCache notifications received: $NOTIFICATIONS"

    if [ "$NOTIFICATIONS" -gt "0" ]; then
        echo "SUCCESS: LMCache received $NOTIFICATIONS notifications"

        # Validate notification content using simple shell commands
        echo "Validating notification content..."
        STATUS_JSON=$(curl -s http://127.0.0.1:9090/status)

        # Check if the expected values are present in the JSON response
        if echo "$STATUS_JSON" | grep -q '"type": "KVAdmitMsg"' && \
           echo "$STATUS_JSON" | grep -q '"instance_id": "lmcache_instance_id"' && \
           echo "$STATUS_JSON" | grep -q '"worker_id": "lmcache_worker_id"' && \
           echo "$STATUS_JSON" | grep -q '"key": "lmcache_test_key"' && \
           echo "$STATUS_JSON" | grep -q '"location": "mooncake_cpu_ram"'; then
            echo "Notification content validation PASSED"
            echo "All expected fields found in notification data"
            echo "LMCache integration test PASSED"
            exit 0
        else
            echo "VALIDATION FAILED: Expected notification content not found"
            echo "Expected to find:"
            echo "  - type: KVAdmitMsg"
            echo "  - instance_id: lmcache_instance_id"
            echo "  - worker_id: lmcache_worker_id"
            echo "  - key: lmcache_test_key"
            echo "  - location: mooncake_cpu_ram"
            echo "Actual response:"
            echo "$STATUS_JSON"
            echo "LMCache integration test FAILED - notification content validation failed"
            exit 1
        fi
    else
        # Expected to fail if LMCache server is not running
        echo "LMCache integration test FAILED"
        exit 1
    fi

else
    echo "ERROR: LMCache server failed to start"
    echo "LMCache integration test FAILED"
    exit 1
fi
