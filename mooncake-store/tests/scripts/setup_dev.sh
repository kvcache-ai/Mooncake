#!/bin/bash

# Exit on any error
set -e

echo "Starting development environment setup..."

# Start master service in background
echo "Starting master service..."
../../build/mooncake-store/src/master &
MASTER_PID=$!

# Wait for master service to be ready
sleep 2

# Verify master service is running
if ! ps -p $MASTER_PID > /dev/null; then
    echo "Failed to start master service"
    exit 1
fi

echo "Master service started successfully (PID: $MASTER_PID)"

# Export PID for cleanup
export MASTER_PID

echo "Development environment ready!"
