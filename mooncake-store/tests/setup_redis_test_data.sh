#!/bin/bash
# Setup script for Redis connector E2E test

set -e

REDIS_HOST=${MOONCAKE_REDIS_HOST:-127.0.0.1}
REDIS_PORT=${MOONCAKE_REDIS_PORT:-6379}

echo "Setting up Redis test data at $REDIS_HOST:$REDIS_PORT"

# Check if Redis is available
if ! command -v redis-cli &> /dev/null; then
    echo "redis-cli not found, skipping Redis test setup"
    exit 0
fi

# Check if Redis is running
if ! redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping &> /dev/null; then
    echo "Redis not running at $REDIS_HOST:$REDIS_PORT, skipping test setup"
    exit 0
fi

# Generate random test data
for i in {1..10}; do
    KEY="mooncake_test_$(openssl rand -hex 4)"
    VALUE="test_data_$(openssl rand -base64 32)"
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" SET "$KEY" "$VALUE" > /dev/null
    echo "Created test key: $KEY"
done

echo "Redis test data setup complete"
echo "Run tests with: MOONCAKE_REDIS_HOST=$REDIS_HOST MOONCAKE_REDIS_PORT=$REDIS_PORT ctest -R connector_e2e_test -V"
