#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
BUILD_DIR="${BUILD_DIR:-$REPO_ROOT/build_ninja}"
LOG_DIR="${LOG_DIR:-/tmp/mooncake_prefix_cache_e2e}"

MASTER_HOST="${MASTER_HOST:-127.0.0.1}"
MASTER_PORT="${MASTER_PORT:-50051}"
MASTER_RPC="${MASTER_HOST}:${MASTER_PORT}"
METADATA_URL="http://${MASTER_HOST}:8080/metadata"

MASTER_PID=""

cleanup() {
    echo "[cleanup] Stopping services..."
    [[ -n "$MASTER_PID" ]] && kill "$MASTER_PID" >/dev/null 2>&1 || true
    # Ensure no leftover master processes
    pkill -f 'mooncake_master.*rpc_port=50051' >/dev/null 2>&1 || true
    echo "[cleanup] Done."
}
trap cleanup EXIT

rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"

echo "=== Building ... ==="
# Ensure we are on the right branch and built
pushd "$REPO_ROOT" >/dev/null
LIBRARY_PATH=/data/lizhijun/anaconda3/lib:${LIBRARY_PATH:-} \
    cmake --build "$BUILD_DIR" --target mooncake_master store -j$(nproc) 2>&1 | \
    tail -5
popd >/dev/null

echo "=== Starting mooncake_master ==="
# Kill any stale master
pkill -f 'mooncake_master.*rpc_port=50051' >/dev/null 2>&1 || true
sleep 1

LD_LIBRARY_PATH=/data/lizhijun/anaconda3/lib:/usr/local/cuda-12.9/targets/x86_64-linux/lib:$BUILD_DIR/mooncake-common:${LD_LIBRARY_PATH:-} \
"$BUILD_DIR/mooncake-store/src/mooncake_master" \
    --rpc_address="$MASTER_HOST" \
    --rpc_port="$MASTER_PORT" \
    --http_metadata_server_port=8080 \
    --enable_http_metadata_server=true \
    --default_kv_lease_ttl=5000 \
    --logtostderr=true \
    --v=1 \
    >"$LOG_DIR/master.log" 2>&1 &
MASTER_PID=$!

# Wait for master to be ready
echo "Waiting for master to be ready..."
MASTER_READY=0
for i in $(seq 1 15); do
    if grep -qE "Master service started|listening on|HTTP metadata server" \
        "$LOG_DIR/master.log" 2>/dev/null; then
        MASTER_READY=1
        break
    fi
    sleep 1
done

if [[ $MASTER_READY -eq 0 ]]; then
    echo "ERROR: Master failed to start. Last 20 lines of log:"
    tail -20 "$LOG_DIR/master.log"
    exit 1
fi
echo "Master is ready."

echo "=== Running E2E tests ==="
export LD_LIBRARY_PATH=/data/lizhijun/anaconda3/lib:/usr/local/cuda-12.9/targets/x86_64-linux/lib:$BUILD_DIR/mooncake-common:${LD_LIBRARY_PATH:-}
export PYTHONPATH="$BUILD_DIR/mooncake-integration"
export METADATA_URL="$METADATA_URL"
export MASTER_RPC="$MASTER_RPC"

python3 "$SCRIPT_DIR/test_prefix_cache_e2e.py" \
    >"$LOG_DIR/client.log" 2>&1
TEST_EXIT_CODE=$?

echo ""
echo "============================================="
echo "           TEST RESULTS"
echo "============================================="
cat "$LOG_DIR/client.log"
echo ""
echo "============================================="
echo "           MASTER LOG (last 30 lines)"
echo "============================================="
tail -30 "$LOG_DIR/master.log"
echo ""
echo "============================================="
echo "Test exit code: $TEST_EXIT_CODE"
echo "Logs: $LOG_DIR"
echo "============================================="

exit $TEST_EXIT_CODE
