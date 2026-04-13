#!/usr/bin/env bash
#
# Orchestrates cross-node Get benchmark over TCP and RDMA.
#
# Prerequisites:
#   - Both nodes built and installed (same branch)
#   - Passwordless SSH from Node 0 → Node 1
#   - No stale mooncake_master running
#
# Usage (run from Node 0 = 192.168.22.70):
#   bash mooncake-store/tests/run_cross_node_bench.sh
#
set -euo pipefail

NODE0=192.168.22.70
NODE1=192.168.22.72
PROJECT=/root/Mooncake
VENV=/root/sglang-venv
MASTER_BIN="$PROJECT/build/mooncake-store/src/mooncake_master"
BENCH_SCRIPT="$PROJECT/mooncake-store/tests/cross_node_get_benchmark.py"

MASTER_ADDR="$NODE0:50051"
METADATA_ADDR="http://$NODE0:8080/metadata"

cleanup() {
    echo ">>> Cleaning up ..."
    ssh root@"$NODE1" "pkill -f cross_node_get_benchmark || true" 2>/dev/null || true
    pkill -f cross_node_get_benchmark || true
    pkill -f mooncake_master || true
    sleep 1
}
trap cleanup EXIT

# --- 0. Cleanup stale processes on both nodes ---
echo ">>> Killing stale processes ..."
ssh root@"$NODE1" "pkill -f cross_node_get_benchmark || true; pkill -f mooncake_master || true" 2>/dev/null || true
pkill -f cross_node_get_benchmark || true
pkill -f mooncake_master || true
sleep 1

# --- 1. Start master on Node 0 ---
echo ">>> Starting mooncake_master on $NODE0 ..."
"$MASTER_BIN" \
    --enable_http_metadata_server=true \
    --eviction_high_watermark_ratio=0.95 \
    --default_kv_lease_ttl=5000000000 &
MASTER_PID=$!
sleep 2

if ! kill -0 "$MASTER_PID" 2>/dev/null; then
    echo "ERROR: mooncake_master failed to start"
    exit 1
fi
echo ">>> Master running (pid=$MASTER_PID)"

# --- Helper: run one protocol ---
run_protocol() {
    local proto=$1
    echo ""
    echo "================================================================"
    echo "  Running cross-node benchmark: protocol=$proto"
    echo "================================================================"

    # Start provider on Node 0 (background)
    echo ">>> Starting provider on $NODE0 ($proto) ..."
    source "$VENV/bin/activate"
    python3 "$BENCH_SCRIPT" \
        --role provider \
        --protocol "$proto" \
        --local-hostname "$NODE0" \
        --metadata-server "$METADATA_ADDR" \
        --master-server "$MASTER_ADDR" \
        --global-segment-size 4096 \
        --local-buffer-size 2048 &
    PROVIDER_PID=$!
    sleep 3

    # Start consumer on Node 1
    echo ">>> Starting consumer on $NODE1 ($proto) ..."
    ssh root@"$NODE1" "
        source $VENV/bin/activate
        cd $PROJECT
        python3 $BENCH_SCRIPT \
            --role consumer \
            --protocol $proto \
            --local-hostname $NODE1 \
            --metadata-server $METADATA_ADDR \
            --master-server $MASTER_ADDR \
            --global-segment-size 0 \
            --local-buffer-size 2048
    "

    echo ">>> Consumer finished, waiting for provider to exit ..."
    wait "$PROVIDER_PID" 2>/dev/null || true
    sleep 1
}

# --- 2. Run TCP benchmark ---
run_protocol tcp

# --- 3. Run RDMA benchmark ---
run_protocol rdma

echo ""
echo ">>> All benchmarks complete."
