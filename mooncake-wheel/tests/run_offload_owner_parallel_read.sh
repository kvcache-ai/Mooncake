#!/bin/bash
# Single-host, TCP-only check of concurrent LOCAL_DISK owner reads.
# Starts a private master + N owner clients, writes past segment capacity so
# values are evicted to SSD, then reads disk-only keys at concurrency 1 and $CONC.
set -euo pipefail

WORK=${WORK:-/tmp/ownerpar}
OWNERS=${OWNERS:-3}
SEGMENT_MB=${SEGMENT_MB:-256}
VALUE_MB=${VALUE_MB:-4}
NUM_KEYS=${NUM_KEYS:-600}
BATCH=${BATCH:-32}
CONC=${CONC:-4}
SETTLE=${SETTLE:-15}

HERE=$(cd "$(dirname "$0")" && pwd)
T="python3 $HERE/test_offload_owner_parallel_read.py"
COMMON=(--value-mb "$VALUE_MB" --num-keys "$NUM_KEYS")

# mooncake links libcuda; CPU-only hosts need the CUDA stub.
if ! ldconfig -p | grep -q 'libcuda.so.1 '; then
    mkdir -p /tmp/culib
    ln -sf "$(ls /usr/local/cuda*/lib64/stubs/libcuda.so | head -1)" /tmp/culib/libcuda.so.1
    export LD_LIBRARY_PATH=/tmp/culib:${LD_LIBRARY_PATH:-}
fi

rm -rf "$WORK" && mkdir -p "$WORK"
export MC_METADATA_SERVER=http://127.0.0.1:8080/metadata
export MASTER_SERVER=127.0.0.1:50051
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend
export MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE=$((8 * 1024 * 1024 * 1024))
export MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES=$((8 * 1024 * 1024 * 1024))
export MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1
export GLOG_logtostderr=1 GLOG_minloglevel=0

PIDS=()
cleanup() {
    kill "${PIDS[@]}" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

mooncake_master --enable_http_metadata_server=true --http_metadata_server_port=8080 \
    --enable_offload=true --default_kv_lease_ttl=500 \
    --eviction_high_watermark_ratio=0.8 --eviction_ratio=0.2 \
    >"$WORK/master.log" 2>&1 &
PIDS+=($!)
sleep 2

for i in $(seq 1 "$OWNERS"); do
    $T owner --hostname "127.0.0.1:$((17000 + i))" --segment-mb "$SEGMENT_MB" \
        --ssd-path "$WORK/ssd$i" >"$WORK/owner$i.log" 2>&1 &
    PIDS+=($!)
done
for i in $(seq 1 "$OWNERS"); do
    for _ in $(seq 1 60); do
        grep -q OWNER_READY "$WORK/owner$i.log" && break
        sleep 1
    done
    grep -q OWNER_READY "$WORK/owner$i.log" || { tail -30 "$WORK/owner$i.log"; exit 1; }
done
echo "owners ready: $OWNERS"
grep -h "Offload RPC server started" "$WORK"/owner*.log || true

$T write --hostname 127.0.0.1:17100 --settle-seconds "$SETTLE" "${COMMON[@]}" \
    >"$WORK/write.log" 2>&1 || { tail -30 "$WORK/write.log"; exit 1; }
grep WRITE_DONE "$WORK/write.log"

for c in 1 "$CONC"; do
    MC_OFFLOAD_OWNER_READ_CONCURRENCY=$c $T read --hostname "127.0.0.1:$((17200 + c))" \
        --batch "$BATCH" "${COMMON[@]}" >"$WORK/read_c$c.log" 2>&1 \
        || { tail -30 "$WORK/read_c$c.log"; exit 1; }
    grep -E "disk_only_keys|RESULT" "$WORK/read_c$c.log"
    grep -m3 "Offload owner reads" "$WORK/read_c$c.log" || echo "no owner-read log for c=$c"
done
