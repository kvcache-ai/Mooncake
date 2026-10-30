#!/bin/bash
# Single-host, TCP-only check of concurrent LOCAL_DISK owner reads.
# Starts a private master + N owner clients, writes past segment capacity so
# values are evicted to SSD, then checks disk-only reads at concurrency 1 and
# $CONC:
#   A  every batch read API (batch_get_into / _multi_buffers / batch_get_buffer)
#   B  several reader threads, many rounds, sharing one client
#   C  a dummy client reading through a standalone real client over IPC
#   D  one owner killed: only its keys may fail, identically at c=1 and c=$CONC
set -euo pipefail

WORK=${WORK:-/tmp/ownerpar}
OWNERS=${OWNERS:-3}
SEGMENT_MB=${SEGMENT_MB:-256}
VALUE_MB=${VALUE_MB:-4}
NUM_KEYS=${NUM_KEYS:-600}
BATCH=${BATCH:-8}
CONC=${CONC:-4}
SETTLE=${SETTLE:-15}
STRESS_THREADS=${STRESS_THREADS:-4}
STRESS_ROUNDS=${STRESS_ROUNDS:-30}
READ_TIMEOUT=${READ_TIMEOUT:-300}

HERE=$(cd "$(dirname "$0")" && pwd)
T="python3 $HERE/test_offload_owner_parallel_read.py"
COMMON=(--value-mb "$VALUE_MB" --num-keys "$NUM_KEYS")

# mooncake links libcuda; CPU-only hosts need the CUDA stub.
if ! ldconfig -p | grep -q 'libcuda.so.1 '; then
    mkdir -p /tmp/culib
    ln -sf "$(ls /usr/local/cuda*/lib64/stubs/libcuda.so | head -1)" /tmp/culib/libcuda.so.1
    export LD_LIBRARY_PATH=/tmp/culib:${LD_LIBRARY_PATH:-}
fi

# A stale master on these ports silently swallows our owners' mounts.
if command -v ss >/dev/null && ss -ltn | grep -qE ':(50051|8080|9003)\s'; then
    echo "ports 50051/8080/9003 already in use; kill the old mooncake_master first:"
    ss -ltnp | grep -E ':(50051|8080|9003)\s'
    exit 1
fi

rm -rf "$WORK" && mkdir -p "$WORK"

# Preflight: owner segments, writer buffer, two concurrent readers (fault
# phase) with their read buffers, plus the real client and slack, must fit in
# RAM (and the cgroup limit); every value may end up on disk under $WORK.
READER_MB=$((128 + STRESS_THREADS * BATCH * VALUE_MB))
NEED_MEM_MB=$((OWNERS * (SEGMENT_MB + 16) + 128 + 2 * READER_MB + 512))
NEED_DISK_MB=$((NUM_KEYS * VALUE_MB * 12 / 10))
AVAIL_MEM_MB=$(awk '/MemAvailable/ {print int($2 / 1024)}' /proc/meminfo)
CG_LIMIT=$(cat /sys/fs/cgroup/memory.max 2>/dev/null \
    || cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || echo max)
if [[ "$CG_LIMIT" =~ ^[0-9]+$ ]] && ((CG_LIMIT / 1024 / 1024 < AVAIL_MEM_MB)); then
    AVAIL_MEM_MB=$((CG_LIMIT / 1024 / 1024))
fi
AVAIL_DISK_MB=$(df -Pm "$WORK" | awk 'NR == 2 {print $4}')
echo "preflight: mem need=${NEED_MEM_MB}MB avail=${AVAIL_MEM_MB}MB," \
    "disk need=${NEED_DISK_MB}MB avail=${AVAIL_DISK_MB}MB ($WORK)"
if ((AVAIL_MEM_MB < NEED_MEM_MB || AVAIL_DISK_MB < NEED_DISK_MB)); then
    echo "insufficient resources; lower SEGMENT_MB / NUM_KEYS / VALUE_MB or move WORK"
    exit 1
fi
export MC_METADATA_SERVER=http://127.0.0.1:8080/metadata
export MASTER_SERVER=127.0.0.1:50051
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend
export MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE=$((8 * 1024 * 1024 * 1024))
export MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES=$((8 * 1024 * 1024 * 1024))
export MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1
export GLOG_logtostderr=1 GLOG_minloglevel=0

PIDS=()
OWNER_PIDS=()
cleanup() {
    kill "${PIDS[@]}" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

# MASTER_V=1 makes PutStart log why allocation failed.
GLOG_v=${MASTER_V:-0} mooncake_master --enable_http_metadata_server=true --http_metadata_server_port=8080 \
    --enable_offload=true --default_kv_lease_ttl=500 \
    --eviction_high_watermark_ratio=0.8 --eviction_ratio=0.2 \
    >"$WORK/master.log" 2>&1 &
PIDS+=($!)
sleep 2

for i in $(seq 1 "$OWNERS"); do
    $T owner --hostname "127.0.0.1:$((17000 + i))" --segment-mb "$SEGMENT_MB" \
        --ssd-path "$WORK/ssd$i" >"$WORK/owner$i.log" 2>&1 &
    PIDS+=($!)
    OWNER_PIDS+=($!)
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
# Outlast the master's client_ttl (10s) so an owner that stopped pinging has
# already been expired and its segment unmounted before we start writing.
sleep 12
grep -h "action=client_expired" "$WORK/master.log" && {
    echo "owner clients expired; segments were unmounted"; exit 1
}
curl -s 127.0.0.1:9003/metrics \
    | grep -E "^master_(total_capacity_bytes|allocated_bytes|mount_segment_(requests|failures)_total)" \
    || echo "master metrics unavailable"

$T write --hostname 127.0.0.1:17100 --settle-seconds "$SETTLE" "${COMMON[@]}" \
    >"$WORK/write.log" 2>&1 || {
    tail -30 "$WORK/write.log"
    echo "== master.log"; tail -30 "$WORK/master.log"
    exit 1
}
grep WRITE_DONE "$WORK/write.log"

# Freeze the disk-only key set so every phase reads the same keys.
KEYS="$WORK/disk_keys.txt"
$T list --hostname 127.0.0.1:17150 "${COMMON[@]}" --keys-file "$KEYS" \
    >"$WORK/list.log" 2>&1 || { tail -30 "$WORK/list.log"; exit 1; }
grep disk_only_keys "$WORK/list.log"

PASSED=()
FAILED=()
record() {  # record <name> <ok 0/1>
    if [ "$2" = 0 ]; then PASSED+=("$1"); echo "  PASS $1"; else FAILED+=("$1"); echo "  FAIL $1"; fi
}

# The owner-read log must show the concurrency we asked for: never above 1
# at c=1, and at least one batch with more than one owner in flight at c>1.
check_conc() {  # check_conc <log> <requested>
    local max
    max=$(grep "Offload owner reads" "$1" | grep -oE "concurrency=[0-9]+" \
        | cut -d= -f2 | sort -n | tail -1)
    [ -n "$max" ] || { echo "  no owner-read log in $1"; return 1; }
    if [ "$2" = 1 ]; then [ "$max" = 1 ]; else [ "$max" -ge 2 ]; fi \
        || { echo "  max owner concurrency=$max, requested $2"; return 1; }
}

PORT=17200
reader() {  # reader <name> <concurrency> <conc-log or ""> [read args...]
    local name=$1 c=$2 conc_log=$3 ok=0
    shift 3
    PORT=$((PORT + 1))
    MC_OFFLOAD_OWNER_READ_CONCURRENCY=$c timeout "$READ_TIMEOUT" \
        $T read --hostname "127.0.0.1:$PORT" --batch "$BATCH" "${COMMON[@]}" \
        --keys-file "$KEYS" "$@" >"$WORK/$name.log" 2>&1 || ok=1
    grep -E "RESULT|crashed|FAIL" "$WORK/$name.log" || tail -5 "$WORK/$name.log"
    if [ "$ok" = 0 ]; then
        check_conc "${conc_log:-$WORK/$name.log}" "$c" || ok=1
    fi
    record "$name" "$ok"
}

echo "== A: batch read APIs"
for api in into multi buffer; do
    for c in 1 "$CONC"; do
        reader "A_${api}_c$c" "$c" "" --api "$api"
    done
done

echo "== B: $STRESS_THREADS threads x $STRESS_ROUNDS rounds"
for api in into multi; do
    reader "B_${api}_c$CONC" "$CONC" "" --api "$api" \
        --threads "$STRESS_THREADS" --rounds "$STRESS_ROUNDS"
done

echo "== C: dummy client via standalone real client"
RC_N=0
for c in 1 "$CONC"; do
    for api in into multi; do
        RC_N=$((RC_N + 1))
        rc_port=$((50060 + RC_N))
        rc_log="$WORK/C_realclient_${api}_c$c.log"
        # The owner loop runs in the real client, so it gets the env var.
        MC_OFFLOAD_OWNER_READ_CONCURRENCY=$c mooncake_client \
            --host="127.0.0.1:$((17400 + RC_N))" --port="$rc_port" \
            --protocol=tcp --global_segment_size=0 --local_buffer_size=128MB \
            --master_server_address="$MASTER_SERVER" \
            --metadata_server="$MC_METADATA_SERVER" >"$rc_log" 2>&1 &
        rc_pid=$!
        PIDS+=($rc_pid)
        for _ in $(seq 1 60); do
            (exec 3<>"/dev/tcp/127.0.0.1/$rc_port") 2>/dev/null && break
            sleep 0.5
        done
        sleep 1
        reader "C_dummy_${api}_c$c" "$c" "$rc_log" --api "$api" \
            --dummy "127.0.0.1:$rc_port" --threads 2 --rounds 5
        kill "$rc_pid" 2>/dev/null || true
        wait "$rc_pid" 2>/dev/null || true
    done
done

echo "== D: kill owner $OWNERS, then read at c=1 and c=$CONC in parallel"
kill -9 "${OWNER_PIDS[$((OWNERS - 1))]}"
# Start both readers at once: the master expires the dead owner after its
# client_ttl, and both must see the same replica state.
for c in 1 "$CONC"; do
    PORT=$((PORT + 1))
    MC_OFFLOAD_OWNER_READ_CONCURRENCY=$c timeout "$READ_TIMEOUT" \
        $T read --hostname "127.0.0.1:$PORT" --batch "$BATCH" "${COMMON[@]}" \
        --keys-file "$KEYS" --expect-failures \
        --failed-keys-file "$WORK/D_failed_c$c.txt" >"$WORK/D_fault_c$c.log" 2>&1 &
    eval "D_PID_$c=$!"
done
for c in 1 "$CONC"; do
    ok=0
    eval "wait \$D_PID_$c" || ok=1
    grep -E "RESULT|crashed|FAIL" "$WORK/D_fault_c$c.log" || tail -5 "$WORK/D_fault_c$c.log"
    record "D_fault_c$c" "$ok"
done
if cmp -s "$WORK/D_failed_c1.txt" "$WORK/D_failed_c$CONC.txt"; then
    record "D_same_failed_keys" 0
else
    echo "  failed key sets differ:"
    diff "$WORK/D_failed_c1.txt" "$WORK/D_failed_c$CONC.txt" | head -10 || true
    record "D_same_failed_keys" 1
fi

echo "== summary: ${#PASSED[@]} passed, ${#FAILED[@]} failed"
if [ "${#FAILED[@]}" -gt 0 ]; then
    printf '  failed: %s\n' "${FAILED[@]}"
    exit 1
fi
echo "ALL PASS"
