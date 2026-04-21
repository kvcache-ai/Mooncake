#!/usr/bin/env bash
# start_target.sh — launch one customer_pattern target with the canonical
# customer shape (hugetlbfs, per-MR alloc, 16 NIC full coverage), pick a fresh
# RPC port, and write /tmp/drift_peer.txt (ip:port,base0,base1) for the
# initiator to poll.
#
# Env overrides:
#   TARGETS_PER_NUMA   default 40    (→ 80 MRs across 2 NUMA = 328 GB, customer shape)
#   BUFFER_SIZE_MB     default 4198  (4.1 GB per MR)
#   PEER_FILE          default /tmp/drift_peer.txt
#   LOG_DIR            default /tmp  (target.<port>.log written there)
#   RUST_BIN           default <repo>/mooncake-transfer-engine/rust/target/release/customer_pattern
#   REPO               default /home/ubuntu/Mooncake
set -euo pipefail

REPO="${REPO:-/home/ubuntu/Mooncake}"
RUST_BIN="${RUST_BIN:-$REPO/mooncake-transfer-engine/rust/target/release/customer_pattern}"

# customer_pattern is linked against libasio.so inside the Mooncake build dir.
# That path is not on the system ld.so search list, so propagate it here so a
# bare SSH-launched invocation can dlopen() it.
export LD_LIBRARY_PATH="$REPO/build/mooncake-asio:${LD_LIBRARY_PATH:-}"
TARGETS_PER_NUMA="${TARGETS_PER_NUMA:-40}"
BUFFER_SIZE_MB="${BUFFER_SIZE_MB:-4198}"
PEER_FILE="${PEER_FILE:-/tmp/drift_peer.txt}"
LOG_DIR="${LOG_DIR:-/tmp}"
# Target stays alive for this long, then runs the clean customer teardown
# recipe (unregister → explicit_release munmap+close+unlink → wait_memory_restore)
# so we get accurate free-pool recovery timings. The restart loop should set
# this to match its RESTART_INTERVAL_S.
TARGET_LIFETIME="${TARGET_LIFETIME:-120}"

# Pick an --rpc-port arg that's unlikely to collide. NOTE: this is only the
# metadata-server port arg; the actual P2P handshake socket binds to a
# different, randomized port (we grep it from the log after startup and
# publish THAT one in the peer file).
PORT=$(( $(date +%s) % 30000 + 20000 ))

# Resolve primary non-loopback IPv4 (same logic customer_pattern's get_host_ip
# uses, but we need it ahead of process start).
IP=$(hostname -I | awk '{print $1}')
if [[ -z "$IP" ]]; then
  echo "start_target: cannot detect primary IPv4" >&2
  exit 1
fi

TARGET_LOG="$LOG_DIR/target.$PORT.log"
echo "[start_target] launching on $IP:$PORT, log=$TARGET_LOG, MRs=$TARGETS_PER_NUMA*2, per-MR=${BUFFER_SIZE_MB}MB"

# /dev/hugepages needs world-writable-with-sticky bit so non-root mmap works.
if [[ ! -w /dev/hugepages ]]; then
  sudo chmod 1777 /dev/hugepages || true
fi

# Enforce hugepage reservation. Other benchmarks may have lowered
# vm.nr_hugepages; force it back up before we mmap. Size needed for one
# target = num_numa × TARGETS_PER_NUMA × BUFFER_SIZE_MB / 2 MB, plus margin.
# Default to 500000 (≈1 TB) which covers 2 × 80 × 4.1 GB with headroom for
# the sibling initiator process on the same host if any.
NR_HUGEPAGES_REQUIRED="${NR_HUGEPAGES_REQUIRED:-500000}"
CUR_HPG=$(awk '/HugePages_Total/{print $2}' /proc/meminfo)
if (( CUR_HPG < NR_HUGEPAGES_REQUIRED )); then
  echo "[start_target] raising vm.nr_hugepages from $CUR_HPG → $NR_HUGEPAGES_REQUIRED"
  sudo sysctl -w vm.nr_hugepages="$NR_HUGEPAGES_REQUIRED" >/dev/null || true
  sleep 3
  NEW_HPG=$(awk '/HugePages_Total/{print $2}' /proc/meminfo)
  if (( NEW_HPG < NR_HUGEPAGES_REQUIRED )); then
    echo "[start_target] WARN: could only get $NEW_HPG hugepages (wanted $NR_HUGEPAGES_REQUIRED)" >&2
  fi
fi

# Sweep orphan hugepage files from any previously-crashed target — these
# pin pages even after the process is gone.
sudo rm -f /dev/hugepages/mooncake_cp_* 2>/dev/null || true

# Launch in background; redirect both streams to the per-port log.
# --target-auto-exit + --manual-release + --wait-memory-restore makes the
# target (a) exit on its own after TARGET_LIFETIME seconds, (b) run the
# canonical customer teardown (unregister → explicit_release → poll free
# pool back to baseline), so restart_target_loop can measure exit timing
# from post-mortem logs without killing with SIGKILL.
"$RUST_BIN" \
  --mode target \
  --metadata-server P2PHANDSHAKE \
  --rpc-port "$PORT" \
  --num-numa 2 \
  --buffers-per-numa "$TARGETS_PER_NUMA" \
  --buffer-size-mb "$BUFFER_SIZE_MB" \
  --buffer-source hugetlbfs \
  --per-mr-alloc \
  --target-auto-exit \
  --target-lifetime "$TARGET_LIFETIME" \
  --manual-release \
  --wait-memory-restore \
  > "$TARGET_LOG" 2>&1 &

TARGET_PID=$!
echo $TARGET_PID > "/tmp/drift_target.pid"

# Wait until the target logs the NUMA 1 base address (last thing before
# "ready — sleeping forever"). Grep the log.
BASE0=""
BASE1=""
DEADLINE=$(( $(date +%s) + 300 ))
while true; do
  if ! kill -0 $TARGET_PID 2>/dev/null; then
    echo "[start_target] target process died — tail of log:" >&2
    tail -n 40 "$TARGET_LOG" >&2
    exit 1
  fi
  BASE0=$(grep -oE 'base 0x[0-9a-f]+ .*--target-base-numa0' "$TARGET_LOG" \
          | head -n 1 | grep -oE '0x[0-9a-f]+' | head -n 1 || true)
  BASE1=$(grep -oE 'base 0x[0-9a-f]+ .*--target-base-numa1' "$TARGET_LOG" \
          | head -n 1 | grep -oE '0x[0-9a-f]+' | head -n 1 || true)
  if [[ -z "$BASE0" ]]; then
    # per-mr-alloc variant prints "first base 0x...  (copy as --target-base-numaN)"
    BASE0=$(grep -oE 'first base 0x[0-9a-f]+ .*--target-base-numa0' "$TARGET_LOG" \
            | head -n 1 | grep -oE '0x[0-9a-f]+' | head -n 1 || true)
    BASE1=$(grep -oE 'first base 0x[0-9a-f]+ .*--target-base-numa1' "$TARGET_LOG" \
            | head -n 1 | grep -oE '0x[0-9a-f]+' | head -n 1 || true)
  fi
  # Ready signal: either sleep-forever mode or auto-exit countdown has started.
  if [[ -n "$BASE0" && -n "$BASE1" ]] && \
     grep -qE 'ready — sleeping forever|auto-exit in [0-9]+s' "$TARGET_LOG"; then
    break
  fi
  if (( $(date +%s) > DEADLINE )); then
    echo "[start_target] timeout waiting for target ready on $IP:$PORT" >&2
    tail -n 40 "$TARGET_LOG" >&2
    echo "[start_target] NOT killing target pid=$TARGET_PID; it will self-exit via --target-auto-exit + --wait-memory-restore. Inspect /tmp/target.$PORT.log manually." >&2
    exit 1
  fi
  sleep 1
done

# The customer_pattern target logs the actual P2P handshake listen address
# as:  "Transfer Engine RPC using P2P handshake, listening on <IP>:<PORT>"
# That PORT is randomized and differs from --rpc-port above. Grep it.
HANDSHAKE_PORT=$(grep -oE 'P2P handshake, listening on [0-9.]+:[0-9]+' "$TARGET_LOG" \
                 | head -n 1 | grep -oE ':[0-9]+$' | tr -d ':')
if [[ -z "$HANDSHAKE_PORT" ]]; then
  echo "[start_target] could not find P2P handshake port in $TARGET_LOG" >&2
  tail -n 40 "$TARGET_LOG" >&2
  exit 1
fi

echo "${IP}:${HANDSHAKE_PORT},${BASE0},${BASE1}" > "$PEER_FILE"

# Surface target-side init / alloc / register timings so orchestrate can
# collect them per restart. One TSV line per target startup:
#   <unix_ts>\t<port>\t<alloc_s>\t<register_s>\t<total_gb>
TARGET_TIMINGS_FILE="${TARGET_TIMINGS_FILE:-$LOG_DIR/target_timings.tsv}"
TIMING=$(grep -oE 'TOTAL alloc=[0-9.]+s, register=[0-9.]+s +\([0-9.]+ GB' "$TARGET_LOG" | tail -n 1 || true)
ALLOC_S=$(echo "$TIMING" | grep -oE 'alloc=[0-9.]+' | head -n 1 | cut -d= -f2 || echo "")
REG_S=$(echo "$TIMING" | grep -oE 'register=[0-9.]+' | head -n 1 | cut -d= -f2 || echo "")
TOTAL_GB=$(echo "$TIMING" | grep -oE '\([0-9.]+ GB' | head -n 1 | grep -oE '[0-9.]+' || echo "")
if [[ ! -f "$TARGET_TIMINGS_FILE" ]]; then
  echo -e "unix_ts\trpc_port\thandshake_port\talloc_s\tregister_s\ttotal_gb" > "$TARGET_TIMINGS_FILE"
fi
echo -e "$(date +%s)\t$PORT\t$HANDSHAKE_PORT\t$ALLOC_S\t$REG_S\t$TOTAL_GB" >> "$TARGET_TIMINGS_FILE"

echo "[start_target] ready: $(cat $PEER_FILE)  (rpc-port=$PORT handshake=$HANDSHAKE_PORT) alloc=${ALLOC_S}s register=${REG_S}s total=${TOTAL_GB}GB"
