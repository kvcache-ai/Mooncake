#!/usr/bin/env bash
# run_initiator.sh — launch one long-lived customer_pattern initiator in
# --drift-iters mode, polling the peer files rsynced in from P5EN-2 / P5EN-3.
#
# Single-target variant: only reads PEER_FILE_A.
# Dual-target variant:   reads PEER_FILE_A for even iterations, PEER_FILE_B
#                        for odd iterations (achieved via a tiny merger
#                        python one-liner if TARGETS=2).
#
# Env overrides:
#   DRIFT_ITERS               default 50
#   INITIATOR_THREADS         default 8
#   REQUESTS_PER_THREAD       default 4
#   BLOCK_SIZE                default 524288 (0.5 MB)
#   BATCH_SIZE                default 16
#   BUFFER_SIZE_MB            default 4198
#   TARGETS                   default 2     (1 or 2)
#   PEER_FILE_A               default /tmp/drift_peer_A.txt
#   PEER_FILE_B               default /tmp/drift_peer_B.txt
#   DRIFT_PEER_FILE           default /tmp/drift_peer_cur.txt (polled by bin)
#   MERGED_LOG_DIR            default /tmp
#   RUST_BIN                  default <repo>/mooncake-transfer-engine/rust/target/release/customer_pattern
set -euo pipefail

REPO="${REPO:-/home/ubuntu/Mooncake}"
RUST_BIN="${RUST_BIN:-$REPO/mooncake-transfer-engine/rust/target/release/customer_pattern}"
export LD_LIBRARY_PATH="$REPO/build/mooncake-asio:${LD_LIBRARY_PATH:-}"
DRIFT_ITERS="${DRIFT_ITERS:-50}"
# Customer-aligned default burst: threads=20, 100 reqs/thread, 0.5 MB each
# (17 MB total per "request" approximated as 34 × 0.5 MB batch submission).
INITIATOR_THREADS="${INITIATOR_THREADS:-20}"
REQUESTS_PER_THREAD="${REQUESTS_PER_THREAD:-100}"
BLOCK_SIZE="${BLOCK_SIZE:-524288}"
BATCH_SIZE="${BATCH_SIZE:-34}"
BUFFER_SIZE_MB="${BUFFER_SIZE_MB:-4198}"
TARGETS="${TARGETS:-2}"
PEER_FILE_A="${PEER_FILE_A:-/tmp/drift_peer_A.txt}"
PEER_FILE_B="${PEER_FILE_B:-/tmp/drift_peer_B.txt}"
DRIFT_PEER_FILE="${DRIFT_PEER_FILE:-/tmp/drift_peer_cur.txt}"
MERGED_LOG_DIR="${MERGED_LOG_DIR:-/tmp}"
DRIFT_CSV="${DRIFT_CSV:-$MERGED_LOG_DIR/drift.csv}"
INITIATOR_LOG="${INITIATOR_LOG:-$MERGED_LOG_DIR/initiator.log}"

mkdir -p "$MERGED_LOG_DIR"
: > "$DRIFT_PEER_FILE"

# Hugetlbfs mount needs sticky-bit world-writable for non-root mmap.
if [[ ! -w /dev/hugepages ]]; then
  sudo chmod 1777 /dev/hugepages 2>/dev/null || true
fi

# Enforce hugepage reservation. Initiator needs ≈ batch × block × threads
# × 2 NUMA of hugepages, but it shares the machine budget if another
# benchmark runs, so we insist on a comfortable pool. Default 50K (~100 GB)
# covers the customer-shape initiator buffer; bump if needed.
NR_HUGEPAGES_REQUIRED="${NR_HUGEPAGES_REQUIRED:-500000}"
CUR_HPG=$(awk '/HugePages_Total/{print $2}' /proc/meminfo)
if (( CUR_HPG < NR_HUGEPAGES_REQUIRED )); then
  echo "[run_initiator] raising vm.nr_hugepages from $CUR_HPG → $NR_HUGEPAGES_REQUIRED"
  sudo sysctl -w vm.nr_hugepages="$NR_HUGEPAGES_REQUIRED" >/dev/null || true
  sleep 3
fi

# Sweep orphan hugepage files from any previously-crashed initiator.
sudo rm -f /dev/hugepages/mooncake_cp_drift_init_* 2>/dev/null || true

# Merger loop: publishes a fresh peer every time the "other side" gets a new
# value. Alternates between PEER_FILE_A and PEER_FILE_B, but only advances
# when the corresponding source file has a value we haven't published yet —
# i.e. it follows per-source freshness, not a shared last_published. Without
# this, two steady-state peer files trap the merger in a ping-pong between
# two stale values even as restart_target_loop rewrites them.
(
  trap 'exit 0' TERM INT
  idx=0
  last_a=""
  last_b=""
  while true; do
    if [[ "$TARGETS" == "1" ]]; then
      SRC="$PEER_FILE_A"
      prev="$last_a"
    else
      if (( idx % 2 == 0 )); then
        SRC="$PEER_FILE_A"
        prev="$last_a"
      else
        SRC="$PEER_FILE_B"
        prev="$last_b"
      fi
    fi

    if [[ -s "$SRC" ]]; then
      line=$(head -n 1 "$SRC" | tr -d '\r\n')
      # Publish if we have a value AND it's different from the one we last
      # published *from this same source*. That way a rewrite to src A
      # always gets surfaced next time idx%2==0, regardless of whether B has
      # also changed in the meantime.
      if [[ -n "$line" && "$line" != "$prev" ]]; then
        echo "$line" > "$DRIFT_PEER_FILE"
        if [[ "$SRC" == "$PEER_FILE_A" ]]; then
          last_a="$line"
        else
          last_b="$line"
        fi
        idx=$(( idx + 1 ))
      fi
    fi
    sleep 0.5
  done
) &
MERGER_PID=$!
trap 'kill -TERM $MERGER_PID 2>/dev/null || true' EXIT INT TERM

# Wait until we have an initial peer before starting the initiator.
DEADLINE=$(( $(date +%s) + 120 ))
while [[ ! -s "$DRIFT_PEER_FILE" ]]; do
  if (( $(date +%s) > DEADLINE )); then
    echo "[run_initiator] timeout waiting for initial peer in $DRIFT_PEER_FILE" >&2
    exit 1
  fi
  sleep 0.5
done
echo "[run_initiator] initial peer: $(cat $DRIFT_PEER_FILE)"

"$RUST_BIN" \
  --mode initiator \
  --metadata-server P2PHANDSHAKE \
  --rpc-port 12345 \
  --operation read \
  --threads "$INITIATOR_THREADS" \
  --block-size "$BLOCK_SIZE" \
  --batch-size "$BATCH_SIZE" \
  --buffer-size-mb "$BUFFER_SIZE_MB" \
  --dual-numa-initiator \
  --buffer-source hugetlbfs \
  --warmup-connect \
  --requests-per-thread "$REQUESTS_PER_THREAD" \
  --drift-iters "$DRIFT_ITERS" \
  --drift-peer-file "$DRIFT_PEER_FILE" \
  --drift-poll-timeout-s 300 \
  --drift-csv "$DRIFT_CSV" \
  > "$INITIATOR_LOG" 2>&1

echo "[run_initiator] done, csv=$DRIFT_CSV, log=$INITIATOR_LOG"
