#!/usr/bin/env bash
# Run only within an approved idle-machine reservation; CPU_MASK must stay inside it.
set -euo pipefail
ROLE=${1:?target or initiator}
RUNTIME=${2:?absolute path to runtime-window}
CPU_MASK=${3:?approved logical CPU list}
LOCAL_IP=${4:?host private IP}
TARGET_SEG=${5:-}
SLOTS=${6:-16}
BATCH=${7:-4096}
DURATION=${8:-20}
OP=${9:-write}
BUFFER_BYTES=${BUFFER_BYTES:-8589934592}
# This binary allocates total_buffer_size ON EACH NUMA NODE. Two NUMA nodes
# register 16 GiB/host. Active: 8 threads * 4096 * 256 KiB = 8 GiB/host.
COMMON=(--backend=classic --seg_type=DRAM --xport_type=rdma --metadata_type=p2p
  --seg_name="$LOCAL_IP" --total_buffer_size="$BUFFER_BYTES")
if [[ "$ROLE" == initiator ]]; then
  : "${TARGET_SEG:?copy target_seg_name printed by the target}"
  COMMON+=(--target_seg_name="$TARGET_SEG" --op_type="$OP"
    --start_block_size=262144 --max_block_size=262144
    --start_batch_size="$BATCH" --max_batch_size="$BATCH"
    --start_num_threads=8 --max_num_threads=8 --duration="$DURATION"
    --experimental_inflight_batches="$SLOTS"
    --result_output_jsonl="${RESULT_JSONL:-result.jsonl}")
  if [[ "${WINDOW_REQUESTS:-32}" != legacy ]]; then
    COMMON+=(--experimental_window_requests="${WINDOW_REQUESTS:-32}")
  fi
elif [[ "$ROLE" != target ]]; then
  echo "role must be target or initiator" >&2; exit 2
fi
# New controls are opt-in; all unlisted MC variables are cleared by env -i.
CONFIG=${CONFIG:-baseline}
EXTRA=()
case "$CONFIG" in
  baseline) ;;
  pin_peer|fast|source)
    EXTRA+=("MC_EXPERIMENTAL_SUBMIT_CPUS=${SUBMIT_CPUS:?eight CPUs in thread order}"
      "MC_EXPERIMENTAL_WORKER_CPUS=${WORKER_CPUS:?eight CPUs in mlx5_0..7 order}"
      MC_ENABLE_DEST_DEVICE_AFFINITY=1)
    if [[ "$CONFIG" != pin_peer ]]; then
      EXTRA+=(MC_EXPERIMENTAL_COUNTER_POLL=1 MC_EXPERIMENTAL_SKIP_UNUSED_NOTIFY_LOCK=1)
    fi
    if [[ "$CONFIG" == source ]]; then
      EXTRA+=("MC_EXPERIMENTAL_SOURCE_PREFETCH_STRIDE=${PREFETCH_STRIDE:-64}"
        "MC_EXPERIMENTAL_SOURCE_PREFETCH_CHUNKS=${PREFETCH_AHEAD:-1}")
    fi
    ;;
  *) echo "unknown CONFIG" >&2; exit 2 ;;
esac
# Explicit allowlist: preserve env -i isolation while enabling recorded cases.
for key in MC_EXPERIMENTAL_CHUNK_STRIDE MC_EXPERIMENTAL_PHASE_CHUNKS \
  MC_EXPERIMENTAL_SOURCE_THREAD_GAP MC_EXPERIMENTAL_TARGET_THREAD_GAP \
  MC_EXPERIMENTAL_PROGRESS_PREFETCH MC_EXPERIMENTAL_PROGRESS_EVERY \
  MC_EXPERIMENTAL_PROGRESS_STRIDE MC_EXPERIMENTAL_PROGRESS_AHEAD \
  MC_EXPERIMENTAL_PROGRESS_SPAN; do
  if [[ -n "${!key:-}" ]]; then EXTRA+=("$key=${!key}"); fi
done
exec env -i PATH="$PATH" LD_LIBRARY_PATH="$RUNTIME/lib" GLOG_logtostderr=1 \
  MC_TE_FILTERS=mlx5_0,mlx5_1,mlx5_2,mlx5_3,mlx5_4,mlx5_5,mlx5_6,mlx5_7 \
  MC_GID_INDEX=3 MC_MIN_RPC_PORT=21000 MC_MAX_RPC_PORT=21001 \
  MC_WORKERS_PER_CTX=1 MC_NUM_QP_PER_EP="${QPS:-2}" MC_MAX_WR=1024 \
  MC_MAX_CQE_PER_CTX=32768 MC_RDMA_SIGNAL_INTERVAL=32 \
  MC_RDMA_WORKER_IDLE_SPIN_US=1000 MC_MLX5_QP_LAG_PORT_BALANCE=1 \
  MC_SLICE_SIZE=262144 MC_EXPERIMENTAL_RR_QP=1 \
  MC_EXPERIMENTAL_REUSE_REQUESTS="${REUSE_REQUESTS:-1}" \
  MC_EXPERIMENTAL_SPLIT_CPU=0 MC_EXPERIMENTAL_TARGET_WARM_BYTES=0 \
  MC_EXPERIMENTAL_TARGET_WARM_US=0 MC_EXPERIMENTAL_INITIATOR_WARM_US=0 \
  MC_EXPERIMENTAL_THREAD_NICS="${NIC_ORDER:?set eight NIC names in thread order after checking NUMA topology}" \
  "${EXTRA[@]}" taskset -c "$CPU_MASK" "$RUNTIME/bin/tebench" "${COMMON[@]}"
