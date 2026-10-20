#!/usr/bin/env bash
# Source from a runner that owns the EXIT trap. Configure SPDK_DIR, RPC_SOCKET,
# TARGET_NQN, TARGET_HOST and TARGET_PORT before starting a target.
# TARGET_PID remains available to the runner for fault injection and cleanup.

nof_target_rpc() {
  python3 "$SPDK_DIR/scripts/rpc.py" -s "$RPC_SOCKET" -t 2 "$@"
}

nof_target_start() {
  local log=$1 deadline=$((SECONDS + ${TIMEOUT_SEC:-30}))
  [[ -z "${TARGET_PID:-}" ]] || { echo "NoF target already started" >&2; return 1; }
  [[ ! -e "$RPC_SOCKET" && ! -e "$RPC_SOCKET.lock" ]] || {
    echo "NoF target socket already exists: $RPC_SOCKET" >&2
    return 1
  }
  NOF_TARGET_SETUP_LOG="${log%.log}-setup.log"
  "$SPDK_DIR/build/bin/nvmf_tgt" -m "${TARGET_CORE_MASK:-0x1}" -u \
    -s "${TARGET_MEM_MB:-512}" --iova-mode=va --disable-cpumask-locks \
    --num-trace-entries=0 --huge-unlink -r "$RPC_SOCKET" >"$log" 2>&1 &
  # Record ownership before RPC setup, so failed startup can also be cleaned up.
  TARGET_PID=$!
  until nof_target_rpc bdev_get_bdevs >"${log%.log}-rpc-ready.log" 2>&1; do
    kill -0 "$TARGET_PID" 2>/dev/null && (( SECONDS < deadline )) || return 1
    sleep 0.1
  done
  nof_target_rpc nvmf_create_transport -t TCP >"$NOF_TARGET_SETUP_LOG" || return
  nof_target_rpc nvmf_create_subsystem "$TARGET_NQN" -a -s MC_NOF_E2E \
    >>"$NOF_TARGET_SETUP_LOG"
}

nof_target_add_malloc_ns() {
  local nsid=$1 size_mb=$2 block_size=$3
  nof_target_rpc bdev_malloc_create -b "Malloc$nsid" "$size_mb" "$block_size" \
    >>"$NOF_TARGET_SETUP_LOG" || return
  nof_target_rpc nvmf_subsystem_add_ns "$TARGET_NQN" "Malloc$nsid" -n "$nsid" \
    >>"$NOF_TARGET_SETUP_LOG"
}

nof_target_start_listener() {
  nof_target_rpc nvmf_subsystem_add_listener "$TARGET_NQN" -t TCP \
    -a "$TARGET_HOST" -s "$TARGET_PORT" >>"$NOF_TARGET_SETUP_LOG"
}

nof_target_stop() {
  [[ -n "${TARGET_PID:-}" ]] || return 0
  # Also stop a target paused with SIGSTOP; only signal the runner's own child.
  kill -KILL "$TARGET_PID" 2>/dev/null || true
  wait "$TARGET_PID" 2>/dev/null || true
  TARGET_PID=""
  rm -f "$RPC_SOCKET" "$RPC_SOCKET.lock"
}
