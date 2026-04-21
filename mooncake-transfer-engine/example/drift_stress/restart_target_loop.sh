#!/usr/bin/env bash
# restart_target_loop.sh — restart the target every RESTART_INTERVAL_S
# seconds for MAX_RESTARTS iterations. Each restart picks a new RPC port →
# customer_pattern segment_id drifts, exercising the "segment_id per call"
# path on the initiator side.
#
# The target uses --target-auto-exit with TARGET_LIFETIME=RESTART_INTERVAL_S,
# so each target self-exits via the clean customer teardown recipe
# (unregister → explicit_release munmap+close+unlink → wait_memory_restore).
# We time the exit: kill_request → process_gone → hugepages_free_restored.
#
# Env overrides:
#   RESTART_INTERVAL_S   default 120    (also used as TARGET_LIFETIME)
#   MAX_RESTARTS         default 10
#   PEER_FILE            default /tmp/drift_peer.txt
#   TARGET_PID_FILE      default /tmp/drift_target.pid
#   LOG_DIR              default /tmp
#   EXIT_TIMINGS_FILE    default $LOG_DIR/target_exit_timings.tsv
#   START_TARGET_SH      default <repo>/mooncake-transfer-engine/example/drift_stress/start_target.sh
set -euo pipefail

REPO="${REPO:-/home/ubuntu/Mooncake}"
START_TARGET_SH="${START_TARGET_SH:-$REPO/mooncake-transfer-engine/example/drift_stress/start_target.sh}"
RESTART_INTERVAL_S="${RESTART_INTERVAL_S:-120}"
MAX_RESTARTS="${MAX_RESTARTS:-10}"
PEER_FILE="${PEER_FILE:-/tmp/drift_peer.txt}"
TARGET_PID_FILE="${TARGET_PID_FILE:-/tmp/drift_target.pid}"
LOG_DIR="${LOG_DIR:-/tmp}"
EXIT_TIMINGS_FILE="${EXIT_TIMINGS_FILE:-$LOG_DIR/target_exit_timings.tsv}"

# Baseline free hugepages (for "restored" threshold). Treat "free_restored"
# as the first moment we observe free >= baseline - SLACK.
BASELINE_FREE=$(awk '/^HugePages_Free/{print $2}' /proc/meminfo)
SLACK=64  # allow 128 MB (64 × 2 MB) of slack
RESTORED_THRESHOLD=$(( BASELINE_FREE - SLACK ))

# Pass through to start_target.sh so self-exit time matches the restart cadence.
export REPO LOG_DIR PEER_FILE TARGET_LIFETIME="$RESTART_INTERVAL_S"

if [[ ! -f "$EXIT_TIMINGS_FILE" ]]; then
  echo -e "iter\tkill_request_ts\tprocess_gone_ts\tfree_restored_ts\tfree_at_gone\tfree_at_restored\tbaseline_free" > "$EXIT_TIMINGS_FILE"
fi

cleanup() {
  if [[ -f "$TARGET_PID_FILE" ]]; then
    local pid
    pid=$(cat "$TARGET_PID_FILE")
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "[restart_loop] killing target $pid on exit (SIGKILL fallback)"
      kill -KILL "$pid" 2>/dev/null || true
    fi
  fi
}
trap cleanup EXIT INT TERM

for i in $(seq 1 "$MAX_RESTARTS"); do
  echo "[restart_loop] iter $i/$MAX_RESTARTS  (lifetime=${RESTART_INTERVAL_S}s)"

  # Clear peer file so initiator blocks until fresh target comes up.
  : > "$PEER_FILE"

  bash "$START_TARGET_SH"

  # Target is now ready. Poll for self-exit (takes TARGET_LIFETIME + teardown).
  # The start_target script wrote the PID to TARGET_PID_FILE.
  TARGET_PID=$(cat "$TARGET_PID_FILE")
  KILL_REQUEST_TS=$(( $(date +%s) + RESTART_INTERVAL_S ))  # expected self-exit trigger
  echo "[restart_loop] iter $i: pid=$TARGET_PID, will self-exit around $(date -d @$KILL_REQUEST_TS +%H:%M:%S)"

  # Wait until process exits (poll 1 Hz, hard cap at lifetime + 300s for teardown).
  HARD_DEADLINE=$(( $(date +%s) + RESTART_INTERVAL_S + 300 ))
  while kill -0 "$TARGET_PID" 2>/dev/null; do
    if (( $(date +%s) > HARD_DEADLINE )); then
      echo "[restart_loop] iter $i: self-exit deadline exceeded, SIGKILL"
      kill -KILL "$TARGET_PID" 2>/dev/null || true
      break
    fi
    sleep 1
  done
  PROCESS_GONE_TS=$(date +%s)
  FREE_AT_GONE=$(awk '/^HugePages_Free/{print $2}' /proc/meminfo)
  echo "[restart_loop] iter $i: process gone at $PROCESS_GONE_TS, free=$FREE_AT_GONE"

  # Poll hugepage free until restored (or 120s cap).
  FREE_RESTORED_TS=""
  FREE_AT_RESTORED=""
  POLL_DEADLINE=$(( $(date +%s) + 120 ))
  while (( $(date +%s) < POLL_DEADLINE )); do
    FREE=$(awk '/^HugePages_Free/{print $2}' /proc/meminfo)
    if (( FREE >= RESTORED_THRESHOLD )); then
      FREE_RESTORED_TS=$(date +%s)
      FREE_AT_RESTORED=$FREE
      break
    fi
    sleep 1
  done
  if [[ -z "$FREE_RESTORED_TS" ]]; then
    FREE_RESTORED_TS=$(date +%s)
    FREE_AT_RESTORED=$(awk '/^HugePages_Free/{print $2}' /proc/meminfo)
    echo "[restart_loop] iter $i: free NOT restored within 120s, free=$FREE_AT_RESTORED (baseline=$BASELINE_FREE)"
  fi
  echo "[restart_loop] iter $i: free restored at $FREE_RESTORED_TS (Δ=$(( FREE_RESTORED_TS - PROCESS_GONE_TS ))s, free=$FREE_AT_RESTORED)"

  echo -e "$i\t$KILL_REQUEST_TS\t$PROCESS_GONE_TS\t$FREE_RESTORED_TS\t$FREE_AT_GONE\t$FREE_AT_RESTORED\t$BASELINE_FREE" >> "$EXIT_TIMINGS_FILE"
done

echo "[restart_loop] all $MAX_RESTARTS iterations done"
