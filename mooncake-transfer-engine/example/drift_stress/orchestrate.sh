#!/usr/bin/env bash
# orchestrate.sh — local driver. SSHes into P5EN-1/2/3, starts target loops
# on 2 and 3, starts initiator on 1, copies drift peer files from 2/3 to 1,
# and collects logs when done.
#
# Runs entirely from your laptop. No changes to the P5EN filesystem other
# than writing into /tmp and ~/Mooncake (already rsync'd).
#
# Env overrides (all optional):
#   DRIFT_ITERS           default 50
#   RESTART_INTERVAL_S    default 15
#   MAX_RESTARTS          default 150
#   TARGETS_PER_NUMA      default 80
#   BUFFER_SIZE_MB        default 4198
#   LOG_DIR               default example/drift_stress/logs/<utc_ts>
#   KEY                   default ~/Documents/account/.../henanwan-us-east-2.pem
set -euo pipefail

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
HOST_A="${HOST_A:-ec2-18-189-225-222.us-east-2.compute.amazonaws.com}"  # P5EN-1, initiator
HOST_B="${HOST_B:-ec2-18-222-61-73.us-east-2.compute.amazonaws.com}"    # P5EN-2, target A
HOST_C="${HOST_C:-ec2-3-130-251-102.us-east-2.compute.amazonaws.com}"   # P5EN-3, target B
USER_NAME="${USER_NAME:-ubuntu}"
KEY="${KEY:-$HOME/Documents/account/579019700964/henanwan/henanwan-us-east-2.pem}"

DRIFT_ITERS="${DRIFT_ITERS:-50}"
RESTART_INTERVAL_S="${RESTART_INTERVAL_S:-15}"
MAX_RESTARTS="${MAX_RESTARTS:-150}"
TARGETS_PER_NUMA="${TARGETS_PER_NUMA:-80}"
BUFFER_SIZE_MB="${BUFFER_SIZE_MB:-4198}"

TS=$(date -u +%Y%m%dT%H%M%SZ)
LOG_DIR="${LOG_DIR:-$SCRIPT_DIR/logs/$TS}"
mkdir -p "$LOG_DIR"
echo "[orchestrate] logs → $LOG_DIR"

SSH_OPTS=(-i "$KEY" -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30)
ssh_a() { ssh "${SSH_OPTS[@]}" "${USER_NAME}@${HOST_A}" "$@"; }
ssh_b() { ssh "${SSH_OPTS[@]}" "${USER_NAME}@${HOST_B}" "$@"; }
ssh_c() { ssh "${SSH_OPTS[@]}" "${USER_NAME}@${HOST_C}" "$@"; }

REMOTE_DIR="/home/ubuntu/Mooncake/mooncake-transfer-engine/example/drift_stress"

# 1. Sanity-check: binary exists on every host.
for h in "$HOST_A" "$HOST_B" "$HOST_C"; do
  ssh "${SSH_OPTS[@]}" "${USER_NAME}@$h" "test -x /home/ubuntu/Mooncake/mooncake-transfer-engine/rust/target/release/customer_pattern" \
    || { echo "[orchestrate] missing customer_pattern bin on $h"; exit 1; }
done

# 2. Make sure scripts are present and executable on every host.
for h in "$HOST_A" "$HOST_B" "$HOST_C"; do
  ssh "${SSH_OPTS[@]}" "${USER_NAME}@$h" "chmod +x ${REMOTE_DIR}/*.sh"
done

# 3. Clean old peer files / pid files so stale state from a prior run can't confuse us.
for h in "$HOST_B" "$HOST_C"; do
  ssh "${SSH_OPTS[@]}" "${USER_NAME}@$h" "rm -f /tmp/drift_peer.txt /tmp/drift_target.pid /tmp/target.*.log" || true
done
ssh_a "rm -f /tmp/drift_peer_A.txt /tmp/drift_peer_B.txt /tmp/drift_peer_cur.txt /tmp/drift.csv /tmp/initiator.log"

# 4. Kick off target loops on B and C (background, disowned from ssh session).
for h in "$HOST_B" "$HOST_C"; do
  ssh "${SSH_OPTS[@]}" "${USER_NAME}@$h" \
    "RESTART_INTERVAL_S=$RESTART_INTERVAL_S MAX_RESTARTS=$MAX_RESTARTS \
     TARGETS_PER_NUMA=$TARGETS_PER_NUMA BUFFER_SIZE_MB=$BUFFER_SIZE_MB \
     nohup bash ${REMOTE_DIR}/restart_target_loop.sh > /tmp/restart_loop.log 2>&1 &"
done
echo "[orchestrate] target loops launched on $HOST_B, $HOST_C"

# 5. Background rsync: every 1s pull /tmp/drift_peer.txt from B→A as
# /tmp/drift_peer_A.txt, and from C→A as /tmp/drift_peer_B.txt.
sync_peers() {
  while true; do
    scp -q "${SSH_OPTS[@]}" "${USER_NAME}@${HOST_B}:/tmp/drift_peer.txt" /tmp/.drift_peer_A.$$ 2>/dev/null || true
    if [[ -s /tmp/.drift_peer_A.$$ ]]; then
      scp -q "${SSH_OPTS[@]}" /tmp/.drift_peer_A.$$ "${USER_NAME}@${HOST_A}:/tmp/drift_peer_A.txt" 2>/dev/null || true
    fi
    scp -q "${SSH_OPTS[@]}" "${USER_NAME}@${HOST_C}:/tmp/drift_peer.txt" /tmp/.drift_peer_B.$$ 2>/dev/null || true
    if [[ -s /tmp/.drift_peer_B.$$ ]]; then
      scp -q "${SSH_OPTS[@]}" /tmp/.drift_peer_B.$$ "${USER_NAME}@${HOST_A}:/tmp/drift_peer_B.txt" 2>/dev/null || true
    fi
    sleep 1
  done
}
sync_peers &
SYNC_PID=$!
cleanup_local() {
  kill -TERM $SYNC_PID 2>/dev/null || true
  rm -f /tmp/.drift_peer_A.$$ /tmp/.drift_peer_B.$$
}
trap cleanup_local EXIT INT TERM

# 6. Launch initiator on A (foreground over SSH — we block until it's done).
echo "[orchestrate] launching initiator on $HOST_A (DRIFT_ITERS=$DRIFT_ITERS)"
ssh_a "DRIFT_ITERS=$DRIFT_ITERS TARGETS=2 \
       MERGED_LOG_DIR=/tmp \
       DRIFT_CSV=/tmp/drift.csv \
       INITIATOR_LOG=/tmp/initiator.log \
       BUFFER_SIZE_MB=$BUFFER_SIZE_MB \
       bash ${REMOTE_DIR}/run_initiator.sh" || true

# 7. Stop target loops. We bounded them via MAX_RESTARTS but kill anyway.
for h in "$HOST_B" "$HOST_C"; do
  ssh "${SSH_OPTS[@]}" "${USER_NAME}@$h" "pkill -f restart_target_loop.sh || true; pkill -f 'customer_pattern.*--mode target' || true"
done

# 8. Collect logs.
scp -q "${SSH_OPTS[@]}" "${USER_NAME}@${HOST_A}:/tmp/drift.csv" "$LOG_DIR/drift.csv" || true
scp -q "${SSH_OPTS[@]}" "${USER_NAME}@${HOST_A}:/tmp/initiator.log" "$LOG_DIR/initiator.log" || true
scp -q "${SSH_OPTS[@]}" "${USER_NAME}@${HOST_B}:/tmp/restart_loop.log" "$LOG_DIR/restart_loop_B.log" || true
scp -q "${SSH_OPTS[@]}" "${USER_NAME}@${HOST_C}:/tmp/restart_loop.log" "$LOG_DIR/restart_loop_C.log" || true
# last target log from each box (latest port)
for pair in "B:$HOST_B" "C:$HOST_C"; do
  label="${pair%%:*}"
  host="${pair##*:}"
  scp -q "${SSH_OPTS[@]}" "${USER_NAME}@${host}:/tmp/target.*.log" "$LOG_DIR/target_${label}_<port>.log" 2>/dev/null || true
done

bash "${SCRIPT_DIR}/parse_results.sh" "$LOG_DIR" > "$LOG_DIR/summary.txt" || true
echo "[orchestrate] done. Results in $LOG_DIR/"
echo "[orchestrate] tail summary:"
cat "$LOG_DIR/summary.txt" || true
