#!/usr/bin/env bash
# NoF stalled-I/O regression for kvcache-ai/Mooncake#3864.
#
# Same local TCP nvmf_tgt setup as run_nof_heartbeat_tcp_e2e.sh, but the target
# is suspended with SIGSTOP instead of being killed: the TCP connection stays
# established and the io qpair keeps looking healthy while the target stops
# answering. Without an I/O timeout the client's put/get blocks forever on the
# SPDK future. With it, the client must fail within MC_NOF_IO_TIMEOUT_MS plus a
# margin, the worker must reclaim every outstanding sub-I/O before the caller
# is told, and the client must keep serving requests afterwards (fail-fast on
# the disconnected qpair, no crash, no data mismatch).
#
# Runs NoF-only (CLIENT_GLOBAL_SEGMENT_SIZE=0) so a memory replica cannot hide
# a stuck NoF transfer. Needs an SPDK build under extern/spdk, hugepages and
# passwordless sudo like the heartbeat e2e. The whole run is bounded by
# RUN_TIMEOUT_SEC through an outer watchdog, so a regression cannot hang CI.
set -euo pipefail

RUN_TIMEOUT_SEC=${RUN_TIMEOUT_SEC:-300}
if [[ -z "${NOF_STALL_E2E_CHILD:-}" ]]; then
  export NOF_STALL_E2E_CHILD=1
  exec timeout --foreground -k 10 "$RUN_TIMEOUT_SEC" "$BASH" "$0" "$@"
fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
BUILD_DIR=${BUILD_DIR:-"$REPO_ROOT/build"}
LOG_DIR=${LOG_DIR:-/tmp/mooncake_nof_stalled_io_e2e}
MASTER_RPC=${MASTER_RPC:-127.0.0.1:50051}
MASTER_HOST=${MASTER_RPC%:*}
MASTER_PORT=${MASTER_RPC##*:}
METADATA_HOST=${METADATA_HOST:-127.0.0.1}
METADATA_PORT=${METADATA_PORT:-8080}
METADATA_SERVER="http://$METADATA_HOST:$METADATA_PORT/metadata"
TARGET_HOST=${TARGET_HOST:-127.0.0.1}
TARGET_PORT=${TARGET_PORT:-4420}
TARGET_NQN=${TARGET_NQN:-nqn.2016-06.io.spdk:cnode1}
NOF_SIZE=${NOF_SIZE:-67108864}
PAYLOAD_SIZE=${PAYLOAD_SIZE:-4096}
CLIENT_DURATION=${CLIENT_DURATION:-0}
CLIENT_SLEEP_MS=${CLIENT_SLEEP_MS:-200}
# The master's heartbeat also stops getting answers; keep its unmount well
# after the client-side I/O timeout so the failure observed below comes from
# the timeout and not from the segment disappearing.
HEARTBEAT_INTERVAL=${HEARTBEAT_INTERVAL:-10}
HEARTBEAT_TIMEOUT_MS=${HEARTBEAT_TIMEOUT_MS:-500}
HEARTBEAT_FAILURES=${HEARTBEAT_FAILURES:-3}
CLIENT_GLOBAL_SEGMENT_SIZE=${CLIENT_GLOBAL_SEGMENT_SIZE:-0}
CLIENT_LOCAL_BUFFER_SIZE=${CLIENT_LOCAL_BUFFER_SIZE:-33554432}
CLIENT_MEMORY_REPLICA_NUM=${CLIENT_MEMORY_REPLICA_NUM:-0}
CLIENT_NOF_REPLICA_NUM=${CLIENT_NOF_REPLICA_NUM:-1}
PRE_FAULT_SUCCESS_TARGET=${PRE_FAULT_SUCCESS_TARGET:-3}
IO_TIMEOUT_MS=${IO_TIMEOUT_MS:-3000}
# Detection happens on the worker's next poll after the deadline and the
# drain is local, so a few seconds of slack is plenty.
STALL_SLACK_SEC=${STALL_SLACK_SEC:-10}
POST_TIMEOUT_OBSERVATION_SEC=${POST_TIMEOUT_OBSERVATION_SEC:-10}

TARGET_PID=""
MASTER_PID=""
META_PID=""
CLIENT_PID=""

cleanup() {
  [[ -n "$CLIENT_PID" ]] && kill "$CLIENT_PID" >/dev/null 2>&1 || true
  [[ -n "$MASTER_PID" ]] && kill "$MASTER_PID" >/dev/null 2>&1 || true
  if [[ -n "$TARGET_PID" ]]; then
    kill -CONT "$TARGET_PID" >/dev/null 2>&1 || true
    kill "$TARGET_PID" >/dev/null 2>&1 || true
  fi
  [[ -n "$META_PID" ]] && kill "$META_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

count_pattern() {
  local file=$1
  local pattern=$2
  grep -c "$pattern" "$file" 2>/dev/null || true
}

# Number of put/get result lines of the given kind after line `start`.
count_results_after() {
  local file=$1
  local start=$2
  local kind=$3  # ok | fail | any
  awk -v start="$start" -v kind="$kind" '
    NR > start {
      if (kind == "ok" && ($0 ~ /put_ok/ || $0 ~ /get_ok/)) count++
      else if (kind == "fail" && ($0 ~ /put_fail/ || $0 ~ /get_fail/)) count++
      else if (kind == "any" && ($0 ~ /put_ok/ || $0 ~ /get_ok/ || $0 ~ /put_fail/ || $0 ~ /get_fail/)) count++
    }
    END { print count + 0 }
  ' "$file"
}

fail() {
  echo "FAIL: $*"
  {
    echo "=== client tail ==="
    tail -n 80 "$LOG_DIR/client.log"
    echo "=== master tail ==="
    tail -n 40 "$LOG_DIR/master.log"
  } 2>/dev/null || true
  exit 1
}

rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"

sudo -n sh -c 'echo 512 > /proc/sys/vm/nr_hugepages'
sudo -n umount /dev/hugepages >/dev/null 2>&1 || true
sudo -n mkdir -p /dev/hugepages
sudo -n mount -t hugetlbfs -o pagesize=2M,mode=1777 none /dev/hugepages
grep -E 'HugePages_Total|HugePages_Free|Hugepagesize' /proc/meminfo >"$LOG_DIR/hugepages.log"

pkill -f '/nvmf_tgt' >/dev/null 2>&1 || true
pkill -f 'mooncake_master' >/dev/null 2>&1 || true
pkill -f 'http_metadata_server.py' >/dev/null 2>&1 || true
sleep 1

"$REPO_ROOT/extern/spdk/build/bin/nvmf_tgt" -m 0x1 -u --iova-mode=va --wait-for-rpc >"$LOG_DIR/target.log" 2>&1 &
TARGET_PID=$!
sleep 3

python3 "$REPO_ROOT/extern/spdk/scripts/rpc.py" framework_start_init >/dev/null
python3 "$REPO_ROOT/extern/spdk/scripts/rpc.py" framework_wait_init >/dev/null
python3 "$REPO_ROOT/extern/spdk/scripts/rpc.py" bdev_malloc_create -b Malloc0 64 4096 >/dev/null
python3 "$REPO_ROOT/extern/spdk/scripts/rpc.py" nvmf_create_transport -t TCP >/dev/null || true
python3 "$REPO_ROOT/extern/spdk/scripts/rpc.py" nvmf_create_subsystem "$TARGET_NQN" -a -s SPDK00000000000001 >/dev/null || true
python3 "$REPO_ROOT/extern/spdk/scripts/rpc.py" nvmf_subsystem_add_ns "$TARGET_NQN" Malloc0 >/dev/null || true
python3 "$REPO_ROOT/extern/spdk/scripts/rpc.py" nvmf_subsystem_add_listener "$TARGET_NQN" -t tcp -a "$TARGET_HOST" -s "$TARGET_PORT" >/dev/null || true

python3 "$REPO_ROOT/mooncake-wheel/mooncake/http_metadata_server.py" --host "$METADATA_HOST" --port "$METADATA_PORT" >"$LOG_DIR/metadata.log" 2>&1 &
META_PID=$!
sleep 2

"$BUILD_DIR/mooncake-store/src/mooncake_master" \
  --rpc_address="$MASTER_HOST" \
  --rpc_port="$MASTER_PORT" \
  --enable_http_metadata_server=false \
  --logtostderr=true \
  --nof_heartbeat_interval_sec="$HEARTBEAT_INTERVAL" \
  --nof_heartbeat_probe_timeout_ms="$HEARTBEAT_TIMEOUT_MS" \
  --nof_heartbeat_failures_threshold="$HEARTBEAT_FAILURES" >"$LOG_DIR/master.log" 2>&1 &
MASTER_PID=$!
sleep 3

PYTHONPATH="$BUILD_DIR/mooncake-integration" MC_NOF_TRTYPE=TCP python3 - <<PY >"$LOG_DIR/register.log" 2>&1
import store
ret = store.MooncakeDistributedNoFRegister().real_register(
    "$TARGET_NQN",
    1,
    "$TARGET_HOST",
    int("$TARGET_PORT"),
    0,
    int("$NOF_SIZE"),
    "$MASTER_RPC",
)
print(f"register_ret {ret}", flush=True)
raise SystemExit(0 if ret == 0 else 1)
PY

# MC_NOF_DEBUG + GLOG_logtostderr put the worker's nof_qos_state snapshots and
# the timeout/drain messages into client.log next to the put/get results.
PYTHONPATH="$BUILD_DIR/mooncake-integration" \
MC_NOF_IO_TIMEOUT_MS="$IO_TIMEOUT_MS" MC_NOF_DEBUG=1 GLOG_logtostderr=1 \
python3 "$SCRIPT_DIR/store_client_e2e.py" \
  --local-hostname "127.0.0.1:50071" \
  --metadata-server "$METADATA_SERVER" \
  --master-server "$MASTER_RPC" \
  --global-segment-size "$CLIENT_GLOBAL_SEGMENT_SIZE" \
  --local-buffer-size "$CLIENT_LOCAL_BUFFER_SIZE" \
  --memory-replica-num "$CLIENT_MEMORY_REPLICA_NUM" \
  --nof-replica-num "$CLIENT_NOF_REPLICA_NUM" \
  --payload-size "$PAYLOAD_SIZE" \
  --duration-sec "$CLIENT_DURATION" \
  --sleep-ms "$CLIENT_SLEEP_MS" \
  --key-prefix "nof-stalled-io" >"$LOG_DIR/client.log" 2>&1 &
CLIENT_PID=$!

deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
  put_ok_count=$(count_pattern "$LOG_DIR/client.log" 'put_ok')
  get_ok_count=$(count_pattern "$LOG_DIR/client.log" 'get_ok')
  if (( put_ok_count >= PRE_FAULT_SUCCESS_TARGET && get_ok_count >= PRE_FAULT_SUCCESS_TARGET )); then
    break
  fi
  sleep 1
done

put_ok_count=$(count_pattern "$LOG_DIR/client.log" 'put_ok')
get_ok_count=$(count_pattern "$LOG_DIR/client.log" 'get_ok')
if (( put_ok_count < PRE_FAULT_SUCCESS_TARGET || get_ok_count < PRE_FAULT_SUCCESS_TARGET )); then
  fail "client did not reach enough initial success: put_ok=$put_ok_count get_ok=$get_ok_count"
fi

# --- stall: the target stops answering but its connection stays up ---------
pre_stall_line_count=$(wc -l <"$LOG_DIR/client.log")
kill -STOP "$TARGET_PID"
stall_start=$SECONDS

stall_window_sec=$(( IO_TIMEOUT_MS / 1000 + STALL_SLACK_SEC ))
first_failure_sec=-1
while (( SECONDS - stall_start < stall_window_sec )); do
  if (( $(count_results_after "$LOG_DIR/client.log" "$pre_stall_line_count" fail) > 0 )); then
    first_failure_sec=$((SECONDS - stall_start))
    break
  fi
  sleep 1
done

if (( first_failure_sec < 0 )); then
  fail "client I/O still unresolved ${stall_window_sec}s after the target was suspended (MC_NOF_IO_TIMEOUT_MS=$IO_TIMEOUT_MS); this is the #3864 hang"
fi

timeout_lines=$(count_pattern "$LOG_DIR/client.log" 'NoF I/O timed out')
drained_lines=$(count_pattern "$LOG_DIR/client.log" 'aborted sub-I/Os')
if (( timeout_lines <= 0 || drained_lines <= 0 )); then
  fail "client failed the I/O but the worker did not report a timeout + drain (timed_out=$timeout_lines drained=$drained_lines)"
fi
if (( $(count_pattern "$LOG_DIR/client.log" 'outstanding io < 0') > 0 || $(count_pattern "$LOG_DIR/client.log" 'inflight blocks < 0') > 0 )); then
  fail "sub-I/O accounting went negative during the drain"
fi

# --- after the timeout: the client must keep going, fail-fast on the dead
# qpair, and the worker's accounting must be back at zero -------------------
post_timeout_line_count=$(wc -l <"$LOG_DIR/client.log")
observation_deadline=$((SECONDS + POST_TIMEOUT_OBSERVATION_SEC))
post_timeout_results=0
while (( SECONDS < observation_deadline )); do
  post_timeout_results=$(count_results_after "$LOG_DIR/client.log" "$post_timeout_line_count" any)
  (( post_timeout_results >= 2 )) && break
  sleep 1
done
if (( post_timeout_results < 2 )); then
  fail "client stopped making progress after the first timed-out I/O (results=$post_timeout_results)"
fi
if ! kill -0 "$CLIENT_PID" >/dev/null 2>&1; then
  fail "client process died after the timed-out I/O"
fi
if (( $(count_pattern "$LOG_DIR/client.log" 'data_mismatch') > 0 )); then
  fail "data mismatch observed"
fi

last_qos_state=$(grep 'nof_qos_state' "$LOG_DIR/client.log" | tail -n 1 || true)
if [[ -n "$last_qos_state" ]]; then
  if ! grep -q 'inflight_read=0 inflight_write=0' <<<"$last_qos_state" || \
     ! grep -q 'state=0' <<<"$last_qos_state" || \
     ! grep -q 'total_outstanding_io=0' <<<"$last_qos_state"; then
    fail "worker accounting did not return to baseline: $last_qos_state"
  fi
else
  echo "warning: no nof_qos_state snapshot found in client.log"
fi

# --- resume the target: nothing may crash on either side --------------------
kill -CONT "$TARGET_PID"
sleep 3
if ! kill -0 "$TARGET_PID" >/dev/null 2>&1; then
  fail "nvmf_tgt died after being resumed"
fi
if ! kill -0 "$CLIENT_PID" >/dev/null 2>&1; then
  fail "client process died after the target was resumed"
fi
post_resume_line_count=$(wc -l <"$LOG_DIR/client.log")
sleep 5
post_resume_results=$(count_results_after "$LOG_DIR/client.log" "$post_resume_line_count" any)
post_resume_successes=$(count_results_after "$LOG_DIR/client.log" "$post_resume_line_count" ok)

kill "$CLIENT_PID" >/dev/null 2>&1 || true
wait "$CLIENT_PID" >/dev/null 2>&1 || true
CLIENT_PID=""

{
  echo "=== hugepages ==="
  cat "$LOG_DIR/hugepages.log"
  echo "=== register ==="
  cat "$LOG_DIR/register.log"
  echo "=== client ==="
  cat "$LOG_DIR/client.log"
  echo "=== master tail ==="
  tail -n 100 "$LOG_DIR/master.log"
  echo "=== target tail ==="
  tail -n 60 "$LOG_DIR/target.log"
  echo "=== verdict ==="
  echo "io_timeout_ms=$IO_TIMEOUT_MS"
  echo "first_failure_sec_after_stall=$first_failure_sec"
  echo "timeout_lines=$timeout_lines"
  echo "drained_lines=$drained_lines"
  echo "post_timeout_results=$post_timeout_results"
  echo "post_resume_results=$post_resume_results"
  echo "post_resume_successes=$post_resume_successes"
  echo "last_qos_state=$last_qos_state"
  echo "pre_fault_put_ok=$put_ok_count"
  echo "pre_fault_get_ok=$get_ok_count"
  echo "client_global_segment_size=$CLIENT_GLOBAL_SEGMENT_SIZE"
} >"$LOG_DIR/summary.log"

cat "$LOG_DIR/summary.log"
