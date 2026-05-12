#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
BUILD_DIR=${BUILD_DIR:-"$REPO_ROOT/build"}
LOG_DIR=${LOG_DIR:-/tmp/mooncake_nof_heartbeat_e2e}
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
CLIENT_DURATION=${CLIENT_DURATION:-20}
CLIENT_SLEEP_MS=${CLIENT_SLEEP_MS:-200}
HEARTBEAT_INTERVAL=${HEARTBEAT_INTERVAL:-2}
HEARTBEAT_TIMEOUT_MS=${HEARTBEAT_TIMEOUT_MS:-500}
HEARTBEAT_FAILURES=${HEARTBEAT_FAILURES:-3}
CLIENT_GLOBAL_SEGMENT_SIZE=${CLIENT_GLOBAL_SEGMENT_SIZE:-67108864}
CLIENT_LOCAL_BUFFER_SIZE=${CLIENT_LOCAL_BUFFER_SIZE:-33554432}
CLIENT_MEMORY_REPLICA_NUM=${CLIENT_MEMORY_REPLICA_NUM:-1}
CLIENT_NOF_REPLICA_NUM=${CLIENT_NOF_REPLICA_NUM:-1}
PRE_FAULT_SUCCESS_TARGET=${PRE_FAULT_SUCCESS_TARGET:-3}

TARGET_PID=""
MASTER_PID=""
META_PID=""
CLIENT_PID=""

cleanup() {
  [[ -n "$CLIENT_PID" ]] && kill "$CLIENT_PID" >/dev/null 2>&1 || true
  [[ -n "$MASTER_PID" ]] && kill "$MASTER_PID" >/dev/null 2>&1 || true
  [[ -n "$TARGET_PID" ]] && kill "$TARGET_PID" >/dev/null 2>&1 || true
  [[ -n "$META_PID" ]] && kill "$META_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_pattern() {
  local file=$1
  local pattern=$2
  local timeout=$3
  local waited=0
  while (( waited < timeout )); do
    if grep -q "$pattern" "$file" 2>/dev/null; then
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  return 1
}

count_pattern() {
  local file=$1
  local pattern=$2
  grep -c "$pattern" "$file" 2>/dev/null || true
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

"$REPO_ROOT/thirdparties/spdk/build/bin/nvmf_tgt" -m 0x1 -u --iova-mode=va --wait-for-rpc >"$LOG_DIR/target.log" 2>&1 &
TARGET_PID=$!
sleep 3

python3 "$REPO_ROOT/thirdparties/spdk/scripts/rpc.py" framework_start_init >/dev/null
python3 "$REPO_ROOT/thirdparties/spdk/scripts/rpc.py" framework_wait_init >/dev/null
python3 "$REPO_ROOT/thirdparties/spdk/scripts/rpc.py" bdev_malloc_create -b Malloc0 64 4096 >/dev/null
python3 "$REPO_ROOT/thirdparties/spdk/scripts/rpc.py" nvmf_create_transport -t TCP >/dev/null || true
python3 "$REPO_ROOT/thirdparties/spdk/scripts/rpc.py" nvmf_create_subsystem "$TARGET_NQN" -a -s SPDK00000000000001 >/dev/null || true
python3 "$REPO_ROOT/thirdparties/spdk/scripts/rpc.py" nvmf_subsystem_add_ns "$TARGET_NQN" Malloc0 >/dev/null || true
python3 "$REPO_ROOT/thirdparties/spdk/scripts/rpc.py" nvmf_subsystem_add_listener "$TARGET_NQN" -t tcp -a "$TARGET_HOST" -s "$TARGET_PORT" >/dev/null || true

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

PYTHONPATH="$BUILD_DIR/mooncake-integration" python3 "$SCRIPT_DIR/store_client_e2e.py" \
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
  --key-prefix "nof-heartbeat" >"$LOG_DIR/client.log" 2>&1 &
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
  echo "client did not reach enough initial success: put_ok=$put_ok_count get_ok=$get_ok_count"
  exit 1
fi

pre_fault_line_count=$(wc -l <"$LOG_DIR/client.log")

kill "$TARGET_PID" >/dev/null 2>&1 || true
TARGET_PID=""

if ! wait_for_pattern "$LOG_DIR/master.log" 'action=unmount_nof_segment_by_heartbeat' $((HEARTBEAT_INTERVAL * HEARTBEAT_FAILURES + 20)); then
  echo "master did not unmount nof segment by heartbeat"
  exit 1
fi

post_unmount_line_count=$(wc -l <"$LOG_DIR/client.log")

wait "$CLIENT_PID"
CLIENT_PID=""

post_fault_successes=$(awk -v start="$pre_fault_line_count" '
  NR > start && ($0 ~ /put_ok/ || $0 ~ /get_ok/) { count++ }
  END { print count + 0 }
' "$LOG_DIR/client.log")

post_fault_failures=$(awk -v start="$pre_fault_line_count" '
  NR > start && ($0 ~ /put_fail/ || $0 ~ /get_fail/) { count++ }
  END { print count + 0 }
' "$LOG_DIR/client.log")

post_unmount_successes=$(awk -v start="$post_unmount_line_count" '
  NR > start && ($0 ~ /put_ok/ || $0 ~ /get_ok/) { count++ }
  END { print count + 0 }
' "$LOG_DIR/client.log")

post_unmount_failures=$(awk -v start="$post_unmount_line_count" '
  NR > start && ($0 ~ /put_fail/ || $0 ~ /get_fail/) { count++ }
  END { print count + 0 }
' "$LOG_DIR/client.log")

if [[ "$CLIENT_GLOBAL_SEGMENT_SIZE" -eq 0 ]]; then
  if [[ "$post_unmount_failures" -le 0 ]]; then
    echo "expected IO failures after target down in nof-only mode"
    exit 1
  fi
else
  if [[ "$post_unmount_successes" -le 0 ]]; then
    echo "no successful IO observed after target down"
    exit 1
  fi
fi

{
  echo "=== hugepages ==="
  cat "$LOG_DIR/hugepages.log"
  echo "=== register ==="
  cat "$LOG_DIR/register.log"
  echo "=== client ==="
  cat "$LOG_DIR/client.log"
  echo "=== master tail ==="
  tail -n 200 "$LOG_DIR/master.log"
  echo "=== metadata tail ==="
  tail -n 80 "$LOG_DIR/metadata.log"
  echo "=== target tail ==="
  tail -n 120 "$LOG_DIR/target.log"
  echo "=== verdict ==="
  echo "post_fault_successes=$post_fault_successes"
  echo "post_fault_failures=$post_fault_failures"
  echo "post_unmount_successes=$post_unmount_successes"
  echo "post_unmount_failures=$post_unmount_failures"
  echo "pre_fault_put_ok=$put_ok_count"
  echo "pre_fault_get_ok=$get_ok_count"
  echo "pre_fault_line_count=$pre_fault_line_count"
  echo "post_unmount_line_count=$post_unmount_line_count"
  echo "client_global_segment_size=$CLIENT_GLOBAL_SEGMENT_SIZE"
  echo "client_memory_replica_num=$CLIENT_MEMORY_REPLICA_NUM"
  echo "client_nof_replica_num=$CLIENT_NOF_REPLICA_NUM"
} >"$LOG_DIR/summary.log"

cat "$LOG_DIR/summary.log"
