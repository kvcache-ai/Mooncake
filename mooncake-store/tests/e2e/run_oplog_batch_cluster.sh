#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)

die() {
  echo "error: $*" >&2
  exit 1
}

usage() {
  echo "Usage: $0 <up|status|collect|restart|down|smoke|restart-smoke|failpoint-smoke|failpoint-crash-smoke|remove-boundary-smoke|standby-read-smoke|promotion-catchup-smoke|non-ha-smoke> [options]" >&2
}

parse_up_options() {
  BUILD_DIR="$REPO_ROOT/build"
  RUN_DIR=""
  MASTER_COUNT=3
  ENABLE_HA=true
  CLIENT_COUNT=0
  CLUSTER_ID="oplog-test-$$"
  PROTOCOL=tcp
  OPLOG_STORE_TYPE=etcd_batch_record
  BATCH_ENTRIES=1024
  RETRY_TIMEOUT_SEC=180
  MASTER_CONFIG=""
  USE_ETCD_OBSERVER=true
  NON_HA_WORKERS=4
  FAILPOINT_DIR=""
  FAILPOINT_TIMEOUT_SEC=30
  START_TIMEOUT_SEC=30
  ETCD_ENDPOINTS=""
  ETCD_BIN=${ETCD_BIN:-etcd}
  while (($#)); do
    case "$1" in
      --build-dir)
        (($# >= 2)) || die "--build-dir requires a value"
        BUILD_DIR=$2
        shift 2
        ;;
      --run-dir)
        (($# >= 2)) || die "--run-dir requires a value"
        RUN_DIR=$2
        shift 2
        ;;
      --masters)
        (($# >= 2)) || die "--masters requires a value"
        MASTER_COUNT=$2
        shift 2
        ;;
      --clients)
        (($# >= 2)) || die "--clients requires a value"
        CLIENT_COUNT=$2
        shift 2
        ;;
      --cluster-id)
        (($# >= 2)) || die "--cluster-id requires a value"
        CLUSTER_ID=$2
        shift 2
        ;;
      --protocol)
        (($# >= 2)) || die "--protocol requires a value"
        PROTOCOL=$2
        shift 2
        ;;
      --oplog-store)
        (($# >= 2)) || die "--oplog-store requires a value"
        OPLOG_STORE_TYPE=$2
        shift 2
        ;;
      --batch-entries)
        (($# >= 2)) || die "--batch-entries requires a value"
        BATCH_ENTRIES=$2
        shift 2
        ;;
      --retry-timeout-sec)
        (($# >= 2)) || die "--retry-timeout-sec requires a value"
        RETRY_TIMEOUT_SEC=$2
        shift 2
        ;;
      --master-config)
        (($# >= 2)) || die "--master-config requires a value"
        MASTER_CONFIG=$2
        shift 2
        ;;
      --no-etcd-observer)
        USE_ETCD_OBSERVER=false
        shift
        ;;
      --non-ha-workers)
        (($# >= 2)) || die "--non-ha-workers requires a value"
        NON_HA_WORKERS=$2
        shift 2
        ;;
      --timeout-sec)
        (($# >= 2)) || die "--timeout-sec requires a value"
        START_TIMEOUT_SEC=$2
        shift 2
        ;;
      --failpoint-dir)
        (($# >= 2)) || die "--failpoint-dir requires a value"
        FAILPOINT_DIR=$2
        shift 2
        ;;
      --failpoint-timeout-sec)
        (($# >= 2)) || die "--failpoint-timeout-sec requires a value"
        FAILPOINT_TIMEOUT_SEC=$2
        shift 2
        ;;
      --etcd-endpoints)
        (($# >= 2)) || die "--etcd-endpoints requires a value"
        ETCD_ENDPOINTS=$2
        shift 2
        ;;
      --etcd-bin)
        (($# >= 2)) || die "--etcd-bin requires a value"
        ETCD_BIN=$2
        shift 2
        ;;
      *) die "unknown option: $1" ;;
    esac
  done
  [[ -d "$BUILD_DIR" ]] || die "build directory does not exist: $BUILD_DIR"
  [[ "$MASTER_COUNT" =~ ^[1-9][0-9]*$ ]] || die "masters must be positive"
  [[ "$CLIENT_COUNT" =~ ^[0-9]+$ ]] || die "clients must be non-negative"
  [[ "$BATCH_ENTRIES" =~ ^[1-9][0-9]*$ ]] ||
    die "batch-entries must be positive"
  [[ "$START_TIMEOUT_SEC" =~ ^[1-9][0-9]*$ ]] ||
    die "timeout-sec must be positive"
  [[ "$FAILPOINT_TIMEOUT_SEC" =~ ^[1-9][0-9]*$ ]] ||
    die "failpoint-timeout-sec must be positive"
  [[ "$NON_HA_WORKERS" =~ ^[1-9][0-9]*$ ]] ||
    die "non-ha-workers must be positive"
  [[ "$PROTOCOL" == tcp || "$PROTOCOL" == rdma ]] ||
    die "protocol must be tcp or rdma"
  [[ -z "$MASTER_CONFIG" || -f "$MASTER_CONFIG" ]] ||
    die "master config does not exist: $MASTER_CONFIG"
  if [[ -z "$RUN_DIR" ]]; then
    RUN_DIR="/tmp/mooncake-oplog-test/$(date +%Y%m%d-%H%M%S)-$$"
  fi
}

require_executable() {
  [[ -x "$1" ]] || die "missing executable: $1"
}

up_cluster() {
  MASTER_BIN="$BUILD_DIR/mooncake-store/src/mooncake_master"
  INSPECTOR_BIN="$BUILD_DIR/mooncake-store/tools/oplog_batch_inspector"
  METADATA_SCRIPT="$REPO_ROOT/mooncake-wheel/mooncake/http_metadata_server.py"
  require_executable "$MASTER_BIN"
  [[ "$USE_ETCD_OBSERVER" != true ]] || require_executable "$INSPECTOR_BIN"
  [[ -f "$METADATA_SCRIPT" ]] || die "missing executable: $METADATA_SCRIPT"
  command -v python3 >/dev/null || die "missing executable: python3"
  command -v curl >/dev/null || die "missing executable: curl"
  command -v setsid >/dev/null || die "missing executable: setsid"
  if [[ "$USE_ETCD_OBSERVER" == true && -z "$ETCD_ENDPOINTS" ]]; then
    command -v "$ETCD_BIN" >/dev/null || die "missing executable: $ETCD_BIN"
  fi
  python3 -c 'import aiohttp' >/dev/null 2>&1 ||
    die "missing Python dependency aiohttp; activate the project Python environment"

  mkdir -p "$RUN_DIR"/{audit,configs,etcd,logs,metrics,pids,workload}
  METADATA_PORT=$(find_free_port)
  local etcd_client_port
  local etcd_peer_port
  if [[ "$USE_ETCD_OBSERVER" == true && -z "$ETCD_ENDPOINTS" ]]; then
    etcd_client_port=$(find_free_port)
    etcd_peer_port=$(find_free_port)
    ETCD_ENDPOINTS="127.0.0.1:$etcd_client_port"
    start_process etcd \
      "$ETCD_BIN" --name "$CLUSTER_ID-etcd" \
      --data-dir "$RUN_DIR/etcd/data" \
      --listen-client-urls "http://127.0.0.1:$etcd_client_port" \
      --advertise-client-urls "http://127.0.0.1:$etcd_client_port" \
      --listen-peer-urls "http://127.0.0.1:$etcd_peer_port" \
      --initial-advertise-peer-urls "http://127.0.0.1:$etcd_peer_port" \
      --initial-cluster "$CLUSTER_ID-etcd=http://127.0.0.1:$etcd_peer_port"
    wait_http "http://127.0.0.1:$etcd_client_port/health" "$START_TIMEOUT_SEC" ||
      die "local etcd did not become healthy; see $RUN_DIR/logs/etcd.err"
  fi

  start_process metadata python3 "$METADATA_SCRIPT" --host 127.0.0.1 \
    --port "$METADATA_PORT"
  wait_port 127.0.0.1 "$METADATA_PORT" "$START_TIMEOUT_SEC" ||
    die "metadata server did not start; see $RUN_DIR/logs/metadata.err"

  RPC_PORTS=()
  ADMIN_PORTS=()
  local index
  for ((index = 0; index < MASTER_COUNT; ++index)); do
    local rpc_port
    local admin_port
    rpc_port=$(find_free_port)
    admin_port=$(find_free_port)
    RPC_PORTS+=("$rpc_port")
    ADMIN_PORTS+=("$admin_port")
    start_master "$index"
  done

  write_cluster_env
  write_manifest
  wait_for_single_leader "$START_TIMEOUT_SEC" || {
    collect_cluster || true
    die "cluster did not elect exactly one ready leader; see $RUN_DIR/logs"
  }

  CLIENT_BIN="$BUILD_DIR/mooncake-store/tests/e2e/client_runner"
  if ((CLIENT_COUNT > 0)); then
    require_executable "$CLIENT_BIN"
    for ((index = 0; index < CLIENT_COUNT; ++index)); do
      start_process "client-$index" "$CLIENT_BIN" \
        --port="$((17812 + index))" \
        --master_server_entry="etcd://$ETCD_ENDPOINTS" \
        --engine_meta_url="http://127.0.0.1:$METADATA_PORT/metadata" \
        --protocol="$PROTOCOL"
    done
  fi
  echo "cluster started: $RUN_DIR"
  status_cluster
}

find_free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

wait_http() {
  local url=$1
  local timeout=$2
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    curl -fsS "$url" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  return 1
}

wait_port() {
  local host=$1
  local port=$2
  local timeout=$3
  python3 -c 'import socket,sys,time
host,port,timeout=sys.argv[1],int(sys.argv[2]),float(sys.argv[3])
deadline=time.time()+timeout
while time.time()<deadline:
    try:
        with socket.create_connection((host,port),timeout=.2): sys.exit(0)
    except OSError: time.sleep(.1)
sys.exit(1)' "$host" "$port" "$timeout"
}

start_process() {
  local name=$1
  shift
  setsid "$@" </dev/null >>"$RUN_DIR/logs/$name.out" \
    2>>"$RUN_DIR/logs/$name.err" &
  local pid=$!
  printf '%s\n' "$pid" >"$RUN_DIR/pids/$name.pid"
  local deadline=$((SECONDS + 2))
  while [[ ! -r "/proc/$pid/cmdline" ]] && ((SECONDS < deadline)); do
    sleep 0.05
  done
  sleep 0.05
  if kill -0 "$pid" 2>/dev/null && [[ -r "/proc/$pid/cmdline" ]]; then
    tr '\0' ' ' <"/proc/$pid/cmdline" >"$RUN_DIR/pids/$name.cmd"
  else
    wait "$pid" || true
    die "$name exited during startup; see $RUN_DIR/logs/$name.err"
  fi
}

start_master() {
  local index=$1
  local -a environment=()
  if [[ -n "$FAILPOINT_DIR" ]]; then
    mkdir -p "$FAILPOINT_DIR"
    environment=(env "MOONCAKE_TEST_FAILPOINT_DIR=$FAILPOINT_DIR"
      "MOONCAKE_TEST_FAILPOINT_TIMEOUT_SEC=$FAILPOINT_TIMEOUT_SEC")
  fi
  local -a ha_args=(--enable_ha=false)
  local -a config_args=()
  [[ -z "$MASTER_CONFIG" ]] || config_args=(--config_path="$MASTER_CONFIG")
  if [[ "$ENABLE_HA" == true ]]; then
    ha_args=(--enable_ha=true --ha_backend_type=etcd
      --ha_backend_connstring="$ETCD_ENDPOINTS"
      --etcd_endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID"
      --oplog_store_type="$OPLOG_STORE_TYPE"
      --oplog_batch_max_entries="$BATCH_ENTRIES"
      --batch_oplog_retry_timeout_sec="$RETRY_TIMEOUT_SEC")
  fi
  start_process "master-$index" "${environment[@]}" "$MASTER_BIN" \
    "${config_args[@]}" "${ha_args[@]}" \
    --rpc_address=127.0.0.1 --rpc_port="${RPC_PORTS[$index]}" \
    --metrics_port="${ADMIN_PORTS[$index]}" \
    --enable_http_metadata_server=false --logtostderr=true
}

write_cluster_env() {
  {
    printf 'RUN_DIR=%q\n' "$RUN_DIR"
    printf 'BUILD_DIR=%q\n' "$BUILD_DIR"
    printf 'CLUSTER_ID=%q\n' "$CLUSTER_ID"
    printf 'ETCD_ENDPOINTS=%q\n' "$ETCD_ENDPOINTS"
    printf 'METADATA_PORT=%q\n' "$METADATA_PORT"
    printf 'MASTER_COUNT=%q\n' "$MASTER_COUNT"
    printf 'ENABLE_HA=%q\n' "$ENABLE_HA"
    printf 'PROTOCOL=%q\n' "$PROTOCOL"
    printf 'OPLOG_STORE_TYPE=%q\n' "$OPLOG_STORE_TYPE"
    printf 'BATCH_ENTRIES=%q\n' "$BATCH_ENTRIES"
    printf 'RETRY_TIMEOUT_SEC=%q\n' "$RETRY_TIMEOUT_SEC"
    printf 'MASTER_CONFIG=%q\n' "$MASTER_CONFIG"
    printf 'USE_ETCD_OBSERVER=%q\n' "$USE_ETCD_OBSERVER"
    printf 'NON_HA_WORKERS=%q\n' "$NON_HA_WORKERS"
    printf 'START_TIMEOUT_SEC=%q\n' "$START_TIMEOUT_SEC"
    printf 'FAILPOINT_DIR=%q\n' "$FAILPOINT_DIR"
    printf 'FAILPOINT_TIMEOUT_SEC=%q\n' "$FAILPOINT_TIMEOUT_SEC"
    printf 'RPC_PORTS=%q\n' "${RPC_PORTS[*]}"
    printf 'ADMIN_PORTS=%q\n' "${ADMIN_PORTS[*]}"
    printf 'INSPECTOR_BIN=%q\n' "$INSPECTOR_BIN"
  } >"$RUN_DIR/cluster.env"
}

write_manifest() {
  python3 -c 'import json,subprocess,sys,time
path,run_dir,build,cluster,endpoints,masters=sys.argv[1:]
try: commit=subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip()
except Exception: commit="unknown"
with open(path,"w",encoding="utf-8") as f:
 json.dump({"schema_version":1,"run_id":run_dir.rsplit("/",1)[-1],"repo_commit":commit,"build_dir":build,"cluster_id":cluster,"etcd_endpoints":endpoints,"masters":int(masters),"created_at_epoch":time.time()},f,sort_keys=True)
 f.write("\n")' "$RUN_DIR/manifest.json" "$RUN_DIR" "$BUILD_DIR" \
    "$CLUSTER_ID" "$ETCD_ENDPOINTS" "$MASTER_COUNT"
}

load_cluster_env() {
  [[ -f "$RUN_DIR/cluster.env" ]] || die "missing cluster.env: $RUN_DIR"
  # This file is generated by this script with shell-escaped values.
  source "$RUN_DIR/cluster.env"
  read -r -a RPC_PORTS <<<"$RPC_PORTS"
  read -r -a ADMIN_PORTS <<<"$ADMIN_PORTS"
  MASTER_COUNT=${MASTER_COUNT:-${#RPC_PORTS[@]}}
  ENABLE_HA=${ENABLE_HA:-true}
  PROTOCOL=${PROTOCOL:-tcp}
  OPLOG_STORE_TYPE=${OPLOG_STORE_TYPE:-etcd_batch_record}
  BATCH_ENTRIES=${BATCH_ENTRIES:-1024}
  RETRY_TIMEOUT_SEC=${RETRY_TIMEOUT_SEC:-180}
  MASTER_CONFIG=${MASTER_CONFIG:-}
  USE_ETCD_OBSERVER=${USE_ETCD_OBSERVER:-true}
  NON_HA_WORKERS=${NON_HA_WORKERS:-4}
  START_TIMEOUT_SEC=${START_TIMEOUT_SEC:-30}
  FAILPOINT_DIR=${FAILPOINT_DIR:-}
  FAILPOINT_TIMEOUT_SEC=${FAILPOINT_TIMEOUT_SEC:-30}
  MASTER_BIN="$BUILD_DIR/mooncake-store/src/mooncake_master"
}

restart_masters() {
  require_executable "$MASTER_BIN"
  local pid_file
  shopt -s nullglob
  for pid_file in "$RUN_DIR"/pids/master-*.pid; do
    stop_pid_file "$pid_file"
  done
  local index
  for ((index = 0; index < MASTER_COUNT; ++index)); do
    start_master "$index"
  done
  wait_for_single_leader "$START_TIMEOUT_SEC" || {
    collect_cluster || true
    die "restarted cluster did not elect exactly one ready leader; see $RUN_DIR/logs"
  }
  echo "masters restarted: $RUN_DIR"
}

wait_for_single_leader() {
  local timeout=$1
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    local ready=0
    local port
    for port in "${ADMIN_PORTS[@]}"; do
      local health
      health=$(curl -fsS "http://127.0.0.1:$port/health" 2>/dev/null || true)
      grep -Eq '"service_ready"[[:space:]]*:[[:space:]]*true' <<<"$health" &&
        ready=$((ready + 1))
    done
    ((ready == 1)) && return 0
    sleep 0.2
  done
  return 1
}

status_cluster() {
  [[ ${#ADMIN_PORTS[@]} -gt 0 ]] || load_cluster_env
  echo "run_dir=$RUN_DIR cluster_id=$CLUSTER_ID etcd=$ETCD_ENDPOINTS"
  local index
  for index in "${!ADMIN_PORTS[@]}"; do
    local health
    health=$(curl -fsS "http://127.0.0.1:${ADMIN_PORTS[$index]}/health" 2>/dev/null || echo unavailable)
    echo "master-$index rpc=127.0.0.1:${RPC_PORTS[$index]} admin=127.0.0.1:${ADMIN_PORTS[$index]} $health"
  done
  if [[ "$USE_ETCD_OBSERVER" == true ]]; then
    LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" summary \
      --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json || true
  fi
}

collect_cluster() {
  [[ ${#ADMIN_PORTS[@]} -gt 0 ]] || load_cluster_env
  local index
  for index in "${!ADMIN_PORTS[@]}"; do
    curl -fsS "http://127.0.0.1:${ADMIN_PORTS[$index]}/health" \
      >"$RUN_DIR/metrics/master-$index-health.json" 2>/dev/null || true
    curl -fsS "http://127.0.0.1:${ADMIN_PORTS[$index]}/metrics" \
      >"$RUN_DIR/metrics/master-$index.prom" 2>/dev/null || true
  done
  if [[ "$USE_ETCD_OBSERVER" == true ]]; then
    LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" summary \
      --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json \
      >"$RUN_DIR/audit/summary.json" || true
    LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" verify \
      --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json \
      >"$RUN_DIR/audit/verify.json" || true
  fi
  echo "artifacts collected: $RUN_DIR"
}

run_client_smoke() {
  local label=$1
  local hold_seconds=${2:-0}
  local clientctl="$BUILD_DIR/mooncake-store/tests/e2e/clientctl"
  require_executable "$clientctl"
  local client_port master_entry
  client_port=$(find_free_port)
  master_entry="etcd://$ETCD_ENDPOINTS"
  [[ "$ENABLE_HA" == true ]] || master_entry="127.0.0.1:${RPC_PORTS[0]}"
  if ! {
    printf 'create %s %s\n' "$label" "$client_port"
    printf 'mount %s segment-%s %s\n' "$label" "$label" \
      "$((64 * 1024 * 1024))"
    printf 'put %s oplog-%s-%s value-%s\n' \
      "$label" "$label" "$CLUSTER_ID" "$label"
    printf 'get %s oplog-%s-%s\n' "$label" "$label" "$CLUSTER_ID"
    if ((hold_seconds > 0)); then
      printf 'sleep %s\n' "$hold_seconds"
    fi
    printf 'terminate\n'
  } | MC_STORE_CLUSTER_ID="$CLUSTER_ID" LSAN_OPTIONS=detect_leaks=0 \
    "$clientctl" \
    --master_server_entry="$master_entry" \
    --engine_meta_url="http://127.0.0.1:$METADATA_PORT/metadata" \
    --protocol="$PROTOCOL" >"$RUN_DIR/workload/$label.log" 2>&1; then
    return 1
  fi
  grep -Fq "Successfully put value" "$RUN_DIR/workload/$label.log" &&
    grep -Fq "Get value: value-$label" "$RUN_DIR/workload/$label.log"
}

read_durable_sequence() {
  LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" summary \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["durable_prefix"]["last_seq"])'
}

smoke_cluster() {
  up_cluster
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" verify \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json \
    >"$RUN_DIR/audit/before-workload.json"; then
    smoke_failed "initial OpLog verification failed"
    return 1
  fi

  if ! run_client_smoke smoke; then
    smoke_failed "minimal put/get workload failed"
    return 1
  fi

  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" wait \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" \
    --last_seq=1 --timeout_sec="$START_TIMEOUT_SEC"; then
    smoke_failed "OpLog did not become durable after workload"
    return 1
  fi
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" verify \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json \
    >"$RUN_DIR/audit/after-workload.json"; then
    smoke_failed "final OpLog verification failed"
    return 1
  fi

  collect_cluster
  down_cluster
  echo "PASS: smoke completed; artifacts: $RUN_DIR"
}

restart_smoke_cluster() {
  up_cluster
  if ! run_client_smoke before-restart; then
    smoke_failed "pre-restart put/get workload failed"
    return 1
  fi
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" wait \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" \
    --last_seq=1 --timeout_sec="$START_TIMEOUT_SEC"; then
    smoke_failed "pre-restart OpLog did not become durable"
    return 1
  fi
  local before_sequence
  before_sequence=$(read_durable_sequence) || {
    smoke_failed "failed to read pre-restart durable sequence"
    return 1
  }

  restart_masters
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" verify \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json \
    >"$RUN_DIR/audit/after-restart.json"; then
    smoke_failed "existing OpLog history failed verification after restart"
    return 1
  fi
  if ! run_client_smoke after-restart; then
    smoke_failed "post-restart put/get workload failed"
    return 1
  fi
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" wait \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" \
    --last_seq="$((before_sequence + 1))" \
    --timeout_sec="$START_TIMEOUT_SEC"; then
    smoke_failed "durable sequence did not advance after restart"
    return 1
  fi
  local after_sequence
  after_sequence=$(read_durable_sequence) || {
    smoke_failed "failed to read post-restart durable sequence"
    return 1
  }
  if ((after_sequence <= before_sequence)); then
    smoke_failed "durable sequence did not increase after restart"
    return 1
  fi

  collect_cluster
  down_cluster
  echo "PASS: restart smoke completed; before_seq=$before_sequence after_seq=$after_sequence artifacts: $RUN_DIR"
}

failpoint_smoke_cluster() {
  [[ -n "$FAILPOINT_DIR" ]] || FAILPOINT_DIR="$RUN_DIR/failpoints"
  local fault_ctl="$SCRIPT_DIR/oplog_fault_ctl.sh"
  require_executable "$fault_ctl"
  up_cluster

  local before_sequence
  before_sequence=$(read_durable_sequence) || {
    smoke_failed "failed to read initial durable sequence"
    return 1
  }
  local name=batch_txn_succeeded_before_callback
  FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint arm \
    --run-dir "$RUN_DIR" --name "$name"

  run_client_smoke failpoint-smoke &
  local client_pid=$!
  if ! FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint wait \
    --run-dir "$RUN_DIR" --name "$name" \
    --timeout-sec "$START_TIMEOUT_SEC"; then
    wait "$client_pid" || true
    smoke_failed "failpoint was not hit; ensure the build enables MOONCAKE_ENABLE_TEST_FAILPOINTS"
    return 1
  fi
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" verify \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json \
    >"$RUN_DIR/audit/during-failpoint.json"; then
    FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
      --run-dir "$RUN_DIR" --name "$name" || true
    wait "$client_pid" || true
    smoke_failed "OpLog verification failed while writer was paused"
    return 1
  fi
  FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
    --run-dir "$RUN_DIR" --name "$name"
  if ! wait "$client_pid"; then
    smoke_failed "client workload failed after failpoint release"
    return 1
  fi
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" wait \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" \
    --last_seq="$((before_sequence + 1))" \
    --timeout_sec="$START_TIMEOUT_SEC"; then
    smoke_failed "durable sequence did not advance after failpoint release"
    return 1
  fi
  local after_sequence
  after_sequence=$(read_durable_sequence) || {
    smoke_failed "failed to read final durable sequence"
    return 1
  }

  collect_cluster
  down_cluster
  echo "PASS: failpoint smoke completed; before_seq=$before_sequence after_seq=$after_sequence artifacts: $RUN_DIR"
}

failpoint_crash_smoke_cluster() {
  [[ -n "$FAILPOINT_DIR" ]] || FAILPOINT_DIR="$RUN_DIR/failpoints"
  local fault_ctl="$SCRIPT_DIR/oplog_fault_ctl.sh"
  require_executable "$fault_ctl"
  up_cluster

  local before_sequence
  before_sequence=$(read_durable_sequence) || {
    smoke_failed "failed to read initial durable sequence"
    return 1
  }
  local name=batch_txn_succeeded_before_callback
  FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint arm \
    --run-dir "$RUN_DIR" --name "$name"
  run_client_smoke before-leader-crash &
  local client_pid=$!
  if ! FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint wait \
    --run-dir "$RUN_DIR" --name "$name" \
    --timeout-sec "$START_TIMEOUT_SEC"; then
    wait "$client_pid" || true
    smoke_failed "failpoint was not hit; ensure the build enables MOONCAKE_ENABLE_TEST_FAILPOINTS"
    return 1
  fi

  local hit_pid
  hit_pid=$(<"$FAILPOINT_DIR/$name.hit")
  local master_name=""
  local pid_file
  for pid_file in "$RUN_DIR"/pids/master-*.pid; do
    if [[ $(<"$pid_file") == "$hit_pid" ]]; then
      master_name=$(basename "$pid_file" .pid)
      break
    fi
  done
  if [[ -z "$master_name" ]]; then
    wait "$client_pid" || true
    smoke_failed "failpoint PID does not match a managed master: $hit_pid"
    return 1
  fi
  "$fault_ctl" process kill --run-dir "$RUN_DIR" --name "$master_name"
  wait "$client_pid" || true

  if ! wait_for_single_leader "$START_TIMEOUT_SEC"; then
    smoke_failed "cluster did not elect a replacement leader"
    return 1
  fi
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" verify \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json \
    >"$RUN_DIR/audit/after-leader-crash.json"; then
    smoke_failed "durable OpLog failed verification after leader crash"
    return 1
  fi
  if ! run_client_smoke after-leader-crash; then
    smoke_failed "replacement leader did not complete a new client workload"
    return 1
  fi
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" wait \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" \
    --last_seq="$((before_sequence + 2))" \
    --timeout_sec="$START_TIMEOUT_SEC"; then
    smoke_failed "durable sequence did not advance after leader recovery"
    return 1
  fi
  local after_sequence
  after_sequence=$(read_durable_sequence) || {
    smoke_failed "failed to read recovered durable sequence"
    return 1
  }

  collect_cluster
  down_cluster
  echo "PASS: failpoint crash smoke completed; killed=$master_name before_seq=$before_sequence after_seq=$after_sequence artifacts: $RUN_DIR"
}

remove_boundary_smoke_cluster() {
  [[ -n "$FAILPOINT_DIR" ]] || FAILPOINT_DIR="$RUN_DIR/failpoints"
  local fault_ctl="$SCRIPT_DIR/oplog_fault_ctl.sh"
  local clientctl="$BUILD_DIR/mooncake-store/tests/e2e/clientctl"
  require_executable "$fault_ctl"
  require_executable "$clientctl"
  up_cluster

  local fifo="$RUN_DIR/workload/remove-boundary.fifo"
  local log="$RUN_DIR/workload/remove-boundary.log"
  local client_port key name
  client_port=$(find_free_port)
  key="remove-boundary-$CLUSTER_ID"
  name=remove-boundary
  mkfifo "$fifo"
  exec 3<>"$fifo"
  MC_STORE_CLUSTER_ID="$CLUSTER_ID" LSAN_OPTIONS=detect_leaks=0 \
    "$clientctl" --master_server_entry="etcd://$ETCD_ENDPOINTS" \
    --engine_meta_url="http://127.0.0.1:$METADATA_PORT/metadata" \
    --protocol="$PROTOCOL" <"$fifo" >"$log" 2>&1 &
  local client_pid=$!

  printf 'create %s %s\nmount %s segment-%s %s\nput %s %s value-before-delete\n' \
    "$name" "$client_port" "$name" "$name" "$((64 * 1024 * 1024))" \
    "$name" "$key" >&3
  if ! wait_file_text "$log" "Successfully put value for key: $key" \
    "$START_TIMEOUT_SEC"; then
    stop_boundary_client "$client_pid"
    smoke_failed "failed to prepare object for remove boundary test"
    return 1
  fi
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" wait \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" \
    --last_seq=1 --timeout_sec="$START_TIMEOUT_SEC"; then
    stop_boundary_client "$client_pid"
    smoke_failed "initial put did not become durable"
    return 1
  fi
  local before_sequence
  before_sequence=$(read_durable_sequence) || {
    stop_boundary_client "$client_pid"
    smoke_failed "failed to read durable sequence after initial put"
    return 1
  }

  local point=batch_before_txn
  FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint arm \
    --run-dir "$RUN_DIR" --name "$point"
  printf 'delete %s %s\n' "$name" "$key" >&3
  if ! FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint wait \
    --run-dir "$RUN_DIR" --name "$point" \
    --timeout-sec "$START_TIMEOUT_SEC"; then
    stop_boundary_client "$client_pid"
    smoke_failed "pre-transaction failpoint was not hit; ensure the build enables MOONCAKE_ENABLE_TEST_FAILPOINTS"
    return 1
  fi
  printf 'get %s %s\n' "$name" "$key" >&3
  if ! wait_file_text "$log" "Failed to get value: OBJECT_NOT_FOUND" \
    "$START_TIMEOUT_SEC"; then
    FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
      --run-dir "$RUN_DIR" --name "$point" || true
    stop_boundary_client "$client_pid"
    smoke_failed "removed object remained visible before durable commit"
    return 1
  fi
  local during_sequence
  during_sequence=$(read_durable_sequence) || {
    FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
      --run-dir "$RUN_DIR" --name "$point" || true
    stop_boundary_client "$client_pid"
    smoke_failed "failed to read durable sequence while failpoint was held"
    return 1
  }
  if [[ "$during_sequence" != "$before_sequence" ]]; then
    FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
      --run-dir "$RUN_DIR" --name "$point" || true
    stop_boundary_client "$client_pid"
    smoke_failed "durable sequence advanced while pre-transaction failpoint was held"
    return 1
  fi

  FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
    --run-dir "$RUN_DIR" --name "$point"
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" wait \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" \
    --last_seq="$((before_sequence + 1))" \
    --timeout_sec="$START_TIMEOUT_SEC"; then
    stop_boundary_client "$client_pid"
    smoke_failed "remove did not become durable after failpoint release"
    return 1
  fi
  stop_boundary_client "$client_pid"
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" verify \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json \
    >"$RUN_DIR/audit/after-remove-boundary.json"; then
    smoke_failed "OpLog verification failed after durable remove"
    return 1
  fi
  local after_sequence
  after_sequence=$(read_durable_sequence) || {
    smoke_failed "failed to read durable sequence after remove"
    return 1
  }

  collect_cluster
  down_cluster
  echo "PASS: remove boundary smoke completed; before_seq=$before_sequence during_seq=$during_sequence after_seq=$after_sequence artifacts: $RUN_DIR"
}

standby_read_smoke_cluster() {
  [[ -n "$FAILPOINT_DIR" ]] || FAILPOINT_DIR="$RUN_DIR/failpoints"
  local fault_ctl="$SCRIPT_DIR/oplog_fault_ctl.sh"
  require_executable "$fault_ctl"
  up_cluster

  local point=standby_prefix_read_before_batch
  FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint arm \
    --run-dir "$RUN_DIR" --name "$point"
  run_client_smoke standby-read &
  local client_pid=$!
  if ! FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint wait \
    --run-dir "$RUN_DIR" --name "$point" \
    --timeout-sec "$START_TIMEOUT_SEC"; then
    wait "$client_pid" || true
    smoke_failed "standby read failpoint was not hit; ensure the build enables MOONCAKE_ENABLE_TEST_FAILPOINTS"
    return 1
  fi
  if ! wait "$client_pid"; then
    FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
      --run-dir "$RUN_DIR" --name "$point" || true
    smoke_failed "client workload failed while standby reader was paused"
    return 1
  fi

  local hit_pid master_index=""
  hit_pid=$(<"$FAILPOINT_DIR/$point.hit")
  local index
  for index in "${!RPC_PORTS[@]}"; do
    if [[ $(<"$RUN_DIR/pids/master-$index.pid") == "$hit_pid" ]]; then
      master_index=$index
      break
    fi
  done
  if [[ -z "$master_index" ]]; then
    FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
      --run-dir "$RUN_DIR" --name "$point" || true
    smoke_failed "failpoint PID does not match a managed master: $hit_pid"
    return 1
  fi
  local health
  health=$(curl -fsS "http://127.0.0.1:${ADMIN_PORTS[$master_index]}/health")
  if ! grep -Eq '"role"[[:space:]]*:[[:space:]]*"standby"' <<<"$health"; then
    FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
      --run-dir "$RUN_DIR" --name "$point" || true
    smoke_failed "standby reader failpoint was hit by non-standby master-$master_index"
    return 1
  fi

  local durable_sequence applied_during
  durable_sequence=$(read_durable_sequence) || {
    FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
      --run-dir "$RUN_DIR" --name "$point" || true
    smoke_failed "failed to read durable sequence while standby was paused"
    return 1
  }
  applied_during=$(read_master_metric "$master_index" \
    ha_oplog_applied_sequence_id) || {
    FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
      --run-dir "$RUN_DIR" --name "$point" || true
    smoke_failed "failed to read paused standby applied sequence"
    return 1
  }
  if ((applied_during >= durable_sequence)); then
    FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
      --run-dir "$RUN_DIR" --name "$point" || true
    smoke_failed "paused standby was not behind durable sequence"
    return 1
  fi

  FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
    --run-dir "$RUN_DIR" --name "$point"
  if ! wait_master_metric_at_least "$master_index" \
    ha_oplog_applied_sequence_id "$durable_sequence" "$START_TIMEOUT_SEC"; then
    smoke_failed "standby did not catch up after failpoint release"
    return 1
  fi
  local applied_after
  applied_after=$(read_master_metric "$master_index" \
    ha_oplog_applied_sequence_id) || return 1
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" verify \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json \
    >"$RUN_DIR/audit/after-standby-read.json"; then
    smoke_failed "OpLog verification failed after standby catch-up"
    return 1
  fi

  collect_cluster
  down_cluster
  echo "PASS: standby read smoke completed; master=master-$master_index durable_seq=$durable_sequence applied_during=$applied_during applied_after=$applied_after artifacts: $RUN_DIR"
}

promotion_catchup_smoke_cluster() {
  [[ -n "$FAILPOINT_DIR" ]] || FAILPOINT_DIR="$RUN_DIR/failpoints"
  local fault_ctl="$SCRIPT_DIR/oplog_fault_ctl.sh"
  local clientctl="$BUILD_DIR/mooncake-store/tests/e2e/clientctl"
  require_executable "$fault_ctl"
  require_executable "$clientctl"
  up_cluster

  local fifo="$RUN_DIR/workload/promotion-business.fifo"
  local log="$RUN_DIR/workload/promotion-business.log"
  local client_port client_name=promotion-business
  local keep_key="promotion-keep-$CLUSTER_ID"
  local removed_key="promotion-removed-$CLUSTER_ID"
  client_port=$(find_free_port)
  mkfifo "$fifo"
  exec 3<>"$fifo"
  MC_STORE_CLUSTER_ID="$CLUSTER_ID" LSAN_OPTIONS=detect_leaks=0 \
    "$clientctl" --master_server_entry="etcd://$ETCD_ENDPOINTS" \
    --engine_meta_url="http://127.0.0.1:$METADATA_PORT/metadata" \
    --protocol="$PROTOCOL" <"$fifo" >"$log" 2>&1 &
  local client_pid=$!
  printf 'create %s %s\nmount %s segment-%s %s\n' \
    "$client_name" "$client_port" "$client_name" "$client_name" \
    "$((64 * 1024 * 1024))" >&3
  printf 'put %s %s keep-value\nput %s %s removed-value\n' \
    "$client_name" "$keep_key" "$client_name" "$removed_key" >&3
  printf 'sleep 6\ndelete %s %s\nget %s %s\n' \
    "$client_name" "$removed_key" "$client_name" "$removed_key" >&3
  if ! wait_file_text "$log" \
    "Successfully deleted value for key: $removed_key" \
    "$START_TIMEOUT_SEC" ||
    ! wait_file_count "$log" "Failed to get value: OBJECT_NOT_FOUND" 1 \
      "$START_TIMEOUT_SEC"; then
    stop_boundary_client "$client_pid"
    smoke_failed "failed to prepare promotion business state"
    return 1
  fi
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" wait \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" \
    --last_seq=1 --timeout_sec="$START_TIMEOUT_SEC"; then
    stop_boundary_client "$client_pid"
    smoke_failed "pre-promotion workload did not become durable"
    return 1
  fi
  local durable_before leader_index=""
  durable_before=$(read_durable_sequence) || {
    stop_boundary_client "$client_pid"
    smoke_failed "failed to read pre-promotion durable sequence"
    return 1
  }
  local index health
  for index in "${!ADMIN_PORTS[@]}"; do
    health=$(curl --max-time 1 -fsS \
      "http://127.0.0.1:${ADMIN_PORTS[$index]}/health" 2>/dev/null || true)
    if grep -Eq '"service_ready"[[:space:]]*:[[:space:]]*true' <<<"$health"; then
      leader_index=$index
    else
      wait_master_metric_at_least "$index" ha_oplog_applied_sequence_id \
        "$durable_before" "$START_TIMEOUT_SEC" || {
        stop_boundary_client "$client_pid"
        smoke_failed "master-$index did not catch up before promotion test"
        return 1
      }
    fi
  done
  [[ -n "$leader_index" ]] || {
    stop_boundary_client "$client_pid"
    smoke_failed "failed to identify ready leader"
    return 1
  }

  local point=promotion_final_catch_up_before_complete
  FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint arm \
    --run-dir "$RUN_DIR" --name "$point"
  "$fault_ctl" process kill --run-dir "$RUN_DIR" \
    --name "master-$leader_index"
  if ! FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint wait \
    --run-dir "$RUN_DIR" --name "$point" \
    --timeout-sec "$START_TIMEOUT_SEC"; then
    stop_boundary_client "$client_pid"
    smoke_failed "promotion final catch-up failpoint was not hit"
    return 1
  fi

  local ready=0
  for index in "${!ADMIN_PORTS[@]}"; do
    health=$(curl --max-time 1 -fsS \
      "http://127.0.0.1:${ADMIN_PORTS[$index]}/health" 2>/dev/null || true)
    grep -Eq '"service_ready"[[:space:]]*:[[:space:]]*true' <<<"$health" &&
      ready=$((ready + 1))
  done
  if ((ready != 0)); then
    FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
      --run-dir "$RUN_DIR" --name "$point" || true
    stop_boundary_client "$client_pid"
    smoke_failed "a master became ready before promotion final catch-up completed"
    return 1
  fi

  local promoted_pid
  promoted_pid=$(<"$FAILPOINT_DIR/$point.hit")
  FAILPOINT_DIR="$FAILPOINT_DIR" "$fault_ctl" failpoint release \
    --run-dir "$RUN_DIR" --name "$point"
  if ! wait_for_single_leader "$START_TIMEOUT_SEC"; then
    stop_boundary_client "$client_pid"
    smoke_failed "cluster did not finish promotion after failpoint release"
    return 1
  fi
  local promoted_index=""
  for index in "${!ADMIN_PORTS[@]}"; do
    health=$(curl --max-time 1 -fsS \
      "http://127.0.0.1:${ADMIN_PORTS[$index]}/health" 2>/dev/null || true)
    if grep -Eq '"service_ready"[[:space:]]*:[[:space:]]*true' \
      <<<"$health"; then
      promoted_index=$index
      break
    fi
  done
  if [[ -z "$promoted_index" ]] ||
    ! wait_file_text "$log" "Reconnected to master" "$START_TIMEOUT_SEC" ||
    ! wait_master_metric_at_least "$promoted_index" master_active_clients 1 \
      "$START_TIMEOUT_SEC"; then
    stop_boundary_client "$client_pid"
    smoke_failed "client did not reconnect to the promoted leader"
    return 1
  fi
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" verify \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json \
    >"$RUN_DIR/audit/after-promotion-catchup.json"; then
    stop_boundary_client "$client_pid"
    smoke_failed "durable OpLog failed verification after promotion"
    return 1
  fi

  printf 'get %s %s\nget %s %s\n' "$client_name" "$keep_key" \
    "$client_name" "$removed_key" >&3
  printf 'batch-smoke %s promotion-batch-%s\n' \
    "$client_name" "$CLUSTER_ID" >&3
  if ! wait_file_count "$log" "Failed to get value: REPLICA_IS_NOT_READY" 1 \
    "$START_TIMEOUT_SEC" ||
    ! wait_file_count "$log" "Failed to get value: OBJECT_NOT_FOUND" 2 \
      "$START_TIMEOUT_SEC" ||
    ! wait_file_text "$log" \
      "Successfully completed batch smoke: promotion-batch-$CLUSTER_ID" \
      "$START_TIMEOUT_SEC"; then
    stop_boundary_client "$client_pid"
    smoke_failed "promoted leader business-state verification failed"
    return 1
  fi
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" wait \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" \
    --last_seq="$((durable_before + 6))" \
    --timeout_sec="$START_TIMEOUT_SEC"; then
    stop_boundary_client "$client_pid"
    smoke_failed "post-promotion batch mutations did not become durable"
    return 1
  fi
  local durable_after
  durable_after=$(read_durable_sequence) || {
    stop_boundary_client "$client_pid"
    smoke_failed "failed to read post-promotion durable sequence"
    return 1
  }
  stop_boundary_client "$client_pid"
  if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" verify \
    --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json \
    >"$RUN_DIR/audit/after-promotion-business.json"; then
    smoke_failed "post-promotion business OpLog verification failed"
    return 1
  fi

  collect_cluster
  down_cluster
  echo "PASS: promotion catch-up smoke completed; put metadata survived with replica rebuild required; remove did not resurrect; batch smoke passed; killed=master-$leader_index promoted_pid=$promoted_pid before_seq=$durable_before after_seq=$durable_after artifacts: $RUN_DIR"
}

non_ha_smoke_cluster() {
  local fault_ctl="$SCRIPT_DIR/oplog_fault_ctl.sh"
  require_executable "$fault_ctl"
  up_cluster

  if [[ "$USE_ETCD_OBSERVER" == true ]]; then
    if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" verify \
      --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json \
      >"$RUN_DIR/audit/non-ha-before.json"; then
      smoke_failed \
        "observer etcd namespace was not empty before non-HA workload"
      return 1
    fi
    "$fault_ctl" process stop --run-dir "$RUN_DIR" --name etcd
  fi

  local label=non-ha key clientctl client_port
  key="non-ha-$CLUSTER_ID"
  clientctl="$BUILD_DIR/mooncake-store/tests/e2e/clientctl"
  client_port=$(find_free_port)
  require_executable "$clientctl"
  if ! {
    printf 'create %s %s\n' "$label" "$client_port"
    printf 'mount %s segment-%s %s\n' "$label" "$label" \
      "$((64 * 1024 * 1024))"
    printf 'put %s %s non-ha-value\n' "$label" "$key"
    printf 'get %s %s\n' "$label" "$key"
    printf 'get %s missing-%s\n' "$label" "$key"
    printf 'put %s %s duplicate-value\n' "$label" "$key"
    printf 'get %s %s\n' "$label" "$key"
    printf 'delete %s %s\n' "$label" "$key"
    printf 'sleep 6\n'
    printf 'delete %s %s\n' "$label" "$key"
    printf 'get %s %s\n' "$label" "$key"
    printf 'batch-smoke %s batch-%s\n' "$label" "$CLUSTER_ID"
    printf 'unmount %s segment-%s\n' "$label" "$label"
    printf 'mount %s segment-%s %s\n' "$label" "$label" \
      "$((64 * 1024 * 1024))"
    printf 'put %s remount-%s remount-value\n' "$label" "$CLUSTER_ID"
    printf 'sleep 6\n'
    printf 'delete %s remount-%s\n' "$label" "$CLUSTER_ID"
    printf 'unmount %s segment-%s\n' "$label" "$label"
    printf 'terminate\n'
  } | MC_STORE_CLUSTER_ID="$CLUSTER_ID" LSAN_OPTIONS=detect_leaks=0 \
    "$clientctl" --master_server_entry="127.0.0.1:${RPC_PORTS[0]}" \
    --engine_meta_url="http://127.0.0.1:$METADATA_PORT/metadata" \
    --protocol="$PROTOCOL" >"$RUN_DIR/workload/non-ha.log" 2>&1; then
    resume_observer_etcd "$fault_ctl"
    smoke_failed "non-HA client workflow failed while etcd was stopped"
    return 1
  fi
  if ! grep -Fq "Successfully put value for key: $key" \
    "$RUN_DIR/workload/non-ha.log" ||
    ! grep -Fq "Get value: non-ha-value" "$RUN_DIR/workload/non-ha.log" ||
    ! grep -Fq "Failed to get value: OBJECT_NOT_FOUND" \
      "$RUN_DIR/workload/non-ha.log" ||
    ! grep -Fq "Failed to delete value: OBJECT_HAS_LEASE" \
      "$RUN_DIR/workload/non-ha.log" ||
    ! grep -Fq "Successfully deleted value for key: $key" \
      "$RUN_DIR/workload/non-ha.log" ||
    ! grep -Fq "Failed to get value: OBJECT_NOT_FOUND" \
      "$RUN_DIR/workload/non-ha.log" ||
    ! grep -Fq "Successfully completed batch smoke: batch-$CLUSTER_ID" \
      "$RUN_DIR/workload/non-ha.log" ||
    ! grep -Fq "Successfully put value for key: remount-$CLUSTER_ID" \
      "$RUN_DIR/workload/non-ha.log" ||
    [[ $(grep -Fc "Successfully mounted segment on client $label" \
      "$RUN_DIR/workload/non-ha.log") -ne 2 ]] ||
    [[ $(grep -Fc "Successfully unmounted segment from client $label" \
      "$RUN_DIR/workload/non-ha.log") -ne 2 ]]; then
    resume_observer_etcd "$fault_ctl"
    smoke_failed "non-HA client workflow returned unexpected results"
    return 1
  fi
  if [[ $(grep -Fc "Get value: non-ha-value" \
    "$RUN_DIR/workload/non-ha.log") -ne 2 ]]; then
    resume_observer_etcd "$fault_ctl"
    smoke_failed "repeated non-HA put did not preserve the original value"
    return 1
  fi
  if ! wait_master_metric_equal 0 master_active_clients 0 \
    "$START_TIMEOUT_SEC"; then
    resume_observer_etcd "$fault_ctl"
    smoke_failed "non-HA lifecycle client was not cleaned up"
    return 1
  fi

  local -a worker_pids=()
  local worker
  for ((worker = 0; worker < NON_HA_WORKERS; ++worker)); do
    run_client_smoke "non-ha-worker-$worker" 2 &
    worker_pids+=("$!")
  done
  local worker_failed=0 pid
  for pid in "${worker_pids[@]}"; do
    wait "$pid" || worker_failed=1
  done
  if ((worker_failed)); then
    resume_observer_etcd "$fault_ctl"
    smoke_failed "concurrent non-HA client workload failed"
    return 1
  fi

  if [[ "$USE_ETCD_OBSERVER" == true ]]; then
    "$fault_ctl" process continue --run-dir "$RUN_DIR" --name etcd
    wait_http "http://$ETCD_ENDPOINTS/health" "$START_TIMEOUT_SEC" || {
      smoke_failed "observer etcd did not resume"
      return 1
    }
    if ! LSAN_OPTIONS=detect_leaks=0 "$INSPECTOR_BIN" verify \
      --endpoints="$ETCD_ENDPOINTS" --cluster_id="$CLUSTER_ID" --json \
      >"$RUN_DIR/audit/non-ha-after.json"; then
      smoke_failed "non-HA workload created invalid OpLog state"
      return 1
    fi
    if ! python3 -c 'import json,sys
d=json.load(open(sys.argv[1], encoding="utf-8"))
assert d["batch_count"] == 0
assert d["entry_count"] == 0
assert d["legacy_max_seq"] == 0
assert d["durable_prefix"] is None' "$RUN_DIR/audit/non-ha-after.json"; then
      smoke_failed "non-HA workload unexpectedly created an OpLog namespace"
      return 1
    fi
  fi

  collect_cluster
  down_cluster
  echo "PASS: non-HA smoke completed without an HA backend; $NON_HA_WORKERS concurrent clients passed; observer_etcd=$USE_ETCD_OBSERVER; artifacts: $RUN_DIR"
}

resume_observer_etcd() {
  local fault_ctl=$1
  [[ "$USE_ETCD_OBSERVER" != true ]] ||
    "$fault_ctl" process continue --run-dir "$RUN_DIR" --name etcd || true
}

stop_boundary_client() {
  local pid=$1
  printf 'terminate\n' >&3 2>/dev/null || true
  exec 3>&-
  wait "$pid" 2>/dev/null || true
}

wait_file_text() {
  local file=$1
  local expected=$2
  local timeout=$3
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    [[ -f "$file" ]] && grep -Fq "$expected" "$file" && return 0
    sleep 0.1
  done
  return 1
}

wait_file_count() {
  local file=$1
  local expected=$2
  local count=$3
  local timeout=$4
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    [[ -f "$file" ]] &&
      [[ $(grep -Fc "$expected" "$file") -ge "$count" ]] && return 0
    sleep 0.1
  done
  return 1
}

read_master_metric() {
  local index=$1
  local metric=$2
  curl -fsS "http://127.0.0.1:${ADMIN_PORTS[$index]}/metrics" |
    awk -v name="$metric" '$1 == name { print $2; found = 1; exit } END { if (!found) exit 1 }'
}

wait_master_metric_at_least() {
  local index=$1
  local metric=$2
  local target=$3
  local timeout=$4
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    local value
    value=$(read_master_metric "$index" "$metric" 2>/dev/null || true)
    [[ "$value" =~ ^[0-9]+$ ]] && ((value >= target)) && return 0
    sleep 0.1
  done
  return 1
}

wait_master_metric_equal() {
  local index=$1
  local metric=$2
  local target=$3
  local timeout=$4
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    local value
    value=$(read_master_metric "$index" "$metric" 2>/dev/null || true)
    [[ "$value" == "$target" ]] && return 0
    sleep 0.1
  done
  return 1
}

smoke_failed() {
  echo "error: $1" >&2
  collect_cluster || true
  printf 'environment preserved; stop it with: %q down --run-dir %q\n' \
    "$0" "$RUN_DIR" >&2
}

parse_run_dir() {
  RUN_DIR=""
  while (($#)); do
    case "$1" in
      --run-dir)
        (($# >= 2)) || die "--run-dir requires a value"
        RUN_DIR=$2
        shift 2
        ;;
      *) die "unknown option: $1" ;;
    esac
  done
  [[ -n "$RUN_DIR" ]] || die "--run-dir is required"
  [[ -d "$RUN_DIR" ]] || die "run directory does not exist: $RUN_DIR"
}

pid_matches() {
  local pid=$1
  local expected_file=$2
  [[ -r "/proc/$pid/cmdline" && -r "$expected_file" ]] || return 1
  local actual
  actual=$(tr '\0' ' ' <"/proc/$pid/cmdline")
  grep -Fqx "$actual" "$expected_file"
}

stop_pid_file() {
  local pid_file=$1
  local expected_file=${pid_file%.pid}.cmd
  local pid
  pid=$(<"$pid_file")
  [[ "$pid" =~ ^[0-9]+$ ]] || return 0
  kill -0 "$pid" 2>/dev/null || return 0
  pid_matches "$pid" "$expected_file" || {
    echo "warning: refusing to stop PID $pid because command does not match" >&2
    return 0
  }
  kill -TERM "$pid" 2>/dev/null || return 0
  local deadline=$((SECONDS + 5))
  while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do
    sleep 0.1
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid"
  fi
  return 0
}

down_cluster() {
  local pid_file
  shopt -s nullglob
  for pid_file in "$RUN_DIR"/pids/client-*.pid; do
    stop_pid_file "$pid_file"
  done
  for pid_file in "$RUN_DIR"/pids/master-*.pid; do
    stop_pid_file "$pid_file"
  done
  for pid_file in "$RUN_DIR"/pids/metadata.pid "$RUN_DIR"/pids/etcd.pid; do
    [[ -f "$pid_file" ]] && stop_pid_file "$pid_file"
  done
  echo "cluster is stopped: $RUN_DIR"
}

main() {
  (($# >= 1)) || {
    usage
    exit 1
  }
  local command=$1
  shift
  case "$command" in
    up | smoke | restart-smoke | failpoint-smoke | failpoint-crash-smoke | remove-boundary-smoke | standby-read-smoke | promotion-catchup-smoke | non-ha-smoke)
      parse_up_options "$@"
      if [[ "$command" == non-ha-smoke ]]; then
        MASTER_COUNT=1
        ENABLE_HA=false
      fi
      if [[ "$USE_ETCD_OBSERVER" != true && "$ENABLE_HA" == true ]]; then
        die "--no-etcd-observer is only supported by non-ha-smoke"
      fi
      if [[ "$command" == up ]]; then
        up_cluster
      elif [[ "$command" == smoke ]]; then
        smoke_cluster
      elif [[ "$command" == restart-smoke ]]; then
        restart_smoke_cluster
      elif [[ "$command" == failpoint-smoke ]]; then
        failpoint_smoke_cluster
      elif [[ "$command" == remove-boundary-smoke ]]; then
        remove_boundary_smoke_cluster
      elif [[ "$command" == standby-read-smoke ]]; then
        standby_read_smoke_cluster
      elif [[ "$command" == promotion-catchup-smoke ]]; then
        promotion_catchup_smoke_cluster
      elif [[ "$command" == non-ha-smoke ]]; then
        non_ha_smoke_cluster
      else
        failpoint_crash_smoke_cluster
      fi
      ;;
    status | collect | restart)
      parse_run_dir "$@"
      load_cluster_env
      if [[ "$command" == status ]]; then
        status_cluster
      elif [[ "$command" == collect ]]; then
        collect_cluster
      else
        restart_masters
      fi
      ;;
    down)
      parse_run_dir "$@"
      down_cluster
      ;;
    *) die "unknown command: $command" ;;
  esac
}

main "$@"
