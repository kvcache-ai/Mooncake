#!/usr/bin/env bash
# Sourced by run_oplog_batch_cluster.sh; all maintenance targets a fresh local etcd.

capacity_sample() {
  local directory
  printf -v directory '%s/capacity/sample-%06d' "$RUN_DIR" "$CAPACITY_SAMPLE"
  [[ -z "${1:-}" ]] || directory="$RUN_DIR/capacity/$1"
  CAPACITY_SAMPLE=$((CAPACITY_SAMPLE + 1))
  python3 "$CAPACITY_HELPER" sample "$directory" --endpoint "$ETCD_ENDPOINTS" \
    --cluster "$CLUSTER_ID" --ports "${ADMIN_PORTS[0]},${ADMIN_PORTS[1]}"
  CAPACITY_LAST_SAMPLE="$directory/sample.json"
}

capacity_client() {
  local mode=$1 manifest=$2
  shift 2
  "$CAPACITY_CLIENT" --mode="$mode" --manifest="$manifest" \
    --port="$(find_free_port)" "${CAPACITY_CLIENT_ARGS[@]}" "$@" \
    >>"$RUN_DIR/workload/$mode.log" 2>&1
}

capacity_wait_pruned() {
  local deadline=$((SECONDS + START_TIMEOUT_SEC))
  while ((SECONDS < deadline)); do
    capacity_sample
    if python3 "$CAPACITY_HELPER" ready "$CAPACITY_LAST_SAMPLE"; then return; fi
    sleep 1
  done
  die "pruning did not catch up; see $RUN_DIR/capacity"
}

capacity_audit() {
  local phase=$1
  wait_master_metric_at_least "$(ready_leader_index)" master_active_clients 1 "$START_TIMEOUT_SEC"
  capacity_client verify "$RUN_DIR/workload/anchor.ack"
  capacity_client verify "$RUN_DIR/workload/current.ack"
  if [[ -s "$RUN_DIR/workload/deleted.ack" ]]; then
    capacity_client verify-absent "$RUN_DIR/workload/deleted.ack"
  fi
  capacity_sample "$phase"
  printf '%s acknowledged-data audit passed\n' "$phase" >>"$RUN_DIR/capacity/audits.log"
}

capacity_cluster() (
  local mode=$1
  [[ -z "$ETCD_ENDPOINTS" ]] || die "capacity tests require a new local etcd; external endpoints are forbidden"
  [[ ! -e "$RUN_DIR" ]] || die "capacity tests require a fresh run directory"
  [[ "$CAPACITY_SECONDS" -ge 30 ]] || die "capacity-seconds must be at least 30"
  [[ -z "$MASTER_CONFIG" ]] || die "capacity tests generate their own master config"
  command -v etcdctl >/dev/null || die "missing executable: etcdctl"
  CAPACITY_HELPER="$SCRIPT_DIR/../ha/snapshot/batch_oplog/capacity.py"
  CAPACITY_CLIENT="$BUILD_DIR/mooncake-store/tests/e2e/oplog_ha_client"
  require_executable "$CAPACITY_CLIENT"
  MASTER_COUNT=2
  CLIENT_COUNT=0
  ENABLE_OPLOG_SNAPSHOT=true
  SNAPSHOT_CHUNK_OBJECT_COUNT=64
  mkdir -p "$RUN_DIR"/{configs,capacity}
  MASTER_CONFIG="$RUN_DIR/configs/capacity.yaml"
  printf 'snapshot_interval_seconds: 2\ndefault_kv_lease_ttl: 1s\n' >"$MASTER_CONFIG"
  # These scoped settings are inherited only by the disposable local etcd.
  export ETCD_QUOTA_BACKEND_BYTES=67108864
  [[ "$mode" != capacity-nospace ]] || export ETCD_QUOTA_BACKEND_BYTES=16777216
  export ETCD_AUTO_COMPACTION_RETENTION=0
  "$ETCD_BIN" --version >"$RUN_DIR/capacity/etcd-version.txt"
  etcdctl version >"$RUN_DIR/capacity/etcdctl-version.txt"
  printf 'mode=%s\nseconds=%s\nmax_batches=%s\nquota_bytes=%s\n' \
    "$mode" "$CAPACITY_SECONDS" "$CAPACITY_MAX_BATCHES" "$ETCD_QUOTA_BACKEND_BYTES" \
    >"$RUN_DIR/capacity/config.txt"
  CAPACITY_SAMPLE=0
  local pressure_pid=""
  trap 'down_cluster >/dev/null 2>&1 || true' EXIT
  up_cluster
  export MC_STORE_CLUSTER_ID="$CLUSTER_ID" ETCDCTL_API=3
  CAPACITY_CLIENT_ARGS=(--master_server_entry="etcd://$ETCD_ENDPOINTS"
    --engine_meta_url="http://127.0.0.1:$METADATA_PORT/metadata"
    --protocol="$PROTOCOL" --payload_size=1024 --key_prefix="capacity-$CLUSTER_ID")
  start_process provider-0 "$CAPACITY_CLIENT" --mode=provider \
    --port="$(find_free_port)" "${CAPACITY_CLIENT_ARGS[@]}"
  wait_file_text "$RUN_DIR/logs/provider-0.out" provider_ready "$START_TIMEOUT_SEC"
  capacity_client seed "$RUN_DIR/workload/anchor.ack" --count=16
  local deadline=$((SECONDS + CAPACITY_SECONDS)) index=1000 round=0
  while ((SECONDS < deadline || round < 3)); do
    start_process client-pressure "$CAPACITY_CLIENT" --mode=pressure \
      --manifest="$RUN_DIR/workload/next.ack" --port="$(find_free_port)" \
      "${CAPACITY_CLIENT_ARGS[@]}" --start_index="$index" --duration_sec=10 --sleep_ms=25
    pressure_pid=$(cat "$RUN_DIR/pids/client-pressure.pid")
    while kill -0 "$pressure_pid" 2>/dev/null; do
      capacity_sample
      sleep 2
    done
    wait "$pressure_pid"
    pressure_pid=""
    cp "$RUN_DIR/workload/next.ack" "$RUN_DIR/workload/round-$round.ack"
    index=$((index + $(wc -l <"$RUN_DIR/workload/next.ack")))
    if [[ -s "$RUN_DIR/workload/current.ack" ]]; then
      capacity_client delete "$RUN_DIR/workload/current.ack"
      cat "$RUN_DIR/workload/current.ack" >>"$RUN_DIR/workload/deleted.ack"
    fi
    mv "$RUN_DIR/workload/next.ack" "$RUN_DIR/workload/current.ack"
    capacity_wait_pruned
    round=$((round + 1))
    if [[ "$mode" == capacity-soak ]] && ((round % 6 == 0)); then
      mkdir "$RUN_DIR/capacity/maintenance-$round"
      python3 "$CAPACITY_HELPER" maintain "$RUN_DIR/capacity/maintenance-$round" --endpoint "$ETCD_ENDPOINTS"
    fi
    # Quota recovery needs only enough churn to establish redundant snapshots.
    [[ "$mode" != capacity-nospace || "$round" -lt 3 ]] || break
  done
  python3 "$CAPACITY_HELPER" validate "$RUN_DIR/capacity" --max-batches "$CAPACITY_MAX_BATCHES"
  capacity_audit before-maintenance
  if [[ "$mode" == capacity-nospace ]]; then
    # Quiesce metadata mutations before inducing the quota alarm. Providers stay
    # alive so an etcd restart/recovery cannot be confused with lost payloads.
    local i
    for i in 0 1; do stop_pid_file "$RUN_DIR/pids/master-$i.pid"; done
    python3 "$CAPACITY_HELPER" nospace "$RUN_DIR/capacity" --endpoint "$ETCD_ENDPOINTS"
  else
    python3 "$CAPACITY_HELPER" maintain "$RUN_DIR/capacity" --endpoint "$ETCD_ENDPOINTS"
  fi
  local started=$SECONDS
  restart_masters
  capacity_audit after-restart
  printf 'restart_and_audit_seconds=%s\n' "$((SECONDS - started))" >>"$RUN_DIR/capacity/audits.log"
  local leader standby
  leader=$(ready_leader_index)
  standby=$((1 - leader))
  wait_master_metric_at_least "$standby" ha_oplog_applied_sequence_id \
    "$(read_durable_sequence)" "$START_TIMEOUT_SEC"
  started=$SECONDS
  "$SCRIPT_DIR/oplog_fault_ctl.sh" process kill --run-dir "$RUN_DIR" --name "master-$leader"
  wait "$(cat "$RUN_DIR/pids/master-$leader.pid")" 2>/dev/null || true
  wait_for_single_leader "$START_TIMEOUT_SEC"
  [[ "$(ready_leader_index)" == "$standby" ]] || die "standby did not promote"
  start_master "$leader"
  wait_master_metric_at_least "$leader" ha_oplog_applied_sequence_id \
    "$(read_durable_sequence)" "$START_TIMEOUT_SEC"
  capacity_audit after-promotion
  printf 'promotion_and_audit_seconds=%s\n' "$((SECONDS - started))" >>"$RUN_DIR/capacity/audits.log"
  capacity_client seed "$RUN_DIR/workload/recovered.ack" --start_index="$index" --count=16
  capacity_client verify "$RUN_DIR/workload/recovered.ack"
  capacity_sample after-new-write
  printf 'PASS\n' >"$RUN_DIR/capacity/result.txt"
  printf 'PASS: %s; evidence: %s/capacity\n' "$mode" "$RUN_DIR"
)
