#!/usr/bin/env bash
# Real-process batch snapshot publication, cold restore, suffix and promotion.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/run_oplog_batch_cluster.sh"
parse_up_options "$@"
[[ ! -e "$RUN_DIR" ]] || die "snapshot smoke requires a fresh run directory"
MASTER_COUNT=2
CLIENT_COUNT=0
ENABLE_OPLOG_SNAPSHOT=true
SNAPSHOT_CHUNK_OBJECT_COUNT=2
command -v etcdctl >/dev/null || die "missing etcdctl"
HA_CLIENT="$BUILD_DIR/mooncake-store/tests/e2e/oplog_ha_client"
require_executable "$HA_CLIENT"
mkdir -p "$RUN_DIR/configs"
MASTER_CONFIG="$RUN_DIR/configs/snapshot.yaml"
printf 'snapshot_interval_seconds: 2\nenable_snapshot: true\nenable_snapshot_restore: true\nsnapshot_catalog_store_type: invalid\n' >"$MASTER_CONFIG"
trap 'down_cluster >/dev/null 2>&1 || true' EXIT
# Static dependency failures must terminate before any leadership work.
python3 - "$BUILD_DIR/mooncake-store/src/mooncake_master" "$RUN_DIR" <<'PYTEST'
import os, pathlib, subprocess, sys
binary, root = sys.argv[1:]
invalid_path = pathlib.Path(root) / 'not-a-directory'
invalid_path.write_text('file')
for name, local_path in [('missing', ''), ('file', str(invalid_path))]:
    env = dict(os.environ, MOONCAKE_SNAPSHOT_LOCAL_PATH=local_path)
    result = subprocess.run([binary, '--enable_ha=true', '--enable_oplog=true',
        '--enable_oplog_snapshot=true', '--snapshot_object_store_type=local',
        '--etcd_endpoints=127.0.0.1:1',
        '--logtostderr=true'], env=env, capture_output=True, text=True, timeout=10)
    pathlib.Path(root, 'configs', name + '-failure.log').write_text(result.stderr)
    assert result.returncode != 0, result.stderr
    assert 'Standby dependency initialization failed' in result.stderr, result.stderr
    assert 'candidate' not in result.stderr.lower(), result.stderr
PYTEST
up_cluster
export MC_STORE_CLUSTER_ID="$CLUSTER_ID"
GTEST_FILTER=StandbyControllerTest.BatchSnapshotRepeatedStartStopAndPromotion \
MOONCAKE_TEST_ETCD_ENDPOINTS="$ETCD_ENDPOINTS" \
  "$BUILD_DIR/mooncake-store/tests/hot_standby_snapshot_bootstrap_test" \
  >"$RUN_DIR/audit/controller-lifecycle.log" 2>&1
for index in 0 1; do
  python3 - "$(cat "$RUN_DIR/pids/master-$index.pid")" "$RUN_DIR/snapshots" "$FAILPOINT_DIR" <<'PYENV'
import pathlib, sys
entries = pathlib.Path('/proc', sys.argv[1], 'environ').read_bytes().split(b'\0')
assert ('MOONCAKE_SNAPSHOT_LOCAL_PATH=' + sys.argv[2]).encode() in entries
if sys.argv[3]:
    assert ('MOONCAKE_TEST_FAILPOINT_DIR=' + sys.argv[3]).encode() in entries
PYENV
done
client_args=(--master_server_entry="etcd://$ETCD_ENDPOINTS"
  --engine_meta_url="http://127.0.0.1:$METADATA_PORT/metadata" --protocol=tcp
  --payload_size=4096 --key_prefix="snapshot-$CLUSTER_ID")
start_process provider-0 "$HA_CLIENT" --mode=provider --port="$(find_free_port)" "${client_args[@]}"
wait_file_text "$RUN_DIR/logs/provider-0.out" provider_ready "$START_TIMEOUT_SEC"
run_client() {
  local mode=$1 manifest=$2
  shift 2
  "$HA_CLIENT" --mode="$mode" --manifest="$manifest" --port="$(find_free_port)" \
    "${client_args[@]}" "$@" >>"$RUN_DIR/workload/$mode.log" 2>&1
}
pointer() {
  ETCDCTL_API=3 etcdctl --endpoints="$ETCD_ENDPOINTS" get \
    "/oplog/$CLUSTER_ID/snapshot/$1" --print-value-only
}
wait_snapshot() {
  local previous=$1 minimum=$2 deadline=$((SECONDS + START_TIMEOUT_SEC)) value
  while ((SECONDS < deadline)); do
    value=$(pointer latest)
    if [[ -n "$value" && "$value" != "$previous" ]] &&
       python3 -c 'import json,sys; sys.exit(json.loads(sys.argv[1])["last_included_seq"] < int(sys.argv[2]))' "$value" "$minimum"; then
      printf '%s\n' "$value"
      return 0
    fi
    sleep 0.2
  done
  die "snapshot publication timed out"
}
run_client seed "$RUN_DIR/workload/first.ack" --count=12
first=$(wait_snapshot '' "$(read_durable_sequence)")
run_client seed "$RUN_DIR/workload/second.ack" --count=4 --start_index=100
second=$(wait_snapshot "$first" "$(read_durable_sequence)")
[[ -n "$(pointer fallback)" ]] || die "fallback pointer was not published"
printf '%s\n' "$second" >"$RUN_DIR/audit/latest.json"
python3 - "$RUN_DIR/snapshots" "$RUN_DIR/audit/latest.json" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
descriptor = json.load(open(sys.argv[2]))
manifest = json.loads((root / descriptor['manifest_key']).read_text())
assert len(manifest['object_chunks']) > 1, manifest
for chunk in manifest['object_chunks']:
    assert (root / chunk['key']).stat().st_size == chunk['stored_size']
print('verified multiple snapshot chunks')
PY
leader=$(ready_leader_index)
standby=$((1 - leader))
stop_pid_file "$RUN_DIR/pids/master-$standby.pid"
wait "$(cat "$RUN_DIR/pids/master-$standby.pid")" 2>/dev/null || true
# Mutations now necessarily occur after the last snapshot.
run_client seed "$RUN_DIR/workload/suffix.ack" --count=4 --start_index=200
head -n 1 "$RUN_DIR/workload/first.ack" >"$RUN_DIR/workload/removed.ack"
tail -n +2 "$RUN_DIR/workload/first.ack" >"$RUN_DIR/workload/survivors.ack"
run_client delete "$RUN_DIR/workload/removed.ack"
durable=$(read_durable_sequence)
snapshot_seq=$(pointer latest | python3 -c 'import json,sys; print(json.load(sys.stdin)["last_included_seq"])')
((durable > snapshot_seq)) || die "no suffix after snapshot"
# Restart retains the store and pointers, but must not replace the snapshot
# before we verify the restored cursor.
start_master "$standby"
wait_file_text "$RUN_DIR/logs/master-$standby.err" \
  "Batch snapshot bootstrap complete: snapshot_seq=$snapshot_seq applied_seq=$durable" "$START_TIMEOUT_SEC"
# Exercise controller stop/start through a second cold process restart.
stop_pid_file "$RUN_DIR/pids/master-$standby.pid"
wait "$(cat "$RUN_DIR/pids/master-$standby.pid")" 2>/dev/null || true
start_master "$standby"
wait_master_metric_at_least "$standby" ha_oplog_applied_sequence_id "$durable" "$START_TIMEOUT_SEC"
"$SCRIPT_DIR/oplog_fault_ctl.sh" process kill --run-dir "$RUN_DIR" --name "master-$leader"
wait "$(cat "$RUN_DIR/pids/master-$leader.pid")" 2>/dev/null || true
wait_for_single_leader "$START_TIMEOUT_SEC"
[[ "$(ready_leader_index)" == "$standby" ]] || die "restarted standby did not promote"
wait_master_metric_at_least "$standby" master_active_clients 1 "$START_TIMEOUT_SEC"
for manifest in survivors second suffix; do
  run_client verify "$RUN_DIR/workload/$manifest.ack"
done
run_client verify-absent "$RUN_DIR/workload/removed.ack"
collect_cluster
echo "PASS: two snapshots, cold restore, suffix replay, restart, promotion and data audit: $RUN_DIR"
