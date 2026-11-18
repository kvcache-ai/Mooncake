#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/run_oplog_batch_cluster.sh"

[[ $# == 2 ]] || die 'usage: run_offset_delete_ha.sh BUILD_DIR NEW_RUN_DIR'
build=$(realpath "$1")
run_dir=$2
[[ ! -e "$run_dir" ]] || die 'choose a new run directory'
mkdir -p "$run_dir/configs"
run_dir=$(realpath "$run_dir")
config=$run_dir/configs/offset-delete.yaml
cat > "$config" <<'YAML'
enable_offload: true
snapshot_interval_seconds: 2
client_active_ttl_sec: 120
YAML
parse_up_options --masters 2 --build-dir "$build" --run-dir "$run_dir" \
    --master-config "$config" --memory-allocator offset --enable-oplog-snapshot \
    --snapshot-chunk-object-count 2 --timeout-sec 65
export MOONCAKE_LOCAL_DISK_INCARNATIONS=1 MC_STORE_CLUSTER_ID="$CLUSTER_ID"
actor=$build/mooncake-store/tests/offset_delete_ha_client
require_executable "$actor"
cd "$REPO_ROOT"
trap 'down_cluster >/dev/null 2>&1 || true' EXIT
up_cluster

audit_master() {
    local index=$1 label=$2 pid
    pid=$(<"$RUN_DIR/pids/master-$index.pid")
    pid_matches "$pid" "$RUN_DIR/pids/master-$index.cmd"
    sha256sum "/proc/$pid/exe" > "$RUN_DIR/audit/$label.sha256"
}

terminate_master() {
    local index=$1 pid
    pid=$(<"$RUN_DIR/pids/master-$index.pid")
    pid_matches "$pid" "$RUN_DIR/pids/master-$index.cmd"
    kill -KILL "$pid"
    wait "$pid" 2>/dev/null || true
}

await_snapshot() {
    local label=$1 minimum=$2 desc deadline=$((SECONDS+65))
    while (( SECONDS < deadline )); do
        desc=$(etcdctl --endpoints="$ETCD_ENDPOINTS" \
            get "/oplog/$CLUSTER_ID/snapshot/latest" --print-value-only)
        if [[ -n "$desc" ]] && python3 - "$RUN_DIR/snapshots" "$desc" "$minimum" <<'PY'
import json
import pathlib
import sys

root, desc, minimum = pathlib.Path(sys.argv[1]), json.loads(sys.argv[2]), int(sys.argv[3])
if desc.get("schema_version") != 4 or desc.get("last_included_seq", 0) < minimum:
    sys.exit(1)
manifest = json.loads((root / desc["manifest_key"]).read_bytes())
state = manifest["pending_local_deletes"]
assert (root / state["key"]).stat().st_size == state["stored_size"] > 0
print(f"verified local-delete snapshot at seq {desc['last_included_seq']}")
PY
        then
            printf '%s\n' "$desc" > "$RUN_DIR/audit/$label.json"
            return
        fi
        sleep 0.25
    done
    die "no durable delete snapshot covering sequence $minimum"
}

disk=$RUN_DIR/offset-disk
primary=$(ready_leader_index)
standby=$((1-primary))
audit_master "$primary" initial-primary
audit_master "$standby" initial-standby
sha256sum "$actor" "$MASTER_BIN" > "$RUN_DIR/audit/binaries.sha256"
"$actor" seed "127.0.0.1:${RPC_PORTS[$primary]}" "$disk" \
    > "$RUN_DIR/workload/seed.out" 2> "$RUN_DIR/workload/seed.err"
read -r old_task new_task < "$RUN_DIR/workload/seed.out"
[[ -n "$old_task" && -n "$new_task" && "$old_task" != "$new_task" ]]
echo 'PASS lost ACK redelivery and durable same-key pending tasks'
initial_seq=$(read_durable_sequence)
await_snapshot before-promotion "$initial_seq"
wait_master_metric_at_least "$standby" ha_oplog_applied_sequence_id "$initial_seq" 65

terminate_master "$primary"
wait_for_single_leader 65
[[ $(ready_leader_index) == "$standby" ]]
audit_master "$standby" promoted-primary
"$actor" promoted "127.0.0.1:${RPC_PORTS[$standby]}" "$disk" "$old_task" "$new_task" \
    > "$RUN_DIR/workload/promoted.out" 2> "$RUN_DIR/workload/promoted.err"
cat "$RUN_DIR/workload/promoted.out"

start_master "$primary"
ack_seq=$(read_durable_sequence)
wait_master_metric_at_least "$primary" ha_oplog_applied_sequence_id "$ack_seq" 65
await_snapshot before-cold-start "$ack_seq"
terminate_master "$primary"
terminate_master "$standby"
start_master "$primary"
wait_for_single_leader 65
[[ $(ready_leader_index) == "$primary" ]]
audit_master "$primary" cold-primary
grep -E 'Batch snapshot bootstrap complete: snapshot_seq=[1-9][0-9]*' \
    "$RUN_DIR/logs/master-$primary.err" | tail -1
"$actor" cold "127.0.0.1:${RPC_PORTS[$primary]}" "$disk" "$old_task" "$new_task" \
    > "$RUN_DIR/workload/cold.out" 2> "$RUN_DIR/workload/cold.err"
cat "$RUN_DIR/workload/cold.out"
echo 'PASS OffsetAllocator delete ACK replay and cold snapshot recovery'
