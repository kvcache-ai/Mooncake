#!/usr/bin/env bash
#
# Copyright 2026 KVCache.AI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Orchestrate the existing endpoint-level multi-target tebench interface.
# Nodes in both roles run separate target and initiator processes; this script
# does not add peer-mode or weighted target semantics to the tebench binary.

set -Eeuo pipefail

TOPOLOGY=""
TARGETS_CSV=""
INITIATORS_CSV=""
NODES_CSV=""
TEBENCH=""
BACKEND="tent"
XPORT_TYPE=""
SEG_TYPE="DRAM"
OPERATION="read"
VERIFY_DATA=0
TOTAL_BUFFER_SIZE=$((1 << 30))
BLOCK_SIZE=4096
BATCH_SIZE=1
NUM_THREADS=0
DURATION=5
RANGE_POLICY="shared"
TARGET_RANGE_SIZE=0
LOCAL_GPU_ID=0
TARGET_GPU_ID=0
READY_TIMEOUT=60
RUN_TIMEOUT=300
SSH_USER=""
SSH_PORT=22
RESULT_DIR=""
RUN_ID=""
DRY_RUN=0
KEEP_REMOTE=0
TARGET_ARGS=()
INITIATOR_ARGS=()
REMOTE_ENV=()

usage() {
    cat <<'EOF'
Run tebench across multiple hosts.

Usage:
  run_tebench_multinode.sh --topology m2n \
    --targets host-a,host-b --initiators host-c,host-d [options]

Topologies:
  fanout      One initiator sends to every target.
  incast      Every initiator sends to one target.
  m2n         Every initiator sends to every target.
  all_to_all  Every node publishes a target and sends to every other node.

Scope:
  This launcher builds target plans from tebench's comma-separated
  --target_seg_name interface. It does not implement a tebench peer mode,
  weighted targets, hotspot routing, or arbitrary custom target plans.

Required:
  --topology NAME
  --targets HOSTS             Comma-separated target hosts (not all_to_all).
  --initiators HOSTS          Comma-separated initiator hosts (not all_to_all).
  --nodes HOSTS               Comma-separated peers for all_to_all.

Runtime:
  --tebench PATH              Required absolute path on every host (except dry-run).
  --backend classic|tent      Default: tent.
  --xport-type TYPE           Optional tebench --xport_type.
  --seg-type DRAM|VRAM        Default: DRAM.
  --operation TYPE            read|write|mix|write_seed|read_verify.
  --verify-data               Run write_seed, then read_verify.
  --total-buffer-size BYTES   Default: 1 GiB.
  --block-size BYTES          Fixed block size. Default: 4096.
  --batch-size COUNT          Fixed batch size. Default: 1.
  --num-threads COUNT         0 selects one thread per target.
  --duration SECONDS          Duration per phase. Default: 5.
  --range-policy POLICY       shared|per_initiator. Default: shared.
  --target-range-size BYTES   0 derives the range from total buffer size.
  --local-gpu-id ID           Default: 0.
  --target-gpu-id ID          Default: 0.
  --target-arg FLAG           Extra target flag; may be repeated.
  --initiator-arg FLAG        Extra initiator flag; may be repeated.
  --env KEY=VALUE             Environment entry passed to tebench; repeatable.

Orchestration:
  --ssh-user USER             Prefix hosts that do not include user@.
  --ssh-port PORT             Default: 22.
  --ready-timeout SECONDS     Target/barrier timeout. Default: 60.
  --run-timeout SECONDS       Per-phase timeout. Default: 300.
  --result-dir DIR            Local logs and JSONL destination.
  --run-id ID                 Unique identifier for paths and logs.
  --keep-remote               Keep remote /tmp run directories.
  --dry-run                   Validate and print the launch plan only.

Use the literal host name "local" to run a role without SSH. Host names must
be unique within each list. The script assumes tebench already exists at the
same path on every selected host. Python 3 is required on the launcher host
to combine process results into results.json.
EOF
}

die() {
    echo "ERROR: $*" >&2
    exit 2
}

require_value() {
    [[ $# -ge 2 && -n "$2" ]] || die "$1 requires a value"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --verify-data)
            VERIFY_DATA=1
            shift
            continue
            ;;
        --keep-remote)
            KEEP_REMOTE=1
            shift
            continue
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            continue
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --*=*)
            option="${1%%=*}"
            value="${1#*=}"
            [[ -n "$value" ]] || die "$option requires a value"
            shift
            ;;
        --*)
            require_value "$@"
            option="$1"
            value="$2"
            shift 2
            ;;
        *) die "unknown argument: $1" ;;
    esac

    case "$option" in
        --topology) TOPOLOGY="$value" ;;
        --targets) TARGETS_CSV="$value" ;;
        --initiators) INITIATORS_CSV="$value" ;;
        --nodes) NODES_CSV="$value" ;;
        --tebench) TEBENCH="$value" ;;
        --backend) BACKEND="$value" ;;
        --xport-type) XPORT_TYPE="$value" ;;
        --seg-type) SEG_TYPE="$value" ;;
        --operation) OPERATION="$value" ;;
        --total-buffer-size) TOTAL_BUFFER_SIZE="$value" ;;
        --block-size) BLOCK_SIZE="$value" ;;
        --batch-size) BATCH_SIZE="$value" ;;
        --num-threads) NUM_THREADS="$value" ;;
        --duration) DURATION="$value" ;;
        --range-policy) RANGE_POLICY="$value" ;;
        --target-range-size) TARGET_RANGE_SIZE="$value" ;;
        --local-gpu-id) LOCAL_GPU_ID="$value" ;;
        --target-gpu-id) TARGET_GPU_ID="$value" ;;
        --ready-timeout) READY_TIMEOUT="$value" ;;
        --run-timeout) RUN_TIMEOUT="$value" ;;
        --ssh-user) SSH_USER="$value" ;;
        --ssh-port) SSH_PORT="$value" ;;
        --result-dir) RESULT_DIR="$value" ;;
        --run-id) RUN_ID="$value" ;;
        --target-arg) TARGET_ARGS+=("$value") ;;
        --initiator-arg) INITIATOR_ARGS+=("$value") ;;
        --env) REMOTE_ENV+=("$value") ;;
        *) die "unknown argument: $option" ;;
    esac
done

split_csv() {
    local value="$1"
    local -n output="$2"
    local item
    output=()
    IFS=',' read -ra raw_items <<<"$value"
    for item in "${raw_items[@]}"; do
        item="${item#"${item%%[![:space:]]*}"}"
        item="${item%"${item##*[![:space:]]}"}"
        [[ -n "$item" ]] && output+=("$item")
    done
}

require_uint() {
    local name="$1"
    local value="$2"
    [[ "$value" =~ ^[0-9]+$ ]] || die "$name must be a non-negative integer"
}

require_positive() {
    require_uint "$1" "$2"
    (( "$2" > 0 )) || die "$1 must be greater than zero"
}

validate_unique() {
    local label="$1"
    shift
    local -A seen=()
    local host
    for host in "$@"; do
        [[ -z "${seen[$host]:-}" ]] || die "$label contains duplicate host: $host"
        seen["$host"]=1
    done
}

case "$TOPOLOGY" in
    fanout|incast|m2n|all_to_all) ;;
    *) die "--topology must be fanout, incast, m2n, or all_to_all" ;;
esac
case "$BACKEND" in
    classic|tent) ;;
    *) die "--backend must be classic or tent" ;;
esac
case "$OPERATION" in
    read|write|mix|write_seed|read_verify) ;;
    *) die "--operation must be read, write, mix, write_seed, or read_verify" ;;
esac
case "$RANGE_POLICY" in
    shared|per_initiator) ;;
    *) die "--range-policy must be shared or per_initiator" ;;
esac

for pair in \
    "total-buffer-size:$TOTAL_BUFFER_SIZE" \
    "block-size:$BLOCK_SIZE" \
    "batch-size:$BATCH_SIZE" \
    "duration:$DURATION" \
    "ready-timeout:$READY_TIMEOUT" \
    "run-timeout:$RUN_TIMEOUT" \
    "ssh-port:$SSH_PORT"; do
    require_positive "${pair%%:*}" "${pair#*:}"
done
require_uint "num-threads" "$NUM_THREADS"
require_uint "target-range-size" "$TARGET_RANGE_SIZE"

for entry in "${REMOTE_ENV[@]}"; do
    [[ "$entry" =~ ^[A-Za-z_][A-Za-z0-9_]*=.*$ ]] ||
        die "--env must use KEY=VALUE: $entry"
done

TARGETS=()
INITIATORS=()
NODES=()
if [[ "$TOPOLOGY" == "all_to_all" ]]; then
    [[ -n "$NODES_CSV" ]] || die "--nodes is required for all_to_all"
    [[ -z "$TARGETS_CSV" && -z "$INITIATORS_CSV" ]] ||
        die "all_to_all accepts --nodes instead of --targets/--initiators"
    split_csv "$NODES_CSV" NODES
    ((${#NODES[@]} >= 2)) || die "all_to_all requires at least two nodes"
    validate_unique "--nodes" "${NODES[@]}"
    TARGETS=("${NODES[@]}")
    INITIATORS=("${NODES[@]}")
else
    [[ -n "$TARGETS_CSV" ]] || die "--targets is required"
    [[ -n "$INITIATORS_CSV" ]] || die "--initiators is required"
    [[ -z "$NODES_CSV" ]] ||
        die "--nodes is only valid with --topology=all_to_all"
    split_csv "$TARGETS_CSV" TARGETS
    split_csv "$INITIATORS_CSV" INITIATORS
    ((${#TARGETS[@]} > 0)) || die "--targets is empty"
    ((${#INITIATORS[@]} > 0)) || die "--initiators is empty"
    validate_unique "--targets" "${TARGETS[@]}"
    validate_unique "--initiators" "${INITIATORS[@]}"
fi
if [[ "$TOPOLOGY" == "fanout" && ${#INITIATORS[@]} -ne 1 ]]; then
    die "fanout requires exactly one initiator"
fi
if [[ "$TOPOLOGY" == "incast" && ${#TARGETS[@]} -ne 1 ]]; then
    die "incast requires exactly one target"
fi

target_indices_for() {
    local initiator_index="$1"
    local target_index
    for target_index in "${!TARGETS[@]}"; do
        if [[ "$TOPOLOGY" == "all_to_all" &&
              "${TARGETS[$target_index]}" == "${INITIATORS[$initiator_index]}" ]]; then
            continue
        fi
        printf '%s\n' "$target_index"
    done
}

range_for_initiator() {
    local initiator_index="$1"
    local range_size="$TARGET_RANGE_SIZE"
    if [[ "$range_size" -eq 0 ]]; then
        if [[ "$RANGE_POLICY" == "per_initiator" ]]; then
            range_size=$((TOTAL_BUFFER_SIZE / ${#INITIATORS[@]}))
        else
            range_size="$TOTAL_BUFFER_SIZE"
        fi
    fi
    ((range_size > 0)) || die "derived target range size is zero"
    local offset=0
    if [[ "$RANGE_POLICY" == "per_initiator" ]]; then
        offset=$((initiator_index * range_size))
    fi
    ((offset + range_size <= TOTAL_BUFFER_SIZE)) ||
        die "target ranges exceed --total-buffer-size"
    printf '%s %s\n' "$offset" "$range_size"
}

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
[[ "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]] || die "--run-id contains unsafe characters"
REMOTE_DIR="/tmp/mooncake-tebench-${RUN_ID}"
RESULT_DIR="${RESULT_DIR:-$PWD/tebench-multinode-${RUN_ID}}"

if [[ "$DRY_RUN" -eq 0 ]]; then
    [[ "$TEBENCH" == /* ]] || die "--tebench must be an absolute path"
    command -v python3 >/dev/null || die "python3 is required on the launcher host"
fi

SEGMENTS=()
if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "topology=$TOPOLOGY backend=$BACKEND operation=$OPERATION"
    for target_index in "${!TARGETS[@]}"; do
        SEGMENTS+=("SEGMENT_${target_index}")
        echo "target[$target_index] host=${TARGETS[$target_index]} segment=SEGMENT_${target_index}"
    done
else
    mkdir -p "$RESULT_DIR"
    RESULT_DIR="$(cd "$RESULT_DIR" && pwd)"
fi

phase_plan() {
    local phase="$1"
    local operation="$2"
    local initiator_index target_index
    for initiator_index in "${!INITIATORS[@]}"; do
        local target_names=()
        while IFS= read -r target_index; do
            target_names+=("${SEGMENTS[$target_index]}")
        done < <(target_indices_for "$initiator_index")
        ((${#target_names[@]} > 0)) ||
            die "initiator ${INITIATORS[$initiator_index]} has no targets"
        local joined
        joined="$(IFS=,; echo "${target_names[*]}")"
        local range
        range="$(range_for_initiator "$initiator_index")"
        local offset="${range%% *}"
        local range_size="${range#* }"
        local threads="$NUM_THREADS"
        if [[ "$threads" -eq 0 ]]; then
            threads="${#target_names[@]}"
        fi
        echo "phase=$phase operation=$operation initiator[$initiator_index] host=${INITIATORS[$initiator_index]} targets=$joined offset=$offset range=$range_size threads=$threads"
    done
}

if [[ "$DRY_RUN" -eq 1 ]]; then
    if [[ "$VERIFY_DATA" -eq 1 ]]; then
        phase_plan seed write_seed
        phase_plan verify read_verify
    else
        phase_plan benchmark "$OPERATION"
    fi
    exit 0
fi

SSH_OPTIONS=(
    -p "$SSH_PORT"
    -o BatchMode=yes
    -o ConnectTimeout=10
    -o ServerAliveInterval=10
    -o ServerAliveCountMax=3
    -o StrictHostKeyChecking=yes
)

ssh_target() {
    local host="$1"
    if [[ -n "$SSH_USER" && "$host" != *@* ]]; then
        printf '%s@%s\n' "$SSH_USER" "$host"
    else
        printf '%s\n' "$host"
    fi
}

remote_bash() {
    local host="$1"
    shift
    if [[ "$host" == "local" ]]; then
        bash -s -- "$@"
        return
    fi
    local quoted_args=""
    printf -v quoted_args ' %q' "$@"
    # quoted_args is generated with Bash %q; expansion is intentional here.
    # shellcheck disable=SC2029
    ssh "${SSH_OPTIONS[@]}" -- "$(ssh_target "$host")" \
        "bash -s --$quoted_args"
}

ALL_HOSTS=()
for host in "${TARGETS[@]}" "${INITIATORS[@]}"; do
    found=0
    for existing in "${ALL_HOSTS[@]}"; do
        [[ "$existing" == "$host" ]] && found=1
    done
    [[ "$found" -eq 1 ]] || ALL_HOSTS+=("$host")
done

for host in "${ALL_HOSTS[@]}"; do
    remote_bash "$host" "$TEBENCH" <<'REMOTE_CHECK'
test -x "$1" || {
    echo "tebench is not executable: $1" >&2
    exit 1
}
command -v setsid >/dev/null || {
    echo "setsid is required to manage remote processes" >&2
    exit 1
}
REMOTE_CHECK
done

PROCESS_HOSTS=()
PROCESS_PIDFILES=()
RUN_HOSTS=()
COLLECTED=0

track_process() {
    PROCESS_HOSTS+=("$1")
    PROCESS_PIDFILES+=("$2")
}

stop_remote_process() {
    local host="$1"
    local pid_file="$2"
    remote_bash "$host" "$pid_file" <<'REMOTE_STOP'
pid_file="$1"
[[ -f "$pid_file" ]] || exit 0
pid="$(cat "$pid_file" 2>/dev/null || true)"
[[ "$pid" =~ ^[0-9]+$ ]] || exit 0
kill -0 -- "-$pid" 2>/dev/null || {
    rm -f "$pid_file"
    exit 0
}
kill -TERM -- "-$pid" 2>/dev/null || exit 0
for _ in {1..50}; do
    if ! kill -0 -- "-$pid" 2>/dev/null; then
        rm -f "$pid_file"
        exit 0
    fi
    sleep 0.1
done
kill -KILL -- "-$pid" 2>/dev/null || true
rm -f "$pid_file"
REMOTE_STOP
}

host_key() {
    printf '%s' "$1" | sed 's/[^A-Za-z0-9._-]/_/g'
}

collect_artifacts() {
    [[ "$COLLECTED" -eq 0 ]] || return 0
    local host destination
    local failed=0
    for host in "${RUN_HOSTS[@]}"; do
        destination="$RESULT_DIR/$(host_key "$host")"
        mkdir -p "$destination" || {
            failed=1
            continue
        }
        if [[ "$host" == "local" ]]; then
            if [[ -d "$REMOTE_DIR" ]]; then
                cp -a "$REMOTE_DIR/." "$destination/" || failed=1
            fi
        else
            local archive
            archive="$RESULT_DIR/.$(host_key "$host").tar"
            if remote_bash "$host" "$REMOTE_DIR" >"$archive" <<'REMOTE_ARCHIVE'
run_dir="$1"
tar -C "$run_dir" -cf - .
REMOTE_ARCHIVE
            then
                tar -C "$destination" -xf "$archive" || failed=1
            else
                failed=1
            fi
            rm -f "$archive"
        fi
    done
    if [[ "$failed" -eq 0 ]]; then
        COLLECTED=1
    fi
    return "$failed"
}

aggregate_results() {
    python3 - "$RESULT_DIR" "$RUN_ID" "$TOPOLOGY" "$BACKEND" \
        "${PROCESS_HOSTS[@]}" "${PROCESS_PIDFILES[@]}" <<'PYTHON'
import json
import os
import re
import sys
from pathlib import Path

result_dir, run_id, topology, backend = sys.argv[1:5]
count = (len(sys.argv) - 5) // 2
hosts = sys.argv[5:5 + count]
pidfiles = sys.argv[5 + count:]
results = []
for host, pidfile in zip(hosts, pidfiles):
    name = Path(pidfile).stem
    if "-initiator-" not in name:
        continue
    phase, index = name.rsplit("-initiator-", 1)
    directory = Path(result_dir) / re.sub(r"[^A-Za-z0-9._-]", "_", host)
    record_file = directory / (name + ".jsonl")
    rc_file = directory / (name + ".rc")
    records = []
    if record_file.exists():
        with record_file.open() as source:
            for line in source:
                if line.strip():
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        raise ValueError("Expected a JSON object in " + str(record_file))
                    records.append(record)
    results.append({
        "host": host,
        "phase": phase,
        "initiator_index": int(index),
        "exit_code": int(rc_file.read_text()) if rc_file.exists() else None,
        "records": records,
    })

output = Path(result_dir) / "results.json"
temporary = output.with_suffix(".json.tmp")
try:
    with temporary.open("w") as destination:
        json.dump({
            "schema_version": 1,
            "run_id": run_id,
            "topology": topology,
            "backend": backend,
            "results": results,
        }, destination, indent=2, allow_nan=False)
        destination.write("\n")
    os.replace(temporary, output)
finally:
    if temporary.exists():
        temporary.unlink()
PYTHON
}

remove_remote_dirs() {
    [[ "$KEEP_REMOTE" -eq 0 ]] || return 0
    local host
    local failed=0
    for host in "${RUN_HOSTS[@]}"; do
        remote_bash "$host" "$REMOTE_DIR" <<'REMOTE_REMOVE' || failed=1
run_dir="$1"
case "$run_dir" in
    /tmp/mooncake-tebench-*) rm -rf -- "$run_dir" ;;
    *) echo "Refusing to remove unexpected path: $run_dir" >&2; exit 1 ;;
esac
REMOTE_REMOVE
    done
    return "$failed"
}

cleanup() {
    local rc=$?
    trap - EXIT
    set +e
    local cleanup_failed=0
    local i
    for ((i = ${#PROCESS_HOSTS[@]} - 1; i >= 0; --i)); do
        stop_remote_process "${PROCESS_HOSTS[$i]}" \
            "${PROCESS_PIDFILES[$i]}" || cleanup_failed=1
    done
    collect_artifacts || cleanup_failed=1
    aggregate_results || cleanup_failed=1
    remove_remote_dirs || cleanup_failed=1
    if [[ "$rc" -eq 0 && "$cleanup_failed" -ne 0 ]]; then
        rc=1
        echo "Run completed, but cleanup or artifact collection failed" >&2
    fi
    if [[ "$rc" -ne 0 ]]; then
        echo "Run failed; available artifacts are in $RESULT_DIR" >&2
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for host in "${ALL_HOSTS[@]}"; do
    remote_bash "$host" "$REMOTE_DIR" <<'REMOTE_CREATE' ||
mkdir -- "$1"
REMOTE_CREATE
        die "Cannot create run directory on $host; use a unique --run-id"
    RUN_HOSTS+=("$host")
done

start_target() {
    local target_index="$1"
    local host="${TARGETS[$target_index]}"
    local name="target-${target_index}"
    local pid_file="$REMOTE_DIR/$name.pid"
    local command=(
        env "${REMOTE_ENV[@]}" "$TEBENCH"
        "--backend=$BACKEND"
        "--metadata_type=p2p"
        "--rpc_server_port=0"
        "--seg_type=$SEG_TYPE"
        "--total_buffer_size=$TOTAL_BUFFER_SIZE"
        "--local_gpu_id=$LOCAL_GPU_ID"
    )
    [[ -z "$XPORT_TYPE" ]] || command+=("--xport_type=$XPORT_TYPE")
    command+=("${TARGET_ARGS[@]}")
    remote_bash "$host" "$REMOTE_DIR" "$name" "${command[@]}" <<'REMOTE_TARGET'
run_dir="$1"
name="$2"
shift 2
mkdir -p "$run_dir"
log_file="$run_dir/$name.log"
pid_file="$run_dir/$name.pid"
nohup setsid "$@" >"$log_file" 2>&1 </dev/null &
printf '%s\n' "$!" >"$pid_file"
REMOTE_TARGET
    track_process "$host" "$pid_file"
}

wait_for_segment() {
    local target_index="$1"
    local host="${TARGETS[$target_index]}"
    local name="target-${target_index}"
    local deadline=$((SECONDS + READY_TIMEOUT))
    local state segment
    while ((SECONDS < deadline)); do
        state="$(remote_bash "$host" "$REMOTE_DIR/$name.log" \
            "$REMOTE_DIR/$name.pid" <<'REMOTE_SEGMENT'
log_file="$1"
pid_file="$2"
segment=""
if [[ -f "$log_file" ]]; then
    segment="$(sed -n 's/.*--target_seg_name=\([^[:space:]]*\).*/\1/p' \
        "$log_file" | tail -n 1)"
fi
if [[ -n "$segment" ]]; then
    printf 'SEGMENT=%s\n' "$segment"
elif [[ -f "$pid_file" ]] &&
     kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "STATE=RUNNING"
else
    echo "STATE=EXITED"
fi
REMOTE_SEGMENT
)"
        segment="$(sed -n 's/^SEGMENT=//p' <<<"$state")"
        if [[ -n "$segment" ]]; then
            SEGMENTS[target_index]="$segment"
            echo "Target ${TARGETS[$target_index]} published $segment"
            return 0
        fi
        if grep -q '^STATE=EXITED$' <<<"$state"; then
            echo "Target ${TARGETS[$target_index]} exited before publishing" >&2
            remote_bash "$host" "$REMOTE_DIR/$name.log" <<'REMOTE_LOG' >&2
cat "$1" 2>/dev/null || true
REMOTE_LOG
            return 1
        fi
        sleep 0.2
    done
    echo "Timed out waiting for target ${TARGETS[$target_index]}" >&2
    return 1
}

start_initiator() {
    local phase="$1"
    local operation="$2"
    local initiator_index="$3"
    local host="${INITIATORS[$initiator_index]}"
    local target_names=()
    local target_index
    while IFS= read -r target_index; do
        target_names+=("${SEGMENTS[$target_index]}")
    done < <(target_indices_for "$initiator_index")
    local joined
    joined="$(IFS=,; echo "${target_names[*]}")"
    local range
    range="$(range_for_initiator "$initiator_index")"
    local offset="${range%% *}"
    local range_size="${range#* }"
    local threads="$NUM_THREADS"
    [[ "$threads" -ne 0 ]] || threads="${#target_names[@]}"
    local name="${phase}-initiator-${initiator_index}"
    local ready_file="$REMOTE_DIR/$name.ready"
    local start_file="$REMOTE_DIR/$phase.start"
    local rc_file="$REMOTE_DIR/$name.rc"
    local log_file="$REMOTE_DIR/$name.log"
    local pid_file="$REMOTE_DIR/$name.pid"
    local result_file="$REMOTE_DIR/$name.jsonl"
    local command=(
        env "${REMOTE_ENV[@]}" "$TEBENCH"
        "--backend=$BACKEND"
        "--metadata_type=p2p"
        "--rpc_server_port=0"
        "--seg_type=$SEG_TYPE"
        "--target_seg_name=$joined"
        "--op_type=$operation"
        "--total_buffer_size=$TOTAL_BUFFER_SIZE"
        "--start_block_size=$BLOCK_SIZE"
        "--max_block_size=$BLOCK_SIZE"
        "--start_batch_size=$BATCH_SIZE"
        "--max_batch_size=$BATCH_SIZE"
        "--start_num_threads=$threads"
        "--max_num_threads=$threads"
        "--duration=$DURATION"
        "--target_offset=$offset"
        "--target_range_size=$range_size"
        "--local_gpu_id=$LOCAL_GPU_ID"
        "--target_gpu_id=$TARGET_GPU_ID"
        "--result_output_jsonl=$result_file"
    )
    [[ -z "$XPORT_TYPE" ]] || command+=("--xport_type=$XPORT_TYPE")
    command+=("${INITIATOR_ARGS[@]}")
    remote_bash "$host" "$REMOTE_DIR" "$ready_file" "$start_file" \
        "$rc_file" "$log_file" "$pid_file" "${command[@]}" <<'REMOTE_INITIATOR'
run_dir="$1"
ready_file="$2"
start_file="$3"
rc_file="$4"
log_file="$5"
pid_file="$6"
shift 6
mkdir -p "$run_dir"
rm -f "$ready_file" "$rc_file"
nohup setsid bash -c '
ready_file="$1"
start_file="$2"
rc_file="$3"
log_file="$4"
pid_file="$5"
shift 5
: >"$ready_file"
while [[ ! -e "$start_file" ]]; do sleep 0.05; done
"$@" >"$log_file" 2>&1
rc=$?
printf "%s\n" "$rc" >"$rc_file"
rm -f "$pid_file"
exit "$rc"
' _ "$ready_file" "$start_file" "$rc_file" "$log_file" "$pid_file" "$@" \
    >/dev/null 2>&1 &
printf '%s\n' "$!" >"$pid_file"
REMOTE_INITIATOR
    track_process "$host" "$pid_file"
    LAST_READY_FILE="$ready_file"
}

wait_for_remote_file() {
    local host="$1"
    local path="$2"
    local deadline="$3"
    while ((SECONDS < deadline)); do
        if remote_bash "$host" "$path" <<'REMOTE_EXISTS'
test -e "$1"
REMOTE_EXISTS
        then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

run_phase() {
    local phase="$1"
    local operation="$2"
    local ready_files=()
    local initiator_index
    echo "Launching $phase phase ($operation)"
    for initiator_index in "${!INITIATORS[@]}"; do
        start_initiator "$phase" "$operation" "$initiator_index"
        ready_files+=("$LAST_READY_FILE")
    done
    local deadline=$((SECONDS + READY_TIMEOUT))
    for initiator_index in "${!INITIATORS[@]}"; do
        wait_for_remote_file "${INITIATORS[$initiator_index]}" \
            "${ready_files[$initiator_index]}" "$deadline" ||
            die "initiator ${INITIATORS[$initiator_index]} did not reach the barrier"
    done
    local host
    for host in "${ALL_HOSTS[@]}"; do
        remote_bash "$host" "$REMOTE_DIR/$phase.start" <<'REMOTE_RELEASE'
: >"$1"
REMOTE_RELEASE
    done

    deadline=$((SECONDS + RUN_TIMEOUT))
    local failed=0
    for initiator_index in "${!INITIATORS[@]}"; do
        host="${INITIATORS[$initiator_index]}"
        local name="${phase}-initiator-${initiator_index}"
        local state=""
        while ((SECONDS < deadline)); do
            state="$(remote_bash "$host" "$REMOTE_DIR/$name.rc" \
                "$REMOTE_DIR/$name.pid" <<'REMOTE_STATUS'
rc_file="$1"
pid_file="$2"
if [[ -f "$rc_file" ]]; then
    printf 'RC=%s\n' "$(cat "$rc_file")"
elif [[ -f "$pid_file" ]] &&
     kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "STATE=RUNNING"
else
    echo "STATE=EXITED"
fi
REMOTE_STATUS
)"
            [[ "$state" == RC=* || "$state" == "STATE=EXITED" ]] && break
            sleep 0.2
        done
        if [[ "$state" != "RC=0" ]]; then
            echo "$name on $host failed or timed out: $state" >&2
            failed=1
        fi
    done
    [[ "$failed" -eq 0 ]] || return 1
}

for target_index in "${!TARGETS[@]}"; do
    start_target "$target_index"
done
for target_index in "${!TARGETS[@]}"; do
    wait_for_segment "$target_index"
done

if [[ "$VERIFY_DATA" -eq 1 ]]; then
    run_phase seed write_seed
    run_phase verify read_verify
else
    run_phase benchmark "$OPERATION"
fi

echo "tebench multi-node run completed; collecting artifacts in $RESULT_DIR"
