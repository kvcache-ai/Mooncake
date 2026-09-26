#!/usr/bin/env bash
# Copyright 2026 Hygon Information Technology Co., Ltd.
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

set -Eeuo pipefail

activate_runtime() {
    set +u
    source /opt/dtk/env.sh
    set -u
}

check_ports_available() {
    local node_name="$1"
    shift
    python3 - "${node_name}" "$@" <<'PY'
import socket
import sys

node_name = sys.argv[1]
sockets = []
try:
    for raw_port in sys.argv[2:]:
        port = int(raw_port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        try:
            # This socket is only a conservative availability probe and is
            # closed before any service starts; it is not a listening service.
            sock.bind(("0.0.0.0", port))
        except OSError as exc:
            print(f"ERROR: {node_name} TCP port {port} is unavailable: {exc}", file=sys.stderr)
            sys.exit(1)
        sockets.append(sock)
    print(f"Store CI ports are available on {node_name}: {' '.join(sys.argv[2:])}")
finally:
    for sock in sockets:
        sock.close()
PY
}

allocate_free_ports() {
    local count="$1"
    local range_min="$2"
    local range_max="$3"
    python3 - "${count}" "${range_min}" "${range_max}" <<'PY'
import random
import socket
import sys

count, range_min, range_max = map(int, sys.argv[1:])
candidates = list(range(range_min, range_max + 1))
random.SystemRandom().shuffle(candidates)
sockets = []
ports = []
try:
    for port in candidates:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        try:
            # Hold every candidate socket until the full unique set is chosen.
            sock.bind(("0.0.0.0", port))
        except OSError:
            sock.close()
            continue
        sockets.append(sock)
        ports.append(port)
        if len(ports) == count:
            print(" ".join(map(str, ports)))
            break
    else:
        print(
            f"ERROR: could not allocate {count} ports from {range_min}-{range_max}",
            file=sys.stderr,
        )
        sys.exit(1)
finally:
    for sock in sockets:
        sock.close()
PY
}

prepare_container() {
    local wheel_path="$1"
    local dtk_tarball="/tmp/$(basename "${DTK_PKG_URL}")"
    local dtk_dir=""

    wget -q "${DTK_PKG_URL}" -O "${dtk_tarball}"
    tar -xzf "${dtk_tarball}" -C /opt
    dtk_dir="$(find /opt -mindepth 1 -maxdepth 1 -type d -name 'dtk-*' -print -quit)"
    if [ -z "${dtk_dir}" ]; then
        echo "ERROR: the DTK archive did not create /opt/dtk-*" >&2
        exit 1
    fi
    ln -s "${dtk_dir}" /opt/dtk
    activate_runtime

    python3 -m pip install "${wheel_path}"
    command -v mooncake_master >/dev/null
    command -v mooncake_client >/dev/null
    python3 -c 'from mooncake.store import MooncakeDistributedStore, ReplicateConfig'

}

run_master() {
    local local_ip="$1"
    local rpc_port="$2"
    local metrics_port="$3"
    local metadata_port="$4"
    activate_runtime
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy
    exec mooncake_master \
        --enable_ha=false \
        --rpc_address="${local_ip}" \
        --rpc_port="${rpc_port}" \
        --metrics_port="${metrics_port}" \
        --enable_http_metadata_server=true \
        --http_metadata_server_host=0.0.0.0 \
        --http_metadata_server_port="${metadata_port}" \
        --default_kv_lease_ttl=300000 \
        --logtostderr=true
}

run_client() {
    local local_ip="$1"
    local primary_ip="$2"
    local device_filter="$3"
    local master_rpc_port="$4"
    local metadata_port="$5"
    local client_port="$6"
    activate_runtime
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy
    if [ "${device_filter}" = "__auto__" ]; then
        device_filter=""
    fi
    if [ -n "${device_filter}" ]; then
        export MC_TE_FILTERS="${device_filter}"
    else
        unset MC_TE_FILTERS || true
    fi
    exec mooncake_client \
        --host="${local_ip}" \
        --port="${client_port}" \
        --global_segment_size=4GB \
        --local_buffer_size=0 \
        --master_server_address="${primary_ip}:${master_rpc_port}" \
        --metadata_server="http://${primary_ip}:${metadata_port}/metadata" \
        --protocol=rdma \
        --logtostderr=true
}

run_store_benchmark() {
    local local_ip="$1"
    local primary_ip="$2"
    local device_filter="$3"
    local master_rpc_port="$4"
    local metadata_port="$5"
    local bench_port="$6"
    activate_runtime
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy
    if [ "${device_filter}" = "__auto__" ]; then
        device_filter=""
    fi
    if [ -n "${device_filter}" ]; then
        export MC_TE_FILTERS="${device_filter}"
    else
        unset MC_TE_FILTERS || true
    fi
    exec python3 /work/store_kv_bench.py \
        --scenario=verify_write \
        --local-hostname="${local_ip}:${bench_port}" \
        --metadata-server="http://${primary_ip}:${metadata_port}/metadata" \
        --master-server="${primary_ip}:${master_rpc_port}" \
        --protocol=rdma \
        --device-name="" \
        --global-segment-size=0 \
        --local-buffer-size=134217728 \
        --io-api=plain \
        --numjobs=1 \
        --iodepth=1 \
        --batch-size=4 \
        --nr-objects=16 \
        --key-prefix=ci-store \
        --key-size=32 \
        --value-size=1048576 \
        --memory-replica-num=1 \
        --nof-replica-num=0 \
        --verify \
        --pattern=rdma \
        --log-level=INFO
}

case "${1:-}" in
    __container_prepare)
        prepare_container "$2"
        exit 0
        ;;
    __container_check_ports)
        shift
        check_ports_available "$@"
        exit 0
        ;;
    __host_allocate_ports)
        allocate_free_ports "$2" "$3" "$4"
        exit 0
        ;;
    __container_master)
        run_master "$2" "$3" "$4" "$5"
        exit 0
        ;;
    __container_client)
        run_client "$2" "$3" "$4" "$5" "$6" "$7"
        exit 0
        ;;
    __container_benchmark)
        shift
        run_store_benchmark "$@"
        exit 0
        ;;
esac

: "${TEST_IMAGE:?TEST_IMAGE is required}"
: "${DTK_PKG_URL:?DTK_PKG_URL is required}"
test "$#" -eq 1

for command_name in ip cut awk docker flock getent grep python3 realpath scp ssh timeout; do
    command -v "${command_name}" >/dev/null 2>&1 || {
        echo "ERROR: required command is missing: ${command_name}" >&2
        exit 1
    }
done

WHEEL_PATH="$(realpath "$1")"
SELF_PATH="$(realpath "${BASH_SOURCE[0]}")"
BENCH_PY_PATH="$(realpath "$(dirname "${BASH_SOURCE[0]}")/../../mooncake-store/benchmarks/store_kv_bench.py")"
if [ ! -f "${WHEEL_PATH}" ]; then
    echo "ERROR: wheel not found: ${WHEEL_PATH}" >&2
    exit 1
fi

: "${TARGET_HOST:?TARGET_HOST is required}"
: "${INITIATOR_HOST:?INITIATOR_HOST is required}"
: "${TARGET_FILTER:?TARGET_FILTER is required}"
: "${INITIATOR_FILTER:?INITIATOR_FILTER is required}"
PRIMARY_HOST="${TARGET_HOST}"
SECONDARY_HOST="${INITIATOR_HOST}"
PRIMARY_FILTER="${TARGET_FILTER}"
SECONDARY_FILTER="${INITIATOR_FILTER}"
REMOTE_USER=github
SSH_PORT=22
PORT_RANGE_MIN=20000
PORT_RANGE_MAX=29999
LOCK_FILE=/tmp/mooncake-ci-cross-node.lock
LOCK_TIMEOUT=900
SETUP_TIMEOUT=900
READY_TIMEOUT=180
RUN_TIMEOUT=600
LOG_DIR="${RUNNER_TEMP}/mooncake-store-logs"
RUN_TOKEN="${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-$$"
PRIMARY_CONTAINER="mooncake-store-primary-${RUN_TOKEN}"
SECONDARY_CONTAINER="mooncake-store-secondary-${RUN_TOKEN}"
REMOTE_WORK_DIR="/tmp/mooncake-ci-store-${RUN_TOKEN}"
WHEEL_BASENAME="$(basename "${WHEEL_PATH}")"

resolve_ipv4() {
    getent ahostsv4 "$1" 2>/dev/null | awk 'NR == 1 { print $1 }'
}

PRIMARY_IP="$(resolve_ipv4 "${PRIMARY_HOST}")"
SECONDARY_IP="$(resolve_ipv4 "${SECONDARY_HOST}")"
if [ -z "${PRIMARY_IP}" ] || [ -z "${SECONDARY_IP}" ]; then
    echo "ERROR: failed to determine the primary or secondary IPv4 address" >&2
    exit 1
fi
if [ "${PRIMARY_IP}" = "${SECONDARY_IP}" ]; then
    echo "ERROR: primary and secondary resolve to the same IP" >&2
    exit 1
fi
if ! ip -o -4 address show | awk '{print $4}' | cut -d/ -f1 | grep -Fxq "${PRIMARY_IP}"; then
    echo "ERROR: TARGET_HOST must resolve to this runner's local service address" >&2
    exit 1
fi
SECONDARY_SSH_HOST="${SECONDARY_HOST}"
REMOTE="${REMOTE_USER}@${SECONDARY_SSH_HOST}"

SSH_OPTIONS=(-p "${SSH_PORT}" -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=10 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=yes)
SCP_OPTIONS=(-P "${SSH_PORT}" -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes)

mkdir -p "${LOG_DIR}"
LOG_NAMES=(primary-lock secondary-lock primary-port-check secondary-port-check primary-prepare secondary-prepare primary-master secondary-client store-kv-bench)
for log_name in "${LOG_NAMES[@]}"; do
    : >"${LOG_DIR}/${log_name}.log"
done

PRIMARY_LOCK_PID=""
SECONDARY_LOCK_PID=""

print_logs() {
    local log_file=""
    for log_file in "${LOG_DIR}"/*.log; do
        echo "===== $(basename "${log_file}") ====="
        cat "${log_file}"
    done
}

cleanup() {
    local rc=$?
    trap - EXIT
    set +e
    docker rm -f "${PRIMARY_CONTAINER}" >/dev/null 2>&1 || true
    ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- "${SECONDARY_CONTAINER}" "${REMOTE_WORK_DIR}" <<'REMOTE_CLEANUP' >/dev/null 2>&1
container_name="$1"
remote_dir="$2"
docker rm -f "${container_name}" >/dev/null 2>&1 || true
case "${remote_dir}" in
    /tmp/mooncake-ci-store-*) rm -rf -- "${remote_dir}" ;;
    *) echo "Refusing to remove unexpected path: ${remote_dir}" >&2 ;;
esac
REMOTE_CLEANUP
    if [ -n "${SECONDARY_LOCK_PID}" ]; then
        kill "${SECONDARY_LOCK_PID}" >/dev/null 2>&1 || true
        wait "${SECONDARY_LOCK_PID}" >/dev/null 2>&1 || true
    fi
    if [ -n "${PRIMARY_LOCK_PID}" ]; then
        kill "${PRIMARY_LOCK_PID}" >/dev/null 2>&1 || true
        wait "${PRIMARY_LOCK_PID}" >/dev/null 2>&1 || true
    fi
    if [ "${rc}" -ne 0 ]; then
        print_logs
    fi
    exit "${rc}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_log() {
    local log_file="$1"
    local pattern="$2"
    local process_id="$3"
    local description="$4"
    local attempt=0
    for ((attempt = 1; attempt <= READY_TIMEOUT; attempt++)); do
        if grep -q "${pattern}" "${log_file}"; then
            return 0
        fi
        if ! kill -0 "${process_id}" 2>/dev/null; then
            echo "ERROR: ${description} exited before becoming ready" >&2
            return 1
        fi
        sleep 1
    done
    echo "ERROR: ${description} was not ready within ${READY_TIMEOUT}s" >&2
    return 1
}

wait_for_lock() {
    local log_file="$1"
    local process_id="$2"
    local node_name="$3"
    local attempt=0
    for ((attempt = 1; attempt <= LOCK_TIMEOUT + 10; attempt++)); do
        if grep -q '^LOCK_ACQUIRED$' "${log_file}"; then
            return 0
        fi
        if ! kill -0 "${process_id}" 2>/dev/null; then
            echo "ERROR: failed to acquire the cross-node test lock on ${node_name}" >&2
            return 1
        fi
        sleep 1
    done
    echo "ERROR: timed out waiting for the cross-node test lock on ${node_name}" >&2
    return 1
}

echo "Primary:   ${PRIMARY_HOST}"
echo "Secondary: ${SECONDARY_HOST}"
echo "Image:     ${TEST_IMAGE}"

echo "Acquiring the cross-node test lock on ${PRIMARY_HOST}..."
PARENT_PID="$$"
(
    exec 9>"${LOCK_FILE}"
    if ! flock -w "${LOCK_TIMEOUT}" 9; then
        echo "ERROR: lock wait timed out"
        exit 75
    fi
    echo "LOCK_ACQUIRED"
    while kill -0 "${PARENT_PID}" 2>/dev/null; do sleep 5; done
) >"${LOG_DIR}/primary-lock.log" 2>&1 &
PRIMARY_LOCK_PID=$!
wait_for_lock "${LOG_DIR}/primary-lock.log" "${PRIMARY_LOCK_PID}" "${PRIMARY_HOST}"

echo "Acquiring the cross-node test lock on ${SECONDARY_HOST}..."
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- "${LOCK_FILE}" "${LOCK_TIMEOUT}" \
    >"${LOG_DIR}/secondary-lock.log" 2>&1 <<'REMOTE_LOCK' &
set -Eeuo pipefail
lock_file="$1"
lock_timeout="$2"
session_parent="$PPID"
command -v flock >/dev/null
exec 9>"${lock_file}"
if ! flock -w "${lock_timeout}" 9; then
    echo "ERROR: lock wait timed out"
    exit 75
fi
echo "LOCK_ACQUIRED"
while kill -0 "${session_parent}" 2>/dev/null; do sleep 5; done
REMOTE_LOCK
SECONDARY_LOCK_PID=$!
wait_for_lock "${LOG_DIR}/secondary-lock.log" "${SECONDARY_LOCK_PID}" "${SECONDARY_HOST}"
echo "Cross-node test locks acquired."

ssh "${SSH_OPTIONS[@]}" "${REMOTE}" mkdir -p "${REMOTE_WORK_DIR}"
scp "${SCP_OPTIONS[@]}" "${WHEEL_PATH}" "${SELF_PATH}" "${REMOTE}:${REMOTE_WORK_DIR}/"

DOCKER_ENV_ARGS=(
    -e "DTK_PKG_URL=${DTK_PKG_URL}"
    -e "PIP_INDEX_URL=${PIP_INDEX_URL}"
)

docker run -d --name "${PRIMARY_CONTAINER}" --network host --privileged \
    --volume "${WHEEL_PATH}:/work/${WHEEL_BASENAME}:ro" \
    --volume "${SELF_PATH}:/work/test_mooncake_store_cross_node.sh:ro" \
    --volume "${BENCH_PY_PATH}:/work/store_kv_bench.py:ro" \
    "${DOCKER_ENV_ARGS[@]}" --entrypoint /bin/bash "${TEST_IMAGE}" -lc 'exec sleep infinity' >/dev/null

ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- \
    "${REMOTE_WORK_DIR}" "${WHEEL_BASENAME}" "$(basename "${SELF_PATH}")" \
    "${SECONDARY_CONTAINER}" "${TEST_IMAGE}" "${DTK_PKG_URL}" \
    "${PIP_INDEX_URL}" <<'REMOTE_CONTAINER' >/dev/null
set -Eeuo pipefail
remote_dir="$1"; wheel="$2"; script="$3"; container="$4"; image="$5"
dtk_url="$6"; pip_index="$7"
env_args=(-e "DTK_PKG_URL=${dtk_url}")
[ -n "${pip_index}" ] && env_args+=(-e "PIP_INDEX_URL=${pip_index}")
docker run -d --name "${container}" --network host --privileged \
    --volume "${remote_dir}/${wheel}:/work/${wheel}:ro" \
    --volume "${remote_dir}/${script}:/work/test_mooncake_store_cross_node.sh:ro" \
    "${env_args[@]}" --entrypoint /bin/bash "${image}" -lc 'exec sleep infinity'
REMOTE_CONTAINER

echo "Preparing DTK and standard wheel on both nodes in parallel..."
timeout "${SETUP_TIMEOUT}" docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/test_mooncake_store_cross_node.sh \
    __container_prepare "/work/${WHEEL_BASENAME}" >"${LOG_DIR}/primary-prepare.log" 2>&1 &
PRIMARY_PREP_PID=$!
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- "${SECONDARY_CONTAINER}" "${WHEEL_BASENAME}" "${SETUP_TIMEOUT}" \
    >"${LOG_DIR}/secondary-prepare.log" 2>&1 <<'REMOTE_PREPARE' &
set -Eeuo pipefail
timeout "$3" docker exec "$1" /bin/bash /work/test_mooncake_store_cross_node.sh __container_prepare "/work/$2"
REMOTE_PREPARE
SECONDARY_PREP_PID=$!

set +e
wait "${PRIMARY_PREP_PID}"; PRIMARY_PREP_RC=$?
wait "${SECONDARY_PREP_PID}"; SECONDARY_PREP_RC=$?
set -e
if [ "${PRIMARY_PREP_RC}" -ne 0 ] || [ "${SECONDARY_PREP_RC}" -ne 0 ]; then
    echo "ERROR: Store environment preparation failed (primary=${PRIMARY_PREP_RC}, secondary=${SECONDARY_PREP_RC})" >&2
    exit 1
fi

read -r PRIMARY_MASTER_RPC_PORT PRIMARY_MASTER_METRICS_PORT PRIMARY_METADATA_PORT BENCH_PORT \
    <<<"$(allocate_free_ports 4 "${PORT_RANGE_MIN}" "${PORT_RANGE_MAX}")"
read -r SECONDARY_CLIENT_PORT \
    <<<"$(ssh "${SSH_OPTIONS[@]}" "${REMOTE}" /bin/bash \
        "${REMOTE_WORK_DIR}/$(basename "${SELF_PATH}")" __host_allocate_ports \
        1 "${PORT_RANGE_MIN}" "${PORT_RANGE_MAX}")"

for allocated_port in \
    "${PRIMARY_MASTER_RPC_PORT}" "${PRIMARY_MASTER_METRICS_PORT}" "${PRIMARY_METADATA_PORT}" \
    "${BENCH_PORT}" \
    "${SECONDARY_CLIENT_PORT}"; do
    if ! [[ "${allocated_port}" =~ ^[0-9]+$ ]]; then
        echo "ERROR: automatic Store port allocation returned an invalid value" >&2
        exit 1
    fi
done
echo "Primary ports:   master=${PRIMARY_MASTER_RPC_PORT}/${PRIMARY_MASTER_METRICS_PORT} metadata=${PRIMARY_METADATA_PORT} bench=${BENCH_PORT}"
echo "Secondary ports: client=${SECONDARY_CLIENT_PORT}"

echo "Checking auto-allocated Store CI ports on both nodes..."
docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/test_mooncake_store_cross_node.sh \
    __container_check_ports "${PRIMARY_HOST}" \
    "${PRIMARY_MASTER_RPC_PORT}" "${PRIMARY_MASTER_METRICS_PORT}" "${PRIMARY_METADATA_PORT}" \
    "${BENCH_PORT}" \
    >"${LOG_DIR}/primary-port-check.log" 2>&1
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" docker exec "${SECONDARY_CONTAINER}" \
    /bin/bash /work/test_mooncake_store_cross_node.sh \
    __container_check_ports "${SECONDARY_HOST}" \
    "${SECONDARY_CLIENT_PORT}" \
    >"${LOG_DIR}/secondary-port-check.log" 2>&1

docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/test_mooncake_store_cross_node.sh __container_master \
    "${PRIMARY_IP}" "${PRIMARY_MASTER_RPC_PORT}" "${PRIMARY_MASTER_METRICS_PORT}" "${PRIMARY_METADATA_PORT}" \
    >"${LOG_DIR}/primary-master.log" 2>&1 &
PRIMARY_MASTER_PID=$!
wait_for_log "${LOG_DIR}/primary-master.log" 'Master service started' "${PRIMARY_MASTER_PID}" "primary master"

ssh "${SSH_OPTIONS[@]}" "${REMOTE}" docker exec "${SECONDARY_CONTAINER}" \
    /bin/bash /work/test_mooncake_store_cross_node.sh __container_client \
    "${SECONDARY_IP}" "${PRIMARY_IP}" "${SECONDARY_FILTER}" "${PRIMARY_MASTER_RPC_PORT}" "${PRIMARY_METADATA_PORT}" "${SECONDARY_CLIENT_PORT}" \
    >"${LOG_DIR}/secondary-client.log" 2>&1 &
SECONDARY_CLIENT_PID=$!
wait_for_log "${LOG_DIR}/secondary-client.log" 'Starting real client service' "${SECONDARY_CLIENT_PID}" "secondary client"

timeout "${RUN_TIMEOUT}" docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/test_mooncake_store_cross_node.sh \
    __container_benchmark "${PRIMARY_IP}" "${PRIMARY_IP}" "${PRIMARY_FILTER}" \
    "${PRIMARY_MASTER_RPC_PORT}" "${PRIMARY_METADATA_PORT}" "${BENCH_PORT}" \
    >"${LOG_DIR}/store-kv-bench.log" 2>&1

for phase in write_verify verify_read; do
    grep -q "=== phase ${phase} ===" "${LOG_DIR}/store-kv-bench.log"
done
if grep -Eq 'failed_requests=[1-9]|failed_kvs=[1-9]|misses=[1-9]|verify_failures=[1-9]| ERROR |Traceback' \
    "${LOG_DIR}/store-kv-bench.log"; then
    echo "ERROR: Mooncake Store benchmark reported failures" >&2
    exit 1
fi

echo "Mooncake Store RDMA write/read verification passed."
grep -E '=== |failed_requests|failed_kvs|misses|verify_failures|overall summary' \
    "${LOG_DIR}/store-kv-bench.log"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
        echo "### Mooncake Store RDMA KV benchmark"
        echo
        echo "- write_verify: passed"
        echo "- Storage provider: secondary node only"
    } >>"${GITHUB_STEP_SUMMARY}"
fi
