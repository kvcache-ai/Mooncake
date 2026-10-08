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

prepare_container_runtime() {
    local wheel_path="$1"
    local dtk_tarball="/tmp/$(basename "${DTK_PKG_URL}")"
    local dtk_dir=""

    echo "Installing DTK from ${DTK_PKG_URL}..."
    wget -q "${DTK_PKG_URL}" -O "${dtk_tarball}"
    tar -xzf "${dtk_tarball}" -C /opt
    dtk_dir="$(find /opt -mindepth 1 -maxdepth 1 -type d -name 'dtk-*' -print -quit)"
    if [ -z "${dtk_dir}" ]; then
        echo "ERROR: the DTK archive did not create /opt/dtk-*" >&2
        exit 1
    fi
    ln -s "${dtk_dir}" /opt/dtk
    set +u
    source /opt/dtk/env.sh
    set -u

    echo "Installing standard wheel ${wheel_path}..."
    python3 -m pip install "${wheel_path}"
    python3 -m pip show mooncake-transfer-engine
    if ! command -v transfer_engine_bench >/dev/null 2>&1; then
        echo "ERROR: transfer_engine_bench was not installed by the wheel" >&2
        exit 1
    fi
}

activate_container_runtime() {
    set +u
    # shellcheck disable=SC1091
    source /opt/dtk/env.sh
    set -u
    if ! command -v transfer_engine_bench >/dev/null 2>&1; then
        echo "ERROR: prepared container does not contain transfer_engine_bench" >&2
        exit 1
    fi
}

run_container_target() {
    local target_ip="$1"

    activate_container_runtime
    export MC_TE_FILTERS="${TARGET_FILTER}"
    echo "Starting target on ${target_ip} with MC_TE_FILTERS=${MC_TE_FILTERS}..."
    exec transfer_engine_bench \
        --mode=target \
        --auto_discovery \
        --protocol=rdma \
        --metadata_server=P2PHANDSHAKE \
        --threads=2 \
        --local_server_name="${target_ip}"
}

run_container_initiator() {
    local initiator_ip="$1"
    local target_ip="$2"
    local target_port="$3"
    local operation="$4"
    local initiator_filter="$5"

    activate_container_runtime
    export MC_TE_FILTERS="${initiator_filter}"
    echo "Starting ${operation} initiator on ${initiator_ip}; target is ${target_ip}:${target_port}; MC_TE_FILTERS=${MC_TE_FILTERS}..."
    exec transfer_engine_bench \
        --mode=initiator \
        --operation="${operation}" \
        --auto_discovery \
        --protocol=rdma \
        --metadata_server=P2PHANDSHAKE \
        --local_server_name="${initiator_ip}" \
        --segment_id="${target_ip}:${target_port}" \
        --threads=2 \
        --duration=10
}

# The same file is mounted into each container and used as its entry point.
case "${1:-}" in
    __container_prepare)
        prepare_container_runtime "$2"
        exit 0
        ;;
    __container_target)
        run_container_target "$2"
        exit 0
        ;;
    __container_initiator)
        run_container_initiator "$2" "$3" "$4" "$5" "$6"
        exit 0
        ;;
esac

: "${TEST_IMAGE:?TEST_IMAGE is required}"
: "${DTK_PKG_URL:?DTK_PKG_URL is required}"
test "$#" -eq 1

for command_name in ip cut awk docker flock getent grep realpath scp sed ssh timeout; do
    if ! command -v "${command_name}" >/dev/null 2>&1; then
        echo "ERROR: required command is missing: ${command_name}" >&2
        exit 1
    fi
done

WHEEL_PATH="$(realpath "$1")"
SELF_PATH="$(realpath "${BASH_SOURCE[0]}")"
if [ ! -f "${WHEEL_PATH}" ]; then
    echo "ERROR: wheel not found: ${WHEEL_PATH}" >&2
    exit 1
fi

: "${TARGET_HOST:?TARGET_HOST is required}"
: "${INITIATOR_HOST:?INITIATOR_HOST is required}"
: "${TARGET_FILTER:?TARGET_FILTER is required}"
: "${INITIATOR_FILTER:?INITIATOR_FILTER is required}"
REMOTE_USER=github
SSH_PORT=22
SETUP_TIMEOUT=600
READY_TIMEOUT=300
SETTLE_SECONDS=5
RUN_TIMEOUT=300
LOCK_FILE=/tmp/mooncake-ci-cross-node.lock
LOCK_TIMEOUT=900
LOG_DIR="${RUNNER_TEMP}/transfer-engine-logs"
RUN_TOKEN="${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-$$"
TARGET_CONTAINER="mooncake-ci-target-${RUN_TOKEN}"
INITIATOR_CONTAINER="mooncake-ci-initiator-${RUN_TOKEN}"
REMOTE_WORK_DIR="/tmp/mooncake-ci-standard-${RUN_TOKEN}"
WHEEL_BASENAME="$(basename "${WHEEL_PATH}")"
TARGET_LOG="${LOG_DIR}/target.log"
INITIATOR_LOG="${LOG_DIR}/initiator.log"
TARGET_LOCK_LOG="${LOG_DIR}/target-lock.log"
INITIATOR_LOCK_LOG="${LOG_DIR}/initiator-lock.log"

resolve_ipv4() {
    getent ahostsv4 "$1" 2>/dev/null | awk 'NR == 1 { print $1 }'
}

TARGET_IP="$(resolve_ipv4 "${TARGET_HOST}")"
INITIATOR_IP="$(resolve_ipv4 "${INITIATOR_HOST}")"
if [ -z "${TARGET_IP}" ] || [ -z "${INITIATOR_IP}" ]; then
    echo "ERROR: failed to determine the target or initiator IPv4 address" >&2
    exit 1
fi
if [ "${TARGET_IP}" = "${INITIATOR_IP}" ]; then
    echo "ERROR: target and initiator resolve to the same IP" >&2
    exit 1
fi
if ! ip -o -4 address show | awk '{print $4}' | cut -d/ -f1 | grep -Fxq "${TARGET_IP}"; then
    echo "ERROR: TARGET_HOST must resolve to this runner's local service address" >&2
    exit 1
fi
INITIATOR_SSH_HOST="${INITIATOR_HOST}"
REMOTE="${REMOTE_USER}@${INITIATOR_SSH_HOST}"

mkdir -p "${LOG_DIR}"
: >"${TARGET_LOG}"
: >"${INITIATOR_LOG}"
: >"${TARGET_LOCK_LOG}"
: >"${INITIATOR_LOCK_LOG}"
TARGET_LOCK_PID=""
INITIATOR_LOCK_PID=""

SSH_OPTIONS=(
    -p "${SSH_PORT}"
    -o BatchMode=yes
    -o ConnectTimeout=10
    -o ServerAliveInterval=10
    -o ServerAliveCountMax=3
    -o StrictHostKeyChecking=yes
)
SCP_OPTIONS=(
    -P "${SSH_PORT}"
    -o BatchMode=yes
    -o ConnectTimeout=10
    -o StrictHostKeyChecking=yes
)
DOCKER_ENV_ARGS=(
    -e "DTK_PKG_URL=${DTK_PKG_URL}"
    -e "PIP_INDEX_URL=${PIP_INDEX_URL}"
)

print_logs() {
    echo "===== target lock (${TARGET_HOST}) ====="
    cat "${TARGET_LOCK_LOG}"
    echo "===== initiator lock (${INITIATOR_HOST}) ====="
    cat "${INITIATOR_LOCK_LOG}"
    echo "===== target (${TARGET_HOST}) full log ====="
    cat "${TARGET_LOG}"
    echo "===== initiator (${INITIATOR_HOST}) full log ====="
    cat "${INITIATOR_LOG}"
}

cleanup() {
    local rc=$?
    trap - EXIT
    set +e

    if docker inspect "${TARGET_CONTAINER}" >/dev/null 2>&1; then
        docker rm -f "${TARGET_CONTAINER}" >/dev/null 2>&1
    fi

    ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- \
        "${INITIATOR_CONTAINER}" "${REMOTE_WORK_DIR}" <<'REMOTE_CLEANUP' >/dev/null 2>&1
container_name="$1"
remote_dir="$2"
docker rm -f "${container_name}" >/dev/null 2>&1 || true
case "${remote_dir}" in
    /tmp/mooncake-ci-standard-*) rm -rf -- "${remote_dir}" ;;
    *) echo "Refusing to remove unexpected path: ${remote_dir}" >&2 ;;
esac
REMOTE_CLEANUP

    if [ -n "${INITIATOR_LOCK_PID}" ]; then
        kill "${INITIATOR_LOCK_PID}" >/dev/null 2>&1 || true
        wait "${INITIATOR_LOCK_PID}" >/dev/null 2>&1 || true
    fi
    if [ -n "${TARGET_LOCK_PID}" ]; then
        kill "${TARGET_LOCK_PID}" >/dev/null 2>&1 || true
        wait "${TARGET_LOCK_PID}" >/dev/null 2>&1 || true
    fi

    if [ "${rc}" -ne 0 ]; then
        print_logs
    fi
    exit "${rc}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

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

echo "Target:    ${TARGET_HOST}"
echo "Initiator: ${INITIATOR_HOST}"
echo "Image:     ${TEST_IMAGE}"

echo "Acquiring the cross-node test lock on ${TARGET_HOST}..."
PARENT_PID="$$"
(
    exec 9>"${LOCK_FILE}"
    if ! flock -w "${LOCK_TIMEOUT}" 9; then
        echo "ERROR: lock wait timed out"
        exit 75
    fi
    echo "LOCK_ACQUIRED"
    while kill -0 "${PARENT_PID}" 2>/dev/null; do sleep 5; done
) >"${TARGET_LOCK_LOG}" 2>&1 &
TARGET_LOCK_PID=$!
wait_for_lock "${TARGET_LOCK_LOG}" "${TARGET_LOCK_PID}" "${TARGET_HOST}"

echo "Acquiring the cross-node test lock on ${INITIATOR_HOST}..."
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- "${LOCK_FILE}" "${LOCK_TIMEOUT}" \
    >"${INITIATOR_LOCK_LOG}" 2>&1 <<'REMOTE_LOCK' &
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
INITIATOR_LOCK_PID=$!
wait_for_lock "${INITIATOR_LOCK_LOG}" "${INITIATOR_LOCK_PID}" "${INITIATOR_HOST}"
echo "Cross-node test locks acquired."

echo "Copying the wheel and test scripts to ${INITIATOR_HOST}..."
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" mkdir -p "${REMOTE_WORK_DIR}"
scp "${SCP_OPTIONS[@]}" "${WHEEL_PATH}" "${SELF_PATH}" \
    "${REMOTE}:${REMOTE_WORK_DIR}/"

echo "Creating target container ${TARGET_CONTAINER} on ${TARGET_HOST}..."
docker run -d \
    --name "${TARGET_CONTAINER}" \
    --network host \
    --privileged \
    --volume "${WHEEL_PATH}:/work/${WHEEL_BASENAME}:ro" \
    --volume "${SELF_PATH}:/work/test_transfer_engine_cross_node.sh:ro" \
    "${DOCKER_ENV_ARGS[@]}" \
    -e "TARGET_FILTER=${TARGET_FILTER}" \
    --entrypoint /bin/bash \
    "${TEST_IMAGE}" \
    -lc 'exec sleep infinity' >/dev/null

echo "Creating initiator container ${INITIATOR_CONTAINER} on ${INITIATOR_HOST}..."
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- \
    "${REMOTE_WORK_DIR}" "${WHEEL_BASENAME}" "$(basename "${SELF_PATH}")" \
    "${INITIATOR_CONTAINER}" "${TEST_IMAGE}" "${DTK_PKG_URL}" \
    "${PIP_INDEX_URL}" <<'REMOTE_START' >/dev/null
set -Eeuo pipefail
remote_dir="$1"
wheel_basename="$2"
script_basename="$3"
container_name="$4"
image="$5"
dtk_pkg_url="$6"
pip_index_url="$7"

docker_env_args=(-e "DTK_PKG_URL=${dtk_pkg_url}")
[ -n "${pip_index_url}" ] && docker_env_args+=(-e "PIP_INDEX_URL=${pip_index_url}")

docker run -d \
    --name "${container_name}" \
    --network host \
    --privileged \
    --volume "${remote_dir}/${wheel_basename}:/work/${wheel_basename}:ro" \
    --volume "${remote_dir}/${script_basename}:/work/test_transfer_engine_cross_node.sh:ro" \
    "${docker_env_args[@]}" \
    --entrypoint /bin/bash \
    "${image}" \
    -lc 'exec sleep infinity'
REMOTE_START

echo "Preparing DTK and wheel on ${TARGET_HOST} and ${INITIATOR_HOST} in parallel..."
timeout "${SETUP_TIMEOUT}" docker exec "${TARGET_CONTAINER}" \
    /bin/bash /work/test_transfer_engine_cross_node.sh \
    __container_prepare "/work/${WHEEL_BASENAME}" \
    >"${TARGET_LOG}" 2>&1 &
TARGET_PREP_PID=$!

ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- \
    "${INITIATOR_CONTAINER}" "${WHEEL_BASENAME}" "${SETUP_TIMEOUT}" \
    >"${INITIATOR_LOG}" 2>&1 <<'REMOTE_PREPARE' &
set -Eeuo pipefail
container_name="$1"
wheel_basename="$2"
setup_timeout="$3"
timeout "${setup_timeout}" docker exec "${container_name}" \
    /bin/bash /work/test_transfer_engine_cross_node.sh \
    __container_prepare "/work/${wheel_basename}"
REMOTE_PREPARE
INITIATOR_PREP_PID=$!

set +e
wait "${TARGET_PREP_PID}"
TARGET_PREP_RC=$?
wait "${INITIATOR_PREP_PID}"
INITIATOR_PREP_RC=$?
set -e

if [ "${TARGET_PREP_RC}" -ne 0 ] || [ "${INITIATOR_PREP_RC}" -ne 0 ]; then
    echo "ERROR: environment preparation failed (target=${TARGET_PREP_RC}, initiator=${INITIATOR_PREP_RC})" >&2
    exit 1
fi
echo "Both test containers are ready."

echo "Starting target service on ${TARGET_HOST}..."
docker exec -e "TARGET_FILTER=${TARGET_FILTER}" "${TARGET_CONTAINER}" \
    /bin/bash /work/test_transfer_engine_cross_node.sh \
    __container_target "${TARGET_IP}" \
    >>"${TARGET_LOG}" 2>&1 &
TARGET_PROCESS_PID=$!

TARGET_PORT=""
for ((attempt = 1; attempt <= READY_TIMEOUT; attempt++)); do
    TARGET_PORT="$(
        sed -nE 's/.*Transfer Engine RPC using .*, listening on [^:[:space:]]+:([0-9]+).*/\1/p' \
            "${TARGET_LOG}" | tail -n 1
    )"
    if [ -n "${TARGET_PORT}" ]; then
        break
    fi
    if ! kill -0 "${TARGET_PROCESS_PID}" 2>/dev/null; then
        echo "ERROR: target service exited before publishing its RPC port" >&2
        exit 1
    fi
    sleep 1
done

if [ -z "${TARGET_PORT}" ]; then
    echo "ERROR: target did not publish an RPC port within ${READY_TIMEOUT}s" >&2
    exit 1
fi
echo "Target endpoint: ${TARGET_HOST}:${TARGET_PORT}"
sleep "${SETTLE_SECONDS}"

if ! kill -0 "${TARGET_PROCESS_PID}" 2>/dev/null; then
    echo "ERROR: target service exited during initialization" >&2
    exit 1
fi

for operation in read write; do
    echo "Starting ${operation} initiator on ${INITIATOR_HOST}..."
    if ! ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- \
        "${INITIATOR_CONTAINER}" "${INITIATOR_IP}" "${TARGET_IP}" \
        "${TARGET_PORT}" "${operation}" "${INITIATOR_FILTER}" "${RUN_TIMEOUT}" \
        >>"${INITIATOR_LOG}" 2>&1 <<'REMOTE_TEST'; then
set -Eeuo pipefail
container_name="$1"
initiator_ip="$2"
target_ip="$3"
target_port="$4"
operation="$5"
initiator_filter="$6"
run_timeout="$7"

timeout "${run_timeout}" docker exec "${container_name}" \
    /bin/bash /work/test_transfer_engine_cross_node.sh \
    __container_initiator "${initiator_ip}" "${target_ip}" "${target_port}" \
    "${operation}" "${initiator_filter}"
REMOTE_TEST
        echo "ERROR: ${operation} initiator failed" >&2
        exit 1
    fi
done

if ! kill -0 "${TARGET_PROCESS_PID}" 2>/dev/null; then
    echo "ERROR: target service exited unexpectedly during the test" >&2
    exit 1
fi

SUCCESS_COUNT="$(grep -c 'Test completed:' "${INITIATOR_LOG}" || true)"
if [ "${SUCCESS_COUNT}" -ne 2 ]; then
    echo "ERROR: expected two successful read/write completions, found ${SUCCESS_COUNT}" >&2
    exit 1
fi

echo "Cross-node RDMA read/write tests passed:"
grep 'Test completed:' "${INITIATOR_LOG}"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
        echo "### Transfer Engine cross-node test"
        echo
        echo "- Read/write result: passed"
        echo
        echo '```text'
        grep 'Test completed:' "${INITIATOR_LOG}"
        echo '```'
    } >>"${GITHUB_STEP_SUMMARY}"
fi
