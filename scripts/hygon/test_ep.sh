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
    # shellcheck disable=SC1091
    source /opt/dtk/env.sh
    set -u

    echo "Installing wheel ${wheel_path}..."
    python3 -m pip install "${wheel_path}"
    python3 -m pip show mooncake-transfer-engine
}

run_container_ep_test() {
    set +u
    # shellcheck disable=SC1091
    source /opt/dtk/env.sh
    set -u

    echo "Verifying mooncake.ep import..."
    python3 -c "import mooncake.ep; print('[EP CI] Successfully imported mooncake.ep')"

    echo "Checking DCU device availability..."
    python3 -c "import torch; print(f'[EP CI] torch.cuda.is_available(): {torch.cuda.is_available()}, device_count: {torch.cuda.device_count()}')"

    echo "Running EP multi-rank dispatch/combine test with Gloo..."
    export MOONCAKE_EP_USE_HYGON=1
    export HSA_FORCE_FINE_GRAIN_PCIE=1
    python3 /work/test_ep_grid.py TestMooncakeEPBuffer.test_ep_base_nofail_t256_h2048_e288_k8
    echo "[EP CI] EP test completed successfully."
}

case "${1:-}" in
    __container_prepare)
        prepare_container_runtime "$2"
        exit 0
        ;;
    __container_ep_test)
        run_container_ep_test
        exit 0
        ;;
esac

: "${TEST_IMAGE:?TEST_IMAGE is required}"
: "${DTK_PKG_URL:?DTK_PKG_URL is required}"
test "$#" -eq 1

for command_name in docker flock realpath; do
    if ! command -v "${command_name}" >/dev/null 2>&1; then
        echo "ERROR: required command is missing: ${command_name}" >&2
        exit 1
    fi
done

WHEEL_PATH="$(realpath "$1")"
SELF_PATH="$(realpath "${BASH_SOURCE[0]}")"
TEST_GRID_PY_PATH="$(realpath "$(dirname "${SELF_PATH}")/../../python/tests/ep/test_ep_grid.py")"

if [ ! -f "${WHEEL_PATH}" ]; then
    echo "ERROR: wheel not found: ${WHEEL_PATH}" >&2
    exit 1
fi
if [ ! -f "${TEST_GRID_PY_PATH}" ]; then
    echo "ERROR: test_ep_grid.py not found: ${TEST_GRID_PY_PATH}" >&2
    exit 1
fi

SETUP_TIMEOUT=600
RUN_TIMEOUT=300
LOCK_FILE=/tmp/mooncake-ci-cross-node.lock
LOCK_TIMEOUT=900
LOG_DIR="${RUNNER_TEMP}/ep-logs"
RUN_TOKEN="${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-$$"
EP_CONTAINER="mooncake-ci-ep-${RUN_TOKEN}"
WHEEL_BASENAME="$(basename "${WHEEL_PATH}")"
EP_LOG="${LOG_DIR}/ep.log"
EP_LOCK_LOG="${LOG_DIR}/ep-lock.log"

mkdir -p "${LOG_DIR}"
: >"${EP_LOG}"
: >"${EP_LOCK_LOG}"
TARGET_LOCK_PID=""

DOCKER_ENV_ARGS=(
    -e "DTK_PKG_URL=${DTK_PKG_URL}"
)
if [ -n "${PIP_INDEX_URL:-}" ]; then
    DOCKER_ENV_ARGS+=(-e "PIP_INDEX_URL=${PIP_INDEX_URL}")
fi

cleanup() {
    local rc=$?
    trap - EXIT
    set +e

    if docker inspect "${EP_CONTAINER}" >/dev/null 2>&1; then
        docker rm -f "${EP_CONTAINER}" >/dev/null 2>&1
    fi

    if [ -n "${TARGET_LOCK_PID}" ]; then
        kill "${TARGET_LOCK_PID}" >/dev/null 2>&1 || true
        wait "${TARGET_LOCK_PID}" >/dev/null 2>&1 || true
    fi

    if [ "${rc}" -ne 0 ]; then
        echo "===== EP lock log ====="
        cat "${EP_LOCK_LOG}" 2>/dev/null || true
        echo "===== EP test full log ====="
        cat "${EP_LOG}" 2>/dev/null || true
    fi
    exit "${rc}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_lock() {
    local log_file="$1"
    local process_id="$2"
    local deadline=$((SECONDS + LOCK_TIMEOUT))
    while [ "${SECONDS}" -lt "${deadline}" ]; do
        if grep -q "LOCK_ACQUIRED" "${log_file}" 2>/dev/null; then
            return 0
        fi
        if ! kill -0 "${process_id}" 2>/dev/null; then
            echo "ERROR: lock acquisition failed unexpectedly" >&2
            cat "${log_file}" >&2
            return 1
        fi
        sleep 1
    done
    echo "ERROR: timed out waiting for test lock" >&2
    return 1
}

echo "Acquiring test lock..."
PARENT_PID="$$"
(
    exec 9>"${LOCK_FILE}"
    if ! flock -w "${LOCK_TIMEOUT}" 9; then
        echo "ERROR: lock wait timed out"
        exit 75
    fi
    echo "LOCK_ACQUIRED"
    while kill -0 "${PARENT_PID}" 2>/dev/null; do sleep 5; done
) >"${EP_LOCK_LOG}" 2>&1 &
TARGET_LOCK_PID=$!
wait_for_lock "${EP_LOCK_LOG}" "${TARGET_LOCK_PID}"
echo "Test lock acquired."

echo "Starting EP container ${EP_CONTAINER}..."
docker run -d \
    --name "${EP_CONTAINER}" \
    --network host \
    --privileged \
    --volume "${WHEEL_PATH}:/work/${WHEEL_BASENAME}:ro" \
    --volume "${SELF_PATH}:/work/test_ep.sh:ro" \
    --volume "${TEST_GRID_PY_PATH}:/work/test_ep_grid.py:ro" \
    "${DOCKER_ENV_ARGS[@]}" \
    --entrypoint /bin/bash \
    "${TEST_IMAGE}" \
    -lc 'exec sleep infinity' >/dev/null

echo "Preparing DTK and wheel in container..."
timeout "${SETUP_TIMEOUT}" docker exec "${EP_CONTAINER}" \
    /bin/bash /work/test_ep.sh \
    __container_prepare "/work/${WHEEL_BASENAME}" \
    >"${EP_LOG}" 2>&1

echo "Running EP tests in container..."
timeout "${RUN_TIMEOUT}" docker exec "${EP_CONTAINER}" \
    /bin/bash /work/test_ep.sh \
    __container_ep_test \
    >>"${EP_LOG}" 2>&1

echo "Mooncake EP test completed successfully."
