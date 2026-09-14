#!/usr/bin/env bash
set -euo pipefail

# Run on the bastion. The workflow is intentionally parameterized even though the
# default validation uses the configured target and client lists.
BUILD_HOST="${NOF_BUILD_HOST:-}"
BUILD_ROOT="${NOF_BUILD_ROOT:-}"
SPDK_PREFIX="${MOONCAKE_SPDK_PREFIX:-}"
INITIATORS_SPEC="${NOF_INITIATORS:-}"
INITIATOR_DIR="${NOF_INITIATOR_DIR:-/tmp/mooncake-nof-multi-client}"
SPDK_LIB_DIR="${NOF_SPDK_LIB_DIR:-${MOONCAKE_SPDK_PREFIX:+${MOONCAKE_SPDK_PREFIX}/install/lib}}"
REDIS_URL="${NOF_REDIS_URL:-}"
TIMEOUT_SECONDS="${NOF_CLIENT_TIMEOUT_SECONDS:-300}"
ENABLE_CORES="${NOF_ENABLE_CORES:-0}"
STARTUP_SETTLE_SECONDS="${NOF_STARTUP_SETTLE_SECONDS:-2}"
TEST_IMAGE_BYTES="${NOF_TEST_IMAGE_BYTES:-16G}"
OBJECTS_PER_CLIENT="${NOF_OBJECTS_PER_CLIENT:-16}"
VALUE_BYTES="${NOF_VALUE_BYTES:-65536}"
BATCH_SIZE="${NOF_BATCH_SIZE:-32}"
REPLICA_COUNT="${NOF_REPLICA_COUNT:-1}"
RESET_TARGETS="${NOF_RESET_TARGETS:-0}"
NVMF_SERVICE="${NOF_NVMF_SERVICE:-}"
TARGET_IMAGE="${NOF_TARGET_IMAGE:-}"
DEVICE_BYTES="${NOF_DEVICE_BYTES:-17179869184}"
TARGET_WORKERS="${NOF_TARGET_WORKERS:-2}"
QUEUE_DEPTH="${NOF_QUEUE_DEPTH:-64}"
SUBMIT_CHUNK_BYTES="${NOF_SUBMIT_CHUNK_BYTES:-4194304}"
INFLIGHT_BYTES="${NOF_INFLIGHT_BYTES:-67108864}"
BARRIER_TIMEOUT_SECONDS="${NOF_BARRIER_TIMEOUT_SECONDS:-120}"
READ_RETRIES="${NOF_READ_RETRIES:-20}"
READ_RETRY_DELAY_MS="${NOF_READ_RETRY_DELAY_MS:-250}"
TEST_PREFIX="${NOF_TEST_PREFIX:-nof-multi-client}"
ROUTE_CONTROL="${NOF_ROUTE_CONTROL:-EmbeddedWrh}"
CLIENT_IDS_SPEC="${NOF_CLIENT_IDS:-}"
TARGETS_SPEC="${NOF_TARGETS:-}"
BARRIER_REDIS_URL="${NOF_BARRIER_REDIS_URL:-${REDIS_URL}}"
BUILD_PROFILE="${NOF_BUILD_PROFILE:-debug}"
RUN_UNIT_TESTS="${NOF_RUN_UNIT_TESTS:-0}"
RUN_TAG="${NOF_RUN_TAG:-$(date +%Y%m%d-%H%M%S)}"
STAGE_DIR="${NOF_STAGE_DIR:-/tmp/nof-multi-client-${RUN_TAG}}"
BINARY_NAME="nof_multi_client"
BUILD_TARGET_DIR="${NOF_BUILD_TARGET_DIR:-/tmp/mooncake-nof-build-${RUN_TAG}}"
BINARY_REMOTE_PATH="${BUILD_TARGET_DIR}/${BUILD_PROFILE}/${BINARY_NAME}"
TARGETS=()

require_env() {
  local name="$1"
  local value="$2"
  [[ -n "${value}" ]] || {
    echo "${name} is required" >&2
    exit 1
  }
}

case "${BUILD_PROFILE}" in
  debug|release) ;;
  *) echo "NOF_BUILD_PROFILE must be debug or release, got ${BUILD_PROFILE}" >&2; exit 2 ;;
esac
require_env NOF_BUILD_HOST "${BUILD_HOST}"
require_env NOF_BUILD_ROOT "${BUILD_ROOT}"
require_env NOF_TARGETS "${TARGETS_SPEC}"
require_env NOF_CLIENT_IDS "${CLIENT_IDS_SPEC}"
require_env NOF_INITIATORS "${INITIATORS_SPEC}"
require_env NOF_REDIS_URL "${REDIS_URL}"
require_env MOONCAKE_SPDK_PREFIX "${SPDK_PREFIX}"
require_env NOF_SPDK_LIB_DIR "${SPDK_LIB_DIR}"
for command in rsync ssh; do
  command -v "${command}" >/dev/null || { echo "${command} is required" >&2; exit 1; }
done

IFS=',' read -r -a TARGETS <<<"${TARGETS_SPEC}"
[[ "${#TARGETS[@]}" -gt 0 ]] || { echo "NOF_TARGETS must not be empty" >&2; exit 1; }
for target in "${TARGETS[@]}"; do
  IFS='|' read -r public_ip lan_ip target_id subnqn port <<<"${target}"
  [[ -n "${public_ip}" && -n "${lan_ip}" && -n "${target_id}" && -n "${subnqn}" && -n "${port}" ]] || {
    echo "NOF_TARGETS entries must be public_ip|traddr|target_id|subnqn|port: ${target}" >&2
    exit 1
  }
done
IFS=';' read -r -a INITIATORS <<<"${INITIATORS_SPEC}"
[[ "${#INITIATORS[@]}" -gt 0 ]] || { echo "NOF_INITIATORS must not be empty" >&2; exit 1; }
for initiator in "${INITIATORS[@]}"; do
  IFS='|' read -r initiator_host initiator_ip initiator_clients <<<"${initiator}"
  [[ -n "${initiator_host}" && -n "${initiator_ip}" && -n "${initiator_clients}" ]] || {
    echo "NOF_INITIATORS entries must be host|bind_ip|client_ids: ${initiator}" >&2
    exit 1
  }
done

rm -rf "${STAGE_DIR}"
mkdir -p "${STAGE_DIR}"

echo "building on ${BUILD_HOST}:${BUILD_ROOT}"
ssh -o BatchMode=yes -o ConnectTimeout=8 "${BUILD_HOST}" \
  "cd '${BUILD_ROOT}' && CARGO_TARGET_DIR='${BUILD_TARGET_DIR}' MOONCAKE_SPDK_PREFIX='${SPDK_PREFIX}' MOONCAKE_ENABLE_CUDA=0 NOF_BUILD_PROFILE='${BUILD_PROFILE}' NOF_RUN_UNIT_TESTS='${RUN_UNIT_TESTS}' scripts/e2e/build-nof-multi-client.sh"

if [[ "${RESET_TARGETS}" == 1 ]]; then
  [[ -n "${NVMF_SERVICE}" && -n "${TARGET_IMAGE}" ]] || {
    echo "NOF_NVMF_SERVICE and NOF_TARGET_IMAGE are required when NOF_RESET_TARGETS=1" >&2
    exit 1
  }
  echo "resetting provisioned NoF images"
  for target in "${TARGETS[@]}"; do
    IFS='|' read -r public_ip lan_ip target_id subnqn port <<<"${target}"
    ssh -o BatchMode=yes -o ConnectTimeout=8 "root@${public_ip}" \
      "set -euo pipefail; systemctl stop '${NVMF_SERVICE}'; mkdir -p -- \"\$(dirname '${TARGET_IMAGE}')\"; rm -f '${TARGET_IMAGE}'; truncate -s '${TEST_IMAGE_BYTES}' '${TARGET_IMAGE}'; systemctl start '${NVMF_SERVICE}'; test \"\$(systemctl is-active '${NVMF_SERVICE}')\" = active"
  done
fi

echo "staging binary and runner via rsync"
rsync -a -e 'ssh -o BatchMode=yes' \
  "${BUILD_HOST}:${BINARY_REMOTE_PATH}" \
  "${STAGE_DIR}/nof_multi_client"
rsync -a -e 'ssh -o BatchMode=yes' \
  "${BUILD_HOST}:${BUILD_ROOT}/scripts/e2e/run-nof-multi-client.sh" \
  "${STAGE_DIR}/run-nof-multi-client.sh"
chmod 755 "${STAGE_DIR}/nof_multi_client" "${STAGE_DIR}/run-nof-multi-client.sh"
sha256sum "${STAGE_DIR}/nof_multi_client" | tee "${STAGE_DIR}/binary.sha256"
local_hash="$(awk '{print $1}' "${STAGE_DIR}/binary.sha256")"
pids=()
for initiator in "${INITIATORS[@]}"; do
  IFS='|' read -r initiator_host initiator_ip initiator_clients <<<"${initiator}"
  echo "staging to ${initiator_host} (${initiator_ip}); local_clients=${initiator_clients}"
  ssh -o BatchMode=yes -o ConnectTimeout=8 "${initiator_host}" "mkdir -p -- '${INITIATOR_DIR}'"
  rsync -a -e 'ssh -o BatchMode=yes' "${STAGE_DIR}/" "${initiator_host}:${INITIATOR_DIR}/"
  remote_hash="$(ssh -o BatchMode=yes -o ConnectTimeout=8 "${initiator_host}" \
    "sha256sum '${INITIATOR_DIR}/nof_multi_client' | awk '{print \$1}'")"
  [[ "${local_hash}" == "${remote_hash}" ]] || {
    echo "staged binary hash mismatch on ${initiator_host}: local=${local_hash} remote=${remote_hash}" >&2
    exit 1
  }

  echo "running on ${initiator_host} (${initiator_ip})"
  ssh -o BatchMode=yes -o ConnectTimeout=8 "${initiator_host}" \
    "NOF_BINARY='${INITIATOR_DIR}/nof_multi_client' \
   NOF_LOG_DIR='${INITIATOR_DIR}/logs' \
   NOF_SPDK_LIB_DIR='${SPDK_LIB_DIR}' \
   NOF_REDIS_URL='${REDIS_URL}' \
   NOF_BARRIER_REDIS_URL='${BARRIER_REDIS_URL}' \
   NOF_BIND_IP='${initiator_ip}' \
   NOF_TARGETS='${TARGETS_SPEC}' \
   NOF_CLIENT_IDS='${CLIENT_IDS_SPEC}' \
   NOF_LOCAL_CLIENT_IDS='${initiator_clients}' \
   NOF_RESET_TARGETS=0 \
   NOF_SKIP_TARGET_SSH_CHECK=1 \
   NOF_CLIENT_TIMEOUT_SECONDS='${TIMEOUT_SECONDS}' \
   NOF_ENABLE_CORES='${ENABLE_CORES}' \
   NOF_OBJECTS_PER_CLIENT='${OBJECTS_PER_CLIENT}' \
   NOF_VALUE_BYTES='${VALUE_BYTES}' \
   NOF_BATCH_SIZE='${BATCH_SIZE}' \
   NOF_REPLICA_COUNT='${REPLICA_COUNT}' \
   NOF_NVMF_SERVICE='${NVMF_SERVICE}' \
   NOF_TARGET_IMAGE='${TARGET_IMAGE}' \
   NOF_DEVICE_BYTES='${DEVICE_BYTES}' \
   NOF_TARGET_WORKERS='${TARGET_WORKERS}' \
   NOF_QUEUE_DEPTH='${QUEUE_DEPTH}' \
   NOF_SUBMIT_CHUNK_BYTES='${SUBMIT_CHUNK_BYTES}' \
   NOF_INFLIGHT_BYTES='${INFLIGHT_BYTES}' \
   NOF_BARRIER_TIMEOUT_SECONDS='${BARRIER_TIMEOUT_SECONDS}' \
   NOF_READ_RETRIES='${READ_RETRIES}' \
   NOF_READ_RETRY_DELAY_MS='${READ_RETRY_DELAY_MS}' \
   NOF_TEST_PREFIX='${TEST_PREFIX}' \
   NOF_STARTUP_SETTLE_SECONDS='${STARTUP_SETTLE_SECONDS}' \
   NOF_ROUTE_CONTROL='${ROUTE_CONTROL}' \
   NOF_RUN_TAG='${RUN_TAG}' \
   '${INITIATOR_DIR}/run-nof-multi-client.sh'" &
  pids+=("$!")
done

status=0
for pid in "${pids[@]}"; do
  if ! wait "${pid}"; then
    status=1
  fi
done
if [[ "${status}" != 0 ]]; then
  echo "multi-initiator NoF run failed; stage=${STAGE_DIR}" >&2
  exit "${status}"
fi

echo "multi-client NoF run completed; stage=${STAGE_DIR} initiators=${INITIATORS_SPEC}"
