#!/usr/bin/env bash
set -euo pipefail

# Run on the bastion. The workflow is intentionally parameterized even though the
# default validation uses the configured target and client lists.
BUILD_HOST="${NOF_BUILD_HOST:-root@10.88.0.5}"
BUILD_ROOT="${NOF_BUILD_ROOT:-/nvme/cruz.zxp/Mooncake-Store-RS-nof-pr2-integrated}"
SPDK_PREFIX="${MOONCAKE_SPDK_PREFIX:-/nvme/cruz.zxp/spdk-26.05-host}"
INITIATOR_HOST="${NOF_INITIATOR_HOST:-root@182.92.21.56}"
INITIATOR_IP="${NOF_BIND_IP:-192.168.22.80}"
INITIATOR_DIR="${NOF_INITIATOR_DIR:-/tmp/mooncake-nof-multi-client}"
SPDK_LIB_DIR="${NOF_SPDK_LIB_DIR:-/opt/spdk-26.05/install/lib}"
REDIS_URL="${NOF_REDIS_URL:-redis://192.168.22.78:6382/0}"
TIMEOUT_SECONDS="${NOF_CLIENT_TIMEOUT_SECONDS:-300}"
ENABLE_CORES="${NOF_ENABLE_CORES:-0}"
STARTUP_SETTLE_SECONDS="${NOF_STARTUP_SETTLE_SECONDS:-2}"
TEST_IMAGE_BYTES="${NOF_TEST_IMAGE_BYTES:-16G}"
OBJECTS_PER_CLIENT="${NOF_OBJECTS_PER_CLIENT:-16}"
VALUE_BYTES="${NOF_VALUE_BYTES:-65536}"
BATCH_SIZE="${NOF_BATCH_SIZE:-32}"
REPLICA_COUNT="${NOF_REPLICA_COUNT:-2}"
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
CLIENT_IDS_SPEC="${NOF_CLIENT_IDS:-client-0,client-1,client-2,client-3}"
TARGETS_SPEC="${NOF_TARGETS:-8.141.27.180|192.168.22.78|nof-78|nqn.2026-09.io.mooncake:nof-78|4420,182.92.21.56|192.168.22.80|nof-80|nqn.2026-09.io.mooncake:nof-80|4420,47.93.122.112|192.168.22.81|nof-81|nqn.2026-09.io.mooncake:nof-81|4420,59.110.29.176|192.168.22.82|nof-82|nqn.2026-09.io.mooncake:nof-82|4420}"
BUILD_PROFILE="${NOF_BUILD_PROFILE:-debug}"
RUN_UNIT_TESTS="${NOF_RUN_UNIT_TESTS:-0}"
RUN_TAG="${NOF_RUN_TAG:-$(date +%Y%m%d-%H%M%S)}"
STAGE_DIR="${NOF_STAGE_DIR:-/tmp/nof-multi-client-${RUN_TAG}}"
BINARY_NAME="nof_multi_client"
BINARY_RELATIVE_PATH="target/${BUILD_PROFILE}/${BINARY_NAME}"
TARGETS=()

case "${BUILD_PROFILE}" in
  debug|release) ;;
  *) echo "NOF_BUILD_PROFILE must be debug or release, got ${BUILD_PROFILE}" >&2; exit 2 ;;
esac
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

rm -rf "${STAGE_DIR}"
mkdir -p "${STAGE_DIR}"

echo "building on ${BUILD_HOST}:${BUILD_ROOT}"
ssh -o BatchMode=yes -o ConnectTimeout=8 "${BUILD_HOST}" \
  "cd '${BUILD_ROOT}' && MOONCAKE_SPDK_PREFIX='${SPDK_PREFIX}' MOONCAKE_ENABLE_CUDA=0 NOF_BUILD_PROFILE='${BUILD_PROFILE}' NOF_RUN_UNIT_TESTS='${RUN_UNIT_TESTS}' scripts/e2e/build-nof-multi-client.sh"

echo "resetting provisioned NoF images"
for target in "${TARGETS[@]}"; do
  IFS='|' read -r public_ip lan_ip target_id subnqn port <<<"${target}"
  ssh -o BatchMode=yes -o ConnectTimeout=8 "root@${public_ip}" \
    'set -euo pipefail; systemctl stop mooncake-nvmf.service; rm -f /var/lib/mooncake-nof/nof.img; truncate -s '${TEST_IMAGE_BYTES}' /var/lib/mooncake-nof/nof.img; systemctl start mooncake-nvmf.service; test "$(systemctl is-active mooncake-nvmf.service)" = active'
done

echo "staging binary and runner via rsync"
rsync -a -e 'ssh -o BatchMode=yes' \
  "${BUILD_HOST}:${BUILD_ROOT}/${BINARY_RELATIVE_PATH}" \
  "${STAGE_DIR}/nof_multi_client"
rsync -a -e 'ssh -o BatchMode=yes' \
  "${BUILD_HOST}:${BUILD_ROOT}/scripts/e2e/run-nof-multi-client.sh" \
  "${STAGE_DIR}/run-nof-multi-client.sh"
chmod 755 "${STAGE_DIR}/nof_multi_client" "${STAGE_DIR}/run-nof-multi-client.sh"
sha256sum "${STAGE_DIR}/nof_multi_client" | tee "${STAGE_DIR}/binary.sha256"
ssh -o BatchMode=yes -o ConnectTimeout=8 "${INITIATOR_HOST}" "mkdir -p -- '${INITIATOR_DIR}'"
rsync -a -e 'ssh -o BatchMode=yes' "${STAGE_DIR}/" "${INITIATOR_HOST}:${INITIATOR_DIR}/"
local_hash="$(awk '{print $1}' "${STAGE_DIR}/binary.sha256")"
remote_hash="$(ssh -o BatchMode=yes -o ConnectTimeout=8 "${INITIATOR_HOST}" \
  "sha256sum '${INITIATOR_DIR}/nof_multi_client' | awk '{print \$1}'")"
[[ "${local_hash}" == "${remote_hash}" ]] || {
  echo "staged binary hash mismatch: local=${local_hash} remote=${remote_hash}" >&2
  exit 1
}

echo "running on ${INITIATOR_HOST} (${INITIATOR_IP})"
ssh -o BatchMode=yes -o ConnectTimeout=8 "${INITIATOR_HOST}" \
  "NOF_BINARY='${INITIATOR_DIR}/nof_multi_client' \
   NOF_LOG_DIR='${INITIATOR_DIR}/logs' \
   NOF_SPDK_LIB_DIR='${SPDK_LIB_DIR}' \
   NOF_REDIS_URL='${REDIS_URL}' \
   NOF_BIND_IP='${INITIATOR_IP}' \
   NOF_TARGETS='${TARGETS_SPEC}' \
   NOF_CLIENT_IDS='${CLIENT_IDS_SPEC}' \
   NOF_RESET_TARGETS=0 \
   NOF_SKIP_TARGET_SSH_CHECK=1 \
   NOF_CLIENT_TIMEOUT_SECONDS='${TIMEOUT_SECONDS}' \
   NOF_ENABLE_CORES='${ENABLE_CORES}' \
   NOF_OBJECTS_PER_CLIENT='${OBJECTS_PER_CLIENT}' \
   NOF_VALUE_BYTES='${VALUE_BYTES}' \
   NOF_BATCH_SIZE='${BATCH_SIZE}' \
   NOF_REPLICA_COUNT='${REPLICA_COUNT}' \
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
   '${INITIATOR_DIR}/run-nof-multi-client.sh'"

echo "multi-client NoF run completed; stage=${STAGE_DIR} initiator=${INITIATOR_HOST}:${INITIATOR_DIR}"
