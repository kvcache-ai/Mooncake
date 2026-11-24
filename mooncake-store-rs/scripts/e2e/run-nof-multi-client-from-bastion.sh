#!/usr/bin/env bash
set -euo pipefail

# Run on the bastion. The workflow is intentionally parameterized even though the
# default validation uses the configured target and client lists.
BUILD_HOST="${NOF_BUILD_HOST:-}"
BUILD_ROOT="${NOF_BUILD_ROOT:-}"
SPDK_PREFIX="${MOONCAKE_SPDK_PREFIX:-}"
INITIATORS_SPEC="${NOF_INITIATORS:-}"
INITIATOR_DIR="${NOF_INITIATOR_DIR:-/tmp/mooncake-nof-multi-client}"
SPDK_LIB_DIR="${NOF_SPDK_LIB_DIR:-}"
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
SUBMIT_CHUNK_BYTES="${NOF_SUBMIT_CHUNK_BYTES:-4194304}"
BARRIER_TIMEOUT_SECONDS="${NOF_BARRIER_TIMEOUT_SECONDS:-120}"
LEASE_TTL_MS="${NOF_LEASE_TTL_MS:-600000}"
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
KEYSPACE="${NOF_KEYSPACE:-mc/store-rs/nof-multi-client/${RUN_TAG}}"
BARRIER_RUN_ID="${NOF_BARRIER_RUN_ID:-${RUN_TAG}}"
STAGE_DIR="${NOF_STAGE_DIR:-/tmp/nof-multi-client-${RUN_TAG}}"
BINARY_NAME="nof_multi_client"
BUILD_TARGET_DIR="${NOF_BUILD_TARGET_DIR:-/tmp/mooncake-nof-build-${RUN_TAG}}"
BINARY_REMOTE_PATH="${BUILD_TARGET_DIR}/${BUILD_PROFILE}/${BINARY_NAME}"
BUILD_MANIFEST_REMOTE_PATH="${BINARY_REMOTE_PATH}.build-manifest"
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
[[ "$(basename -- "${STAGE_DIR}")" == nof-multi-client-* ]] || {
  echo "NOF_STAGE_DIR basename must start with nof-multi-client-" >&2
  exit 2
}
for command in rsync ssh; do
  command -v "${command}" >/dev/null || { echo "${command} is required" >&2; exit 1; }
done

IFS=',' read -r -a TARGETS <<<"${TARGETS_SPEC}"
[[ "${#TARGETS[@]}" -gt 0 ]] || { echo "NOF_TARGETS must not be empty" >&2; exit 1; }
for target in "${TARGETS[@]}"; do
  IFS='|' read -r public_ip lan_ip target_id subnqn port transport <<<"${target}"
  [[ -n "${public_ip}" && -n "${lan_ip}" && -n "${target_id}" && -n "${subnqn}" && -n "${port}" ]] || {
    echo "NOF_TARGETS entries must be public_ip|traddr|target_id|subnqn|port[|transport]: ${target}" >&2
    exit 1
  }
  transport="${transport:-tcp}"
  [[ "${transport,,}" == tcp || "${transport,,}" == rdma ]] || {
    echo "NOF_TARGETS transport must be tcp or rdma: ${target}" >&2
    exit 1
  }
done
IFS=';' read -r -a INITIATORS <<<"${INITIATORS_SPEC}"
[[ "${#INITIATORS[@]}" -gt 0 ]] || { echo "NOF_INITIATORS must not be empty" >&2; exit 1; }
declare -A expected_clients=()
declare -A assigned_clients=()
IFS=',' read -r -a CLIENT_IDS <<<"${CLIENT_IDS_SPEC}"
for client_id in "${CLIENT_IDS[@]}"; do
  [[ -n "${client_id}" && -z "${expected_clients[${client_id}]:-}" ]] || {
    echo "NOF_CLIENT_IDS contains an empty or duplicate client: ${client_id}" >&2
    exit 1
  }
  expected_clients["${client_id}"]=1
done
for initiator in "${INITIATORS[@]}"; do
  IFS='|' read -r initiator_host initiator_ip initiator_clients initiator_host_nqn <<<"${initiator}"
  [[ -n "${initiator_host}" && -n "${initiator_ip}" && -n "${initiator_clients}" ]] || {
    echo "NOF_INITIATORS entries must be host|bind_ip|client_ids[|host_nqn]: ${initiator}" >&2
    exit 1
  }
  IFS=',' read -r -a initiator_client_ids <<<"${initiator_clients}"
  for client_id in "${initiator_client_ids[@]}"; do
    [[ -n "${expected_clients[${client_id}]:-}" ]] || {
      echo "initiator ${initiator_host} contains unknown client ${client_id}" >&2
      exit 1
    }
    [[ -z "${assigned_clients[${client_id}]:-}" ]] || {
      echo "client ${client_id} is assigned to more than one initiator" >&2
      exit 1
    }
    assigned_clients["${client_id}"]=1
  done
done
for client_id in "${CLIENT_IDS[@]}"; do
  [[ -n "${assigned_clients[${client_id}]:-}" ]] || {
    echo "client ${client_id} is not assigned to an initiator" >&2
    exit 1
  }
done

rm -rf "${STAGE_DIR}"
mkdir -p "${STAGE_DIR}"

echo "building on ${BUILD_HOST}:${BUILD_ROOT}"
EXPECTED_COMMIT="$(ssh -o BatchMode=yes -o ConnectTimeout=8 "${BUILD_HOST}" \
  "cd '${BUILD_ROOT}' && git rev-parse HEAD")"
ssh -o BatchMode=yes -o ConnectTimeout=8 "${BUILD_HOST}" \
  "cd '${BUILD_ROOT}' && CARGO_TARGET_DIR='${BUILD_TARGET_DIR}' MOONCAKE_SPDK_PREFIX='${SPDK_PREFIX}' MOONCAKE_ENABLE_CUDA=0 NOF_BUILD_PROFILE='${BUILD_PROFILE}' NOF_RUN_UNIT_TESTS='${RUN_UNIT_TESTS}' NOF_EXPECTED_COMMIT='${EXPECTED_COMMIT}' scripts/e2e/build-nof-multi-client.sh"

if [[ "${RESET_TARGETS}" == 1 ]]; then
  [[ -n "${NVMF_SERVICE}" && -n "${TARGET_IMAGE}" ]] || {
    echo "NOF_NVMF_SERVICE and NOF_TARGET_IMAGE are required when NOF_RESET_TARGETS=1" >&2
    exit 1
  }
  declare -A reset_hosts=()
  for target in "${TARGETS[@]}"; do
    IFS='|' read -r public_ip _ <<<"${target}"
    [[ -z "${reset_hosts[${public_ip}]:-}" ]] || {
      echo "NOF_RESET_TARGETS supports one target per SSH host; provision multi-target hosts separately" >&2
      exit 1
    }
    reset_hosts["${public_ip}"]=1
  done
  echo "resetting provisioned NoF images"
  for target in "${TARGETS[@]}"; do
    IFS='|' read -r public_ip lan_ip target_id subnqn port transport <<<"${target}"
    ssh -o BatchMode=yes -o ConnectTimeout=8 "root@${public_ip}" \
      "set -euo pipefail; systemctl stop '${NVMF_SERVICE}'; mkdir -p -- \"\$(dirname '${TARGET_IMAGE}')\"; rm -f '${TARGET_IMAGE}'; truncate -s '${TEST_IMAGE_BYTES}' '${TARGET_IMAGE}'; systemctl start '${NVMF_SERVICE}'; test \"\$(systemctl is-active '${NVMF_SERVICE}')\" = active"
  done
fi

echo "staging binary and runner via rsync"
rsync -a -e 'ssh -o BatchMode=yes' \
  "${BUILD_HOST}:${BINARY_REMOTE_PATH}" \
  "${STAGE_DIR}/nof_multi_client"
rsync -a -e 'ssh -o BatchMode=yes' \
  "${BUILD_HOST}:${BUILD_MANIFEST_REMOTE_PATH}" \
  "${STAGE_DIR}/build-manifest.txt"
rsync -a -e 'ssh -o BatchMode=yes' \
  "${BUILD_HOST}:${BUILD_ROOT}/scripts/e2e/run-nof-multi-client.sh" \
  "${STAGE_DIR}/run-nof-multi-client.sh"
chmod 755 "${STAGE_DIR}/nof_multi_client" "${STAGE_DIR}/run-nof-multi-client.sh"
sha256sum "${STAGE_DIR}/nof_multi_client" | tee "${STAGE_DIR}/binary.sha256"
local_hash="$(awk '{print $1}' "${STAGE_DIR}/binary.sha256")"
BUILD_COMMIT="$(awk -F= '$1 == "build_commit" { print $2 }' "${STAGE_DIR}/build-manifest.txt")"
manifest_hash="$(awk -F= '$1 == "binary_sha256" { print $2 }' "${STAGE_DIR}/build-manifest.txt")"
[[ "${BUILD_COMMIT}" == "${EXPECTED_COMMIT}" && "${manifest_hash}" == "${local_hash}" ]] || {
  echo "staged artifact does not match its build manifest" >&2
  exit 1
}
pids=()
for initiator in "${INITIATORS[@]}"; do
  IFS='|' read -r initiator_host initiator_ip initiator_clients initiator_host_nqn <<<"${initiator}"
  if [[ -z "${initiator_host_nqn}" ]]; then
    initiator_host_nqn="nqn.2026-09.io.mooncake:${initiator_clients%%,*}"
  fi
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
   NOF_KEYSPACE='${KEYSPACE}' \
   NOF_BARRIER_RUN_ID='${BARRIER_RUN_ID}' \
   NOF_BARRIER_REDIS_URL='${BARRIER_REDIS_URL}' \
   NOF_BIND_IP='${initiator_ip}' \
   NOF_HOST_NQN='${initiator_host_nqn}' \
   NOF_BUILD_COMMIT='${BUILD_COMMIT}' \
   NOF_TARGETS='${TARGETS_SPEC}' \
   NOF_CLIENT_IDS='${CLIENT_IDS_SPEC}' \
   NOF_LOCAL_CLIENT_IDS='${initiator_clients}' \
   NOF_SKIP_TARGET_SSH_CHECK=1 \
   NOF_CLIENT_TIMEOUT_SECONDS='${TIMEOUT_SECONDS}' \
   NOF_ENABLE_CORES='${ENABLE_CORES}' \
   NOF_OBJECTS_PER_CLIENT='${OBJECTS_PER_CLIENT}' \
   NOF_VALUE_BYTES='${VALUE_BYTES}' \
   NOF_BATCH_SIZE='${BATCH_SIZE}' \
   NOF_REPLICA_COUNT='${REPLICA_COUNT}' \
   NOF_NVMF_SERVICE='${NVMF_SERVICE}' \
   NOF_DEVICE_BYTES='${DEVICE_BYTES}' \
   NOF_SUBMIT_CHUNK_BYTES='${SUBMIT_CHUNK_BYTES}' \
   NOF_BARRIER_TIMEOUT_SECONDS='${BARRIER_TIMEOUT_SECONDS}' \
   NOF_LEASE_TTL_MS='${LEASE_TTL_MS}' \
   NOF_READ_RETRIES='${READ_RETRIES}' \
   NOF_READ_RETRY_DELAY_MS='${READ_RETRY_DELAY_MS}' \
   NOF_TEST_PREFIX='${TEST_PREFIX}' \
   NOF_STARTUP_SETTLE_SECONDS='${STARTUP_SETTLE_SECONDS}' \
   NOF_ROUTE_CONTROL='${ROUTE_CONTROL}' \
   NOF_EXPECT_NOF_COPIES='${NOF_EXPECT_NOF_COPIES:-}' \
   NOF_EXPECT_POST_WAIT_NOF_COPIES='${NOF_EXPECT_POST_WAIT_NOF_COPIES:-}' \
   NOF_EXPECT_POST_WAIT_TOTAL_NOF_COPIES='${NOF_EXPECT_POST_WAIT_TOTAL_NOF_COPIES:-}' \
   NOF_EXPECT_MAX_TARGET_COPY_SKEW='${NOF_EXPECT_MAX_TARGET_COPY_SKEW:-}' \
   NOF_EXPECT_ABSENT_TARGETS='${NOF_EXPECT_ABSENT_TARGETS:-}' \
   NOF_EXPECT_MISSING_ROUTES='${NOF_EXPECT_MISSING_ROUTES:-false}' \
   NOF_EXPECT_POST_WAIT_MISSING_ROUTES='${NOF_EXPECT_POST_WAIT_MISSING_ROUTES:-false}' \
   NOF_EXIT_AFTER_POST_WAIT='${NOF_EXIT_AFTER_POST_WAIT:-false}' \
   NOF_POST_OFFLOAD_WAIT_SECONDS='${NOF_POST_OFFLOAD_WAIT_SECONDS:-0}' \
   NOF_REONLINE_WAIT_SECONDS='${NOF_REONLINE_WAIT_SECONDS:-0}' \
   NOF_WATERMARK_HIGH_BYTES='${NOF_WATERMARK_HIGH_BYTES:-}' \
   NOF_WATERMARK_LOW_BYTES='${NOF_WATERMARK_LOW_BYTES:-}' \
   NOF_READ_ONLY='${NOF_READ_ONLY:-false}' \
   NOF_DELETE_AND_REWRITE='${NOF_DELETE_AND_REWRITE:-false}' \
   NOF_HANDOFF_DEPARTING_CLIENT='${NOF_HANDOFF_DEPARTING_CLIENT:-}' \
   NOF_HANDOFF_ABRUPT_EXIT='${NOF_HANDOFF_ABRUPT_EXIT:-false}' \
   NOF_HANDOFF_WAIT_SECONDS='${NOF_HANDOFF_WAIT_SECONDS:-5}' \
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
