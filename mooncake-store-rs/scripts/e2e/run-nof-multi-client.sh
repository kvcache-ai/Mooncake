#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BINARY="${NOF_BINARY:-${ROOT_DIR}/target/debug/nof_multi_client}"
LOG_DIR="${NOF_LOG_DIR:-${ROOT_DIR}/target/nof-multi-client}"
RUN_TAG="${NOF_RUN_TAG:-$(date +%Y%m%d-%H%M%S)}"
KEYSPACE="${NOF_KEYSPACE:-mc/store-rs/nof-multi-client/${RUN_TAG}}"
BARRIER_RUN_ID="${NOF_BARRIER_RUN_ID:-${RUN_TAG}}"
REDIS_URL="${NOF_REDIS_URL:-redis://127.0.0.1:6379/0}"
BIND_IP="${NOF_BIND_IP:-127.0.0.1}"
HOST_NQN="${NOF_HOST_NQN:-}"
BUILD_COMMIT="${NOF_BUILD_COMMIT:-unknown}"
SPDK_LIB_DIR="${NOF_SPDK_LIB_DIR:-${MOONCAKE_SPDK_PREFIX:+${MOONCAKE_SPDK_PREFIX}/install/lib}}"
SKIP_TARGET_SSH_CHECK="${NOF_SKIP_TARGET_SSH_CHECK:-1}"
NVMF_SERVICE="${NOF_NVMF_SERVICE:-}"
TIMEOUT_SECONDS="${NOF_CLIENT_TIMEOUT_SECONDS:-180}"
ENABLE_CORES="${NOF_ENABLE_CORES:-0}"
STARTUP_SETTLE_SECONDS="${NOF_STARTUP_SETTLE_SECONDS:-2}"
ROUTE_CONTROL="${NOF_ROUTE_CONTROL:-EmbeddedWrh}"
RPC_BASE_PORT="${NOF_RPC_BASE_PORT:-21000}"
CLIENT_IDS_SPEC="${NOF_CLIENT_IDS:-client-0}"
LOCAL_CLIENT_IDS_SPEC="${NOF_LOCAL_CLIENT_IDS:-${CLIENT_IDS_SPEC}}"
TARGETS_SPEC="${NOF_TARGETS:-}"
HANDOFF_DEPARTING_CLIENT="${NOF_HANDOFF_DEPARTING_CLIENT:-}"
HANDOFF_ABRUPT_EXIT="${NOF_HANDOFF_ABRUPT_EXIT:-false}"

[[ -x "${BINARY}" ]] || { echo "NOF_BINARY is not executable: ${BINARY}" >&2; exit 1; }
[[ -n "${SPDK_LIB_DIR}" ]] || { echo "NOF_SPDK_LIB_DIR is required, or set MOONCAKE_SPDK_PREFIX" >&2; exit 1; }
[[ -d "${SPDK_LIB_DIR}" ]] || { echo "NOF_SPDK_LIB_DIR does not exist: ${SPDK_LIB_DIR}" >&2; exit 1; }
[[ -n "${TARGETS_SPEC}" ]] || { echo "NOF_TARGETS is required" >&2; exit 1; }
[[ -n "${HOST_NQN}" ]] || { echo "NOF_HOST_NQN is required" >&2; exit 1; }
for command in ssh ldd sha256sum timeout; do
  command -v "${command}" >/dev/null || { echo "${command} is required" >&2; exit 1; }
done
if [[ "${ENABLE_CORES}" == 1 ]]; then
  ulimit -c unlimited
fi

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
IFS=',' read -r -a clients <<<"${CLIENT_IDS_SPEC}"
[[ "${#clients[@]}" -gt 0 && -n "${clients[0]}" ]] || { echo "NOF_CLIENT_IDS must not be empty" >&2; exit 1; }
IFS=',' read -r -a local_clients <<<"${LOCAL_CLIENT_IDS_SPEC}"
[[ "${#local_clients[@]}" -gt 0 && -n "${local_clients[0]}" ]] || { echo "NOF_LOCAL_CLIENT_IDS must not be empty" >&2; exit 1; }

RUNTIME_LIB_DIR="${NOF_RUNTIME_LIB_DIR:-${TMPDIR:-/tmp}/nof-multi-client-runtime}"
mkdir -p "${RUNTIME_LIB_DIR}"
if ! ldconfig -p 2>/dev/null | grep -q 'libaio.so.1 ('; then
  libaio_compat="$(ldconfig -p 2>/dev/null | awk '$1 == "libaio.so.1t64" { print $NF; exit }')"
  if [[ -n "${libaio_compat}" ]]; then
    ln -sfn "${libaio_compat}" "${RUNTIME_LIB_DIR}/libaio.so.1"
  fi
fi

check_target_endpoint() {
  local lan_ip="$1"
  local port="$2"
  local transport="$3"
  if [[ "${transport,,}" == tcp ]]; then
    timeout 2 bash -c "</dev/tcp/${lan_ip}/${port}"
  fi
}

for target in "${TARGETS[@]}"; do
  IFS='|' read -r public_ip lan_ip target_id subnqn port transport <<<"${target}"
  transport="${transport:-tcp}"
  if [[ "${SKIP_TARGET_SSH_CHECK}" != 1 ]]; then
    [[ -n "${NVMF_SERVICE}" ]] || {
      echo "NOF_NVMF_SERVICE is required when NOF_SKIP_TARGET_SSH_CHECK=0" >&2
      exit 1
    }
    ssh -o BatchMode=yes -o ConnectTimeout=8 "root@${public_ip}" \
      "test \"\$(systemctl is-active '${NVMF_SERVICE}')\" = active"
  else
    check_target_endpoint "${lan_ip}" "${port}" "${transport}"
  fi
done

redis_ping() {
  local url="$1"
  if command -v redis-cli >/dev/null; then
    redis-cli -u "${url}" ping >/dev/null
    return
  fi
  command -v python3 >/dev/null || { echo "redis-cli or python3 is required to validate REDIS_URL" >&2; exit 1; }
  python3 - "${url}" <<'PY2'
import socket
import sys
from urllib.parse import urlparse

parsed = urlparse(sys.argv[1])
if parsed.scheme != "redis" or not parsed.hostname:
    raise SystemExit(f"unsupported Redis URL: {sys.argv[1]}")
port = parsed.port or 6379
with socket.create_connection((parsed.hostname, port), timeout=3) as sock:
    sock.sendall(b"*1\r\n$4\r\nPING\r\n")
    response = sock.recv(64)
if not response.startswith(b"+PONG"):
    raise SystemExit(f"Redis PING failed: {response!r}")
PY2
}

if [[ "${REDIS_URL}" == redis://* ]]; then
  redis_ping "${REDIS_URL}"
fi

mkdir -p "${LOG_DIR}/${RUN_TAG}"
export LD_LIBRARY_PATH="${RUNTIME_LIB_DIR}:${SPDK_LIB_DIR}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
RUNTIME_DEPS="$(LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" ldd -r "${BINARY}" 2>&1 || true)"
if grep -Eq 'not found|undefined symbol:' <<<"${RUNTIME_DEPS}"; then
  echo "NoF runtime dependencies are unresolved on the initiator" >&2
  printf '%s\n' "${RUNTIME_DEPS}" >&2
  exit 1
fi
{
  printf 'binary=%s\n' "${BINARY}"
  sha256sum "${BINARY}"
  printf 'build_commit=%s\n' "${BUILD_COMMIT}"
  printf 'run_tag=%s\nkeyspace=%s\nbind_ip=%s\nredis_url=%s\n' \
    "${RUN_TAG}" "${KEYSPACE}" "${BIND_IP}" "${REDIS_URL}"
  printf 'host_nqn=%s\n' "${HOST_NQN}"
  printf 'target_inventory=%s\nclient_ids=%s\nlocal_client_ids=%s\nroute_control=%s\n' \
    "${TARGETS_SPEC}" "${CLIENT_IDS_SPEC}" "${LOCAL_CLIENT_IDS_SPEC}" "${ROUTE_CONTROL}"
  printf 'nvmf_service=%s\n' "${NVMF_SERVICE}"
  printf 'startup_settle_seconds=%s read_protocol=parallel_batch_get_into\n' \
    "${STARTUP_SETTLE_SECONDS}"
  printf 'hostname=%s\n' "$(hostname)"
  printf 'kernel=%s\n' "$(uname -srmo)"
  printf 'spdk_lib_dir=%s\n' "${SPDK_LIB_DIR}"
  printf 'runtime_dependencies=resolved\n'
  for name in \
    NOF_DEVICE_BYTES NOF_SUBMIT_CHUNK_BYTES NOF_OBJECTS_PER_CLIENT NOF_VALUE_BYTES \
    NOF_BATCH_SIZE NOF_REPLICA_COUNT NOF_WATERMARK_HIGH_BYTES NOF_WATERMARK_LOW_BYTES \
    NOF_POST_OFFLOAD_WAIT_SECONDS NOF_REONLINE_WAIT_SECONDS NOF_EXPECT_NOF_COPIES \
    NOF_EXPECT_POST_WAIT_NOF_COPIES NOF_EXPECT_POST_WAIT_TOTAL_NOF_COPIES \
    NOF_EXPECT_MAX_TARGET_COPY_SKEW NOF_EXPECT_ABSENT_TARGETS NOF_EXPECT_MISSING_ROUTES \
    NOF_EXPECT_POST_WAIT_MISSING_ROUTES NOF_EXIT_AFTER_POST_WAIT NOF_READ_ONLY \
    NOF_DELETE_AND_REWRITE NOF_LEASE_TTL_MS NOF_HANDOFF_DEPARTING_CLIENT \
    NOF_HANDOFF_ABRUPT_EXIT NOF_HANDOFF_WAIT_SECONDS; do
    printf '%s=%s\n' "${name}" "${!name:-}"
  done
  if command -v ip >/dev/null; then
    ip -br link | sed 's/^/interface=/'
  fi
} >"${LOG_DIR}/${RUN_TAG}/run-manifest.txt"
export NOF_REDIS_URL="${REDIS_URL}"
export NOF_KEYSPACE="${KEYSPACE}"
export NOF_BARRIER_RUN_ID="${BARRIER_RUN_ID}"
export NOF_BIND_IP="${BIND_IP}"
if [[ -n "${HOST_NQN}" ]]; then
  export NOF_HOST_NQN="${HOST_NQN}"
fi
export NOF_TARGETS="${TARGETS_SPEC}"
export NOF_CLIENT_IDS="${CLIENT_IDS_SPEC}"
export NOF_ROUTE_CONTROL="${ROUTE_CONTROL}"
export NOF_STARTUP_SETTLE_SECONDS="${STARTUP_SETTLE_SECONDS}"
export MC_STORE_RS_ENABLE_COLD_TIER="${MC_STORE_RS_ENABLE_COLD_TIER:-1}"
export NOF_BARRIER_REDIS_URL="${NOF_BARRIER_REDIS_URL:-${REDIS_URL}}"

pids=()
pid_clients=()
for index in "${!local_clients[@]}"; do
  client_id="${local_clients[${index}]}"
  [[ -n "${client_id}" ]] || { echo "NOF_CLIENT_IDS contains an empty id" >&2; exit 1; }
  log_file="${LOG_DIR}/${RUN_TAG}/${client_id}.log"
  echo "starting ${client_id}; log=${log_file}"
  NOF_CLIENT_ID="${client_id}" \
  NOF_RPC_PORT="$((RPC_BASE_PORT + index))" \
    timeout "${TIMEOUT_SECONDS}" "${BINARY}" >"${log_file}" 2>&1 &
  pids+=("$!")
  pid_clients+=("${client_id}")
done

status=0
for index in "${!pids[@]}"; do
  pid="${pids[${index}]}"
  client_id="${pid_clients[${index}]}"
  if wait "${pid}"; then
    continue
  else
    exit_code="$?"
  fi
  if [[ "${HANDOFF_ABRUPT_EXIT}" == true
        && "${client_id}" == "${HANDOFF_DEPARTING_CLIENT}"
        && "${exit_code}" == 137 ]]; then
    echo "accepted expected abrupt exit from ${client_id}"
  else
    status=1
  fi
done

for client_id in "${local_clients[@]}"; do
  log_file="${LOG_DIR}/${RUN_TAG}/${client_id}.log"
  echo "===== ${client_id} ====="
  cat "${log_file}"
done

if [[ "${status}" != 0 ]]; then
  echo "multi-client NoF test failed; logs=${LOG_DIR}/${RUN_TAG}" >&2
  exit "${status}"
fi

echo "multi-client NoF test passed: local_clients=${#local_clients[@]} global_clients=${#clients[@]} targets=${#TARGETS[@]} keyspace=${KEYSPACE} logs=${LOG_DIR}/${RUN_TAG}"
