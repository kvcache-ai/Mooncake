#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel 2>/dev/null || pwd)"
if [[ ! -f "${ROOT_DIR}/Cargo.toml" && -f "${ROOT_DIR}/mooncake-store/store-rs/Cargo.toml" ]]; then
  ROOT_DIR="${ROOT_DIR}/mooncake-store/store-rs"
fi
BINARY="${NOF_BINARY:-${ROOT_DIR}/target/debug/nof_multi_client}"
LOG_DIR="${NOF_LOG_DIR:-${ROOT_DIR}/target/nof-multi-client}"
RUN_TAG="${NOF_RUN_TAG:-$(date +%Y%m%d-%H%M%S)}"
KEYSPACE="${NOF_KEYSPACE:-mc/store-rs/nof-multi-client/${RUN_TAG}}"
REDIS_URL="${NOF_REDIS_URL:-redis://127.0.0.1:6379/0}"
BIND_IP="${NOF_BIND_IP:-192.168.22.80}"
SPDK_LIB_DIR="${NOF_SPDK_LIB_DIR:-/opt/spdk-26.05/install/lib}"
RESET_TARGETS="${NOF_RESET_TARGETS:-1}"
SKIP_TARGET_SSH_CHECK="${NOF_SKIP_TARGET_SSH_CHECK:-0}"
TIMEOUT_SECONDS="${NOF_CLIENT_TIMEOUT_SECONDS:-180}"
ENABLE_CORES="${NOF_ENABLE_CORES:-0}"
STARTUP_SETTLE_SECONDS="${NOF_STARTUP_SETTLE_SECONDS:-2}"
TEST_IMAGE_BYTES="${NOF_TEST_IMAGE_BYTES:-16G}"
ROUTE_CONTROL="${NOF_ROUTE_CONTROL:-EmbeddedWrh}"
RPC_BASE_PORT="${NOF_RPC_BASE_PORT:-21000}"
CLIENT_IDS_SPEC="${NOF_CLIENT_IDS:-client-0,client-1,client-2,client-3}"
TARGETS_SPEC="${NOF_TARGETS:-127.0.0.1|127.0.0.1|nof-local|nqn.2026-09.io.mooncake:nof-local|4420}"
BARRIER_DIR="${NOF_BARRIER_DIR:-${LOG_DIR}/${RUN_TAG}/barrier}"

[[ -x "${BINARY}" ]] || { echo "NOF_BINARY is not executable: ${BINARY}" >&2; exit 1; }
[[ -d "${SPDK_LIB_DIR}" ]] || { echo "NOF_SPDK_LIB_DIR does not exist: ${SPDK_LIB_DIR}" >&2; exit 1; }
for command in ssh ldd sha256sum timeout; do
  command -v "${command}" >/dev/null || { echo "${command} is required" >&2; exit 1; }
done
if [[ "${ENABLE_CORES}" == 1 ]]; then
  ulimit -c unlimited
fi

IFS=',' read -r -a TARGETS <<<"${TARGETS_SPEC}"
[[ "${#TARGETS[@]}" -gt 0 ]] || { echo "NOF_TARGETS must not be empty" >&2; exit 1; }
for target in "${TARGETS[@]}"; do
  IFS='|' read -r public_ip lan_ip target_id subnqn port <<<"${target}"
  [[ -n "${public_ip}" && -n "${lan_ip}" && -n "${target_id}" && -n "${subnqn}" && -n "${port}" ]] || {
    echo "NOF_TARGETS entries must be public_ip|traddr|target_id|subnqn|port: ${target}" >&2
    exit 1
  }
done
IFS=',' read -r -a clients <<<"${CLIENT_IDS_SPEC}"
[[ "${#clients[@]}" -gt 0 && -n "${clients[0]}" ]] || { echo "NOF_CLIENT_IDS must not be empty" >&2; exit 1; }

RUNTIME_LIB_DIR="${NOF_RUNTIME_LIB_DIR:-${TMPDIR:-/tmp}/nof-multi-client-runtime}"
mkdir -p "${RUNTIME_LIB_DIR}"
if ! ldconfig -p 2>/dev/null | grep -q 'libaio.so.1 ('; then
  libaio_compat="$(ldconfig -p 2>/dev/null | awk '$1 == "libaio.so.1t64" { print $NF; exit }')"
  if [[ -n "${libaio_compat}" ]]; then
    ln -sfn "${libaio_compat}" "${RUNTIME_LIB_DIR}/libaio.so.1"
  fi
fi

reset_target() {
  local public_ip="$1"
  ssh -o BatchMode=yes -o ConnectTimeout=8 "root@${public_ip}" \
    "set -euo pipefail; systemctl stop mooncake-nvmf.service; rm -f /var/lib/mooncake-nof/nof.img; truncate -s '${TEST_IMAGE_BYTES}' /var/lib/mooncake-nof/nof.img; systemctl start mooncake-nvmf.service; test \"\$(systemctl is-active mooncake-nvmf.service)\" = active"
}

if [[ "${RESET_TARGETS}" == 1 ]]; then
  for target in "${TARGETS[@]}"; do
    IFS='|' read -r public_ip lan_ip target_id subnqn port <<<"${target}"
    echo "resetting NoF backing on ${public_ip} (${lan_ip}, ${target_id})"
    reset_target "${public_ip}"
  done
fi

check_target_endpoint() {
  local lan_ip="$1"
  local port="$2"
  timeout 2 bash -c "</dev/tcp/${lan_ip}/${port}"
}

for target in "${TARGETS[@]}"; do
  IFS='|' read -r public_ip lan_ip target_id subnqn port <<<"${target}"
  if [[ "${SKIP_TARGET_SSH_CHECK}" != 1 ]]; then
    ssh -o BatchMode=yes -o ConnectTimeout=8 "root@${public_ip}" \
      "test \"\$(systemctl is-active mooncake-nvmf.service)\" = active && timeout 2 bash -c '</dev/tcp/${lan_ip}/${port}'"
  else
    check_target_endpoint "${lan_ip}" "${port}"
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
rm -rf "${BARRIER_DIR}"
mkdir -p "${BARRIER_DIR}"
{
  printf 'binary=%s\n' "${BINARY}"
  sha256sum "${BINARY}"
  printf 'run_tag=%s\nkeyspace=%s\nbind_ip=%s\nredis_url=%s\n' \
    "${RUN_TAG}" "${KEYSPACE}" "${BIND_IP}" "${REDIS_URL}"
  printf 'target_inventory=%s\nclient_ids=%s\nroute_control=%s\n' \
    "${TARGETS_SPEC}" "${CLIENT_IDS_SPEC}" "${ROUTE_CONTROL}"
  printf 'startup_settle_seconds=%s read_protocol=parallel_batch_get_into\n' \
    "${STARTUP_SETTLE_SECONDS}"
  printf 'hostname=%s\n' "$(hostname)"
  printf 'kernel=%s\n' "$(uname -srmo)"
  printf 'spdk_lib_dir=%s\n' "${SPDK_LIB_DIR}"
  printf 'runtime_dependencies=resolved\n'
  if command -v ip >/dev/null; then
    ip -br link | sed 's/^/interface=/'
  fi
} >"${LOG_DIR}/${RUN_TAG}/run-manifest.txt"
export NOF_REDIS_URL="${REDIS_URL}"
export NOF_KEYSPACE="${KEYSPACE}"
export NOF_BIND_IP="${BIND_IP}"
export NOF_TARGETS="${TARGETS_SPEC}"
export NOF_CLIENT_IDS="${CLIENT_IDS_SPEC}"
export NOF_ROUTE_CONTROL="${ROUTE_CONTROL}"
export NOF_STARTUP_SETTLE_SECONDS="${STARTUP_SETTLE_SECONDS}"
export MC_STORE_RS_ENABLE_COLD_TIER="${MC_STORE_RS_ENABLE_COLD_TIER:-1}"
export NOF_BARRIER_DIR="${BARRIER_DIR}"

pids=()
for index in "${!clients[@]}"; do
  client_id="${clients[${index}]}"
  [[ -n "${client_id}" ]] || { echo "NOF_CLIENT_IDS contains an empty id" >&2; exit 1; }
  log_file="${LOG_DIR}/${RUN_TAG}/${client_id}.log"
  echo "starting ${client_id}; log=${log_file}"
  NOF_CLIENT_ID="${client_id}" \
  NOF_RPC_PORT="$((RPC_BASE_PORT + index))" \
    timeout "${TIMEOUT_SECONDS}" "${BINARY}" >"${log_file}" 2>&1 &
  pids+=("$!")
done

status=0
for pid in "${pids[@]}"; do
  if ! wait "${pid}"; then
    status=1
  fi
done

for client_id in "${clients[@]}"; do
  log_file="${LOG_DIR}/${RUN_TAG}/${client_id}.log"
  echo "===== ${client_id} ====="
  cat "${log_file}"
done

if [[ "${status}" != 0 ]]; then
  echo "multi-client NoF test failed; logs=${LOG_DIR}/${RUN_TAG}" >&2
  exit "${status}"
fi

echo "multi-client NoF test passed: clients=${#clients[@]} targets=${#TARGETS[@]} keyspace=${KEYSPACE} logs=${LOG_DIR}/${RUN_TAG}"
