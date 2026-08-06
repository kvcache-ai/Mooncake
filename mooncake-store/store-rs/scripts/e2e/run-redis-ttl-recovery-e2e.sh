#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/lib/common.sh"
MODE="${1:-all}"

usage() {
  cat <<'EOF'
Usage: scripts/e2e/run-redis-ttl-recovery-e2e.sh [all|persisted|fresh]

Bring up a local Redis metadata backend plus two storage clients, then validate:

  persisted  - Redis stays down for longer than the lease TTL, restarts from the
               same data directory, old keys remain readable, and new writes work.
  fresh      - Redis stays down for longer than the lease TTL, restarts from an
               empty data directory, storage clients self-heal metadata, and new
               writes/reads still work.

Environment:
  MC_STORE_RS_REFRESH_WHEEL              Rebuild/reinstall the latest wheel into
                                         .venv-wheel before running (default: 1)
  MC_STORE_RS_TTL_RECOVERY_REDIS_PORT    Fixed Redis port; auto-allocates when empty
  MC_STORE_RS_TTL_RECOVERY_PROTOCOL      tcp / rdma / auto (default: tcp)
  MC_STORE_RS_TTL_RECOVERY_TRANSPORT_BACKEND
                                         classic-te / tent (default: classic-te)
  MC_STORE_RS_TTL_RECOVERY_ROUTE_CONTROL embedded-wrh / metadata-only
                                         (default: embedded-wrh)
  MC_STORE_RS_TTL_RECOVERY_ROUTE_TOPK    Route authority width (default: 2)
  MC_STORE_RS_TTL_RECOVERY_REPLICA_NUM   Write replica count (default: 2)
  MC_STORE_RS_TTL_RECOVERY_NUM_KV        Number of keys per phase (default: 32)
  MC_STORE_RS_TTL_RECOVERY_VALUE_SIZE    Value size in bytes (default: 4096)
  MC_STORE_RS_TTL_RECOVERY_BATCH_SIZE    Batch size for rw clients (default: 1)
  MC_STORE_RS_TTL_RECOVERY_STORAGE_BYTES Storage bytes per storage client
                                         (default: 128 MiB)
  MC_STORE_RS_TTL_RECOVERY_SCRATCH_BYTES Scratch bytes per client
                                         (default: 16 MiB)
  MC_STORE_RS_TTL_RECOVERY_LEASE_MS      Client lease TTL used for this e2e
                                         (default: 10000)
  MC_STORE_RS_TTL_RECOVERY_DOWN_SECONDS  Redis downtime between stop/start
                                         (default: 15)
  MC_STORE_RS_TTL_RECOVERY_HOLD_SECONDS  Idle lifetime for storage clients
                                         (default: 600)
  MC_STORE_RS_KEEP_TEMP                  Keep temp dir on failure when set to 1
  MOONCAKE_UPSTREAM_DIR                  Mooncake upstream checkout override
  MOONCAKE_UPSTREAM_BUILD_DIR            Built upstream directory override
EOF
}

if [[ "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${MODE}" != "all" && "${MODE}" != "persisted" && "${MODE}" != "fresh" ]]; then
  echo "unsupported mode: ${MODE}" >&2
  usage >&2
  exit 1
fi

PROTOCOL="${MC_STORE_RS_TTL_RECOVERY_PROTOCOL:-tcp}"
TRANSPORT_BACKEND="${MC_STORE_RS_TTL_RECOVERY_TRANSPORT_BACKEND:-classic-te}"
ROUTE_CONTROL="${MC_STORE_RS_TTL_RECOVERY_ROUTE_CONTROL:-embedded-wrh}"
ROUTE_TOPK="${MC_STORE_RS_TTL_RECOVERY_ROUTE_TOPK:-2}"
REPLICA_NUM="${MC_STORE_RS_TTL_RECOVERY_REPLICA_NUM:-2}"
NUM_KV="${MC_STORE_RS_TTL_RECOVERY_NUM_KV:-32}"
VALUE_SIZE="${MC_STORE_RS_TTL_RECOVERY_VALUE_SIZE:-4096}"
BATCH_SIZE="${MC_STORE_RS_TTL_RECOVERY_BATCH_SIZE:-1}"
STORAGE_BYTES="${MC_STORE_RS_TTL_RECOVERY_STORAGE_BYTES:-$((128 * 1024 * 1024))}"
SCRATCH_BYTES="${MC_STORE_RS_TTL_RECOVERY_SCRATCH_BYTES:-$((16 * 1024 * 1024))}"
LEASE_MS="${MC_STORE_RS_TTL_RECOVERY_LEASE_MS:-10000}"
DOWN_SECONDS="${MC_STORE_RS_TTL_RECOVERY_DOWN_SECONDS:-15}"
HOLD_SECONDS="${MC_STORE_RS_TTL_RECOVERY_HOLD_SECONDS:-600}"
REFRESH_WHEEL="${MC_STORE_RS_REFRESH_WHEEL:-1}"

allocate_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

now_ms() {
  python3 - <<'PY'
import time

print(int(time.time() * 1000))
PY
}

resolve_python_bin() {
  if [[ -x "${REPO_ROOT}/.venv-wheel/bin/python" ]]; then
    printf '%s\n' "${REPO_ROOT}/.venv-wheel/bin/python"
    return 0
  fi
  printf '%s\n' python3
}

python_can_import_store() {
  local python_bin=$1
  "${python_bin}" - <<'PY' >/dev/null 2>&1
from mooncake.store import MooncakeDistributedStore  # noqa: F401
PY
}

ensure_runtime_ready() {
  local python_bin
  python_bin=$(resolve_python_bin)

  if [[ "${REFRESH_WHEEL}" == "1" ]]; then
    echo "==> rebuilding and reinstalling latest wheel into .venv-wheel"
    bash "${REPO_ROOT}/scripts/build/build-wheel.sh"
    bash "${REPO_ROOT}/scripts/build/install-wheel.sh"
    PYTHON_BIN="${REPO_ROOT}/.venv-wheel/bin/python"
    return 0
  fi

  if [[ -x "${python_bin}" ]] && python_can_import_store "${python_bin}"; then
    PYTHON_BIN="${python_bin}"
    return 0
  fi

  if [[ "${python_bin}" != "python3" ]] && command -v python3 >/dev/null 2>&1 && python_can_import_store python3; then
    echo "==> wheel runtime unavailable; falling back to system python3"
    PYTHON_BIN="python3"
    return 0
  fi

  echo "==> installed wheel missing or stale; rebuilding .venv-wheel runtime"
  bash "${REPO_ROOT}/scripts/build/build-wheel.sh"
  bash "${REPO_ROOT}/scripts/build/install-wheel.sh"
  PYTHON_BIN="${REPO_ROOT}/.venv-wheel/bin/python"
}

wait_for_redis_up() {
  local deadline=$((SECONDS + 15))
  while (( SECONDS < deadline )); do
    if redis-cli -p "${REDIS_PORT}" ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "redis on port ${REDIS_PORT} did not become ready" >&2
  exit 1
}

wait_for_redis_down() {
  local deadline=$((SECONDS + 15))
  while (( SECONDS < deadline )); do
    if ! redis-cli -p "${REDIS_PORT}" ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "redis on port ${REDIS_PORT} did not stop in time" >&2
  exit 1
}

start_redis() {
  local data_dir=$1
  mkdir -p "${data_dir}"
  redis-server \
    --port "${REDIS_PORT}" \
    --bind 127.0.0.1 \
    --daemonize yes \
    --save '' \
    --appendonly no \
    --dir "${data_dir}" \
    --dbfilename dump.rdb \
    --pidfile "${data_dir}/redis.pid" \
    --logfile "${data_dir}/redis.log"
  wait_for_redis_up
}

stop_redis() {
  redis-cli -p "${REDIS_PORT}" shutdown nosave >/dev/null 2>&1 || true
  wait_for_redis_down
}

save_redis_snapshot() {
  redis-cli -p "${REDIS_PORT}" SAVE >/dev/null
}

redis_key_exists() {
  local key=$1
  redis-cli -u "${REDIS_URL}" EXISTS "${key}" | tr -d '[:space:]'
}

wait_for_key() {
  local key=$1
  local timeout_seconds=$2
  local deadline=$((SECONDS + timeout_seconds))
  local exists=0

  while (( SECONDS < deadline )); do
    exists=$(redis_key_exists "${key}")
    if [[ "${exists}" == "1" ]]; then
      return 0
    fi
    sleep 0.2
  done

  echo "key did not appear: ${key}" >&2
  exit 1
}

wait_for_route_policy() {
  local deadline=$((SECONDS + 20))
  local key="{${KEYSPACE}}/system/route-policy/default"
  while (( SECONDS < deadline )); do
    if [[ "$(redis-cli -u "${REDIS_URL}" EXISTS "${key}" | tr -d '[:space:]')" == "1" ]]; then
      return 0
    fi
    sleep 0.2
  done
  echo "route policy key did not appear: ${key}" >&2
  exit 1
}

wait_for_storage_metadata() {
  wait_for_key "{${KEYSPACE}}/clients/${STORAGE_A_STABLE}:1" 25
  wait_for_key "{${KEYSPACE}}/clients/${STORAGE_B_STABLE}:1" 25
  wait_for_key "{${KEYSPACE}}/indexes/segments/${STORAGE_A_STABLE}:1" 25
  wait_for_key "{${KEYSPACE}}/indexes/segments/${STORAGE_B_STABLE}:1" 25
  wait_for_route_policy
}

run_real_client() {
  local mode=$1
  local key_prefix=$2
  local stable_id=$3
  local local_host=$4
  local storage_bytes=$5
  shift 5

  local expires_at_ms
  expires_at_ms=$(( $(now_ms) + LEASE_MS ))
  local -a args=(
    "${PYTHON_BIN}"
    -u
    "${REPO_ROOT}/scripts/clients/real_client_rw.py"
    --local_host "${local_host}"
    --metadata_url "${REDIS_URL}"
    --storage-bytes "${storage_bytes}"
    --scratch-bytes "${SCRATCH_BYTES}"
    --protocol "${PROTOCOL}"
    --keyspace "${KEYSPACE}"
    --stable-id "${stable_id}"
    --mode "${mode}"
    --transport-backend "${TRANSPORT_BACKEND}"
    --route-control "${ROUTE_CONTROL}"
    --route-topk "${ROUTE_TOPK}"
    --batch_size "${BATCH_SIZE}"
    --num_kv "${NUM_KV}"
    --value_size "${VALUE_SIZE}"
    --replica_num "${REPLICA_NUM}"
  )
  if [[ -n "${key_prefix}" ]]; then
    args+=(--key_prefix "${key_prefix}")
  fi
  if (( storage_bytes == 0 )); then
    args+=(--routed-writes)
  fi
  args+=("$@")

  MC_STORE_RS_EXPIRES_AT_MS="${expires_at_ms}" \
    PYTHONUNBUFFERED=1 \
    "${args[@]}"
}

start_storage_client() {
  local stable_id=$1
  local local_host=$2
  local log_file=$3
  local expires_at_ms

  expires_at_ms=$(( $(now_ms) + LEASE_MS ))

  MC_STORE_RS_EXPIRES_AT_MS="${expires_at_ms}" \
    PYTHONUNBUFFERED=1 \
    "${PYTHON_BIN}" -u "${REPO_ROOT}/scripts/clients/real_client_rw.py" \
      --local_host "${local_host}" \
      --metadata_url "${REDIS_URL}" \
      --storage-bytes "${STORAGE_BYTES}" \
      --scratch-bytes "${SCRATCH_BYTES}" \
      --protocol "${PROTOCOL}" \
      --keyspace "${KEYSPACE}" \
      --stable-id "${stable_id}" \
      --mode idle \
      --hold-seconds "${HOLD_SECONDS}" \
      --transport-backend "${TRANSPORT_BACKEND}" \
      --route-control "${ROUTE_CONTROL}" \
      --route-topk "${ROUTE_TOPK}" \
      --label pool=pool-a \
      --label storage=true \
      >"${log_file}" 2>&1 &
  PIDS+=($!)
}

run_persisted_restart_phase() {
  echo
  echo "==> persisted restart phase"
  run_real_client write "persisted-before" "${WRITER_STABLE}-before" "127.0.0.1:${WRITER_PORT}" 0
  run_real_client read "persisted-before" "${READER_STABLE}-before" "127.0.0.1:${READER_PORT}" 0

  echo "==> saving Redis snapshot and stopping metadata for ${DOWN_SECONDS}s"
  save_redis_snapshot
  stop_redis
  sleep "${DOWN_SECONDS}"
  start_redis "${REDIS_PERSIST_DIR}"
  wait_for_storage_metadata

  echo "==> verifying pre-outage keys still read after persisted restart"
  run_real_client read "persisted-before" "${READER_STABLE}-persisted" "127.0.0.1:${READER_PORT}" 0

  echo "==> verifying new writes after persisted restart"
  run_real_client write "persisted-after" "${WRITER_STABLE}-persisted" "127.0.0.1:${WRITER_PORT}" 0
  run_real_client read "persisted-after" "${READER_STABLE}-persisted-new" "127.0.0.1:${READER_PORT}" 0
}

run_fresh_restart_phase() {
  echo
  echo "==> fresh restart phase"
  stop_redis
  sleep "${DOWN_SECONDS}"
  start_redis "${REDIS_FRESH_DIR}"
  wait_for_storage_metadata

  echo "==> verifying new writes after empty metadata restart"
  run_real_client write "fresh-after" "${WRITER_STABLE}-fresh" "127.0.0.1:${WRITER_PORT}" 0
  run_real_client read "fresh-after" "${READER_STABLE}-fresh" "127.0.0.1:${READER_PORT}" 0
}

cleanup() {
  local status=$?
  local pid

  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -TERM "${pid}" >/dev/null 2>&1 || true
    fi
  done
  sleep 0.5
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -KILL "${pid}" >/dev/null 2>&1 || true
    fi
  done

  if [[ -n "${REDIS_PORT:-}" ]] && redis-cli -p "${REDIS_PORT}" ping >/dev/null 2>&1; then
    redis-cli -p "${REDIS_PORT}" shutdown nosave >/dev/null 2>&1 || true
  fi

  if [[ -n "${TEMP_DIR:-}" ]]; then
    if [[ "${status}" != "0" && "${MC_STORE_RS_KEEP_TEMP:-0}" == "1" ]]; then
      echo "preserving temp dir: ${TEMP_DIR}" >&2
    else
      rm -rf "${TEMP_DIR}"
    fi
  fi
  exit "${status}"
}

trap cleanup EXIT

mc_scripts_require_command git
mc_scripts_require_command cargo
mc_scripts_require_command python3
mc_scripts_require_command redis-cli
mc_scripts_require_command redis-server

UPSTREAM_BUILD_DIR=$(mc_scripts_resolve_upstream_build_dir "${REPO_ROOT}")
mc_scripts_setup_upstream_runtime_env "${REPO_ROOT}" repo-python "${UPSTREAM_BUILD_DIR}"
export PYTHONDONTWRITEBYTECODE=1

ensure_runtime_ready

TEMP_DIR=$(mktemp -d)
PIDS=()
REDIS_PORT="${MC_STORE_RS_TTL_RECOVERY_REDIS_PORT:-$(allocate_port)}"
REDIS_URL="redis://127.0.0.1:${REDIS_PORT}/0"
RUN_ID=$(date +%s%N)
KEYSPACE="mc/store-rs/e2e/redis-ttl-recovery/${RUN_ID}"
REDIS_PERSIST_DIR="${TEMP_DIR}/redis-persist"
REDIS_FRESH_DIR="${TEMP_DIR}/redis-fresh"
STORAGE_A_STABLE="redis-ttl-storage-a-${RUN_ID}"
STORAGE_B_STABLE="redis-ttl-storage-b-${RUN_ID}"
WRITER_STABLE="redis-ttl-writer-${RUN_ID}"
READER_STABLE="redis-ttl-reader-${RUN_ID}"
STORAGE_A_PORT=$(allocate_port)
STORAGE_B_PORT=$(allocate_port)
WRITER_PORT=$(allocate_port)
READER_PORT=$(allocate_port)

echo "==> protocol:            ${PROTOCOL}"
echo "==> transport backend:   ${TRANSPORT_BACKEND}"
echo "==> route control:       ${ROUTE_CONTROL}"
echo "==> route topk:          ${ROUTE_TOPK}"
echo "==> replica num:         ${REPLICA_NUM}"
echo "==> lease ttl ms:        ${LEASE_MS}"
echo "==> redis down seconds:  ${DOWN_SECONDS}"
echo "==> python:              ${PYTHON_BIN}"
echo "==> upstream build dir:  ${UPSTREAM_BUILD_DIR}"
echo "==> redis url:           ${REDIS_URL}"
echo "==> keyspace:            ${KEYSPACE}"

start_redis "${REDIS_PERSIST_DIR}"

echo "==> starting storage clients"
start_storage_client "${STORAGE_A_STABLE}" "127.0.0.1:${STORAGE_A_PORT}" "${TEMP_DIR}/storage-a.log"
start_storage_client "${STORAGE_B_STABLE}" "127.0.0.1:${STORAGE_B_PORT}" "${TEMP_DIR}/storage-b.log"
wait_for_storage_metadata

if [[ "${MODE}" == "all" || "${MODE}" == "persisted" ]]; then
  run_persisted_restart_phase
fi

if [[ "${MODE}" == "all" || "${MODE}" == "fresh" ]]; then
  run_fresh_restart_phase
fi

echo
echo "PASS: Redis TTL recovery e2e completed"
