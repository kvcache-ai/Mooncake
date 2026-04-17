#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/lib/common.sh"
MODE="${1:-all}"

usage() {
  cat <<'USAGE'
Usage: scripts/e2e/run-local-hot-cache-e2e.sh [all|real|dummy]

Validate the local hot-cache v2 behavior against a real wheel/runtime:

  real   - rw-only real client keeps a locally cached value after the origin key
           is deleted remotely.
  dummy  - two dummy clients share the daemon hot-cache shm; the second dummy can
           still read after the origin key is deleted remotely.
  all    - run both phases (default).

Environment:
  MC_STORE_RS_REFRESH_WHEEL                   Rebuild/reinstall the latest wheel
                                              into .venv-wheel before running
                                              (default: 1)
  MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REDIS_PORT  Fixed Redis port; auto-allocates
                                              when empty
  MC_STORE_RS_LOCAL_HOT_CACHE_E2E_STORAGE_BYTES
                                              Storage bytes for storage daemons
                                              (default: 64 MiB)
  MC_STORE_RS_LOCAL_HOT_CACHE_E2E_SCRATCH_BYTES
                                              Scratch bytes per client
                                              (default: 16 MiB)
  MC_STORE_RS_LOCAL_HOT_CACHE_E2E_CACHE_BYTES Hot-cache capacity in bytes
                                              (default: 1 MiB)
  MC_STORE_RS_LOCAL_HOT_CACHE_E2E_BLOCK_BYTES Hot-cache block size in bytes
                                              (default: 8192)
  MC_STORE_RS_KEEP_TEMP                       Keep temp dir on failure when set
                                              to 1
  MOONCAKE_UPSTREAM_DIR                       Mooncake upstream checkout override
  MOONCAKE_UPSTREAM_BUILD_DIR                 Built upstream directory override
USAGE
}

if [[ "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${MODE}" != "all" && "${MODE}" != "real" && "${MODE}" != "dummy" ]]; then
  echo "unsupported mode: ${MODE}" >&2
  usage >&2
  exit 1
fi

REFRESH_WHEEL="${MC_STORE_RS_REFRESH_WHEEL:-1}"
STORAGE_BYTES="${MC_STORE_RS_LOCAL_HOT_CACHE_E2E_STORAGE_BYTES:-$((64 * 1024 * 1024))}"
SCRATCH_BYTES="${MC_STORE_RS_LOCAL_HOT_CACHE_E2E_SCRATCH_BYTES:-$((16 * 1024 * 1024))}"
HOT_CACHE_BYTES="${MC_STORE_RS_LOCAL_HOT_CACHE_E2E_CACHE_BYTES:-$((1 * 1024 * 1024))}"
HOT_BLOCK_BYTES="${MC_STORE_RS_LOCAL_HOT_CACHE_E2E_BLOCK_BYTES:-8192}"
REDIS_PORT="${MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REDIS_PORT:-}"

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

ensure_runtime_ready() {
  local python_bin
  python_bin=$(resolve_python_bin)

  if [[ "${REFRESH_WHEEL}" == "1" ]]; then
    echo "==> rebuilding and reinstalling latest wheel into .venv-wheel"
    bash "${REPO_ROOT}/scripts/build/build-wheel.sh"
    bash "${REPO_ROOT}/scripts/build/install-pro-wheel.sh"
    PYTHON_BIN="${REPO_ROOT}/.venv-wheel/bin/python"
    return 0
  fi

  if [[ ! -x "${python_bin}" ]]; then
    echo "python runtime not found at ${python_bin}; rebuilding wheel runtime" >&2
    bash "${REPO_ROOT}/scripts/build/build-wheel.sh"
    bash "${REPO_ROOT}/scripts/build/install-pro-wheel.sh"
    PYTHON_BIN="${REPO_ROOT}/.venv-wheel/bin/python"
    return 0
  fi

  if ! "${python_bin}" - <<'PY' >/dev/null 2>&1
from mooncake.store import MooncakeDistributedStore  # noqa: F401
PY
  then
    echo "==> installed wheel missing or stale; rebuilding .venv-wheel runtime"
    bash "${REPO_ROOT}/scripts/build/build-wheel.sh"
    bash "${REPO_ROOT}/scripts/build/install-pro-wheel.sh"
    PYTHON_BIN="${REPO_ROOT}/.venv-wheel/bin/python"
    return 0
  fi

  PYTHON_BIN="${python_bin}"
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

redis_pattern_count() {
  local pattern=$1
  redis-cli -u "${REDIS_URL}" --scan --pattern "${pattern}" | wc -l | tr -d '[:space:]'
}

wait_for_pattern_count() {
  local pattern=$1
  local expected=$2
  local timeout_seconds=$3
  local deadline=$((SECONDS + timeout_seconds))
  local count=0

  while (( SECONDS < deadline )); do
    count=$(redis_pattern_count "${pattern}")
    if (( count >= expected )); then
      return 0
    fi
    sleep 0.2
  done

  echo "pattern ${pattern} reached ${count}, expected at least ${expected}" >&2
  exit 1
}

print_log_tail() {
  local label=$1
  local path=$2
  if [[ -n "${path}" && -f "${path}" ]]; then
    echo "----- ${label}: ${path} (tail -80) -----" >&2
    tail -80 "${path}" >&2 || true
  fi
}

cleanup() {
  local exit_code=$?

  if [[ -n "${DAEMON_PID:-}" ]]; then
    kill "${DAEMON_PID}" >/dev/null 2>&1 || true
    wait "${DAEMON_PID}" >/dev/null 2>&1 || true
  fi

  if [[ -n "${REDIS_PORT:-}" ]]; then
    stop_redis || true
  fi

  if (( exit_code != 0 )); then
    print_log_tail "redis" "${REDIS_LOG:-}"
    print_log_tail "dummy-daemon" "${DAEMON_LOG:-}"
  fi

  if (( exit_code == 0 )) || [[ "${MC_STORE_RS_KEEP_TEMP:-0}" != "1" ]]; then
    rm -rf "${TMP_DIR}"
  else
    echo "keeping temp dir for inspection: ${TMP_DIR}" >&2
  fi

  exit "${exit_code}"
}

mc_scripts_require_command cargo
mc_scripts_require_command python3
mc_scripts_require_command redis-server
mc_scripts_require_command redis-cli

UPSTREAM_BUILD_DIR=$(mc_scripts_resolve_upstream_build_dir "${REPO_ROOT}")
mc_scripts_setup_upstream_runtime_env "${REPO_ROOT}" repo-python "${UPSTREAM_BUILD_DIR}"
export PYTHONDONTWRITEBYTECODE=1

if [[ -z "${REDIS_PORT}" ]]; then
  REDIS_PORT=$(allocate_port)
fi

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/mc-local-hot-cache-e2e.XXXXXX")
REDIS_DIR="${TMP_DIR}/redis"
REDIS_LOG="${REDIS_DIR}/redis.log"
DAEMON_LOG="${TMP_DIR}/dummy-daemon.log"
KEYSPACE="mc/store-rs/e2e/local-hot-cache/$(now_ms)"
REDIS_URL="redis://127.0.0.1:${REDIS_PORT}/0"
export MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REDIS_URL="${REDIS_URL}"
export MC_STORE_RS_LOCAL_HOT_CACHE_E2E_KEYSPACE="${KEYSPACE}"
export MC_STORE_RS_LOCAL_HOT_CACHE_E2E_STORAGE_BYTES="${STORAGE_BYTES}"
export MC_STORE_RS_LOCAL_HOT_CACHE_E2E_SCRATCH_BYTES="${SCRATCH_BYTES}"
export MC_STORE_RS_LOCAL_HOT_CACHE_E2E_CACHE_BYTES="${HOT_CACHE_BYTES}"
export MC_STORE_RS_LOCAL_HOT_CACHE_E2E_BLOCK_BYTES="${HOT_BLOCK_BYTES}"
trap cleanup EXIT

start_redis "${REDIS_DIR}"
ensure_runtime_ready

echo "==> building latest mooncake-store-client binary"
(
  cd "${REPO_ROOT}"
  cargo build -p mooncake-store-py --bin mooncake-store-client
)

export MC_STORE_LOCAL_HOT_CACHE_SIZE="${HOT_CACHE_BYTES}"
export MC_STORE_LOCAL_HOT_BLOCK_SIZE="${HOT_BLOCK_BYTES}"

if [[ "${MODE}" == "all" || "${MODE}" == "real" ]]; then
  echo "==> Phase A: real client keeps a local hot-cache hit after remote delete"
  unset MC_STORE_LOCAL_HOT_CACHE_USE_SHM || true
  export MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REAL_WRITER_HOST="127.0.0.1:$(allocate_port)"
  export MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REAL_READER_HOST="127.0.0.1:$(allocate_port)"
  "${PYTHON_BIN}" - <<'PY'
import os
import time

from mooncake.store import MooncakeDistributedStore, ReplicateConfig

from scripts.clients.real_client_rw import apply_replication_config, setup_store


REDIS_URL = os.environ["MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REDIS_URL"]
KEYSPACE = os.environ["MC_STORE_RS_LOCAL_HOT_CACHE_E2E_KEYSPACE"] + "/real"
STORAGE_BYTES = int(os.environ["MC_STORE_RS_LOCAL_HOT_CACHE_E2E_STORAGE_BYTES"])
SCRATCH_BYTES = int(os.environ["MC_STORE_RS_LOCAL_HOT_CACHE_E2E_SCRATCH_BYTES"])
WRITER_HOST = os.environ["MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REAL_WRITER_HOST"]
READER_HOST = os.environ["MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REAL_READER_HOST"]


def open_store(*, local_host: str, stable_id: str, storage_bytes: int, routed_writes: bool):
    store = MooncakeDistributedStore()
    status = setup_store(
        store,
        local_hostname=local_host,
        metadata_url=REDIS_URL,
        storage_bytes=storage_bytes,
        scratch_bytes=SCRATCH_BYTES,
        protocol="tcp",
        device_names="",
        master_addr="",
        stable_id=stable_id,
        state="active",
        tenant="default",
        labels={"pool": "hot-cache", "storage": "true" if storage_bytes > 0 else "false"},
        routed_writes=routed_writes,
        replica_num=1,
        keyspace=KEYSPACE,
        transport_metadata_url=None,
        transport_rpc_port=None,
        route_control="embedded_wrh",
        route_topk=2,
        transport_backend="classic-te",
    )
    if status != 0:
        raise AssertionError(f"setup failed for {stable_id}: status={status}")
    return store


writer = None
reader = None
try:
    writer = open_store(
        local_host=WRITER_HOST,
        stable_id=f"hot-cache-real-writer-{int(time.time() * 1000)}",
        storage_bytes=STORAGE_BYTES,
        routed_writes=False,
    )
    reader = open_store(
        local_host=READER_HOST,
        stable_id=f"hot-cache-real-reader-{int(time.time() * 1000)}",
        storage_bytes=0,
        routed_writes=True,
    )

    deadline = time.time() + 10.0
    while time.time() < deadline:
        try:
            if writer.list_segments():
                break
        except Exception:
            pass
        time.sleep(0.1)
    else:
        raise AssertionError("writer segment metadata did not appear in time")

    config = ReplicateConfig()
    apply_replication_config(
        config,
        replica_num=1,
        prefer_local=False,
        with_soft_pin=True,
    )
    key = "phase-a-self-hit"
    payload = b"phase-a-local-hot-cache"

    assert writer.put(key, payload, config=config) == 0
    assert reader.get(key) == payload
    assert writer.remove(key) == 0
    assert reader.get(key) == payload
    print("phase A ok: reader served cached bytes after origin delete")
finally:
    if reader is not None:
        reader.close()
    if writer is not None:
        writer.close()
PY
fi

if [[ "${MODE}" == "all" || "${MODE}" == "dummy" ]]; then
  echo "==> Phase B: dummy clients share hot-cache shm after remote delete"
  export MC_STORE_LOCAL_HOT_CACHE_USE_SHM=1
  export MC_STORE_RS_LOCAL_HOT_CACHE_E2E_DAEMON_HOST="127.0.0.1:$(allocate_port)"
  export MC_STORE_RS_LOCAL_HOT_CACHE_E2E_DUMMY_ADDR="127.0.0.1:$(allocate_port)"
  export MC_STORE_RS_LOCAL_HOT_CACHE_E2E_DUMMY_WRITER_HOST="127.0.0.1:$(allocate_port)"
  DAEMON_BIN="${REPO_ROOT}/target/debug/mooncake-store-client"
  env \
    MC_STORE_LOCAL_HOT_CACHE_SIZE="${HOT_CACHE_BYTES}" \
    MC_STORE_LOCAL_HOT_BLOCK_SIZE="${HOT_BLOCK_BYTES}" \
    MC_STORE_LOCAL_HOT_CACHE_USE_SHM=1 \
    "${DAEMON_BIN}" \
      --local-hostname "${MC_STORE_RS_LOCAL_HOT_CACHE_E2E_DAEMON_HOST}" \
      --metadata-url "${REDIS_URL}" \
      --storage-bytes "${STORAGE_BYTES}" \
      --scratch-bytes "${SCRATCH_BYTES}" \
      --protocol tcp \
      --transport-backend classic-te \
      --stable-id "hot-cache-dummy-daemon" \
      --keyspace "${KEYSPACE}/dummy" \
      --label pool=hot-cache \
      --label storage=true \
      --route-control embedded-wrh \
      --route-topk 2 \
      --client-server-address "${MC_STORE_RS_LOCAL_HOT_CACHE_E2E_DUMMY_ADDR}" \
      --drain-on-exit \
      >"${DAEMON_LOG}" 2>&1 &
  DAEMON_PID=$!
  wait_for_pattern_count "${KEYSPACE}/dummy/clients/hot-cache-dummy-daemon:*" 1 15
  wait_for_pattern_count "${KEYSPACE}/dummy/segments/hot-cache-dummy-daemon:*" 1 15

  "${PYTHON_BIN}" - <<'PY'
import os
import time

from mooncake.store import MooncakeDistributedStore, ReplicateConfig

from scripts.clients.dummy_client_rw import wait_for_dummy_ready
from scripts.clients.real_client_rw import apply_replication_config, setup_store


REDIS_URL = os.environ["MC_STORE_RS_LOCAL_HOT_CACHE_E2E_REDIS_URL"]
KEYSPACE = os.environ["MC_STORE_RS_LOCAL_HOT_CACHE_E2E_KEYSPACE"] + "/dummy"
SCRATCH_BYTES = int(os.environ["MC_STORE_RS_LOCAL_HOT_CACHE_E2E_SCRATCH_BYTES"])
DUMMY_ADDR = os.environ["MC_STORE_RS_LOCAL_HOT_CACHE_E2E_DUMMY_ADDR"]
WRITER_HOST = os.environ["MC_STORE_RS_LOCAL_HOT_CACHE_E2E_DUMMY_WRITER_HOST"]


def open_writer():
    store = MooncakeDistributedStore()
    status = setup_store(
        store,
        local_hostname=WRITER_HOST,
        metadata_url=REDIS_URL,
        storage_bytes=0,
        scratch_bytes=SCRATCH_BYTES,
        protocol="tcp",
        device_names="",
        master_addr="",
        stable_id=f"hot-cache-dummy-writer-{int(time.time() * 1000)}",
        state="active",
        tenant="default",
        labels={"pool": "hot-cache", "storage": "false"},
        routed_writes=True,
        replica_num=1,
        keyspace=KEYSPACE,
        transport_metadata_url=None,
        transport_rpc_port=None,
        route_control="embedded_wrh",
        route_topk=2,
        transport_backend="classic-te",
    )
    if status != 0:
        raise AssertionError(f"writer setup failed: status={status}")
    return store


def connect_dummy(name: str):
    deadline = time.time() + 20.0
    last_error = None
    while time.time() < deadline:
        store = MooncakeDistributedStore()
        try:
            status = int(store.setup_dummy(128 * 1024 * 1024, SCRATCH_BYTES, DUMMY_ADDR))
            if status != 0:
                raise RuntimeError(f"setup_dummy status={status}")
            wait_for_dummy_ready(store, timeout_seconds=5.0)
            return store
        except Exception as error:
            last_error = error
            try:
                store.close()
            except Exception:
                pass
            time.sleep(0.2)
    raise AssertionError(f"{name} failed to connect to dummy daemon: {last_error!r}")


def wait_for_dummy_value(store, key: str, expected: bytes, timeout_seconds: float):
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            value = store.get(key)
            if value == expected:
                return
            last_error = AssertionError(f"unexpected value: {value!r}")
        except Exception as error:
            last_error = error
        time.sleep(0.1)
    raise AssertionError(f"dummy get for {key} did not converge: {last_error!r}")


def wait_for_writer_miss(store, key: str, timeout_seconds: float):
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            value = store.get(key)
            if value is None:
                return
            last_error = AssertionError(f"writer still sees value: {value!r}")
        except KeyError:
            return
        except Exception as error:
            last_error = error
        time.sleep(0.1)
    raise AssertionError(f"writer miss for {key} did not converge: {last_error!r}")


writer = None
dummy_one = None
dummy_two = None
try:
    writer = open_writer()
    dummy_one = connect_dummy("dummy-one")
    dummy_two = connect_dummy("dummy-two")

    config = ReplicateConfig()
    apply_replication_config(
        config,
        replica_num=1,
        prefer_local=False,
        with_soft_pin=True,
    )
    key = "phase-b-dummy-shared-hit"
    payload = b"phase-b-shared-hot-cache"

    deadline = time.time() + 10.0
    last_status = None
    while time.time() < deadline:
        last_status = writer.put(key, payload, config=config)
        if last_status == 0:
            break
        time.sleep(0.1)
    else:
        raise AssertionError(f"writer put never succeeded: status={last_status}")

    wait_for_dummy_value(dummy_one, key, payload, timeout_seconds=10.0)
    assert writer.remove(key) == 0
    wait_for_writer_miss(writer, key, timeout_seconds=10.0)
    assert dummy_two.get(key) == payload
    print("phase B ok: second dummy served shared cached bytes after origin delete")
finally:
    if dummy_two is not None:
        dummy_two.close()
    if dummy_one is not None:
        dummy_one.close()
    if writer is not None:
        writer.close()
PY

  kill "${DAEMON_PID}" >/dev/null 2>&1 || true
  wait "${DAEMON_PID}" >/dev/null 2>&1 || true
  unset DAEMON_PID
fi

echo "==> local hot-cache e2e passed"
