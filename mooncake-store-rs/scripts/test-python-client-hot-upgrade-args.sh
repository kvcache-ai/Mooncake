#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)

usage() {
  cat <<'EOF'
Usage: scripts/test-python-client-hot-upgrade-args.sh

Verify Python hot-upgrade startup argument handling for both the native PyO3
binding and the pure-Python wrapper compatibility path.

Environment:
  MOONCAKE_UPSTREAM_DIR       Mooncake upstream submodule path
  MOONCAKE_UPSTREAM_BUILD_DIR Explicit upstream build directory override
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

require_cargo() {
  if command -v cargo >/dev/null 2>&1; then
    return 0
  fi

  local cargo_env="${CARGO_HOME:-${HOME}/.cargo}/env"
  if [[ -f "${cargo_env}" ]]; then
    # shellcheck disable=SC1090
    source "${cargo_env}"
    return 0
  fi

  echo "cargo not found in PATH and ${cargo_env} is missing" >&2
  exit 1
}

require_python() {
  if command -v python3 >/dev/null 2>&1; then
    return 0
  fi

  echo "python3 is required for wrapper verification" >&2
  exit 1
}

list_upstream_dirs() {
  local primary_worktree

  if [[ -n "${MOONCAKE_UPSTREAM_DIR:-}" ]]; then
    printf '%s\n' "${MOONCAKE_UPSTREAM_DIR}"
  fi
  printf '%s\n' "${REPO_ROOT}/third_party/Mooncake"

  if primary_worktree=$(git -C "${REPO_ROOT}" worktree list --porcelain 2>/dev/null | awk '/^worktree / { print substr($0, 10); exit }'); then
    if [[ -n "${primary_worktree}" && "${primary_worktree}" != "${REPO_ROOT}" ]]; then
      printf '%s\n' "${primary_worktree}/third_party/Mooncake"
    fi
  fi
}

resolve_upstream_build_dir() {
  local candidates=()
  local candidate
  local upstream_dir

  if [[ -n "${MOONCAKE_UPSTREAM_BUILD_DIR:-}" ]]; then
    candidates+=("${MOONCAKE_UPSTREAM_BUILD_DIR}")
  fi
  while IFS= read -r upstream_dir; do
    [[ -z "${upstream_dir}" ]] && continue
    candidates+=(
      "${upstream_dir}/build-rust"
      "${upstream_dir}/build-wheel-compat"
    )
  done < <(list_upstream_dirs)

  for candidate in "${candidates[@]}"; do
    if [[ -f "${candidate}/mooncake-transfer-engine/src/libtransfer_engine.so" ]] \
      && [[ -f "${candidate}/mooncake-transfer-engine/tent/src/libtent_shared.so" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  echo "unable to find Mooncake runtime libraries under any known Mooncake upstream tree" >&2
  echo "checked candidates:" >&2
  printf '  %s\n' "${candidates[@]}" >&2
  echo "set MOONCAKE_UPSTREAM_BUILD_DIR to a built upstream directory" >&2
  exit 1
}

require_cargo
require_python
UPSTREAM_BUILD_DIR=$(resolve_upstream_build_dir)
UPSTREAM_DIR=$(cd -- "${UPSTREAM_BUILD_DIR}/.." && pwd)
export MOONCAKE_UPSTREAM_DIR="${UPSTREAM_DIR}"
export MOONCAKE_UPSTREAM_BUILD_DIR="${UPSTREAM_BUILD_DIR}"
export LD_LIBRARY_PATH="${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/src:${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/tent/src:${LD_LIBRARY_PATH:-}"

cd "${REPO_ROOT}"

echo "==> testing Python native setup hot-upgrade parsers"
cargo test -p mooncake-store-py python_setup_parsers

echo "==> testing Python wrapper forwarding compatibility"
REPO_ROOT="${REPO_ROOT}" python3 - <<'PY'
import importlib.util
import os
import pathlib
import sys
import threading
import types

repo_root = pathlib.Path(os.environ["REPO_ROOT"])
package_dir = repo_root / "python" / "mooncake"

pkg = types.ModuleType("mooncake")
pkg.__path__ = [str(package_dir)]
sys.modules["mooncake"] = pkg

runtime = types.ModuleType("mooncake._runtime")
runtime.package_dir = lambda: package_dir
runtime.preload_native_libraries = lambda root: None
sys.modules["mooncake._runtime"] = runtime

native = types.ModuleType("mooncake._store_rs")
native.MooncakeDistributedStore = lambda: object()
native.MooncakeHostMemAllocator = lambda *args, **kwargs: None
native.init_tracing = lambda *args, **kwargs: 0
native.metrics_text = lambda: ""
native.start_metrics_server = lambda bind_addr="127.0.0.1:0": bind_addr
native.stop_metrics_server = lambda: None
native.metrics_server_address = lambda: None
sys.modules["mooncake._store_rs"] = native

spec = importlib.util.spec_from_file_location("mooncake.store", package_dir / "store.py")
module = importlib.util.module_from_spec(spec)
sys.modules["mooncake.store"] = module
assert spec.loader is not None
spec.loader.exec_module(module)

class FakeWorker:
    def __init__(self):
        self.calls = []

    def call(self, name, *args, **kwargs):
        self.calls.append((name, args, kwargs))
        return 0

store = module.MooncakeDistributedStore.__new__(module.MooncakeDistributedStore)
store._worker = FakeWorker()
store._lock = threading.RLock()
store._registered_buffers = {}
store._tracked_keys = set()

store.setup(
    "127.0.0.1",
    "redis://127.0.0.1:6379/0",
    1024,
    512,
    stable_id="py-store-a",
    epoch=3,
    initial_state="standby",
)
name, args, kwargs = store._worker.calls.pop()
assert name == "setup"
assert kwargs["stable_id"] == "py-store-a"
assert kwargs["epoch"] == 3
assert kwargs["initial_state"] == "standby"

store.setup(
    "127.0.0.1",
    "redis://127.0.0.1:6379/0",
    1024,
    512,
    stable_id="py-store-b",
    state="draining",
)
name, args, kwargs = store._worker.calls.pop()
assert kwargs["stable_id"] == "py-store-b"
assert kwargs["initial_state"] == "draining"
assert "state" not in kwargs

store.setup(
    "127.0.0.1:17111",
    "redis://127.0.0.1:6379/0",
    1024,
    512,
    stable_id="py-store-port-a",
)
name, args, kwargs = store._worker.calls.pop()
assert args[0] == "127.0.0.1"
assert kwargs["transport_rpc_port"] == 17111

store.setup(
    {
        "local_hostname": "127.0.0.1",
        "metadata_url": "redis://127.0.0.1:6379/0",
        "stable_id": "py-store-c",
        "epoch": 4,
        "initial_state": "standby",
        "global_segment_size": 2048,
        "local_buffer_size": 1024,
    }
)
name, args, kwargs = store._worker.calls.pop()
assert name == "setup"
assert kwargs["stable_id"] == "py-store-c"
assert kwargs["epoch"] == 4
assert kwargs["initial_state"] == "standby"

store.setup(
    {
        "local_hostname": "127.0.0.1",
        "metadata_server": "redis://127.0.0.1:6379/0",
        "stable_id": "py-store-d",
        "state": "offline",
    }
)
name, args, kwargs = store._worker.calls.pop()
assert kwargs["stable_id"] == "py-store-d"
assert kwargs["epoch"] == 1
assert kwargs["initial_state"] == "offline"

store.setup(
    {
        "local_hostname": "node-a:17112",
        "metadata_server": "redis://127.0.0.1:6379/0",
        "stable_id": "py-store-e",
    }
)
name, args, kwargs = store._worker.calls.pop()
assert args[0] == "node-a"
assert kwargs["transport_rpc_port"] == 17112

setup_env_vars = [
    "MC_STORE_RS_STABLE_ID",
    "MC_STORE_RS_EPOCH",
    "MC_STORE_RS_INITIAL_STATE",
    "MC_STORE_RS_TENANT",
    "MC_STORE_RS_ROUTED_WRITES",
    "MC_STORE_RS_REPLICA_COUNT",
    "MC_STORE_RS_ROUTE_TOPK",
    "MC_STORE_RS_KEYSPACE",
    "MC_STORE_RS_TRANSPORT_METADATA_URL",
    "MC_STORE_RS_TRANSPORT_RPC_PORT",
    "MC_STORE_RS_TRANSPORT_BACKEND",
    "MC_STORE_RS_LOCAL_SEGMENT_NAME",
    "MC_STORE_RS_EXPIRES_AT_MS",
    "MC_STORE_RS_ROUTE_CONTROL",
    "MC_STORE_RS_LABELS",
]

def clear_setup_env():
    for env_name in setup_env_vars:
        os.environ.pop(env_name, None)

try:
    clear_setup_env()
    os.environ.update(
        {
            "MC_STORE_RS_STABLE_ID": "env-store",
            "MC_STORE_RS_EPOCH": "7",
            "MC_STORE_RS_INITIAL_STATE": "standby",
            "MC_STORE_RS_TENANT": "tenant-env",
            "MC_STORE_RS_ROUTED_WRITES": "1",
            "MC_STORE_RS_REPLICA_COUNT": "3",
            "MC_STORE_RS_ROUTE_TOPK": "5",
            "MC_STORE_RS_KEYSPACE": "env/keyspace",
            "MC_STORE_RS_TRANSPORT_METADATA_URL": "redis://127.0.0.1:6380/1",
            "MC_STORE_RS_TRANSPORT_RPC_PORT": "17113",
            "MC_STORE_RS_TRANSPORT_BACKEND": "classic_te",
            "MC_STORE_RS_LOCAL_SEGMENT_NAME": "env-segment",
            "MC_STORE_RS_EXPIRES_AT_MS": "12345",
            "MC_STORE_RS_ROUTE_CONTROL": "embedded_wrh",
            "MC_STORE_RS_LABELS": "pool=env,storage=false",
        }
    )
    store.setup(
        "127.0.0.1",
        "redis://127.0.0.1:6379/0",
        0,
        512,
    )
    name, args, kwargs = store._worker.calls.pop()
    assert kwargs["stable_id"] == "env-store"
    assert kwargs["epoch"] == 7
    assert kwargs["initial_state"] == "standby"
    assert kwargs["tenant"] == "tenant-env"
    assert kwargs["routed_writes"] is True
    assert kwargs["replica_count"] == 3
    assert kwargs["route_topk"] == 5
    assert kwargs["keyspace"] == "env/keyspace"
    assert kwargs["transport_metadata_url"] == "redis://127.0.0.1:6380/1"
    assert kwargs["transport_rpc_port"] == 17113
    assert kwargs["transport_backend"] == "classic_te"
    assert kwargs["local_segment_name"] == "env-segment"
    assert kwargs["expires_at_ms"] == 12345
    assert kwargs["route_control"] == "embedded_wrh"
    assert kwargs["labels"] == {"pool": "env", "storage": "false"}

    os.environ["MC_STORE_RS_LABELS"] = '{"pool": "json", "storage": "true"}'
    store.setup(
        {
            "local_hostname": "127.0.0.1",
            "metadata_server": "redis://127.0.0.1:6379/0",
            "state": "offline",
            "rpc_server_port": 17114,
            "labels": {"pool": "explicit"},
        }
    )
    name, args, kwargs = store._worker.calls.pop()
    assert kwargs["initial_state"] == "offline"
    assert kwargs["transport_rpc_port"] == 17114
    assert kwargs["labels"] == {"pool": "explicit"}
    assert kwargs["route_topk"] == 5
finally:
    clear_setup_env()

try:
    store.setup(
        "127.0.0.1:17111",
        "redis://127.0.0.1:6379/0",
        1024,
        512,
        transport_rpc_port=17112,
    )
except ValueError:
    pass
else:
    raise AssertionError("conflicting transport ports should be rejected")
PY
