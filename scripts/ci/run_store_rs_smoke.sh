#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=${MOONCAKE_ROOT_DIR:?MOONCAKE_ROOT_DIR must name the Mooncake source root}
STORE_RS_DIR=${MOONCAKE_STORE_RS_DIR:?MOONCAKE_STORE_RS_DIR must name the Store-RS source directory}
BUILD_DIR=${MOONCAKE_BUILD_DIR:?MOONCAKE_BUILD_DIR must name the configured CMake build directory}
PYTHON_PREFIX=${MOONCAKE_PYTHON_PREFIX:?MOONCAKE_PYTHON_PREFIX must name the CMake Python component install prefix}
CLASSIC_SHIM=${MOONCAKE_CLASSIC_SHIM_LIB_PATH:?MOONCAKE_CLASSIC_SHIM_LIB_PATH must name the CMake-built classic TE shim}
TENT_SHIM=${MOONCAKE_TENT_SHIM_LIB_PATH:?MOONCAKE_TENT_SHIM_LIB_PATH must name the CMake-built TENT shim}
BUILD_PYTHON_BIN=${MOONCAKE_PYTHON_BIN:-python3}

for path in "${ROOT_DIR}" "${STORE_RS_DIR}" "${BUILD_DIR}" "${PYTHON_PREFIX}"; do
  [[ "${path}" == /* && -d "${path}" ]] || {
    echo "expected an absolute existing directory: ${path}" >&2
    exit 1
  }
done
for path in "${CLASSIC_SHIM}" "${TENT_SHIM}"; do
  [[ "${path}" == /* && -f "${path}" ]] || {
    echo "expected an absolute existing shim library: ${path}" >&2
    exit 1
  }
done
command -v "${BUILD_PYTHON_BIN}" >/dev/null || {
  echo "MOONCAKE_PYTHON_BIN is not executable from PATH: ${BUILD_PYTHON_BIN}" >&2
  exit 1
}
BUILD_PYTHON_BIN=$(command -v "${BUILD_PYTHON_BIN}")

PACKAGE_DIR="${PYTHON_PREFIX}/mooncake"
for path in \
  "${PACKAGE_DIR}/__init__.py" \
  "${PACKAGE_DIR}/store/__init__.py" \
  "${PACKAGE_DIR}/store/rs/store.py" \
  "${PACKAGE_DIR}/mooncake-store-rs-client" \
  "${PACKAGE_DIR}/mooncake-store-rs-admin" \
  "${PACKAGE_DIR}/mooncake-store-rs-bench"; do
  [[ -f "${path}" ]] || {
    echo "CMake Python component install is missing ${path}" >&2
    exit 1
  }
done
[[ -x "${PACKAGE_DIR}/mooncake-store-rs-client" && \
   -x "${PACKAGE_DIR}/mooncake-store-rs-admin" && \
   -x "${PACKAGE_DIR}/mooncake-store-rs-bench" ]] || {
  echo "CMake Python component install is missing executable Store-RS commands" >&2
  exit 1
}

shopt -s nullglob
CPP_EXTENSIONS=("${PACKAGE_DIR}"/_store.cpython-*.so)
RS_EXTENSIONS=("${PACKAGE_DIR}"/_store_rs.*.so)
[[ "${#CPP_EXTENSIONS[@]}" -eq 1 ]] || {
  echo "expected one installed C++ Store extension, found ${#CPP_EXTENSIONS[@]}" >&2
  exit 1
}
[[ "${#RS_EXTENSIONS[@]}" -eq 1 ]] || {
  echo "expected one installed Store-RS extension, found ${#RS_EXTENSIONS[@]}" >&2
  exit 1
}

echo "==> Store-RS Rust unit tests"
(
  export MOONCAKE_ROOT_DIR="${ROOT_DIR}"
  export MOONCAKE_STORE_RS_DIR="${STORE_RS_DIR}"
  export MOONCAKE_BUILD_DIR="${BUILD_DIR}"
  export MOONCAKE_CLASSIC_SHIM_LIB_PATH="${CLASSIC_SHIM}"
  export MOONCAKE_TENT_SHIM_LIB_PATH="${TENT_SHIM}"
  export PYO3_PYTHON="${BUILD_PYTHON_BIN}"
  export CARGO_TARGET_DIR="${BUILD_DIR}/mooncake-store-rs/cargo-tests"

  # shellcheck source=mooncake-store-rs/scripts/lib/common.sh
  source "${STORE_RS_DIR}/scripts/lib/common.sh"
  mc_scripts_setup_upstream_runtime_env none
  cargo test \
    --manifest-path "${STORE_RS_DIR}/Cargo.toml" \
    --workspace --lib --bins --release
)

TEMP_ROOT=${RUNNER_TEMP:-${TMPDIR:-/tmp}}
mkdir -p "${TEMP_ROOT}"
VENV_DIR=$(mktemp -d "${TEMP_ROOT}/store-rs-smoke.XXXXXX")
trap 'rm -rf "${VENV_DIR}"' EXIT

"${BUILD_PYTHON_BIN}" -m venv "${VENV_DIR}"
PYTHON_BIN="${VENV_DIR}/bin/python"
"${PYTHON_BIN}" -m pip install --disable-pip-version-check aiohttp msgpack numpy requests
SITE_PACKAGES=$("${PYTHON_BIN}" -I -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
mkdir -p "${SITE_PACKAGES}/mooncake"
cp -a "${PACKAGE_DIR}/." "${SITE_PACKAGES}/mooncake/"
INSTALLED_PACKAGE_DIR="${SITE_PACKAGES}/mooncake"

# Run against the clean CMake install. Build-tree loader and import paths must
# not supply native libraries or Python modules to these checks.
unset PYTHONPATH LD_LIBRARY_PATH LD_PRELOAD
unset MOONCAKE_BUILD_DIR MOONCAKE_CLASSIC_SHIM_LIB_PATH MOONCAKE_TENT_SHIM_LIB_PATH
unset MOONCAKE_CLASSIC_TE_LIB_PATH MOONCAKE_TENT_LIB_PATH MOONCAKE_SKIP_NATIVE_BUILD
unset MOONCAKE_ROOT_DIR MOONCAKE_STORE_RS_DIR
export PYTHONNOUSERSITE=1
export PATH="${VENV_DIR}/bin:${PATH}"

env -u MOONCAKE_STORE_BACKEND "${PYTHON_BIN}" -I - <<'PY'
from pathlib import Path
import sys

import mooncake
import mooncake.store as store
import mooncake._store as native

assert Path(mooncake.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
assert store._BACKEND == "cpp"
assert store.MooncakeDistributedStore is native.MooncakeDistributedStore
print("default facade selects the installed C++ Store extension")
PY

MOONCAKE_STORE_BACKEND=rs "${PYTHON_BIN}" -I - <<'PY'
import mooncake.store as store
import mooncake._store_rs as native

assert store._BACKEND == "rs"
assert store.BufferPool is native.BufferPool
assert store.MooncakeDistributedStore is store.rs.store.MooncakeDistributedStore
print("MOONCAKE_STORE_BACKEND=rs selects the installed Store-RS extension")
PY

for command in \
  "${INSTALLED_PACKAGE_DIR}/mooncake-store-rs-client" \
  "${INSTALLED_PACKAGE_DIR}/mooncake-store-rs-admin" \
  "${INSTALLED_PACKAGE_DIR}/mooncake-store-rs-bench"; do
  "${command}" --version
  "${command}" --help >/dev/null
done

REDIS_PORT=$("${PYTHON_BIN}" -I - <<'PY'
import socket

with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)

echo "==> Store-RS installed-package dummy and TCP/classic-TE read/write smoke"
env \
  MOONCAKE_ROOT_DIR="${ROOT_DIR}" \
  MOONCAKE_STORE_RS_DIR="${STORE_RS_DIR}" \
  MOONCAKE_PYTHON_BIN="${PYTHON_BIN}" \
  MC_STORE_RS_CLIENT_RW_BIN="${INSTALLED_PACKAGE_DIR}/mooncake-store-rs-client" \
  MC_STORE_RS_REDIS_PORT="${REDIS_PORT}" \
  MC_STORE_RS_TRANSPORT_METADATA_URL="redis://127.0.0.1:${REDIS_PORT}/0" \
  MC_STORE_RS_TRANSPORT_BACKEND=classic-te \
  MC_STORE_RS_TEST_PROTOCOL=tcp \
  MC_STORE_RS_ROUTE_CONTROL=embedded-wrh \
  MC_STORE_RS_ROUTE_TOPK=2 \
  bash "${STORE_RS_DIR}/scripts/tests/client/test-client-rw-cli.sh" all
