#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)

PYTHON_BIN=${PYTHON:-python3}
VENV_DIR=${WHEEL_VENV:-"${REPO_ROOT}/.venv-wheel"}
DIST_DIR=${DIST_DIR:-"${REPO_ROOT}/dist"}
WHEEL_DIR="${DIST_DIR}/wheels"
BIN_DIR="${DIST_DIR}/bin"
UPSTREAM_DIR=${MOONCAKE_UPSTREAM_DIR:-"${REPO_ROOT}/third_party/Mooncake"}
UPSTREAM_BUILD_DIR=${MOONCAKE_UPSTREAM_BUILD_DIR:-"${UPSTREAM_DIR}/build-wheel-compat"}
YALANTINGLIBS_PREFIX=${YALANTINGLIBS_PREFIX:-"${UPSTREAM_BUILD_DIR}/yalantinglibs-install"}
BUILD_JOBS=${BUILD_JOBS:-$(command -v nproc >/dev/null 2>&1 && nproc || getconf _NPROCESSORS_ONLN || echo 8)}

usage() {
  cat <<'EOF'
Usage: scripts/build/build-wheel.sh [maturin build args...]

Environment:
  PYTHON                     Python interpreter used to create the build venv
  WHEEL_VENV                 Virtualenv directory for build tools
  DIST_DIR                   Output directory for wheel and binary artifacts
  MOONCAKE_UPSTREAM_DIR      Mooncake upstream submodule path
  MOONCAKE_UPSTREAM_BUILD_DIR  Upstream build directory used for engine/CLI assets
  YALANTINGLIBS_PREFIX       Install prefix for bundled yalantinglibs
  BUILD_JOBS                 Parallel jobs for CMake builds

Examples:
  ./scripts/build/build-wheel.sh
  ./scripts/build/build-wheel.sh --interpreter python3.11
  DIST_DIR=artifacts ./scripts/build/build-wheel.sh --compatibility manylinux_2_28
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

MATURIN_ARGS=("$@")
HAS_COMPATIBILITY=0
for arg in "${MATURIN_ARGS[@]}"; do
  if [[ "${arg}" == "--compatibility" || "${arg}" == --compatibility=* ]]; then
    HAS_COMPATIBILITY=1
    break
  fi
done
if [[ ${HAS_COMPATIBILITY} -eq 0 ]]; then
  MATURIN_ARGS=(--compatibility linux "${MATURIN_ARGS[@]}")
fi

require_command() {
  local cmd=$1
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "missing required command: ${cmd}" >&2
    exit 1
  fi
}

require_command git
require_command cargo
require_command cmake
require_command patchelf
require_command "${PYTHON_BIN}"

ensure_yalantinglibs() {
  local source_dir="${UPSTREAM_DIR}/extern/yalantinglibs"
  local build_dir="${UPSTREAM_BUILD_DIR}/yalantinglibs-build"
  local config_file="${YALANTINGLIBS_PREFIX}/lib/cmake/yalantinglibs/yalantinglibsConfig.cmake"
  local header_file="${YALANTINGLIBS_PREFIX}/include/ylt/easylog.hpp"

  if [[ -f "${config_file}" && -f "${header_file}" ]]; then
    return 0
  fi
  if [[ ! -d "${source_dir}" ]]; then
    echo "missing yalantinglibs source: ${source_dir}" >&2
    exit 1
  fi

  cmake \
    -S "${source_dir}" \
    -B "${build_dir}" \
    -DCMAKE_INSTALL_PREFIX="${YALANTINGLIBS_PREFIX}" \
    -DBUILD_EXAMPLES=OFF \
    -DBUILD_BENCHMARK=OFF \
    -DBUILD_UNIT_TESTS=OFF
  cmake --build "${build_dir}" -j"${BUILD_JOBS}"
  cmake --install "${build_dir}"
}

mkdir -p "${WHEEL_DIR}" "${BIN_DIR}"

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

VENV_PYTHON="${VENV_DIR}/bin/python"
VENV_BIN="${VENV_DIR}/bin"
if [[ ! -x "${VENV_BIN}/python3" ]]; then
  ln -sf "${VENV_PYTHON}" "${VENV_BIN}/python3"
fi

"${VENV_PYTHON}" -m pip install --upgrade pip >/dev/null
if ! "${VENV_PYTHON}" -m pip show maturin >/dev/null 2>&1; then
  "${VENV_PYTHON}" -m pip install "maturin>=1.7,<2"
fi
if ! "${VENV_PYTHON}" -m pip show build >/dev/null 2>&1; then
  "${VENV_PYTHON}" -m pip install "build>=1.2,<2"
fi

git -C "${REPO_ROOT}" submodule update --init --recursive
ensure_yalantinglibs
export CPATH="${YALANTINGLIBS_PREFIX}/include${CPATH:+:${CPATH}}"

PATH="${VENV_BIN}:${PATH}" cmake \
  -S "${UPSTREAM_DIR}" \
  -B "${UPSTREAM_BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH="${YALANTINGLIBS_PREFIX}" \
  -Dyalantinglibs_DIR="${YALANTINGLIBS_PREFIX}/lib/cmake/yalantinglibs" \
  -DPython3_EXECUTABLE="${VENV_PYTHON}" \
  -DWITH_TE=ON \
  -DWITH_STORE=OFF \
  -DWITH_STORE_RUST=OFF \
  -DBUILD_EXAMPLES=ON \
  -DBUILD_UNIT_TESTS=OFF \
  -DUSE_TENT=ON \
  -DUSE_REDIS=ON \
  -DUSE_HTTP=ON \
  -DUSE_ETCD=OFF \
  -DBUILD_SHARED_LIBS=ON

PATH="${VENV_BIN}:${PATH}" cmake --build "${UPSTREAM_BUILD_DIR}" \
  --target engine transfer_engine_bench tent_shared \
  -j"${BUILD_JOBS}"

export MOONCAKE_UPSTREAM_DIR="${UPSTREAM_DIR}"
export MOONCAKE_UPSTREAM_BUILD_DIR="${UPSTREAM_BUILD_DIR}"

cargo build \
  --manifest-path "${REPO_ROOT}/crates/mooncake-store-py/Cargo.toml" \
  --bins \
  --release

"${VENV_DIR}/bin/maturin" build \
  --release \
  --manifest-path "${REPO_ROOT}/crates/mooncake-store-py/Cargo.toml" \
  --interpreter "${VENV_PYTHON}" \
  --out "${WHEEL_DIR}" \
  "${MATURIN_ARGS[@]}"

install -m 0755 \
  "${REPO_ROOT}/target/release/mooncake-store-client" \
  "${BIN_DIR}/mooncake-store-client"
install -m 0755 \
  "${REPO_ROOT}/target/release/mooncake-store-admin" \
  "${BIN_DIR}/mooncake-store-admin"

LATEST_WHEEL=$(ls -1t "${WHEEL_DIR}"/*.whl 2>/dev/null | head -n 1 || true)
if [[ -z "${LATEST_WHEEL}" ]]; then
  echo "wheel build completed but no wheel was found in ${WHEEL_DIR}" >&2
  exit 1
fi

"${VENV_PYTHON}" - <<'PY' \
  "${LATEST_WHEEL}" \
  "${REPO_ROOT}" \
  "${UPSTREAM_BUILD_DIR}" \
  "${REPO_ROOT}/target/release/build" \
  "${REPO_ROOT}/target/release/mooncake-store-client" \
  "${REPO_ROOT}/target/release/mooncake-store-admin"
import base64
import csv
import hashlib
import pathlib
import shutil
import stat
import subprocess
import sys
import tempfile
import zipfile

wheel_path = pathlib.Path(sys.argv[1])
repo_root = pathlib.Path(sys.argv[2])
upstream_build_dir = pathlib.Path(sys.argv[3])
transport_build_dir = pathlib.Path(sys.argv[4])
store_client_path = pathlib.Path(sys.argv[5])
store_admin_path = pathlib.Path(sys.argv[6])
upstream_py_dir = repo_root / "third_party" / "Mooncake" / "mooncake-wheel" / "mooncake"
transport_shim_out_dirs = sorted(transport_build_dir.glob("mooncake-transport-sys-*/out"))

binary_assets = {
    "mooncake-store-client": store_client_path,
    "mooncake-store-admin": store_admin_path,
    "transfer_engine_bench": upstream_build_dir
    / "mooncake-transfer-engine"
    / "example"
    / "transfer_engine_bench",
}

library_assets = {
    "engine.so": sorted((upstream_build_dir / "mooncake-integration").glob("engine*.so")),
    "libasio.so": [upstream_build_dir / "mooncake-asio" / "libasio.so"],
    "libtransfer_engine.so": [
        upstream_build_dir / "mooncake-transfer-engine" / "src" / "libtransfer_engine.so"
    ],
    "libtent_shared.so": [
        upstream_build_dir / "mooncake-transfer-engine" / "tent" / "src" / "libtent_shared.so"
    ],
    "libmooncake_classic_shim.so": [
        shim_dir / "libmooncake_classic_shim.so" for shim_dir in transport_shim_out_dirs
    ],
    "libmooncake_tent_shim.so": [
        shim_dir / "libmooncake_tent_shim.so" for shim_dir in transport_shim_out_dirs
    ],
}

relative_rpath_assets = {
    "engine.so",
    "libtransfer_engine.so",
    "libtent_shared.so",
    "libmooncake_classic_shim.so",
    "libmooncake_tent_shim.so",
}

python_assets = [
    "http_metadata_server.py",
    "mooncake_config.py",
    "mooncake_connector_v1.py",
    "mooncake_ep_buffer.py",
    "mooncake_store_service.py",
    "transfer_engine_topology_dump.py",
    "vllm_v1_proxy_server.py",
    "ep.py",
    "pg.py",
]


def first_existing(paths):
    for path in paths:
        if path.exists():
            return path
    raise FileNotFoundError(", ".join(str(path) for path in paths))


def set_relative_rpath(path):
    subprocess.run(
        ["patchelf", "--set-rpath", "$ORIGIN", str(path)],
        check=True,
    )


with tempfile.TemporaryDirectory(prefix="mooncake-wheel-") as temp_dir:
    root = pathlib.Path(temp_dir)
    with zipfile.ZipFile(wheel_path) as source_wheel:
        source_wheel.extractall(root)

    package_root = root / "mooncake"
    package_root.mkdir(parents=True, exist_ok=True)

    for name, source in binary_assets.items():
        target = package_root / name
        shutil.copy2(source, target)
        target.chmod(target.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    for name, candidates in library_assets.items():
        source = first_existing(candidates)
        target = package_root / name
        shutil.copy2(source, target)
        if name in relative_rpath_assets:
            set_relative_rpath(target)

    for name in python_assets:
        shutil.copy2(upstream_py_dir / name, package_root / name)

    dist_info = next(root.glob("*.dist-info"))
    record_path = dist_info / "RECORD"
    rows = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if path == record_path:
            rows.append((relative, "", ""))
            continue
        payload = path.read_bytes()
        digest = base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).decode().rstrip("=")
        rows.append((relative, f"sha256={digest}", str(len(payload))))

    with record_path.open("w", newline="") as record_file:
        csv.writer(record_file, lineterminator="\n").writerows(rows)

    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as target_wheel:
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(root).as_posix()
            info = zipfile.ZipInfo.from_file(path, arcname=relative)
            info.compress_type = zipfile.ZIP_DEFLATED
            target_wheel.writestr(info, path.read_bytes())
PY

readarray -t VERSION_INFO < <("${VENV_PYTHON}" - <<'PY' "${REPO_ROOT}/pyproject.toml"
import pathlib
import sys
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

pyproject = pathlib.Path(sys.argv[1])
data = tomllib.loads(pyproject.read_text())
core_version = data["project"]["version"]
meta_version = core_version.split("+", 1)[0]
print(core_version)
print(meta_version)
PY
)
MOONCAKE_CORE_VERSION=${VERSION_INFO[0]}
MOONCAKE_META_VERSION=${VERSION_INFO[1]}

env \
  MOONCAKE_CORE_VERSION="${MOONCAKE_CORE_VERSION}" \
  MOONCAKE_META_VERSION="${MOONCAKE_META_VERSION}" \
  "${VENV_PYTHON}" -m build \
  --wheel \
  --outdir "${WHEEL_DIR}" \
  "${REPO_ROOT}/packages/mooncake-pro"

LATEST_META_WHEEL=$(ls -1t "${WHEEL_DIR}"/mooncake_pro-*.whl 2>/dev/null | head -n 1 || true)

cat <<EOF
wheel:  ${LATEST_WHEEL}
meta:   ${LATEST_META_WHEEL}
client: ${BIN_DIR}/mooncake-store-client
admin:  ${BIN_DIR}/mooncake-store-admin
EOF
