#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)

PYTHON_BIN=${PYTHON:-python3}
VENV_DIR=${WHEEL_VENV:-"${REPO_ROOT}/.venv-wheel"}
DIST_DIR=${DIST_DIR:-"${REPO_ROOT}/dist"}
WHEEL_DIR="${DIST_DIR}/wheels"
BIN_DIR="${DIST_DIR}/bin"
UPSTREAM_DIR=${MOONCAKE_UPSTREAM_DIR:-"${REPO_ROOT}/third_party/Mooncake"}
UPSTREAM_BUILD_DIR=${MOONCAKE_UPSTREAM_BUILD_DIR:-"${UPSTREAM_DIR}/build-wheel-compat"}
BUILD_JOBS=${BUILD_JOBS:-$(command -v nproc >/dev/null 2>&1 && nproc || getconf _NPROCESSORS_ONLN || echo 8)}

usage() {
  cat <<'EOF'
Usage: scripts/build-wheel.sh [maturin build args...]

Environment:
  PYTHON                     Python interpreter used to create the build venv
  WHEEL_VENV                 Virtualenv directory for build tools
  DIST_DIR                   Output directory for wheel and binary artifacts
  MOONCAKE_UPSTREAM_DIR      Mooncake upstream submodule path
  MOONCAKE_UPSTREAM_BUILD_DIR  Upstream build directory used for engine/CLI assets
  BUILD_JOBS                 Parallel jobs for CMake builds

Examples:
  ./scripts/build-wheel.sh
  ./scripts/build-wheel.sh --interpreter python3.11
  DIST_DIR=artifacts ./scripts/build-wheel.sh --compatibility manylinux_2_28
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
require_command "${PYTHON_BIN}"

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

git -C "${REPO_ROOT}" submodule update --init --recursive

PATH="${VENV_BIN}:${PATH}" cmake \
  -S "${UPSTREAM_DIR}" \
  -B "${UPSTREAM_BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DPython3_EXECUTABLE="${VENV_PYTHON}" \
  -DWITH_TE=ON \
  -DWITH_STORE=ON \
  -DWITH_STORE_RUST=OFF \
  -DBUILD_EXAMPLES=ON \
  -DBUILD_UNIT_TESTS=OFF \
  -DUSE_TENT=ON \
  -DUSE_REDIS=ON \
  -DUSE_HTTP=ON \
  -DUSE_ETCD=OFF \
  -DBUILD_SHARED_LIBS=ON

PATH="${VENV_BIN}:${PATH}" cmake --build "${UPSTREAM_BUILD_DIR}" \
  --target engine mooncake_master mooncake_client transfer_engine_bench tent_shared \
  -j"${BUILD_JOBS}"

export MOONCAKE_UPSTREAM_DIR="${UPSTREAM_DIR}"
export MOONCAKE_UPSTREAM_BUILD_DIR="${UPSTREAM_BUILD_DIR}"

cargo build \
  --manifest-path "${REPO_ROOT}/crates/mooncake-store-py/Cargo.toml" \
  --bin mooncake-store-client \
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

LATEST_WHEEL=$(ls -1t "${WHEEL_DIR}"/*.whl 2>/dev/null | head -n 1 || true)
if [[ -z "${LATEST_WHEEL}" ]]; then
  echo "wheel build completed but no wheel was found in ${WHEEL_DIR}" >&2
  exit 1
fi

"${VENV_PYTHON}" - <<'PY' \
  "${LATEST_WHEEL}" \
  "${REPO_ROOT}" \
  "${UPSTREAM_BUILD_DIR}" \
  "${REPO_ROOT}/target/release/mooncake-store-client"
import base64
import csv
import hashlib
import pathlib
import shutil
import stat
import sys
import tempfile
import zipfile

wheel_path = pathlib.Path(sys.argv[1])
repo_root = pathlib.Path(sys.argv[2])
upstream_build_dir = pathlib.Path(sys.argv[3])
store_client_path = pathlib.Path(sys.argv[4])
upstream_py_dir = repo_root / "third_party" / "Mooncake" / "mooncake-wheel" / "mooncake"

binary_assets = {
    "mooncake-store-client": store_client_path,
    "mooncake_master": upstream_build_dir / "mooncake-store" / "src" / "mooncake_master",
    "mooncake_client": upstream_build_dir / "mooncake-store" / "src" / "mooncake_client",
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
        shutil.copy2(source, package_root / name)

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

cat <<EOF
wheel:  ${LATEST_WHEEL}
client: ${BIN_DIR}/mooncake-store-client
EOF
