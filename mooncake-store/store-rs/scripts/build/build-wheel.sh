#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
# store-rs is a subdirectory when it lives inside the Mooncake monorepo,
# where the git toplevel is the enclosing repository rather than this tree.
[ -f "${REPO_ROOT}/Cargo.toml" ] || REPO_ROOT="${REPO_ROOT}/mooncake-store/store-rs"

# ── Timing helpers (only print when CI=true or TIMING=1) ──
_WHEEL_TIMING=${TIMING:-${CI:-0}}
_timer_start() { date +%s; }
_timer_elapsed() {
  local start=$1 label=$2
  local elapsed=$(( $(date +%s) - start ))
  if [[ "${_WHEEL_TIMING}" == "true" || "${_WHEEL_TIMING}" == "1" ]]; then
    echo ">>> [TIMING] build-wheel: ${label}: ${elapsed}s"
  fi
}
_WHEEL_GLOBAL_START=$(_timer_start)

PYTHON_BIN=${PYTHON:-python3}
VENV_DIR=${WHEEL_VENV:-"${REPO_ROOT}/.venv-wheel"}
DIST_DIR=${DIST_DIR:-"${REPO_ROOT}/dist"}
WHEEL_DIR="${DIST_DIR}/wheels"
BIN_DIR="${DIST_DIR}/bin"
UPSTREAM_DIR=${MOONCAKE_UPSTREAM_DIR:-"${REPO_ROOT}/third_party/Mooncake"}
UPSTREAM_BUILD_DIR=${MOONCAKE_UPSTREAM_BUILD_DIR:-"${UPSTREAM_DIR}/build-wheel-compat"}
YALANTINGLIBS_PREFIX=${YALANTINGLIBS_PREFIX:-"${UPSTREAM_BUILD_DIR}/yalantinglibs-install"}
BUILD_JOBS=${BUILD_JOBS:-$(command -v nproc >/dev/null 2>&1 && nproc || getconf _NPROCESSORS_ONLN || echo 8)}
BUILD_GIT_BRANCH=${MC_BUILD_GIT_BRANCH:-$(git -C "${REPO_ROOT}" rev-parse --abbrev-ref HEAD)}
BUILD_GIT_COMMIT=${MC_BUILD_GIT_COMMIT:-$(git -C "${REPO_ROOT}" rev-parse HEAD)}
BUILD_TIME=${MC_BUILD_TIME:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}

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
  YALANTINGLIBS_PREBUILT_DIR Prebuilt yalantinglibs directory (if set, skip build and copy from here)
  PYBIND11_PREBUILT_DIR      Prebuilt pybind11 directory (if set, use instead of submodule)
  MOONCAKE_REUSE_NATIVE_ARTIFACTS
                             Reuse existing native assets in MOONCAKE_UPSTREAM_BUILD_DIR
                             instead of configuring/building upstream CMake targets
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

is_truthy() {
  case "${1:-}" in
    1 | true | TRUE | yes | YES | on | ON)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

AUDITWHEEL_EXCLUDES=(
  --exclude "libcurl.so*"
  --exclude "libibverbs.so*"
  --exclude "libmlx5.so*"
  --exclude "libnuma.so*"
  --exclude "libstdc++.so*"
  --exclude "libgcc_s.so*"
  --exclude "libc.so*"
  --exclude "libnghttp2.so*"
  --exclude "libidn2.so*"
  --exclude "librtmp.so*"
  --exclude "libssh.so*"
  --exclude "libpsl.so*"
  --exclude "libssl.so*"
  --exclude "libcrypto.so*"
  --exclude "libgssapi_krb5.so*"
  --exclude "libldap.so*"
  --exclude "liblber.so*"
  --exclude "libbrotlidec.so*"
  --exclude "libz.so*"
  --exclude "libnl-route-3.so*"
  --exclude "libnl-3.so*"
  --exclude "libm.so*"
  --exclude "liblzma.so*"
  --exclude "libunistring.so*"
  --exclude "libgnutls.so*"
  --exclude "libhogweed.so*"
  --exclude "libnettle.so*"
  --exclude "libgmp.so*"
  --exclude "libkrb5.so*"
  --exclude "libk5crypto.so*"
  --exclude "libcom_err.so*"
  --exclude "libkrb5support.so*"
  --exclude "libsasl2.so*"
  --exclude "libbrotlicommon.so*"
  --exclude "libp11-kit.so*"
  --exclude "libtasn1.so*"
  --exclude "libkeyutils.so*"
  --exclude "libresolv.so*"
  --exclude "libffi.so*"
  --exclude "libcuda.so*"
  --exclude "libcudart.so*"
  --exclude "libamdhip64.so*"
  --exclude "libhsa-runtime64.so*"
  --exclude "librocprofiler-register.so*"
  --exclude "libc10.so*"
  --exclude "libc10_cuda.so*"
  --exclude "libtorch.so*"
  --exclude "libtorch_cpu.so*"
  --exclude "libtorch_cuda.so*"
  --exclude "libtorch_python.so*"
  --exclude "libascendcl.so*"
  --exclude "libhccl.so*"
  --exclude "libmsprofiler.so*"
  --exclude "libgert.so*"
  --exclude "libascendcl_impl.so*"
  --exclude "libge_executor.so*"
  --exclude "libascend_dump.so*"
  --exclude "libgraph.so*"
  --exclude "libruntime.so*"
  --exclude "libascend_watchdog.so*"
  --exclude "libprofapi.so*"
  --exclude "liberror_manager.so*"
  --exclude "libascendalog.so*"
  --exclude "libc_sec.so*"
  --exclude "libhccl_alg.so*"
  --exclude "libhccl_plf.so*"
  --exclude "libascend_protobuf.so*"
  --exclude "libhybrid_executor.so*"
  --exclude "libdavinci_executor.so*"
  --exclude "libge_common.so*"
  --exclude "libge_common_base.so*"
  --exclude "liblowering.so*"
  --exclude "libregister.so*"
  --exclude "libexe_graph.so*"
  --exclude "libmmpa.so*"
  --exclude "libplatform.so*"
  --exclude "libgraph_base.so*"
  --exclude "libruntime_common.so*"
  --exclude "libqos_manager.so*"
  --exclude "libascend_trace.so*"
  --exclude "libmetadef*.so"
  --exclude "libllm_datadist*.so"
  --exclude "ascend_transport*.so"
  --exclude "libaccl_barex.so*"
)

ensure_yalantinglibs() {
  local source_dir="${UPSTREAM_DIR}/extern/yalantinglibs"
  local build_dir="${UPSTREAM_BUILD_DIR}/yalantinglibs-build"
  local config_file="${YALANTINGLIBS_PREFIX}/lib/cmake/yalantinglibs/yalantinglibsConfig.cmake"
  local header_file="${YALANTINGLIBS_PREFIX}/include/ylt/easylog.hpp"

  # 如果已经安装，直接返回
  if [[ -f "${config_file}" && -f "${header_file}" ]]; then
    return 0
  fi

  # 如果设置了预编译目录，直接从那里复制
  if [[ -n "${YALANTINGLIBS_PREBUILT_DIR:-}" && -d "${YALANTINGLIBS_PREBUILT_DIR}" ]]; then
    echo "Using prebuilt yalantinglibs from: ${YALANTINGLIBS_PREBUILT_DIR}"
    mkdir -p "${YALANTINGLIBS_PREFIX}"
    cp -r "${YALANTINGLIBS_PREBUILT_DIR}"/* "${YALANTINGLIBS_PREFIX}/"
    # 验证复制是否成功
    if [[ -f "${config_file}" && -f "${header_file}" ]]; then
      echo "Successfully copied prebuilt yalantinglibs to: ${YALANTINGLIBS_PREFIX}"
      return 0
    else
      echo "Warning: Prebuilt yalantinglibs copy failed, falling back to build from source"
    fi
  fi

  # 从源码编译
  if [[ ! -d "${source_dir}" ]]; then
    echo "missing yalantinglibs source: ${source_dir}" >&2
    exit 1
  fi

  echo "Building yalantinglibs from source..."
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

ensure_pybind11() {
  local pybind_dir="${UPSTREAM_DIR}/extern/pybind11"
  local prebuilt_dir="${PYBIND11_PREBUILT_DIR:-}"

  # 如果已经存在，直接返回
  if [[ -d "${pybind_dir}" && -f "${pybind_dir}/CMakeLists.txt" ]]; then
    return 0
  fi

  # 如果设置了预下载目录，复制到目标位置
  if [[ -n "${prebuilt_dir}" && -d "${prebuilt_dir}" ]]; then
    echo "Using prebuilt pybind11 from: ${prebuilt_dir}"
    mkdir -p "$(dirname "${pybind_dir}")"
    cp -r "${prebuilt_dir}" "${pybind_dir}"
    if [[ -f "${pybind_dir}/CMakeLists.txt" ]]; then
      echo "Successfully copied pybind11 to: ${pybind_dir}"
      return 0
    else
      echo "Warning: Prebuilt pybind11 copy failed" >&2
    fi
  fi

  # 检查 submodule 是否存在
  if [[ ! -d "${pybind_dir}" ]]; then
    echo "missing pybind11: ${pybind_dir} (run 'git submodule update --init --recursive')" >&2
    exit 1
  fi
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

PIP_INDEX=${PIP_INDEX_URL:-"https://mirrors.aliyun.com/pypi/simple/"}
"${VENV_PYTHON}" -m pip install --no-cache-dir --upgrade -i "${PIP_INDEX}" pip >/dev/null
if ! "${VENV_PYTHON}" -m pip show maturin >/dev/null 2>&1; then
  "${VENV_PYTHON}" -m pip install -i "${PIP_INDEX}" "maturin>=1.7,<2"
fi
if ! "${VENV_PYTHON}" -m pip show auditwheel >/dev/null 2>&1; then
  "${VENV_PYTHON}" -m pip install -i "${PIP_INDEX}" "auditwheel>=6,<7"
fi

repair_runtime_wheel() {
  local wheel_path=$1
  local repaired_dir="${WHEEL_DIR}/.auditwheel-repaired"
  local repaired_wheel

  rm -rf "${repaired_dir}"
  mkdir -p "${repaired_dir}"

  "${VENV_PYTHON}" -m auditwheel repair \
    "${wheel_path}" \
    "${AUDITWHEEL_EXCLUDES[@]}" \
    -w "${repaired_dir}"

  repaired_wheel=$(ls -1t "${repaired_dir}"/*.whl 2>/dev/null | head -n 1 || true)
  if [[ -z "${repaired_wheel}" ]]; then
    echo "auditwheel repair completed but produced no repaired wheel" >&2
    exit 1
  fi

  rm -f "${wheel_path}"
  mv "${repaired_wheel}" "${WHEEL_DIR}/"
  rm -rf "${repaired_dir}"
  printf '%s\n' "${WHEEL_DIR}/$(basename "${repaired_wheel}")"
}

restore_cli_libpython_dependency() {
  local wheel_path=$1

  "${VENV_PYTHON}" - <<'PY' "${wheel_path}"
import base64
import csv
import hashlib
import pathlib
import stat
import subprocess
import sys
import tempfile
import zipfile

wheel_path = pathlib.Path(sys.argv[1])
cli_names = [
    "mooncake-store-client",
    "mooncake-store-admin",
    "mooncake-store-bench",
]


def has_needed(path, library_name):
    result = subprocess.run(
        ["patchelf", "--print-needed", str(path)],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    return library_name in result.stdout.splitlines()


def rebuild_record(root):
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


def rewrite_wheel(root):
    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as target_wheel:
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(root).as_posix()
            info = zipfile.ZipInfo.from_file(path, arcname=relative)
            info.compress_type = zipfile.ZIP_DEFLATED
            target_wheel.writestr(info, path.read_bytes())


with tempfile.TemporaryDirectory(prefix="mooncake-wheel-libpython-") as temp_dir:
    root = pathlib.Path(temp_dir)
    with zipfile.ZipFile(wheel_path) as source_wheel:
        source_wheel.extractall(root)

    package_root = root / "mooncake"
    libpython_candidates = sorted(package_root.glob("libpython*.so*"))
    if not libpython_candidates:
        raise FileNotFoundError("runtime wheel does not contain libpython*.so")

    libpython_name = libpython_candidates[0].name
    for name in cli_names:
        cli_path = package_root / name
        if not cli_path.exists():
            continue
        cli_path.chmod(cli_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        if not has_needed(cli_path, libpython_name):
            subprocess.run(["patchelf", "--add-needed", libpython_name, str(cli_path)], check=True)

    rebuild_record(root)
    rewrite_wheel(root)
PY
}

require_file() {
  local path=$1

  if [[ ! -f "${path}" ]]; then
    echo "missing required native artifact: ${path}" >&2
    exit 1
  fi
}

require_glob() {
  local pattern=$1

  if ! compgen -G "${pattern}" >/dev/null; then
    echo "missing required native artifact matching: ${pattern}" >&2
    exit 1
  fi
}

detect_classic_te_lib() {
  local base="${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/src"
  if [[ -f "${base}/libtransfer_engine.a" ]]; then
    printf '%s\n' "${base}/libtransfer_engine.a"
  elif [[ -f "${base}/libtransfer_engine.so" ]]; then
    printf '%s\n' "${base}/libtransfer_engine.so"
  fi
}

require_reusable_native_artifacts() {
  require_file "${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/tent/src/libtent_shared.so"
  require_file "${UPSTREAM_BUILD_DIR}/mooncake-asio/libasio.so"
  require_file "${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/example/transfer_engine_bench"
  require_glob "${UPSTREAM_BUILD_DIR}/mooncake-integration/engine*.so"

  if [[ -n "${MOONCAKE_CLASSIC_SHIM_LIB_PATH:-}" ]]; then
    require_file "${MOONCAKE_CLASSIC_SHIM_LIB_PATH}"
  fi
  if [[ -n "${MOONCAKE_TENT_SHIM_LIB_PATH:-}" ]]; then
    require_file "${MOONCAKE_TENT_SHIM_LIB_PATH}"
  fi
}

if is_truthy "${MOONCAKE_REUSE_NATIVE_ARTIFACTS:-0}"; then
  require_reusable_native_artifacts
  export MOONCAKE_SKIP_NATIVE_BUILD="${MOONCAKE_SKIP_NATIVE_BUILD:-1}"
  export MOONCAKE_CLASSIC_TE_LIB_PATH="${MOONCAKE_CLASSIC_TE_LIB_PATH:-$(detect_classic_te_lib)}"
  export MOONCAKE_TENT_SHARED_LIB_PATH="${MOONCAKE_TENT_SHARED_LIB_PATH:-${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/tent/src/libtent_shared.so}"
  _timer_elapsed $_WHEEL_GLOBAL_START "setup (reused native artifacts)"
else
  # 如果设置了 SKIP_SUBMODULE_UPDATE，跳过 submodule 更新（CI 环境中 checkout 已处理）
  if [[ -z "${SKIP_SUBMODULE_UPDATE:-}" ]]; then
    git -C "${REPO_ROOT}" submodule update --init --recursive
  fi
  ensure_pybind11
  ensure_yalantinglibs
  export CPATH="${YALANTINGLIBS_PREFIX}/include${CPATH:+:${CPATH}}"
  _timer_elapsed $_WHEEL_GLOBAL_START "setup (venv + deps + pybind11 + yalantinglibs)"

  _CMAKE_SHARED_CONF_START=$(_timer_start)
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
    -DBUILD_EXAMPLES=OFF \
    -DBUILD_UNIT_TESTS=OFF \
    -DUSE_TENT=ON \
    -DUSE_REDIS=ON \
    -DUSE_HTTP=ON \
    -DUSE_ETCD=OFF \
    -DBUILD_SHARED_LIBS=ON \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_EXE_LINKER_FLAGS="-Wl,--push-state,--no-as-needed,-lrt,--pop-state"
  _timer_elapsed $_CMAKE_SHARED_CONF_START "cmake configure (shared transfer_engine)"

  _CMAKE_SHARED_BUILD_START=$(_timer_start)
  PATH="${VENV_BIN}:${PATH}" cmake --build "${UPSTREAM_BUILD_DIR}" \
    --target transfer_engine \
    -j"${BUILD_JOBS}"
  _timer_elapsed $_CMAKE_SHARED_BUILD_START "cmake build (shared transfer_engine)"

  _CMAKE_CONF_START=$(_timer_start)
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
    -DBUILD_SHARED_LIBS=OFF \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_EXE_LINKER_FLAGS="-Wl,--push-state,--no-as-needed,-lrt,--pop-state"

  _timer_elapsed $_CMAKE_CONF_START "cmake configure (static engine)"

  _CMAKE_BUILD_START=$(_timer_start)
  PATH="${VENV_BIN}:${PATH}" cmake --build "${UPSTREAM_BUILD_DIR}" \
    --target engine transfer_engine_bench tent_shared \
    -j"${BUILD_JOBS}"
  _timer_elapsed $_CMAKE_BUILD_START "cmake build (static engine + C++ libs)"
fi

export MOONCAKE_UPSTREAM_DIR="${UPSTREAM_DIR}"
export MOONCAKE_UPSTREAM_BUILD_DIR="${UPSTREAM_BUILD_DIR}"

_CARGO_BUILD_START=$(_timer_start)
cargo build \
  --manifest-path "${REPO_ROOT}/crates/mooncake-store-py/Cargo.toml" \
  --bins \
  --release
_timer_elapsed $_CARGO_BUILD_START "cargo build --release"

_MATURIN_START=$(_timer_start)
"${VENV_DIR}/bin/maturin" build \
  --release \
  --manifest-path "${REPO_ROOT}/crates/mooncake-store-py/Cargo.toml" \
  --interpreter "${VENV_PYTHON}" \
  --out "${WHEEL_DIR}" \
  "${MATURIN_ARGS[@]}"
_timer_elapsed $_MATURIN_START "maturin build"

install -m 0755 \
  "${REPO_ROOT}/target/release/mooncake-store-client" \
  "${BIN_DIR}/mooncake-store-client"
install -m 0755 \
  "${REPO_ROOT}/target/release/mooncake-store-admin" \
  "${BIN_DIR}/mooncake-store-admin"
install -m 0755 \
  "${REPO_ROOT}/target/release/mooncake_store_bench" \
  "${BIN_DIR}/mooncake-store-bench"

LATEST_WHEEL=$(ls -1t "${WHEEL_DIR}"/*.whl 2>/dev/null | head -n 1 || true)
if [[ -z "${LATEST_WHEEL}" ]]; then
  echo "wheel build completed but no wheel was found in ${WHEEL_DIR}" >&2
  exit 1
fi

_PY_POST_START=$(_timer_start)
"${VENV_PYTHON}" - <<'PY' \
  "${LATEST_WHEEL}" \
  "${REPO_ROOT}" \
  "${UPSTREAM_BUILD_DIR}" \
  "${REPO_ROOT}/target/release/build" \
  "${REPO_ROOT}/target/release/mooncake-store-client" \
  "${REPO_ROOT}/target/release/mooncake-store-admin" \
  "${REPO_ROOT}/target/release/mooncake_store_bench" \
  "${BUILD_GIT_BRANCH}" \
  "${BUILD_GIT_COMMIT}" \
  "${BUILD_TIME}"
import base64
import csv
import hashlib
import json
import os
import pathlib
import shutil
import stat
import subprocess
import sys
import sysconfig
import tempfile
import zipfile

wheel_path = pathlib.Path(sys.argv[1])
repo_root = pathlib.Path(sys.argv[2])
upstream_build_dir = pathlib.Path(sys.argv[3])
transport_build_dir = pathlib.Path(sys.argv[4])
store_client_path = pathlib.Path(sys.argv[5])
store_admin_path = pathlib.Path(sys.argv[6])
store_bench_path = pathlib.Path(sys.argv[7])
build_git_branch = sys.argv[8]
build_git_commit = sys.argv[9]
build_time = sys.argv[10]
upstream_py_dir = repo_root / "third_party" / "Mooncake" / "mooncake-wheel" / "mooncake"
rl_py_dir = repo_root / "python" / "mooncake_rl"
transport_shim_out_dirs = sorted(transport_build_dir.glob("mooncake-transport-sys-*/out"))


def env_candidate(name):
    value = os.environ.get(name)
    return [pathlib.Path(value)] if value else []

binary_assets = {
    "mooncake-store-client": store_client_path,
    "mooncake-store-admin": store_admin_path,
    "mooncake-store-bench": store_bench_path,
    "transfer_engine_bench": upstream_build_dir
    / "mooncake-transfer-engine"
    / "example"
    / "transfer_engine_bench",
}

library_assets = {
    "engine.so": sorted((upstream_build_dir / "mooncake-integration").glob("engine*.so")),
    "libasio.so": [upstream_build_dir / "mooncake-asio" / "libasio.so"],
    "libtransfer_engine.so": [
        upstream_build_dir / "mooncake-transfer-engine" / "src" / "libtransfer_engine.so",
    ],
    "libtent_shared.so": [
        *env_candidate("MOONCAKE_TENT_SHARED_LIB_PATH"),
        upstream_build_dir / "mooncake-transfer-engine" / "tent" / "src" / "libtent_shared.so"
    ],
    "libmooncake_classic_shim.so": env_candidate("MOONCAKE_CLASSIC_SHIM_LIB_PATH")
    + [
        shim_dir / "libmooncake_classic_shim.so" for shim_dir in transport_shim_out_dirs
    ],
    "libmooncake_tent_shim.so": env_candidate("MOONCAKE_TENT_SHIM_LIB_PATH")
    + [
        shim_dir / "libmooncake_tent_shim.so" for shim_dir in transport_shim_out_dirs
    ],
}

python_libdir = sysconfig.get_config_var("LIBDIR")
python_ldlibrary = sysconfig.get_config_var("LDLIBRARY")
if python_libdir and python_ldlibrary and python_ldlibrary.endswith(".so"):
    python_library = pathlib.Path(python_libdir) / python_ldlibrary
    library_assets[python_ldlibrary] = [python_library]

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


def first_existing_or_none(paths):
    for path in paths:
        if path.exists():
            return path
    return None


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
        set_relative_rpath(target)

    for name, candidates in library_assets.items():
        source = first_existing(candidates)
        target = package_root / name
        shutil.copy2(source, target)
        if name in relative_rpath_assets:
            set_relative_rpath(target)

    for name in python_assets:
        shutil.copy2(upstream_py_dir / name, package_root / name)

    rl_package_root = root / "mooncake_rl"
    if rl_package_root.exists():
        shutil.rmtree(rl_package_root)
    shutil.copytree(
        rl_py_dir,
        rl_package_root,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )

    (package_root / "build-info.json").write_text(
        json.dumps(
            {
                "branch": build_git_branch,
                "commit": build_git_commit,
                "build_time": build_time,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

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

_timer_elapsed $_PY_POST_START "python wheel post-processing"

_AUDITWHEEL_START=$(_timer_start)
LATEST_WHEEL=$(repair_runtime_wheel "${LATEST_WHEEL}")
_timer_elapsed $_AUDITWHEEL_START "auditwheel repair"

_CLI_LIBPYTHON_START=$(_timer_start)
restore_cli_libpython_dependency "${LATEST_WHEEL}"
_timer_elapsed $_CLI_LIBPYTHON_START "restore CLI libpython dependency"

_timer_elapsed $_WHEEL_GLOBAL_START "TOTAL build-wheel.sh"

cat <<EOF
wheel:  ${LATEST_WHEEL}
client: ${BIN_DIR}/mooncake-store-client
admin:  ${BIN_DIR}/mooncake-store-admin
bench:  ${BIN_DIR}/mooncake-store-bench
EOF
