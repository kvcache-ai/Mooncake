#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# scripts/build/build-upstream-libs.sh
#
# Lightweight script that builds only the upstream Mooncake C++ shared
# libraries (libtransfer_engine.so, libtent_shared.so, etc.) without
# constructing a Python wheel or Rust binaries.
#
# This is used by CI jobs that only need LD_LIBRARY_PATH to resolve
# native dependencies (e.g., coverage), avoiding the full build-wheel.sh
# overhead (maturin, auditwheel, cargo build --release, etc.).
# ---------------------------------------------------------------------------

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
# store-rs is a subdirectory when it lives inside the Mooncake monorepo,
# where the git toplevel is the enclosing repository rather than this tree.
[ -f "${REPO_ROOT}/Cargo.toml" ] || REPO_ROOT="${REPO_ROOT}/mooncake-store/store-rs"

PYTHON_BIN=${PYTHON:-python3}
UPSTREAM_DIR=${MOONCAKE_UPSTREAM_DIR:-"${REPO_ROOT}/third_party/Mooncake"}
# Inside the Mooncake monorepo the upstream tree is the enclosing repository
# rather than a submodule. Both arms are guarded on a transfer-engine
# directory, so an uninitialised submodule keeps the submodule path and
# fails with that name rather than silently pointing somewhere unrelated.
if [ ! -d "${UPSTREAM_DIR}/mooncake-transfer-engine" ] \
  && [ -d "${REPO_ROOT}/../../mooncake-transfer-engine" ]; then
  UPSTREAM_DIR=$(cd "${REPO_ROOT}/../.." && pwd)
fi
UPSTREAM_BUILD_DIR=${MOONCAKE_UPSTREAM_BUILD_DIR:-"${UPSTREAM_DIR}/build-wheel-compat"}
YALANTINGLIBS_PREFIX=${YALANTINGLIBS_PREFIX:-"${UPSTREAM_BUILD_DIR}/yalantinglibs-install"}
BUILD_JOBS=${BUILD_JOBS:-$(command -v nproc >/dev/null 2>&1 && nproc || getconf _NPROCESSORS_ONLN || echo 8)}
BUILD_WHEEL_NATIVE_ASSETS=${BUILD_WHEEL_NATIVE_ASSETS:-0}

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

ensure_yalantinglibs() {
  local source_dir="${UPSTREAM_DIR}/extern/yalantinglibs"
  local build_dir="${UPSTREAM_BUILD_DIR}/yalantinglibs-build"
  local config_file="${YALANTINGLIBS_PREFIX}/lib/cmake/yalantinglibs/yalantinglibsConfig.cmake"
  local header_file="${YALANTINGLIBS_PREFIX}/include/ylt/easylog.hpp"

  if [[ -f "${config_file}" && -f "${header_file}" ]]; then
    return 0
  fi

  if [[ -n "${YALANTINGLIBS_PREBUILT_DIR:-}" && -d "${YALANTINGLIBS_PREBUILT_DIR}" ]]; then
    echo "Using prebuilt yalantinglibs from: ${YALANTINGLIBS_PREBUILT_DIR}"
    mkdir -p "${YALANTINGLIBS_PREFIX}"
    cp -r "${YALANTINGLIBS_PREBUILT_DIR}"/* "${YALANTINGLIBS_PREFIX}/"
    if [[ -f "${config_file}" && -f "${header_file}" ]]; then
      return 0
    fi
  fi

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

  if [[ -d "${pybind_dir}" && -f "${pybind_dir}/CMakeLists.txt" ]]; then
    return 0
  fi

  if [[ -n "${prebuilt_dir}" && -d "${prebuilt_dir}" ]]; then
    mkdir -p "$(dirname "${pybind_dir}")"
    cp -r "${prebuilt_dir}" "${pybind_dir}"
    if [[ -f "${pybind_dir}/CMakeLists.txt" ]]; then
      return 0
    fi
  fi

  if [[ ! -d "${pybind_dir}" ]]; then
    echo "missing pybind11: ${pybind_dir}" >&2
    exit 1
  fi
}

if [[ -z "${SKIP_SUBMODULE_UPDATE:-}" ]]; then
  git -C "${REPO_ROOT}" submodule update --init --recursive
fi
ensure_pybind11
ensure_yalantinglibs
export CPATH="${YALANTINGLIBS_PREFIX}/include${CPATH:+:${CPATH}}"

BUILD_EXAMPLES=OFF
BUILD_TARGETS=(transfer_engine tent_shared)
if is_truthy "${BUILD_WHEEL_NATIVE_ASSETS}"; then
  BUILD_EXAMPLES=ON
  BUILD_TARGETS=(engine transfer_engine_bench tent_shared)
fi

# Create a minimal venv just for cmake's Python3_EXECUTABLE requirement
VENV_DIR="${REPO_ROOT}/.venv-upstream-libs"
if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

cmake \
  -S "${UPSTREAM_DIR}" \
  -B "${UPSTREAM_BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH="${YALANTINGLIBS_PREFIX}" \
  -Dyalantinglibs_DIR="${YALANTINGLIBS_PREFIX}/lib/cmake/yalantinglibs" \
  -DPython3_EXECUTABLE="${VENV_DIR}/bin/python" \
  -DWITH_TE=ON \
  -DWITH_STORE=OFF \
  -DWITH_STORE_RUST=OFF \
  -DBUILD_EXAMPLES="${BUILD_EXAMPLES}" \
  -DBUILD_UNIT_TESTS=OFF \
  -DUSE_TENT=ON \
  -DUSE_REDIS=ON \
  -DUSE_HTTP=ON \
  -DUSE_ETCD=OFF \
  -DBUILD_SHARED_LIBS=ON \
  -DCMAKE_EXE_LINKER_FLAGS="-Wl,--push-state,--no-as-needed,-lrt,--pop-state"

cmake --build "${UPSTREAM_BUILD_DIR}" \
  --target "${BUILD_TARGETS[@]}" \
  -j"${BUILD_JOBS}"

echo "Upstream libraries built successfully:"
echo "  libtransfer_engine.so: ${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/src/libtransfer_engine.so"
echo "  libtent_shared.so:     ${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/tent/src/libtent_shared.so"
if is_truthy "${BUILD_WHEEL_NATIVE_ASSETS}"; then
  echo "  engine*.so:            ${UPSTREAM_BUILD_DIR}/mooncake-integration/"
  echo "  libasio.so:            ${UPSTREAM_BUILD_DIR}/mooncake-{common,asio}/libasio.so"
  echo "  transfer_engine_bench: ${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/example/transfer_engine_bench"
fi
