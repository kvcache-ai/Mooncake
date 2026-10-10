#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# scripts/build/build-upstream-libs.sh
#
# Lightweight script that builds only the upstream Mooncake C++ shared
# libraries (libtransfer_engine.so, libtent_shared.so, etc.) without Rust
# binaries.
#
# This is used by CI jobs that only need LD_LIBRARY_PATH to resolve
# native dependencies (e.g., coverage), avoiding an unnecessary Rust release
# build for scripts that only inspect upstream shared libraries.
# ---------------------------------------------------------------------------

PYTHON_BIN=${PYTHON:-python3}
UPSTREAM_DIR=${MOONCAKE_ROOT_DIR:?MOONCAKE_ROOT_DIR must be set to an explicit Mooncake source directory}
UPSTREAM_BUILD_DIR=${MOONCAKE_BUILD_DIR:?MOONCAKE_BUILD_DIR must be set to an explicit CMake build directory}
for path in "${UPSTREAM_DIR}" "${UPSTREAM_BUILD_DIR}"; do
  [[ "${path}" == /* ]] || { echo "MOONCAKE_*_DIR paths must be absolute: ${path}" >&2; exit 1; }
done
[[ -d "${UPSTREAM_DIR}" ]] || { echo "MOONCAKE_ROOT_DIR must be a directory: ${UPSTREAM_DIR}" >&2; exit 1; }
BUILD_JOBS=${BUILD_JOBS:-$(command -v nproc >/dev/null 2>&1 && nproc || getconf _NPROCESSORS_ONLN || echo 8)}

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
  git -C "${UPSTREAM_DIR}" submodule update --init --recursive
fi
ensure_pybind11

BUILD_TARGETS=(transfer_engine tent_shared)

# Create a minimal venv just for cmake's Python3_EXECUTABLE requirement
VENV_DIR="${UPSTREAM_DIR}/.venv-upstream-libs"
if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

cmake \
  -S "${UPSTREAM_DIR}" \
  -B "${UPSTREAM_BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DPython3_EXECUTABLE="${VENV_DIR}/bin/python" \
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
  -DCMAKE_EXE_LINKER_FLAGS="-Wl,--push-state,--no-as-needed,-lrt,--pop-state"

cmake --build "${UPSTREAM_BUILD_DIR}" \
  --target "${BUILD_TARGETS[@]}" \
  -j"${BUILD_JOBS}"

echo "Upstream libraries built successfully:"
echo "  libtransfer_engine.so: ${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/src/libtransfer_engine.so"
echo "  libtent_shared.so:     ${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/tent/src/libtent_shared.so"
