#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd)"

DEPS_PREFIX="${MOONCAKE_DEPS:-/home/inf-daole/.local/mooncake-deps}"
CUDA_ROOT="${CUDA_HOME:-/usr/local/cuda}"
BUILD_DIR="${BUILD_DIR:-${REPO_ROOT}/build}"
INSTALL_PREFIX="${INSTALL_PREFIX:-${REPO_ROOT}/install-cuda-tent}"
JOBS="${JOBS:-$(nproc)}"
BUILD_TYPE="${BUILD_TYPE:-RelWithDebInfo}"
BUILD_EXAMPLES="${BUILD_EXAMPLES:-ON}"
WHEEL_OUTPUT="${WHEEL_OUTPUT:-dist}"
if [[ -z "${PYTHON_EXECUTABLE:-}" && -x "${REPO_ROOT}/.venv/bin/python" ]]; then
    PYTHON_EXECUTABLE="${REPO_ROOT}/.venv/bin/python"
else
    PYTHON_EXECUTABLE="${PYTHON_EXECUTABLE:-python3}"
fi
RUN_INSTALL=0
RUN_WHEEL=0

usage() {
    cat <<USAGE
Usage: $(basename "$0") [--clean] [--install] [--wheel] [--configure-only] [--help]

Environment overrides:
  MOONCAKE_DEPS     Dependency prefix (default: ${DEPS_PREFIX})
  CUDA_HOME         CUDA toolkit root (default: ${CUDA_ROOT})
  BUILD_DIR         CMake build directory (default: ${BUILD_DIR})
  INSTALL_PREFIX    User-writable install prefix (default: ${INSTALL_PREFIX})
  JOBS              Parallel build jobs (default: nproc)
  BUILD_TYPE        CMake build type (default: ${BUILD_TYPE})
  BUILD_EXAMPLES    Build example binaries for wheel CLIs (default: ${BUILD_EXAMPLES})
  WHEEL_OUTPUT      Wheel output directory, relative to mooncake-wheel (default: ${WHEEL_OUTPUT})
  PYTHON_EXECUTABLE Python interpreter for CMake and wheel builds (default: ${PYTHON_EXECUTABLE})
USAGE
}

CLEAN=0
CONFIGURE_ONLY=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --clean)
            CLEAN=1
            ;;
        --install)
            RUN_INSTALL=1
            ;;
        --wheel)
            RUN_WHEEL=1
            ;;
        --configure-only)
            CONFIGURE_ONLY=1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
    shift
done

if [[ ! -d "${DEPS_PREFIX}" ]]; then
    echo "Dependency prefix not found: ${DEPS_PREFIX}" >&2
    exit 1
fi

if [[ ! -d "${CUDA_ROOT}" ]]; then
    echo "CUDA toolkit root not found: ${CUDA_ROOT}" >&2
    exit 1
fi

CUDA_TARGET_LIB_DIR=""
for candidate in \
    "${CUDA_ROOT}/targets/sbsa-linux/lib" \
    "${CUDA_ROOT}/targets/aarch64-linux/lib" \
    "${CUDA_ROOT}/targets/x86_64-linux/lib" \
    "${CUDA_ROOT}/lib64" \
    "${CUDA_ROOT}/lib"; do
    if [[ -d "${candidate}" ]]; then
        CUDA_TARGET_LIB_DIR="${candidate}"
        break
    fi
done

if [[ -z "${CUDA_TARGET_LIB_DIR}" ]]; then
    echo "Could not find CUDA library directory under ${CUDA_ROOT}" >&2
    exit 1
fi

CUDA_STUB_LIB_DIR="${CUDA_TARGET_LIB_DIR}/stubs"
CUDA_LINK_FLAGS=""
if [[ -d "${CUDA_STUB_LIB_DIR}" ]]; then
    CUDA_LINK_FLAGS="-L${CUDA_STUB_LIB_DIR}"
fi

PYTHON_BIN_DIR="$(cd -- "$(dirname -- "${PYTHON_EXECUTABLE}")" >/dev/null 2>&1 && pwd)"
export PATH="${PYTHON_BIN_DIR}:${DEPS_PREFIX}/bin:${DEPS_PREFIX}/go/bin:${CUDA_ROOT}/bin:${PATH}"
export LD_LIBRARY_PATH="${CUDA_TARGET_LIB_DIR}:${CUDA_STUB_LIB_DIR:-}:${DEPS_PREFIX}/lib:${LD_LIBRARY_PATH:-}"
export LIBRARY_PATH="${CUDA_TARGET_LIB_DIR}:${CUDA_STUB_LIB_DIR:-}:${DEPS_PREFIX}/lib:${LIBRARY_PATH:-}"
export PKG_CONFIG_PATH="${DEPS_PREFIX}/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
export CMAKE_PREFIX_PATH="${DEPS_PREFIX}:${CMAKE_PREFIX_PATH:-}"
export CMAKE_INCLUDE_PATH="${DEPS_PREFIX}/include:${CMAKE_INCLUDE_PATH:-}"
export CMAKE_LIBRARY_PATH="${DEPS_PREFIX}/lib:${CMAKE_LIBRARY_PATH:-}"
export CUDAToolkit_ROOT="${CUDA_ROOT}"
export CUDA_HOME="${CUDA_ROOT}"

if [[ "${CLEAN}" -eq 1 ]]; then
    rm -rf -- "${BUILD_DIR}"
fi

cmake -S "${REPO_ROOT}" -B "${BUILD_DIR}" -G Ninja \
    -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
    -DCMAKE_PREFIX_PATH="${DEPS_PREFIX}" \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
    -DCMAKE_INCLUDE_PATH="${DEPS_PREFIX}/include" \
    -DCMAKE_LIBRARY_PATH="${DEPS_PREFIX}/lib" \
    -DCUDAToolkit_ROOT="${CUDA_ROOT}" \
    -DPython3_EXECUTABLE="${PYTHON_EXECUTABLE}" \
    -DPYTHON_EXECUTABLE="${PYTHON_EXECUTABLE}" \
    -DCMAKE_EXE_LINKER_FLAGS="${CUDA_LINK_FLAGS}" \
    -DCMAKE_SHARED_LINKER_FLAGS="${CUDA_LINK_FLAGS}" \
    -DBUILD_UNIT_TESTS=OFF \
    -DBUILD_BENCHMARK=OFF \
    -DBUILD_EXAMPLES="${BUILD_EXAMPLES}" \
    -DWITH_STORE_RUST=OFF \
    -DWITH_STORE_GO=OFF \
    -DWITH_P2P_STORE=OFF \
    -DWITH_EP=OFF \
    -DUSE_CUDA=ON \
    -DWITH_NVIDIA_PEERMEM=OFF \
    -DUSE_MNNVL=ON \
    -DUSE_TENT=ON \
    -DWITH_STORE=ON

if [[ "${CONFIGURE_ONLY}" -eq 1 ]]; then
    exit 0
fi

cmake --build "${BUILD_DIR}" --parallel "${JOBS}"

if [[ "${RUN_INSTALL}" -eq 1 ]]; then
    cmake --install "${BUILD_DIR}"
fi

if [[ "${RUN_WHEEL}" -eq 1 ]]; then
    BUILD_DIR="${BUILD_DIR}" OUTPUT_DIR="${WHEEL_OUTPUT}" PYTHON_EXECUTABLE="${PYTHON_EXECUTABLE}" "${SCRIPT_DIR}/build_wheel.sh"
fi
