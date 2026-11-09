#!/usr/bin/env bash

# Build the normal Mooncake wheel with the optional Store-RS implementation.
# The CMake tree must already be configured with WITH_STORE=ON,
# WITH_STORE_RS=ON, WITH_TE=ON, and USE_TENT=ON, and must contain the ordinary
# wheel's native targets. Keeping WITH_STORE enabled puts mooncake_master and
# both Python Store backends in one T-One artifact. This wrapper builds the
# ordinary wheel targets plus Store-RS, then reuses build_wheel.sh so that the
# test artifact receives the same variant naming and auditwheel repair as the
# regular release wheel.

set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BUILD_DIR=${BUILD_DIR:-build}
PYTHON_VERSION=${PYTHON_VERSION:-${1:-$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')}}
OUTPUT_DIR=${OUTPUT_DIR:-${2:-dist-store-rs}}
STORE_RS_OWNS_PREFIX=false
if [[ -z "${STORE_RS_PYTHON_PREFIX:-}" ]]; then
    STORE_RS_PYTHON_PREFIX=$(mktemp -d "${TMPDIR:-/tmp}/mooncake-store-rs-wheel.XXXXXX")
    STORE_RS_OWNS_PREFIX=true
fi

case "${BUILD_DIR}" in
    /*)
        BUILD_DIR_ABS=${BUILD_DIR}
        case "${BUILD_DIR_ABS}" in
            "${ROOT_DIR}"/*) BUILD_DIR=${BUILD_DIR_ABS#"${ROOT_DIR}"/} ;;
            *)
                echo "BUILD_DIR must be inside the Mooncake checkout: ${ROOT_DIR}" >&2
                exit 1
                ;;
        esac
        ;;
    *) BUILD_DIR_ABS="${ROOT_DIR}/${BUILD_DIR}" ;;
esac

cleanup() {
    local status=$?
    rm -f \
        "${ROOT_DIR}"/mooncake-wheel/mooncake/_store_rs.*.so \
        "${ROOT_DIR}/mooncake-wheel/mooncake/libmooncake_classic_shim.so" \
        "${ROOT_DIR}/mooncake-wheel/mooncake/libmooncake_tent_shim.so" \
        "${ROOT_DIR}/mooncake-wheel/mooncake/libtent_shared.so" \
        "${ROOT_DIR}/mooncake-wheel/mooncake/mooncake-store-rs-client" \
        "${ROOT_DIR}/mooncake-wheel/mooncake/mooncake-store-rs-admin" \
        "${ROOT_DIR}/mooncake-wheel/mooncake/mooncake-store-rs-bench"
    if [[ "${STORE_RS_OWNS_PREFIX}" == "true" ]]; then
        rm -rf "${STORE_RS_PYTHON_PREFIX}"
    fi
    exit "${status}"
}
trap cleanup EXIT

if [[ ! -f "${BUILD_DIR_ABS}/CMakeCache.txt" ]]; then
    echo "Store-RS wheel build requires a configured CMake tree: ${BUILD_DIR_ABS}" >&2
    exit 1
fi
if ! grep -q '^WITH_STORE_RS:BOOL=ON$' "${BUILD_DIR_ABS}/CMakeCache.txt"; then
    echo "CMake tree was not configured with -DWITH_STORE_RS=ON: ${BUILD_DIR_ABS}" >&2
    exit 1
fi
if ! grep -q '^WITH_STORE:BOOL=ON$' "${BUILD_DIR_ABS}/CMakeCache.txt"; then
    echo "T-One Store-RS wheel also requires -DWITH_STORE=ON for mooncake_master: ${BUILD_DIR_ABS}" >&2
    exit 1
fi
if ! grep -q '^USE_REDIS:BOOL=ON$' "${BUILD_DIR_ABS}/CMakeCache.txt"; then
    echo "Store-RS Classic TE requires -DUSE_REDIS=ON for Redis transport metadata: ${BUILD_DIR_ABS}" >&2
    exit 1
fi

cmake --build "${BUILD_DIR_ABS}" --target \
    engine store transfer_engine mooncake_store \
    mooncake_master mooncake_client build_store_rs \
    --parallel "${BUILD_JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)}"

CLASSIC_TE_SHARED_LIB="${BUILD_DIR_ABS}/mooncake-transfer-engine/src/libtransfer_engine.so"
if [[ ! -f "${CLASSIC_TE_SHARED_LIB}" ]]; then
    echo "Store-RS Classic TE wheel requires ${CLASSIC_TE_SHARED_LIB}; configure with -DBUILD_SHARED_LIBS=ON" >&2
    exit 1
fi

cmake --install "${BUILD_DIR_ABS}" \
    --component python \
    --prefix "${STORE_RS_PYTHON_PREFIX}"

# The manylinux CUDA images provide yaml-cpp 0.7 under /opt/conda/lib while
# AlmaLinux installs yaml-cpp 0.6 under /usr/lib64. Store-RS adds
# libtent_shared.so to the wheel, exposing its yaml-cpp dependency to
# auditwheel. Keep the exact library selected by CMake discoverable during
# repair instead of letting auditwheel see only the incompatible system ABI.
if [[ -d /opt/conda/lib ]]; then
    export LD_LIBRARY_PATH="/opt/conda/lib:${LD_LIBRARY_PATH:-}"
fi

cd "${ROOT_DIR}"
INCLUDE_STORE_RS=1 \
STORE_RS_PYTHON_PREFIX="${STORE_RS_PYTHON_PREFIX}" \
PYTHON_VERSION="${PYTHON_VERSION}" \
OUTPUT_DIR="${OUTPUT_DIR}" \
BUILD_DIR="${BUILD_DIR}" \
    ./scripts/build_wheel.sh
