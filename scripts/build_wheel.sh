#!/bin/bash
# Script to build the mooncake wheel package
# Usage: ./scripts/build_wheel.sh [python_version] [output_dir]
# Example: ./scripts/build_wheel.sh 3.10 dist-3.10
# Select an independent CMake build with BUILD_DIR (default: build).
# Example: BUILD_DIR=build-p2p ./scripts/build_wheel.sh 3.10 dist-p2p

set -e  # Exit immediately if a command exits with a non-zero status
set -x

# Get Python version from environment variable or argument
PYTHON_VERSION=${PYTHON_VERSION:-${1:-$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")}}
# Get output directory from environment variable or argument
OUTPUT_DIR=${OUTPUT_DIR:-${2:-"dist"}}
# CMake build directory containing the selected Store architecture.
BUILD_DIR="${BUILD_DIR:-build}"
BUILD_DIR_ABS="$(pwd)/${BUILD_DIR}"
if [ ! -f "${BUILD_DIR}/CMakeCache.txt" ]; then
    echo "Error: missing ${BUILD_DIR}/CMakeCache.txt" >&2
    exit 1
fi

# Only Store enablement and architecture need to be read for identity checks.
STORE_ENABLED=$(sed -n 's/^WITH_STORE:[^=]*=//p' "${BUILD_DIR}/CMakeCache.txt")
case "${STORE_ENABLED^^}" in
    "")
        echo "Error: WITH_STORE is missing from ${BUILD_DIR}/CMakeCache.txt" >&2
        exit 1
        ;;
    0|OFF|NO|FALSE|N|IGNORE|NOTFOUND|*-NOTFOUND) STORE_ENABLED=OFF ;;
    *) STORE_ENABLED=ON ;;
esac
STORE_CHECK_OPTIONS=(--with-store "$STORE_ENABLED")
if [ "$STORE_ENABLED" = "ON" ]; then
    STORE_ARCHITECTURE=$(sed -n 's/^MOONCAKE_STORE_CLIENT_ARCH:[^=]*=//p' "${BUILD_DIR}/CMakeCache.txt")
    STORE_CHECK_OPTIONS+=(--expected-architecture "$STORE_ARCHITECTURE")
fi

# Detect CUDA version (env wins, then nvcc, then /usr/local/cuda/version.txt, else 0.0)
CUDA_VERSION=${CUDA_VERSION:-$(nvcc --version 2>/dev/null | grep -o "release [0-9][0-9]*\.[0-9]*" | awk '{print $2}' || true)}
if [ -z "$CUDA_VERSION" ] && [ -f /usr/local/cuda/version.txt ]; then
    CUDA_VERSION=$(grep -Eo "[0-9]+\.[0-9]+" /usr/local/cuda/version.txt | head -n1)
fi
CUDA_VERSION=${CUDA_VERSION:-"0.0"}
echo "Building wheel for Python ${PYTHON_VERSION} with output directory ${OUTPUT_DIR}"
echo "Detected CUDA version ${CUDA_VERSION}"

# Ensure LD_LIBRARY_PATH includes /usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${BUILD_DIR_ABS}/mooncake-asio:${BUILD_DIR_ABS}/mooncake-common/etcd:${BUILD_DIR_ABS}/mooncake-common/k8s-lease:${BUILD_DIR_ABS}/ep_pg_staging:/usr/local/lib

echo "Cleaning wheel-build directory"
rm -rf mooncake-wheel/mooncake_transfer_engine*
rm -rf mooncake-wheel/build/
rm -f mooncake-wheel/mooncake/*.so
# Remove binaries from the previous architecture before selecting this pair.
rm -f mooncake-wheel/mooncake/mooncake_master mooncake-wheel/mooncake/mooncake_client

echo "Creating directory structure..."

# Copy engine.so to mooncake directory (will be imported by transfer module)
cp "${BUILD_DIR}"/mooncake-integration/engine.*.so mooncake-wheel/mooncake/engine.so

# The current Asio provider still lives in mooncake-asio (F01-T113).
cp "${BUILD_DIR}/mooncake-asio/libasio.so" mooncake-wheel/mooncake/libasio.so

# Copy the private C++ Store extension to the mooncake package.
if compgen -G "${BUILD_DIR}/mooncake-integration/_store.*.so" >/dev/null; then
    echo "Copying _store extension..."
    cp "${BUILD_DIR}"/mooncake-integration/_store.*.so mooncake-wheel/mooncake/
    echo "Copying master binary..."
    # Copy master binary
    cp "${BUILD_DIR}/mooncake-store/src/mooncake_master" mooncake-wheel/mooncake/
    # Copy client binary
    cp "${BUILD_DIR}/mooncake-store/src/mooncake_client" mooncake-wheel/mooncake/
    # Stage the canonical async Store client for the legacy wheel builder.
    cp python/mooncake/async_store.py mooncake-wheel/mooncake/async_store.py
else
    echo "Skipping _store extension (not built - likely WITH_STORE is set to OFF)"
fi

# Copy libmooncake_store.so to mooncake directory (only when BUILD_SHARED_LIBS is set)
if [ -f "${BUILD_DIR}/mooncake-store/src/libmooncake_store.so" ]; then
    echo "Copying libmooncake_store.so..."
    cp "${BUILD_DIR}/mooncake-store/src/libmooncake_store.so" mooncake-wheel/mooncake/libmooncake_store.so
fi

# Copy libmooncake_common.so (only when BUILD_SHARED_LIBS is set).
if [ -f "${BUILD_DIR}/mooncake-common/src/libmooncake_common.so" ]; then
    echo "Copying libmooncake_common.so..."
    cp "${BUILD_DIR}/mooncake-common/src/libmooncake_common.so" mooncake-wheel/mooncake/libmooncake_common.so
fi

# F01-T117 adds the K8s lease runtime alongside its actual enabled backend.
# Copy the Go etcd runtime used by TE metadata or Store HA.
if [ -f "${BUILD_DIR}/mooncake-common/etcd/libetcd_wrapper.so" ]; then
    echo "Copying libetcd_wrapper.so..."
    cp "${BUILD_DIR}/mooncake-common/etcd/libetcd_wrapper.so" mooncake-wheel/mooncake/libetcd_wrapper.so
fi

# Copy libtransfer_engine.so to mooncake directory (only when BUILD_SHARED_LIBS is set)
if [ -f "${BUILD_DIR}/mooncake-transfer-engine/src/libtransfer_engine.so" ]; then
    echo "Copying libtransfer_engine.so..."
    cp "${BUILD_DIR}/mooncake-transfer-engine/src/libtransfer_engine.so" mooncake-wheel/mooncake/libtransfer_engine.so
fi

# Copy ascend_transport.so to mooncake directory (only when USE_ASCEND_DIRECT is set)
if [ -f "${BUILD_DIR}/mooncake-transfer-engine/src/transport/ascend_transport/ascend_transport.so" ]; then
    echo "Copying ascend_transport.so..."
    cp "${BUILD_DIR}/mooncake-transfer-engine/src/transport/ascend_transport/ascend_transport.so" mooncake-wheel/mooncake/ascend_transport.so
fi

# Copy nvlink-allocator.so to mooncake directory (only if it exists - CUDA builds only)
if [ -f "${BUILD_DIR}/mooncake-transfer-engine/nvlink-allocator/nvlink_allocator.so" ] \
   || [ -f /usr/lib/libaccl_barex.so ] \
   || [ -f /usr/lib64/libaccl_barex.so ]; then
    if [ -f "${BUILD_DIR}/mooncake-transfer-engine/nvlink-allocator/nvlink_allocator.so" ]; then
     echo "Copying CUDA nvlink_allocator.so..."
     cp "${BUILD_DIR}/mooncake-transfer-engine/nvlink-allocator/nvlink_allocator.so" mooncake-wheel/mooncake/nvlink_allocator.so
    fi
    echo "Copying allocator libraries..."
    # Copy allocator.py
    cp mooncake-integration/allocator.py mooncake-wheel/mooncake/allocator.py
else
    echo "Skipping nvlink_allocator.so (not built - likely ARM64 or non-CUDA build)"
fi

echo "Copying transfer_engine_bench..."
# Copy transfer_engine_bench
cp "${BUILD_DIR}/mooncake-transfer-engine/example/transfer_engine_bench" mooncake-wheel/mooncake/

if [ -f "${BUILD_DIR}/mooncake-transfer-engine/src/transport/ascend_transport/hccl_transport/ascend_transport_c/libascend_transport_mem.so" ]; then
    cp "${BUILD_DIR}/mooncake-transfer-engine/src/transport/ascend_transport/hccl_transport/ascend_transport_c/libascend_transport_mem.so" mooncake-wheel/mooncake/
    echo "Copying ascend_transport_mem libraries..."
else
    echo "Skipping libascend_transport_mem.so (not built - Ascend disabled)"
fi

python${PYTHON_VERSION} mooncake-wheel/setup.py --check-store-artifacts \
    mooncake-wheel/mooncake "${STORE_CHECK_OPTIONS[@]}"

if [ "$BUILD_WITH_EP" = "1" ]; then
    # F01-T113 replaces this legacy EP/PG build with U's real CMake/JIT staging.
    echo "Building Mooncake EP"
    cd mooncake-ep
    if [ -z "$EP_TORCH_VERSIONS" ]; then
        python setup.py build_ext --build-lib .
    else
        for version in ${EP_TORCH_VERSIONS//;/ }; do
            cuda_major=${CUDA_VERSION%%.*}
            if [ "$cuda_major" -ge 13 ]; then
                # TODO: Fix me when we need to support more CUDA 13 versions or when the CI env is fixed
                pip install torch==$version --index-url https://download.pytorch.org/whl/cu130
            else
                pip install torch==$version
            fi
            python setup.py build_ext --build-lib . --force  # Force build when torch version changes
        done
    fi
    cp mooncake/*.so ../mooncake-wheel/mooncake/
    cd ..
fi

if [ "$BUILD_WITH_EP" = "1" ]; then
    echo "Building Mooncake PG"
    cd mooncake-pg
    if [ -z "$EP_TORCH_VERSIONS" ]; then
        python setup.py build_ext --build-lib .
    else
        for version in ${EP_TORCH_VERSIONS//;/ }; do
            cuda_major=${CUDA_VERSION%%.*}
            if [ "$cuda_major" -ge 13 ]; then
                # TODO: Fix me when we need to support more CUDA 13 versions or when the CI env is fixed
                pip install torch==$version --index-url https://download.pytorch.org/whl/cu130
            else
                pip install torch==$version
            fi
            python setup.py build_ext --build-lib . --force  # Force build when torch version changes
        done
    fi
    cp mooncake/*.so ../mooncake-wheel/mooncake/
    cd ..
fi

echo "Building wheel package..."
# Stage migrated Python modules from their canonical source tree.
MIGRATED_PYTHON_SOURCE_DIR="python/mooncake"
MIGRATED_PYTHON_STAGING_DIR="$(pwd)/mooncake-wheel/mooncake"
STORE_FACADE_SOURCE_DIR="python/mooncake/store"
STORE_FACADE_STAGING_DIR="${MIGRATED_PYTHON_STAGING_DIR}/store"
MIGRATED_PYTHON_INIT_BACKUP=""
# Pending sources: F01-T110 (pools), F01-T114 (structured/ranges/fast-copy),
# F01-T115 (CLI/config), F01-T116 (NoF), and F01-T113 (TE/EP/Reshard).
MIGRATED_PYTHON_MODULES=(
    __init__.py
    py.typed
    _launcher.py
)
cleanup_migrated_python_staging() {
    for module in "${MIGRATED_PYTHON_MODULES[@]}"; do
        if [ "${module}" = "__init__.py" ]; then
            continue
        fi
        rm -f "${MIGRATED_PYTHON_STAGING_DIR}/${module}"
    done
    if [ -n "${MIGRATED_PYTHON_INIT_BACKUP}" ]; then
        cp "${MIGRATED_PYTHON_INIT_BACKUP}" "${MIGRATED_PYTHON_STAGING_DIR}/__init__.py"
        rm -f "${MIGRATED_PYTHON_INIT_BACKUP}"
        MIGRATED_PYTHON_INIT_BACKUP=""
    fi
    rm -rf "${STORE_FACADE_STAGING_DIR}"
}
trap cleanup_migrated_python_staging EXIT
cleanup_migrated_python_staging
MIGRATED_PYTHON_INIT_BACKUP=$(mktemp)
cp "${MIGRATED_PYTHON_STAGING_DIR}/__init__.py" "${MIGRATED_PYTHON_INIT_BACKUP}"
for module in "${MIGRATED_PYTHON_MODULES[@]}"; do
    cp "${MIGRATED_PYTHON_SOURCE_DIR}/${module}" \
       "${MIGRATED_PYTHON_STAGING_DIR}/${module}"
done
cp -R "${STORE_FACADE_SOURCE_DIR}" "${STORE_FACADE_STAGING_DIR}"

# Build the wheel package
cd mooncake-wheel

# Materialize a local copy of the root README.md so that
# `readme = "README.md"` resolves inside this directory. Modern
# setuptools rejects `../`-traversal in the readme path. The file is
# removed by the EXIT trap below.
cp ../README.md README.md

WHEEL_DIR="$(pwd)"
cleanup_wheel_metadata_state() {
    [[ -f "${WHEEL_DIR}/pyproject.toml.backup" ]] && mv "${WHEEL_DIR}/pyproject.toml.backup" "${WHEEL_DIR}/pyproject.toml"
    rm -f "${WHEEL_DIR}/README.md"
    cleanup_migrated_python_staging
}
trap cleanup_wheel_metadata_state EXIT

# TODO(F01-T118, replace-in=UPG1-F22-B02): Receive U's release variants and
# platform-specific tool invocation, external-library exclusions, and NPU ELF
# repair with supported providers. Owner: F22-B02; resolve Python-floor policy
# explicitly and validate each approved platform instead of copying flags.
BUILD_VARIANTS="NON_CUDA_BUILD CU13_BUILD"
BUILD_VARIANT_COUNT=0
for build_variant in $BUILD_VARIANTS; do
    if [ "${!build_variant}" = "1" ]; then
        BUILD_VARIANT_COUNT=$((BUILD_VARIANT_COUNT + 1))
    fi
done
if [ "$BUILD_VARIANT_COUNT" -gt 1 ]; then
    echo "Error: only one of $BUILD_VARIANTS can be set"
    exit 1
fi

# If a previous run was interrupted before the trailing restore (line ~481),
# pyproject.toml is left in a renamed state and pyproject.toml.backup holds the
# pristine original. Restore it first so the variant rename below always starts
# from the clean file and the backup is never overwritten with modified content.
if [ -f pyproject.toml.backup ]; then
    echo "Restoring pyproject.toml from leftover backup of a previous run"
    mv pyproject.toml.backup pyproject.toml
fi

# Handle package name modification for release build variants
if [ "$NON_CUDA_BUILD" = "1" ]; then
    echo "Modifying package name for non-CUDA build"
    # Backup original pyproject.toml
    cp pyproject.toml pyproject.toml.backup
    # Replace package name and description
    sed -i 's/name = "mooncake-transfer-engine"/name = "mooncake-transfer-engine-non-cuda"/' pyproject.toml
    sed -i 's/^description = "\(.*\)"$/description = "\1 (Non-CUDA version)"/' pyproject.toml
    sed -i 's/^keywords = \[\(.*\)\]$/keywords = [\1, "non-cuda"]/' pyproject.toml
    sed -i 's|"Environment :: GPU :: NVIDIA CUDA", ||' pyproject.toml
    echo "Package name modified to: mooncake-transfer-engine-non-cuda"
elif [ "$CU13_BUILD" = "1" ]; then
    echo "Modifying package name for CU13 build"
    # Backup original pyproject.toml
    cp pyproject.toml pyproject.toml.backup
    # Replace package name and description
    sed -i 's/name = "mooncake-transfer-engine"/name = "mooncake-transfer-engine-cuda13"/' pyproject.toml
    sed -i 's/^description = "\(.*\)"$/description = "\1 (CUDA 13 version)"/' pyproject.toml
    sed -i 's/^keywords = \[\(.*\)\]$/keywords = [\1, "cuda13"]/' pyproject.toml
    echo "Package name modified to: mooncake-transfer-engine-cuda13"
else
    echo "Using standard package name: mooncake-transfer-engine"
fi

echo "Cleaning up previous build artifacts..."
rm -rf ${OUTPUT_DIR}/
mkdir -p ${OUTPUT_DIR}

echo "Installing required build packages"
# patchelf is installed from PyPI alongside auditwheel: auditwheel >= 6.8
# requires patchelf >= 0.14.5, but the distro package on Ubuntu 22.04 (and the
# rocm/musa CI images built on it) is 0.14.3, which makes `auditwheel repair`
# abort. The PyPI wheel lands ahead of /usr/bin on PATH and works everywhere
# the Python toolchain does.
if command -v pip &>/dev/null; then
    python${PYTHON_VERSION} -m pip install --upgrade pip build setuptools wheel auditwheel patchelf
elif command -v uv &>/dev/null; then
    uv pip install --upgrade pip
    uv pip install build setuptools wheel auditwheel patchelf
else
    echo "Error: Neither python${PYTHON_VERSION}, pip nor uv found"
    exit 1
fi

# Create directory for repaired wheels
REPAIRED_DIR="repaired_wheels_${PYTHON_VERSION}"
mkdir -p ${REPAIRED_DIR}

# Detect architecture and glibc version for platform tag
ARCH=$(uname -m)

# Detect glibc version and convert to manylinux format (e.g., "2.39" -> "2_39")
# Requires getconf (checked in dependencies.sh) or ldd as fallback
detect_glibc_version() {
    local ver=""

    # Method 1: use getconf (POSIX standard, most reliable)
    # getconf is checked in dependencies.sh, so it should be available
    ver=$(getconf GNU_LIBC_VERSION 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' || true)
    if [ -n "$ver" ]; then
        echo "$ver" | sed 's/\./_/'
        return
    fi

    # Method 2: use ldd --version (fallback, should also be available)
    ver=$(ldd --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -1)
    if [ -n "$ver" ]; then
        echo "$ver" | sed 's/\./_/'
        return
    fi

    # Final fallback: conservative baseline (should not reach here if dependencies are met)
    echo "2_17"
}

GLIBC_VERSION=$(detect_glibc_version)
if [ -z "$GLIBC_VERSION" ]; then
    GLIBC_VERSION="2_17"  # Conservative fallback
    echo "Warning: Could not detect glibc version, using fallback: $GLIBC_VERSION"
fi

# Determine architecture (simplified)
case "$ARCH" in
    aarch64|arm64)
        ARCH_SUFFIX="aarch64"
        ;;
    x86_64)
        ARCH_SUFFIX="x86_64"
        ;;
    *)
        echo "Error: Unknown or unsupported architecture $ARCH. Failing the build."
        exit 1
        ;;
esac

# Set platform tag if not already set
PLATFORM_TAG=${PLATFORM_TAG:-"manylinux_${GLIBC_VERSION}_${ARCH_SUFFIX}"}

echo "Detected architecture: $ARCH_SUFFIX"
echo "Detected glibc version: $GLIBC_VERSION"
echo "Using platform tag: $PLATFORM_TAG"

echo "Repairing wheel with auditwheel for platform: $PLATFORM_TAG"
python${PYTHON_VERSION} -m build --wheel --outdir ${OUTPUT_DIR}
AUDITWHEEL_CMD="auditwheel"

# Bundle OpenSSL when required: the OSS adapter links libcrypto directly, and
# target systems may not provide the OpenSSL ABI used by the wheel builder.
# Keep libssl eligible too, so auditwheel can repair its libcrypto dependency.
${AUDITWHEEL_CMD} repair ${OUTPUT_DIR}/*.whl \
    --exclude libcurl.so* \
    --exclude libibverbs.so* \
    --exclude libmlx5.so* \
    --exclude libnuma.so* \
    --exclude libstdc++.so* \
    --exclude libgcc_s.so* \
    --exclude libc.so* \
    --exclude libnghttp2.so* \
    --exclude libidn2.so* \
    --exclude librtmp.so* \
    --exclude libssh.so* \
    --exclude libpsl.so* \
    --exclude libgssapi_krb5.so* \
    --exclude libldap.so* \
    --exclude liblber.so* \
    --exclude libbrotlidec.so* \
    --exclude libz.so* \
    --exclude libnl-route-3.so* \
    --exclude libnl-3.so* \
    --exclude libm.so* \
    --exclude liblzma.so* \
    --exclude libunistring.so* \
    --exclude libgnutls.so* \
    --exclude libhogweed.so* \
    --exclude libnettle.so* \
    --exclude libgmp.so* \
    --exclude libkrb5.so* \
    --exclude libk5crypto.so* \
    --exclude libcom_err.so* \
    --exclude libkrb5support.so* \
    --exclude libsasl2.so* \
    --exclude libbrotlicommon.so* \
    --exclude libp11-kit.so* \
    --exclude libtasn1.so* \
    --exclude libkeyutils.so* \
    --exclude libresolv.so* \
    --exclude libffi.so* \
    --exclude libcuda.so* \
    --exclude libcudart.so* \
    --exclude libc10.so* \
    --exclude libc10_cuda.so* \
    --exclude libtorch.so* \
    --exclude libtorch_cpu.so* \
    --exclude libtorch_cuda.so* \
    --exclude libtorch_python.so* \
    --exclude libascendcl.so* \
    --exclude libhccl.so* \
    --exclude libmsprofiler.so* \
    --exclude libgert.so* \
    --exclude libascendcl_impl.so* \
    --exclude libge_executor.so* \
    --exclude libascend_dump.so* \
    --exclude libgraph.so* \
    --exclude libruntime.so* \
    --exclude libascend_watchdog.so* \
    --exclude libprofapi.so* \
    --exclude liberror_manager.so* \
    --exclude libascendalog.so* \
    --exclude libc_sec.so* \
    --exclude libhccl_alg.so* \
    --exclude libhccl_plf.so* \
    --exclude libascend_protobuf.so* \
    --exclude libhybrid_executor.so* \
    --exclude libdavinci_executor.so* \
    --exclude libge_common.so* \
    --exclude libge_common_base.so* \
    --exclude liblowering.so* \
    --exclude libregister.so* \
    --exclude libexe_graph.so* \
    --exclude libmmpa.so* \
    --exclude libplatform.so* \
    --exclude libgraph_base.so* \
    --exclude libruntime_common.so* \
    --exclude libqos_manager.so* \
    --exclude libascend_trace.so* \
    --exclude libmetadef*.so \
    --exclude libllm_datadist*.so \
    --exclude ascend_transport*.so \
    --exclude libaccl_barex.so* \
    -w ${REPAIRED_DIR}/ --plat ${PLATFORM_TAG}

# Replace original wheel with repaired wheel
rm -f ${OUTPUT_DIR}/*.whl
mv ${REPAIRED_DIR}/*.whl ${OUTPUT_DIR}/

cd ..

echo "Wheel package built and repaired successfully!"
