#!/bin/bash
# Script to build the mooncake wheel package
# Usage: ./scripts/build_wheel.sh [python_version] [output_dir]
# Example: ./scripts/build_wheel.sh 3.10 dist-3.10

set -e  # Exit immediately if a command exits with a non-zero status
set -x

# Get Python version from environment variable or argument
PYTHON_VERSION=${PYTHON_VERSION:-${1:-$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")}}
# Get output directory from environment variable or argument
OUTPUT_DIR=${OUTPUT_DIR:-${2:-"dist"}}
# Detect CUDA version (env wins, then nvcc, then /usr/local/cuda/version.txt, else 0.0)
CUDA_VERSION=${CUDA_VERSION:-$(nvcc --version 2>/dev/null | grep -o "release [0-9][0-9]*\.[0-9]*" | awk '{print $2}' || true)}
if [ -z "$CUDA_VERSION" ] && [ -f /usr/local/cuda/version.txt ]; then
    CUDA_VERSION=$(grep -Eo "[0-9]+\.[0-9]+" /usr/local/cuda/version.txt | head -n1)
fi
CUDA_VERSION=${CUDA_VERSION:-"0.0"}
echo "Building wheel for Python ${PYTHON_VERSION} with output directory ${OUTPUT_DIR}"
echo "Detected CUDA version ${CUDA_VERSION}"

# Ensure LD_LIBRARY_PATH includes /usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

echo "Cleaning wheel-build directory"
rm -rf mooncake-wheel/mooncake_transfer_engine*
rm -rf mooncake-wheel/build/
rm -f mooncake-wheel/mooncake/*.so

echo "Creating directory structure..."

# Copy engine.so to mooncake directory (will be imported by transfer module)
cp build/mooncake-integration/engine.*.so mooncake-wheel/mooncake/engine.so

# Copy store.so to mooncake directory
if [ -f build/mooncake-integration/store.*.so ]; then
    echo "Copying store.so..."
    cp build/mooncake-integration/store.*.so mooncake-wheel/mooncake/store.so
    echo "Copying master binary..."
    # Copy master binary
    cp build/mooncake-store/src/mooncake_master mooncake-wheel/mooncake/
    # Copy client binary
    cp build/mooncake-store/src/mooncake_client mooncake-wheel/mooncake/
    # Copy async_store.py
    cp mooncake-integration/store/async_store.py mooncake-wheel/mooncake/async_store.py
else
    echo "Skipping store.so (not built - likely WITH_STORE is set to OFF)"
fi

# Copy libmooncake_store.so to mooncake directory (only when BUILD_SHARED_LIBS is set)
if [ -f build/mooncake-store/src/libmooncake_store.so ]; then
    echo "Copying libmooncake_store.so..."
    cp build/mooncake-store/src/libmooncake_store.so mooncake-wheel/mooncake/libmooncake_store.so
fi

# Copy libtransfer_engine.so to mooncake directory (only when USE_ETCD is set)
if [ -f build/mooncake-common/etcd/libetcd_wrapper.so ]; then
    echo "Copying libetcd_wrapper.so..."
    cp build/mooncake-common/etcd/libetcd_wrapper.so mooncake-wheel/mooncake/libetcd_wrapper.so
fi

# Copy libtransfer_engine.so to mooncake directory (only when BUILD_SHARED_LIBS is set)
if [ -f build/mooncake-transfer-engine/src/libtransfer_engine.so ]; then
    echo "Copying libtransfer_engine.so..."
    cp build/mooncake-transfer-engine/src/libtransfer_engine.so mooncake-wheel/mooncake/libtransfer_engine.so
fi

# Copy ascend_transport.so to mooncake directory (only when USE_ASCEND_DIRECT is set)
if [ -f build/mooncake-transfer-engine/src/transport/ascend_transport/ascend_transport.so ]; then
    echo "Copying ascend_transport.so..."
    cp build/mooncake-transfer-engine/src/transport/ascend_transport/ascend_transport.so mooncake-wheel/mooncake/ascend_transport.so
fi

# Copy nvlink-allocator.so to mooncake directory (only if it exists - CUDA builds only)
if [ -f build/mooncake-transfer-engine/nvlink-allocator/nvlink_allocator.so ] \
   || [ -f /usr/lib/libaccl_barex.so ] \
   || [ -f /usr/lib64/libaccl_barex.so ]; then
    if [ -f build/mooncake-transfer-engine/nvlink-allocator/nvlink_allocator.so ]; then
     echo "Copying CUDA nvlink_allocator.so..."
     cp build/mooncake-transfer-engine/nvlink-allocator/nvlink_allocator.so mooncake-wheel/mooncake/nvlink_allocator.so
    fi
    echo "Copying allocator libraries..."
    # Copy allocator.py
    cp mooncake-integration/allocator.py mooncake-wheel/mooncake/allocator.py
else
    echo "Skipping nvlink_allocator.so (not built - likely ARM64 or non-CUDA build)"
fi

echo "Copying transfer_engine_bench..."
# Copy transfer_engine_bench
cp build/mooncake-transfer-engine/example/transfer_engine_bench mooncake-wheel/mooncake/

if [ -f "build/mooncake-transfer-engine/src/transport/ascend_transport/hccl_transport/ascend_transport_c/libascend_transport_mem.so" ]; then
    cp build/mooncake-transfer-engine/src/transport/ascend_transport/hccl_transport/ascend_transport_c/libascend_transport_mem.so mooncake-wheel/mooncake/
    echo "Copying ascend_transport_mem libraries..."
else
    echo "Skipping libascend_transport_mem.so (not built - Ascend disabled)"
fi

if [ "$BUILD_WITH_EP" = "1" ]; then
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
# Build the wheel package
cd mooncake-wheel

# Handle package name modification for non-CUDA builds
if [ "$NON_CUDA_BUILD" = "1" ]; then
    echo "Modifying package name for non-CUDA build"
    # Backup original pyproject.toml
    cp pyproject.toml pyproject.toml.backup
    # Replace package name and description
    sed -i 's/name = "mooncake-transfer-engine"/name = "mooncake-transfer-engine-non-cuda"/' pyproject.toml
    sed -i 's/description = "Python binding of a Mooncake library using pybind11"/description = "Python binding of a Mooncake library using pybind11 (Non-CUDA version)"/' pyproject.toml
    sed -i 's/keywords = \["mooncake", "data transfer", "kv cache", "llm inference"\]/keywords = ["mooncake", "data transfer", "kv cache", "llm inference", "non-cuda"]/' pyproject.toml
    echo "Package name modified to: mooncake-transfer-engine-non-cuda"
else
    echo "Using standard package name: mooncake-transfer-engine"
fi

echo "Cleaning up previous build artifacts..."
rm -rf ${OUTPUT_DIR}/
mkdir -p ${OUTPUT_DIR}

echo "Installing required build packages"
pip install --upgrade pip
pip install build setuptools wheel auditwheel

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

if [ "$PYTHON_VERSION" = "3.8" ]; then
    echo "Repairing wheel with auditwheel for platform: $PLATFORM_TAG"
    python -m build --wheel --outdir ${OUTPUT_DIR}

    echo "python 3.8 auditwheel does not support wild-cards..."
    PATTERNS=(
        "libcurl.so*"
        "libibverbs.so*"
        "libmlx5.so*"
        "libnuma.so*"
        "libstdc++.so*"
        "libgcc_s.so*"
        "libc.so*"
        "libnghttp2.so*"
        "libidn2.so*"
        "librtmp.so*"
        "libssh.so*"
        "libpsl.so*"
        "libssl.so*"
        "libcrypto.so*"
        "libgssapi_krb5.so*"
        "libldap.so*"
        "liblber.so*"
        "libbrotlidec.so*"
        "libz.so*"
        "libnl-route-3.so*"
        "libnl-3.so*"
        "libm.so*"
        "liblzma.so*"
        "libunistring.so*"
        "libgnutls.so*"
        "libhogweed.so*"
        "libnettle.so*"
        "libgmp.so*"
        "libkrb5.so*"
        "libk5crypto.so*"
        "libcom_err.so*"
        "libkrb5support.so*"
        "libsasl2.so*"
        "libbrotlicommon.so*"
        "libp11-kit.so*"
        "libtasn1.so*"
        "libkeyutils.so*"
        "libresolv.so*"
        "libffi.so*"
        "libcuda.so*"
        "libcudart.so*"
        "libc10.so*"
        "libc10_cuda.so*"
        "libtorch.so*"
        "libtorch_cpu.so*"
        "libtorch_cuda.so*"
        "libtorch_python.so*"
        "libascendcl.so*"
        "libhccl.so*"
        "libmsprofiler.so*"
        "libgert.so*"
        "libascendcl_impl.so*"
        "libge_executor.so*"
        "libascend_dump.so*"
        "libgraph.so*"
        "libruntime.so*"
        "libascend_watchdog.so*"
        "libprofapi.so*"
        "liberror_manager.so*"
        "libascendalog.so*"
        "libc_sec.so*"
        "libhccl_alg.so*"
        "libhccl_plf.so*"
        "libascend_protobuf.so*"
        "libhybrid_executor.so*"
        "libdavinci_executor.so*"
        "libge_common.so*"
        "libge_common_base.so*"
        "liblowering.so*"
        "libregister.so*"
        "libexe_graph.so*"
        "libmmpa.so*"
        "libplatform.so*"
        "libgraph_base.so*"
        "libruntime_common.so*"
        "libqos_manager.so*"
        "libascend_trace.so*"
        "libmetadef*.so"
        "libadxl*.so"
    )

    for pattern in "${PATTERNS[@]}"; do
        for libpath in /usr/local/cuda* /usr/local/cuda-12.8/lib* /usr/lib* /usr/local/lib* /lib*; do
            if [ -d "$libpath" ]; then
                for lib in $(find $libpath -name "$pattern" 2>/dev/null); do
                    # Get just the filename
                    libname=$(basename "$lib")
                    EXCLUDE_OPTS="${EXCLUDE_OPTS} --exclude $libname "
                done
            fi
        done
    done

    # Manually fix for libcuda since it needs libcuda.so.1 but I didn't get it.
    EXCLUDE_OPTS="${EXCLUDE_OPTS} --exclude libcuda.so.1 "

    echo "Running auditwheel with exclude options: $EXCLUDE_OPTS"
    auditwheel repair ${OUTPUT_DIR}/*.whl $EXCLUDE_OPTS -w ${REPAIRED_DIR}/ --plat ${PLATFORM_TAG}
else
    echo "Repairing wheel with auditwheel for platform: $PLATFORM_TAG"
    python -m build --wheel --outdir ${OUTPUT_DIR}
    auditwheel repair ${OUTPUT_DIR}/*.whl \
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
    --exclude libssl.so* \
    --exclude libcrypto.so* \
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
fi


# Replace original wheel with repaired wheel
rm -f ${OUTPUT_DIR}/*.whl
mv ${REPAIRED_DIR}/*.whl ${OUTPUT_DIR}/

cd ..

echo "Wheel package built and repaired successfully!"
