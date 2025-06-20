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
echo "Building wheel for Python ${PYTHON_VERSION} with output directory ${OUTPUT_DIR}"

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
else
    echo "Skipping store.so (not built - likely WITH_STORE is set to OFF)"
fi

# Copy nvlink-allocator.so to mooncake directory (only if it exists - CUDA builds only)
if [ -f build/mooncake-transfer-engine/nvlink-allocator/nvlink_allocator.so ]; then
    echo "Copying CUDA nvlink_allocator.so..."
    cp build/mooncake-transfer-engine/nvlink-allocator/nvlink_allocator.so mooncake-wheel/mooncake/nvlink_allocator.so
    echo "Copying allocator libraries..."
    # Copy allocator.py
    cp mooncake-integration/allocator.py mooncake-wheel/mooncake/allocator.py
else
    echo "Skipping nvlink_allocator.so (not built - likely ARM64 or non-CUDA build)"
fi

echo "Copying transfer_engine_bench..."
# Copy transfer_engine_bench
cp build/mooncake-transfer-engine/example/transfer_engine_bench mooncake-wheel/mooncake/


echo "Building wheel package..."
# Build the wheel package
cd mooncake-wheel

echo "Cleaning up previous build artifacts..."
rm -rf ${OUTPUT_DIR}/ 
mkdir -p ${OUTPUT_DIR}

echo "Installing required build packages"
pip install --upgrade pip
pip install build setuptools wheel auditwheel

# Create directory for repaired wheels
REPAIRED_DIR="repaired_wheels_${PYTHON_VERSION}"
mkdir -p ${REPAIRED_DIR}

# Detect architecture and set appropriate platform tag
ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    PLATFORM_TAG=${PLATFORM_TAG:-"manylinux_2_35_aarch64"}
    echo "Building for ARM64 architecture"
elif [ "$ARCH" = "x86_64" ]; then
    PLATFORM_TAG=${PLATFORM_TAG:-"manylinux_2_35_x86_64"}
    echo "Building for x86_64 architecture"
else
    echo "Error: Unknown or unsupported architecture $ARCH. Failing the build."
    exit 1
fi

echo "Repairing wheel with auditwheel for platform: $PLATFORM_TAG"
python -m build --wheel --outdir ${OUTPUT_DIR}
auditwheel repair ${OUTPUT_DIR}/*.whl \
--exclude libcurl.so* \
--exclude libibverbs.so* \
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
-w ${REPAIRED_DIR}/ --plat ${PLATFORM_TAG}


# Replace original wheel with repaired wheel
rm -f ${OUTPUT_DIR}/*.whl
mv ${REPAIRED_DIR}/*.whl ${OUTPUT_DIR}/

cd ..

echo "Wheel package built and repaired successfully!"
