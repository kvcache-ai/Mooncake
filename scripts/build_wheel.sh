#!/bin/bash
# Script to build the mooncake wheel package
# Usage: ./scripts/build_wheel.sh

set -e  # Exit immediately if a command exits with a non-zero status

# Ensure LD_LIBRARY_PATH includes /usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

echo "Creating directory structure..."
mkdir -p mooncake-wheel/mooncake/transfer/

echo "Copying Python modules..."
# Copy mooncake_vllm_adaptor to root level for backward compatibility
cp build/mooncake-integration/mooncake_vllm_adaptor.*.so mooncake-wheel/mooncake/mooncake_vllm_adaptor.so
cp build/mooncake-integration/mooncake_sglang_adaptor.*.so mooncake-wheel/mooncake/mooncake_sglang_adaptor.so

# Copy engine.so to mooncake directory (will be imported by transfer module)
cp build/mooncake-integration/engine.*.so mooncake-wheel/mooncake/engine.so

echo "Copying master binary and shared libraries..."
# Copy master binary and shared libraries
cp build/mooncake-store/src/mooncake_master mooncake-wheel/mooncake/
cp build/mooncake-common/etcd/libetcd_wrapper.so mooncake-wheel/mooncake/


echo "Building wheel package..."
# Build the wheel package
cd mooncake-wheel

echo "Cleaning up previous build artifacts..."
rm -rf dist/ 

echo "Building wheel with default version"
python -m build

# Create directory for repaired wheels
mkdir -p repaired_wheels

echo "Repairing wheel with auditwheel..."
# Use auditwheel to repair the wheel and include missing .so files
auditwheel repair dist/*.whl -w repaired_wheels/ --plat manylinux_2_35_x86_64

# Replace original wheel with repaired wheel
rm -f dist/*.whl
mv repaired_wheels/*.whl dist/

cd ..

echo "Wheel package built and repaired successfully!"
