#!/bin/bash
# Script to build the mooncake wheel package
# Usage: ./scripts/build_wheel.sh

set -e  # Exit immediately if a command exits with a non-zero status

# Ensure LD_LIBRARY_PATH includes /usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

echo "Creating directory structure..."
mkdir -p mooncake-wheel/mooncake/lib_so/

echo "Copying Python modules..."
# Copy mooncake_vllm_adaptor to root level for backward compatibility
cp build/mooncake-integration/mooncake_vllm_adaptor.*.so mooncake-wheel/mooncake/mooncake_vllm_adaptor.so

# Copy engine.so to mooncake directory (will be imported by transfer module)
cp build/mooncake-integration/engine.*.so mooncake-wheel/mooncake/engine.so

# Copy engine.so to mooncake directory (will be imported by transfer module)
cp build/mooncake-integration/store.*.so mooncake-wheel/mooncake/store.so

echo "Copying master binary and shared libraries..."
# Copy master binary and shared libraries
cp build/mooncake-store/src/mooncake_master mooncake-wheel/mooncake/
cp build/mooncake-common/etcd/libetcd_wrapper.so mooncake-wheel/mooncake/lib_so/


if ldd mooncake-wheel/mooncake/engine.so | grep -q "libetcd-cpp-api.so"; then
  echo "Legacy etcd-cpp-api-v3 is enabled, adding dependent libraries..."

  # Copy libetcd-cpp-api.so and its dependencies
  if [ -f /usr/local/lib/libetcd-cpp-api.so ]; then
    cp /usr/local/lib/libetcd-cpp-api.so mooncake-wheel/mooncake/lib_so/
    echo "Copied etcd-cpp-api dependencies to wheel package"
  else
    echo "Warning: libetcd-cpp-api.so not found, skipping dependencies"
    exit 1
  fi
fi

patchelf --set-rpath '$ORIGIN/lib_so' --force-rpath mooncake-wheel/mooncake/mooncake_master
patchelf --set-rpath '$ORIGIN/lib_so' --force-rpath mooncake-wheel/mooncake/*.so


echo "Building wheel package..."
# Build the wheel package
cd mooncake-wheel

echo "Building wheel with default version"
python -m build


cd ..

echo "Wheel package built successfully!"
