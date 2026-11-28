#!/bin/bash

set -e

source "$(dirname "$(readlink -f "$0")")/../scripts/allocator_build_common.sh"

# Get output directory from command line argument, default to current directory
OUTPUT_DIR=${1:-.}

prepare_allocator_build_env "$OUTPUT_DIR" "${2:-}"

echo "Building hip gem allocator to: $OUTPUT_DIR"

ROCM_PATH=${ROCM_PATH:-/opt/rocm}
g++ "${SCRIPT_DIR}/hip_gem_allocator.cpp" -o "$OUTPUT_DIR/hip_gem_allocator.so" \
    -O2 -std=c++17 -shared -fPIC -D__HIP_PLATFORM_AMD__ \
    -I"${ROCM_PATH}/include" ${INCLUDE_FLAGS} \
    -L"${ROCM_PATH}/lib" -lamdhip64 -libverbs -lpthread

echo "Successfully built hip_gem_allocator.so in $OUTPUT_DIR"
