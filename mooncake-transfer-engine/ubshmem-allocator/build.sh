#!/bin/bash

set -e

source "$(dirname "$(readlink -f "$0")")/../scripts/allocator_build_common.sh"

# Get output directory from command line argument, default to current directory
OUTPUT_DIR=${1:-.}

prepare_allocator_build_env "$OUTPUT_DIR" "${2:-}"

echo "Building ubshmem fabric allocator to: $OUTPUT_DIR"

CPP_FILE="${SCRIPT_DIR}/ubshmem_fabric_allocator.cpp"

# Detect CPU architecture
CURRENT_CPU=$(uname -m)
CPU_ARCH="unknown"
if [[ "$CURRENT_CPU" =~ ^(aarch64|arm64)$ ]]; then
    CPU_ARCH="aarch64"
elif [[ "$CURRENT_CPU" =~ ^(x86_64|amd64)$ ]]; then
    CPU_ARCH="x86_64"
else
    echo "Warning: Unsupported cpu arch: $CURRENT_CPU"
fi

# Find Ascend toolkit
if [ -n "$ASCEND_HOME_PATH" ]; then
    echo "Use env ASCEND_HOME_PATH"
    ASCEND_TOOLKIT_ROOT="${ASCEND_HOME_PATH}/${CPU_ARCH}-linux"
else
    ASCEND_TOOLKIT_ROOT=$(find /usr/local/Ascend/ascend-toolkit/latest -maxdepth 1 -type d -name "*-linux" 2>/dev/null | head -n 1)
fi

if [ -z "$ASCEND_TOOLKIT_ROOT" ]; then
    if [ -n "$ASCEND_HOME_PATH" ]; then
        echo "Error: Cannot find Ascend toolkit in ${ASCEND_HOME_PATH}/${CPU_ARCH}-linux"
    else
        echo "Error: Cannot find Ascend toolkit in /usr/local/Ascend/ascend-toolkit/latest"
    fi
    exit 1
fi

ASCEND_INCLUDE_DIR="${ASCEND_TOOLKIT_ROOT}/include"
ASCEND_LIB_DIR="${ASCEND_TOOLKIT_ROOT}/lib64"

echo "Using Ascend toolkit at: $ASCEND_TOOLKIT_ROOT"

g++ "$CPP_FILE" \
    -o "$OUTPUT_DIR/ubshmem_fabric_allocator.so" \
    -shared -fPIC \
    -std=c++11 \
    -I"$ASCEND_INCLUDE_DIR" \
    ${INCLUDE_FLAGS} \
    -L"$ASCEND_LIB_DIR" \
    -lascendcl \
    -DUSE_UBSHMEM=ON

if [ $? -eq 0 ]; then
    echo "Successfully built ubshmem_fabric_allocator.so in $OUTPUT_DIR"
else
    echo "Failed to build ubshmem_fabric_allocator.so"
    exit 1
fi
