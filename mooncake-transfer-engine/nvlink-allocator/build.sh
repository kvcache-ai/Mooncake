#!/bin/bash

set -e

source "$(dirname "$(readlink -f "$0")")/../scripts/allocator_build_common.sh"

# Check for flags
USE_NVCC=false
USE_HIPCC=false
USE_MCC=false
CI_BUILD=false

if [[ "$1" == "--use-nvcc" ]]; then
    USE_NVCC=true
    shift
elif [[ "$1" == "--use-hipcc" ]]; then
    USE_HIPCC=true
    shift
elif [[ "$1" == "--use-mcc" ]]; then
    USE_MCC=true
    shift
elif [[ "$1" == "--ci-build" ]]; then
    CI_BUILD=true
    shift
fi

# Get output directory from command line argument, default to current directory
OUTPUT_DIR=${1:-.}

prepare_allocator_build_env "$OUTPUT_DIR" "${2:-}"

echo "Building nvlink allocator to: $OUTPUT_DIR"

CPP_FILE="${SCRIPT_DIR}/nvlink_allocator.cpp"

# Choose build command based on flags
if [ "$CI_BUILD" = true ]; then
    # CI build: use nvcc without linking cuda
    nvcc "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" -shared -Xcompiler -fPIC -I/usr/local/cuda/include ${INCLUDE_FLAGS} -L/usr/local/cuda-12.8/targets/x86_64-linux/lib -lcuda -DUSE_CUDA=1
elif [ "$USE_NVCC" = true ]; then
    # Regular nvcc build with cuda linking
    nvcc "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" -shared -Xcompiler -fPIC -lcuda -I/usr/local/cuda/include ${INCLUDE_FLAGS} -DUSE_CUDA=1
elif [ "$USE_HIPCC" = true ]; then
    hipify-perl "$CPP_FILE" > "${OUTPUT_DIR}/nvlink_allocator.cpp"
    hipcc "$OUTPUT_DIR/nvlink_allocator.cpp" -o "$OUTPUT_DIR/nvlink_allocator.so" -shared -fPIC -lamdhip64 -I/opt/rocm/include ${INCLUDE_FLAGS} -DUSE_HIP=1
elif [ "$USE_MCC" = true ]; then
    mcc "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" --shared -fPIC -lmusa -I/usr/local/musa/include ${INCLUDE_FLAGS} -DUSE_MUSA=1
else
    # Default g++ build
    g++ "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" --shared -fPIC -lcuda -I/usr/local/cuda/include ${INCLUDE_FLAGS} -DUSE_CUDA=1
fi

if [ $? -eq 0 ]; then
    echo "Successfully built nvlink_allocator.so in $OUTPUT_DIR"
else
    echo "Failed to build nvlink_allocator.so"
    exit 1
fi
