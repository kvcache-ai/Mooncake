#!/bin/bash

set -e

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

# Get include directories from second argument (if provided)
INCLUDE_LIST=""
if [ $# -ge 2 ]; then
    INCLUDE_LIST=${2}
fi

# Add include directory for cuda_alike.h (relative to build.sh location)
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
INCLUDE_LIST="${INCLUDE_LIST:+${INCLUDE_LIST} }${SCRIPT_DIR}/../include"

# Process include directories into flags
INCLUDE_FLAGS=""
if [ -n "$INCLUDE_LIST" ]; then
    INCLUDE_FLAGS=$(echo "$INCLUDE_LIST" | tr ' ' '\n' | sed 's/^/-I/' | paste -sd' ' -)
fi

echo "Building nvlink allocator to: $OUTPUT_DIR"
# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

CPP_FILE=$(dirname $(readlink -f $0))/nvlink_allocator.cpp  # get cpp file path, under same dir with this script

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
