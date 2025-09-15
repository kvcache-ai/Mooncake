#!/bin/bash

set -e

# Check for flags
USE_NVCC=false
CI_BUILD=false

if [[ "$1" == "--use-nvcc" ]]; then
    USE_NVCC=true
    shift
elif [[ "$1" == "--ci-build" ]]; then
    CI_BUILD=true
    shift
fi

# Get output directory from command line argument, default to current directory
OUTPUT_DIR=${1:-.}

echo "Building nvlink allocator to: $OUTPUT_DIR"
# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

CPP_FILE=$(dirname $(readlink -f $0))/nvlink_allocator.cpp  # get cpp file path, under same dir with this script

# Choose build command based on flags
if [ "$CI_BUILD" = true ]; then
    # CI build: use nvcc without linking cuda
    nvcc "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" -shared -Xcompiler -fPIC -I/usr/local/cuda/include
elif [ "$USE_NVCC" = true ]; then
    # Regular nvcc build with cuda linking
    nvcc "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" -shared -Xcompiler -fPIC -lcuda -I/usr/local/cuda/include
else
    # Default g++ build
    g++ "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" --shared -fPIC -lcuda -I/usr/local/cuda/include
fi

if [ $? -eq 0 ]; then
    echo "Successfully built nvlink_allocator.so in $OUTPUT_DIR"
else
    echo "Failed to build nvlink_allocator.so"
    exit 1
fi
