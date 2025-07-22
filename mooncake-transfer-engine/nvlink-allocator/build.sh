#!/bin/bash

set -e

# Check for --use-nvcc flag
USE_NVCC=false
if [[ "$1" == "--use-nvcc" ]]; then
    USE_NVCC=true
    shift
fi

# Get output directory from command line argument, default to current directory
OUTPUT_DIR=${1:-.}

echo "Building nvlink allocator to: $OUTPUT_DIR"
# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

CPP_FILE=$(dirname $(readlink -f $0))/nvlink_allocator.cpp  # get cpp file path, under same dir with this script

# Use nvcc or g++ based on flag
if [ "$USE_NVCC" = true ]; then
    nvcc "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" -shared -Xcompiler -fPIC -lcuda -I/usr/local/cuda/include
else
    g++ "$CPP_FILE" -o "$OUTPUT_DIR/nvlink_allocator.so" --shared -fPIC -lcuda -I/usr/local/cuda/include
fi

if [ $? -eq 0 ]; then
    echo "Successfully built nvlink_allocator.so in $OUTPUT_DIR"
else
    echo "Failed to build nvlink_allocator.so"
    exit 1
fi
