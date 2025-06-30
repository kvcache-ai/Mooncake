#!/bin/bash

set -e

# Get output directory from command line argument, default to current directory
OUTPUT_DIR=${1:-.}

echo "Building nvlink hook to: $OUTPUT_DIR"
# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

CPP_FILE=$(dirname $(readlink -f $0))/hook.cpp  # get cpp file path, under same dir with this script

if command -v nvcc &> /dev/null; then
    nvcc "$CPP_FILE" -o "$OUTPUT_DIR/hook.so" --shared -Xcompiler -fPIC
else
    g++ "$CPP_FILE" -o "$OUTPUT_DIR/hook.so" --shared -fPIC -lcuda  -I/usr/local/cuda/include
fi

if [ $? -eq 0 ]; then
    echo "Successfully built hook.so in $OUTPUT_DIR"
else
    echo "Failed to build hook.so"
    exit 1
fi
