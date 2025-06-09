#!/bin/bash

# Get output directory from command line argument, default to current directory
OUTPUT_DIR=${1:-.}

echo "Building nvlink hook to: $OUTPUT_DIR"
# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"
nvcc hook.cpp -o "$OUTPUT_DIR/hook.so" --shared -Xcompiler -fPIC

if [ $? -eq 0 ]; then
    echo "Successfully built hook.so in $OUTPUT_DIR"
else
    echo "Failed to build hook.so"
    exit 1
fi
