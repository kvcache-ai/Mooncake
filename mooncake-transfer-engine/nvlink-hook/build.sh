#!/bin/bash

# Get output directory from command line argument, default to current directory
OUTPUT_DIR=${1:-.}

echo "Building nvlink hook to: $OUTPUT_DIR"
g++ hook.cpp -o "$OUTPUT_DIR/hook.so" -I/usr/local/cuda/include -lcuda --shared -fPIC

if [ $? -eq 0 ]; then
    echo "Successfully built hook.so in $OUTPUT_DIR"
else
    echo "Failed to build hook.so"
    exit 1
fi
