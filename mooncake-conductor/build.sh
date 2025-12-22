#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 TARGET_PATH BUILD_DIR"
    exit 1
fi

TARGET=$1
BUILD_DIR=$2

cd conductor-ctrl

# Check if go.mod exists
if [ ! -f "go.mod" ]; then
    echo "Error: go.mod file not found"
    exit 1
fi

echo "Cleaning previous build..."
rm -f mooncake_conductor

go mod tidy
echo "Building Go program: mooncake_conductor"

go build -o "$TARGET/mooncake_conductor" main.go


if [ $? -eq 0 ] && [ -f "$TARGET/mooncake_conductor" ]; then
    echo "mooncake_conductor built successfully"
else
    echo "mooncake_conductor build failed"
    exit 1
fi