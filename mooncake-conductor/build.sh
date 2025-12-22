#!/bin/bash

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
go build -o mooncake_conductor main.go


if [ $? -eq 0 ] && [ -f "mooncake_conductor" ]; then
    echo "mooncake_conductor built successfully"
else
    echo "mooncake_conductor build failed"
    exit 1
fi