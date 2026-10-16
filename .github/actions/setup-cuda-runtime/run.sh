#!/usr/bin/env bash
set -e -o pipefail

cuda_driver_library=$(sed -n \
  's/^CUDA_cuda_driver_LIBRARY:FILEPATH=//p' build/CMakeCache.txt)
if [ -z "$cuda_driver_library" ] || [ ! -f "$cuda_driver_library" ]; then
  echo "::error::CMake did not resolve the CUDA driver library"
  exit 1
fi

cuda_driver_dir=$(dirname "$cuda_driver_library")
if [ ! -e "$cuda_driver_dir/libcuda.so.1" ]; then
  sudo ln -s "$(basename "$cuda_driver_library")" \
    "$cuda_driver_dir/libcuda.so.1"
fi
echo "LIBRARY_PATH=$cuda_driver_dir:${LIBRARY_PATH:-}" >> "$GITHUB_ENV"
echo "LD_LIBRARY_PATH=$cuda_driver_dir:${LD_LIBRARY_PATH:-}" >> "$GITHUB_ENV"
