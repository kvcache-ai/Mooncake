# Mooncake HF3FS Plugin

This plugin implements 3FS native API (USRBIO) as a high-performance storage backend for Mooncake.

## Prerequisites

### 1. 3FS Installation
- Build and install [3FS](https://github.com/deepseek-ai/3FS/)
- Required library: `libhf3fs_api_shared.so` (Default location: `3FS_PATH/build/src/lib/api`)  
  → Install to: `/usr/lib/`
- Required header: `hf3fs_usrbio.h` (Default location: `3FS_PATH/src/lib/api`)  
  → Install to: `/usr/include/`

### 2. Mooncake Configuration
- Enable 3FS support during CMake configuration:
```bash

cmake -DUSE_3FS=ON ...
```

- Build and install Mooncake as usual.

## Usage

### Basic Operation
Start master server and specify the 3FS mount point:
```bash

./build/mooncake-store/src/mooncake_master \
    --root_fs_dir=/path/to/3fs_mount_point 
```
### Important Notes
1. The specified directory **must** be a 3FS mount point  
   - If not, the system will automatically fall back to POSIX API
2. For optimal performance:
   - Ensure proper permissions on the 3FS mount point
   - Verify 3FS service is running before execution

### Example
```bash

ROLE=prefill MOONCAKE_STORAGE_ROOT_DIR=/mnt/3fs python3 ./stress_cluster_benchmark.py
```