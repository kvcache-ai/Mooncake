# TENT GPU Vendor Abstraction Layer Design

## Overview

The GPU Vendor Abstraction Layer provides a unified interface for TENT to support multiple GPU vendors through a single codebase. This design follows the same approach as the Transfer Engine (TE) but adapted for TENT's architecture.

## Supported GPU Vendors

| Vendor | API | Compile Flag | Memory Prefix |
|--------|-----|--------------|---------------|
| NVIDIA | CUDA | `USE_CUDA` | `cuda:` |
| Moore Threads | MUSA | `USE_MUSA` | `musa:` |
| AMD | HIP (ROCm) | `USE_HIP` | `hip:` |
| Iluvatar | MACA | `USE_MACA` | `maca:` |
| Huawei | Ascend CANN | `USE_ASCEND` / `USE_ASCEND_DIRECT` | `ascend:` |
| CPU Fallback | None | (none) | `cpu:` |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     TENT Transports                          │
│  (RDMA, NVLink, MNNVL, GDS, IOUring, AscendDirect, etc.)    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              GPU Vendor Abstraction Layer                    │
│                  (gpu_vendor.h)                             │
│  Unified GPU_* macros that map to vendor-specific APIs       │
└─────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
    ┌─────────┐         ┌─────────┐         ┌─────────┐
    │  CUDA   │         │  MUSA   │         │   HIP   │
    │ (nvidia)│         │ (moore) │         │  (amd)  │
    └─────────┘         └─────────┘         └─────────┘
          │                   │                   │
    ┌─────────┐         ┌─────────┐         ┌─────────┐
    │  MACA   │         │ Ascend  │         │   CPU   │
    │(iluvatar)│        │(huawei) │         │ (none)  │
    └─────────┘         └─────────┘         └─────────┘
```

## File Structure

```
tent/include/tent/platform/
├── gpu_vendor.h              # Main abstraction header
└── gpu/
    ├── cuda.h                # NVIDIA CUDA (native)
    ├── musa.h                # Moore Threads MUSA wrapper
    ├── hip.h                 # AMD HIP wrapper
    ├── maca.h                # Iluvatar MACA wrapper
    ├── ascend.h              # Huawei Ascend wrapper
    └── cpu.h                 # CPU-only fallback

tent/src/platform/gpu/
├── CMakeLists.txt
└── gpu_vendor.cpp            # Runtime vendor detection
```

## Usage Example

### In Transport Source Code

```cpp
#include "tent/platform/gpu_vendor.h"

// Allocate GPU memory (works for any vendor)
void* ptr = nullptr;
GPU_CHECK(GPU_MALLOC(&ptr, size));

// Copy memory
GPU_CHECK(GPU_MEMCPY(dst, src, size, GPU_MEMCPY_DEFAULT));

// Free memory
GPU_FREE(ptr);
```

### Error Checking Macro

```cpp
#define GPU_CHECK(call) \
    do { \
        int err = (call); \
        if (err != GPU_SUCCESS) { \
            LOG(ERROR) << "GPU error: " << GPU_GET_ERROR_STRING(err); \
            return Status::InternalError("GPU operation failed"); \
        } \
    } while(0)
```

### Compile-Time Detection

```cpp
if (mooncake::tent::isCudaAvailable()) {
    // CUDA-specific code
} else if (mooncake::tent::isMusaAvailable()) {
    // MUSA-specific code
}
```

### Runtime Detection

```cpp
auto vendor = mooncake::tent::getGpuVendor();
LOG(INFO) << "GPU Vendor: " << mooncake::tent::getGpuVendorName(vendor);
LOG(INFO) << "Memory Prefix: " << mooncake::tent::getGpuMemoryPrefix();
```

## Migration Guide

To migrate existing transport code to use the GPU abstraction:

### Before (CUDA-specific)
```cpp
#include <cuda_runtime.h>

cudaMalloc(&ptr, size);
cudaMemcpy(dst, src, size, cudaMemcpyDefault);
cudaFree(ptr);
```

### After (Vendor-Agnostic)
```cpp
#include "tent/platform/gpu_vendor.h"

GPU_MALLOC(&ptr, size);
GPU_MEMCPY(dst, src, size, GPU_MEMCPY_DEFAULT);
GPU_FREE(ptr);
```

## Build Configuration

### Build with CUDA (NVIDIA)
```bash
cmake -DUSE_CUDA=ON ..
make
```

### Build with MUSA (Moore Threads)
```bash
cmake -DUSE_MUSA=ON ..
make
```

### Build with HIP (AMD)
```bash
cmake -DUSE_HIP=ON ..
make
```

### Build with Ascend (Huawei)
```bash
cmake -DUSE_ASCEND_DIRECT=ON ..
make
```

## Design Decisions

1. **Macro-based Mapping**: Similar to TE, we use C preprocessor macros to map CUDA API calls to vendor-specific APIs. This allows existing CUDA code to work with minimal changes.

2. **Compile-Time Selection**: The vendor is selected at compile time through CMake flags. This avoids runtime overhead and simplifies the build.

3. **Fallback Support**: A CPU-only fallback is provided when no GPU vendor is available.

4. **API Compatibility**: The abstraction provides a "least common denominator" API that works across all vendors. Vendor-specific features can be accessed through conditional compilation.

5. **Consistent with TE**: The design follows TE's gpu_vendor approach for consistency across the Mooncake codebase.

## Limitations

1. **Fabric Memory**: HIP and some other vendors have limited support for CUDA's fabric memory features. The abstraction provides stubs for these cases.

2. **IPC Handles**: Ascend and HIP have different IPC mechanisms. The abstraction provides stubs that return NOT_SUPPORTED.

3. **Driver API**: Some vendors (notably HIP) don't have full driver API support. The abstraction provides stubs where needed.

## Future Extensions

1. **Runtime Plugin Loading**: Extend the transport plugin system to load GPU vendor support at runtime.

2. **Feature Detection**: Add runtime feature detection to query which features are available on the current platform.

3. **Unified Memory**: Add support for unified memory across different vendors.

4. **Multi-Vendor**: Support for systems with mixed GPU vendors (e.g., NVIDIA + AMD in the same system).
