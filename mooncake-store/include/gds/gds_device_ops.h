// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

// ===================================================================
// GDS Device Operations — abstract interface for vendor-specific
// GPU Direct Storage DMA operations.
//
// Follows the pattern of device/accelerator_device.h:
//   - Pure virtual interface with 14 methods
//   - Per-vendor .cpp implementations (src/gds/*_gds_device.cpp)
//   - Factory function for creation (src/gds/gds_device_factory.cpp)
//   - Zero vendor SDK headers exposed through this header.
//
// GdsDeviceFileHandle is void* — compatible with NVIDIA CUfileHandle_t
// and all currently known vendor GDS APIs (Hygon DCU, Ascend, Moore
// Threads all use pointer-type handles).
// ===================================================================

#include <sys/stat.h>
#include <cstddef>
#include <memory>

namespace mooncake {

// Replacement for CUfileError_t. Callers only use .err and
// IsOk()/IsErr(); no vendor types leak through this header.
struct GdsDeviceError {
    int err = -1;
    static constexpr int SUCCESS = 0;

    bool IsOk() const { return err == SUCCESS; }
    bool IsErr() const { return !IsOk(); }
};

// Binary-compatible with CUfileHandle_t (typedef void*).
using GdsDeviceFileHandle = void*;

// ===================================================================
// Abstract interface — one implementation per GPU vendor.
// ===================================================================

class GdsDeviceOps {
   public:
    virtual ~GdsDeviceOps() = default;

    // Device probe & driver lifecycle
    virtual bool ProbeDeviceNode() = 0;
    virtual GdsDeviceError DriverOpen() = 0;

    // File handle management (POSIX fd ↔ GDS handle)
    virtual GdsDeviceError FileHandleRegister(GdsDeviceFileHandle* out,
                                              int fd) = 0;
    virtual void FileHandleDeregister(GdsDeviceFileHandle handle) = 0;

    // GPU buffer registration for DMA
    virtual GdsDeviceError BufRegister(void* ptr, size_t size) = 0;
    virtual void BufDeregister(void* ptr) = 0;

    // DMA I/O
    virtual ssize_t Write(GdsDeviceFileHandle fh, void* buf, size_t size,
                          off_t file_offset) = 0;
    virtual ssize_t Read(GdsDeviceFileHandle fh, void* buf, size_t size,
                         off_t file_offset) = 0;

    // GPU memory helpers (probe only)
    virtual void* Malloc(size_t size) = 0;
    virtual void Free(void* ptr) = 0;
    virtual void Memset(void* ptr, int value, size_t size) = 0;

    // GPU synchronization
    virtual void DeviceSynchronize() = 0;
    virtual int GetDevice() = 0;
    virtual void CopyDeviceToDevice(void* dst, const void* src,
                                    size_t size) = 0;
};

// ── Factory ──
// Returns a heap-allocated implementation based on available vendor SDK
// and runtime probe. Caller owns the pointer.
//
// Factory logic (in gds_device_factory.cpp):
//   USE_GDS_NVIDIA  → probes /dev/nvidia-fs → NvidiaGdsDeviceOps or fallback
//   USE_GDS_HYGON   → probes /dev/hygon-gds  → HygonGdsDeviceOps or fallback
//   No vendor flag  → FallbackGdsDeviceOps (all methods return sentinel).
std::unique_ptr<GdsDeviceOps> CreateGdsDeviceOps();

// Convenience: thread-safe lazy singleton for call sites that need
// a GdsDeviceOps* without access to a GdsContext (e.g.
// CopyDeviceToDevice in real_client.cpp).
GdsDeviceOps* GetGdsDeviceOpsSingleton();

}  // namespace mooncake
