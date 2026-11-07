#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

#include "types.h"

namespace mooncake {

constexpr size_t kCudaIpcHandleSize = 64;

struct CudaIpcBufferHandle {
    std::array<uint8_t, kCudaIpcHandleSize> handle{};
    uint64_t offset = 0;
    uint64_t size = 0;
    // Exporter-local ordinal under its CUDA_VISIBLE_DEVICES; keep for logging
    // / backward compatibility. Prefer pci_bus_id when opening.
    int32_t device_id = -1;
    // Stable GPU identity across CUDA_VISIBLE_DEVICES remapping
    // (cudaDeviceGetPCIBusId). Empty means a legacy exporter.
    std::string pci_bus_id;
};

struct CudaIpcShmBufferRef {
    uint64_t ptr = 0;
    uint64_t size = 0;
};

struct CudaIpcWriteRequest {
    std::string key;
    CudaIpcShmBufferRef metadata;
    CudaIpcBufferHandle payload;
};

struct CudaIpcReadRequest {
    std::string key;
    CudaIpcBufferHandle destination;
    uint64_t source_offset = 0;
    uint64_t size = 0;
};

}  // namespace mooncake

YLT_REFL(mooncake::CudaIpcBufferHandle, handle, offset, size, device_id,
         pci_bus_id);
YLT_REFL(mooncake::CudaIpcShmBufferRef, ptr, size);
YLT_REFL(mooncake::CudaIpcWriteRequest, key, metadata, payload);
YLT_REFL(mooncake::CudaIpcReadRequest, key, destination, source_offset, size);
