#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "types.h"

namespace mooncake {

constexpr size_t kCudaIpcHandleSize = 64;

struct CudaIpcBufferHandle {
    std::array<uint8_t, kCudaIpcHandleSize> handle{};
    uint64_t offset = 0;
    uint64_t size = 0;
    int32_t device_id = -1;
};

}  // namespace mooncake

YLT_REFL(mooncake::CudaIpcBufferHandle, handle, offset, size, device_id);
