#pragma once

#include <stddef.h>
#include <stdint.h>

#include <cuda_runtime.h>

namespace mooncake::tent::device {

struct DeviceOps {
    void (*store_release)(void* dst, const void* src, size_t len);
    void (*load_acquire)(void* dst, const void* src, size_t len);
    void (*store_relaxed)(void* dst, const void* src, size_t len);
    void (*load_relaxed)(void* dst, const void* src, size_t len);

    void (*atomic_add_release)(void* addr, uint64_t value);
    uint64_t (*atomic_load_acquire)(void* addr);
    uint32_t (*atomic_cas_acquire)(void* addr, uint32_t expected,
                                   uint32_t desired);

    void (*mmio_write64)(volatile void* addr, uint64_t value);
    void (*mmio_write32)(volatile void* addr, uint32_t value);

    void (*fence_acq_rel)();

    void (*spin_wait_eq)(volatile void* addr, uint32_t expected);
    void (*spin_wait_ne)(volatile void* addr, uint32_t value);

    void (*memcpy_async)(void* dst, const void* src, size_t len);
};

template <typename PlatformBackend>
struct DeviceOpsOf {
    static __device__ __forceinline__ DeviceOps* get() {
        return PlatformBackend::getOps();
    }
};

}  // namespace mooncake::tent::device
