#pragma once

#include <stddef.h>
#include <stdint.h>

#if defined(USE_MUSA) || defined(MOONCAKE_EP_USE_MUSA)
#include <musa_runtime.h>
#else
#include <cuda_runtime.h>
#endif

namespace mooncake::tent::device {

struct DeviceOps {
    // Bulk byte-copy operations (with memory ordering fences)
    void (*store_release)(void* dst, const void* src, size_t len);
    void (*load_acquire)(void* dst, const void* src, size_t len);
    void (*store_relaxed)(void* dst, const void* src, size_t len);
    void (*load_relaxed)(void* dst, const void* src, size_t len);

    // Atomic-width store/load with release/acquire semantics
    // Use these for flags, counters, signals — NOT for bulk data.
    void (*store_release_32)(volatile void* dst, uint32_t value);
    void (*store_release_64)(volatile void* dst, uint64_t value);
    uint32_t (*load_acquire_32)(const volatile void* src);
    uint64_t (*load_acquire_64)(const volatile void* src);

    // Atomic read-modify-write
    void (*atomic_add_release)(void* addr, uint64_t value);
    uint64_t (*atomic_load_acquire)(void* addr);
    uint32_t (*atomic_cas_acquire)(void* addr, uint32_t expected,
                                   uint32_t desired);

    // MMIO / Doorbell writes
    void (*mmio_write64)(volatile void* addr, uint64_t value);
    void (*mmio_write32)(volatile void* addr, uint32_t value);

    // Memory fence
    void (*fence_acq_rel)();

    // Spin wait
    void (*spin_wait_eq)(volatile void* addr, uint32_t expected);
    void (*spin_wait_ne)(volatile void* addr, uint32_t value);

    // Bulk async copy
    void (*memcpy_async)(void* dst, const void* src, size_t len);
};

template <typename PlatformBackend>
struct DeviceOpsOf {
    static __device__ __forceinline__ DeviceOps* get() {
        return PlatformBackend::getOps();
    }
};

}  // namespace mooncake::tent::device
