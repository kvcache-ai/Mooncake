#pragma once

#include <tent/device/ir/device_ops.cuh>

namespace mooncake::tent::device::cuda_platform {

static __device__ __forceinline__ void cuda_byte_copy(void* dst,
                                                      const void* src,
                                                      size_t len) {
    auto* d = reinterpret_cast<uint8_t*>(dst);
    const auto* s = reinterpret_cast<const uint8_t*>(src);
    for (size_t i = 0; i < len; ++i) d[i] = s[i];
}

static __device__ __forceinline__ void cuda_store_release(void* dst,
                                                          const void* src,
                                                          size_t len) {
    cuda_byte_copy(dst, src, len);
    asm volatile("fence.acq_rel.sys;" ::: "memory");
}

static __device__ __forceinline__ void cuda_load_acquire(void* dst,
                                                         const void* src,
                                                         size_t len) {
    cuda_byte_copy(dst, src, len);
    asm volatile("fence.acq_rel.sys;" ::: "memory");
}

static __device__ __forceinline__ void cuda_store_relaxed(void* dst,
                                                          const void* src,
                                                          size_t len) {
    cuda_byte_copy(dst, src, len);
}

static __device__ __forceinline__ void cuda_load_relaxed(void* dst,
                                                         const void* src,
                                                         size_t len) {
    cuda_byte_copy(dst, src, len);
}

static __device__ __forceinline__ void cuda_atomic_add_release(void* addr,
                                                               uint64_t value) {
    asm volatile("red.add.release.sys.global.u64 [%0], %1;"
                 :
                 : "l"(addr), "l"(value)
                 : "memory");
}

static __device__ __forceinline__ uint64_t
cuda_atomic_load_acquire(void* addr) {
    uint64_t value;
    asm volatile("ld.acquire.sys.global.u64 %0, [%1];"
                 : "=l"(value)
                 : "l"(addr)
                 : "memory");
    return value;
}

static __device__ __forceinline__ uint32_t
cuda_atomic_cas_acquire(void* addr, uint32_t expected, uint32_t desired) {
    auto* ptr = reinterpret_cast<unsigned int*>(addr);
    uint32_t old = atomicCAS(ptr, static_cast<unsigned int>(expected),
                             static_cast<unsigned int>(desired));
    asm volatile("fence.acq_rel.sys;" ::: "memory");
    return old;
}

static __device__ __forceinline__ void cuda_mmio_write64(volatile void* addr,
                                                         uint64_t value) {
    asm volatile("st.release.sys.global.L1::no_allocate.b64 [%0], %1;"
                 :
                 : "l"(addr), "l"(value)
                 : "memory");
}

static __device__ __forceinline__ void cuda_mmio_write32(volatile void* addr,
                                                         uint32_t value) {
    asm volatile("st.release.sys.global.L1::no_allocate.b32 [%0], %1;"
                 :
                 : "l"(addr), "r"(value)
                 : "memory");
}

static __device__ __forceinline__ void cuda_fence_acq_rel() {
    asm volatile("fence.acq_rel.sys;" ::: "memory");
}

static __device__ __forceinline__ void cuda_spin_wait_eq(volatile void* addr,
                                                         uint32_t expected) {
    uint32_t value;
    do {
        asm volatile("ld.acquire.sys.global.u32 %0, [%1];"
                     : "=r"(value)
                     : "l"(addr)
                     : "memory");
        if (value == expected) break;
        __nanosleep(100);
    } while (true);
}

static __device__ __forceinline__ void cuda_spin_wait_ne(volatile void* addr,
                                                         uint32_t value) {
    uint32_t loaded;
    do {
        asm volatile("ld.acquire.sys.global.u32 %0, [%1];"
                     : "=r"(loaded)
                     : "l"(addr)
                     : "memory");
        if (loaded != value) break;
        __nanosleep(100);
    } while (true);
}

static __device__ __forceinline__ void cuda_memcpy_async(void* dst,
                                                         const void* src,
                                                         size_t len) {
    cuda_byte_copy(dst, src, len);
}

static __device__ DeviceOps cuda_device_ops = {
    cuda_store_release,      cuda_load_acquire,       cuda_store_relaxed,
    cuda_load_relaxed,       cuda_atomic_add_release, cuda_atomic_load_acquire,
    cuda_atomic_cas_acquire, cuda_mmio_write64,       cuda_mmio_write32,
    cuda_fence_acq_rel,      cuda_spin_wait_eq,       cuda_spin_wait_ne,
    cuda_memcpy_async};

struct CudaPlatform {
    static __device__ __forceinline__ DeviceOps* getOps() {
        return &cuda_device_ops;
    }
};

static __device__ __forceinline__ DeviceOps* cudaDeviceOps() {
    return CudaPlatform::getOps();
}

}  // namespace mooncake::tent::device::cuda_platform
