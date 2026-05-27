#pragma once

#include <tent/device/ir/device_ops.cuh>

// When compiling with -mtgpu, the MUSA compiler (clang-based) automatically
// provides atomicAdd, atomicCAS, __threadfence_system(), etc.
// Do NOT include mp_ext_60_atomic_functions.h — it conflicts with the
// compiler's built-in declarations (__clang_musa_device_functions.h).
//
// CRITICAL: Do NOT use atomicAdd_system / atomicCAS_system. They cause the
// MUSA compiler (clang-14 based) to enter an infinite SelectionDAG loop,
// producing 1.6GB+ of debug output and never completing compilation.
// Use block-scope atomics + __threadfence_system() instead.
//
// Cache-hint stores (__stcg etc.) are also provided automatically by the
// MUSA toolchain when targeting MP_32+.
//
// Link with -lmusart only (no -lmusadevrt — it doesn't exist).

#if __MUSA_ARCH__ >= 320
// __stcg is available for MMIO writes (bypasses L1, stores to L2)
#define MUSA_HAS_STCG 1
#else
#define MUSA_HAS_STCG 0
#endif

namespace mooncake::tent::device::musa_platform {

// ---------------------------------------------------------------------------
// Byte copy (same as CUDA version)
// ---------------------------------------------------------------------------
static __device__ __forceinline__ void musa_byte_copy(void* dst,
                                                      const void* src,
                                                      size_t len) {
    auto* d = reinterpret_cast<uint8_t*>(dst);
    const auto* s = reinterpret_cast<const uint8_t*>(src);
    for (size_t i = 0; i < len; ++i) d[i] = s[i];
}

// ---------------------------------------------------------------------------
// store_release / load_acquire
//
// MUSA does not expose st.release / ld.acquire via inline ASM (the PTX-style
// syntax is commented out in MT-DeepEP).  The proven pattern is:
//   store_release  =  volatile_store  +  __threadfence_system()
//   load_acquire   =  volatile_load   +  __threadfence_system()
//
// volatile_store / volatile_load are compiler builtins that prevent
// reordering by the compiler; __threadfence_system() provides the
// hardware-level system-scope fence (equivalent to fence.acq_rel.sys).
// ---------------------------------------------------------------------------
static __device__ __forceinline__ void musa_store_release(void* dst,
                                                          const void* src,
                                                          size_t len) {
    // Copy bytes then issue a release fence
    musa_byte_copy(dst, src, len);
    __threadfence_system();
}

static __device__ __forceinline__ void musa_load_acquire(void* dst,
                                                         const void* src,
                                                         size_t len) {
    // Issue an acquire fence before the load
    __threadfence_system();
    musa_byte_copy(dst, src, len);
}

static __device__ __forceinline__ void musa_store_relaxed(void* dst,
                                                          const void* src,
                                                          size_t len) {
    musa_byte_copy(dst, src, len);
}

static __device__ __forceinline__ void musa_load_relaxed(void* dst,
                                                         const void* src,
                                                         size_t len) {
    musa_byte_copy(dst, src, len);
}

// ---------------------------------------------------------------------------
// Atomic-width store_release / load_acquire
//
// MUSA lacks st.release / ld.acquire PTX equivalents. Use the proven
// fence + volatile store/load pattern with system-scope fence.
// ---------------------------------------------------------------------------

static __device__ __forceinline__ void musa_store_release_32(volatile void* dst,
                                                              uint32_t value) {
    __threadfence_system();
    volatile uint32_t* vptr = reinterpret_cast<volatile uint32_t*>(dst);
    *vptr = value;
}

static __device__ __forceinline__ void musa_store_release_64(volatile void* dst,
                                                              uint64_t value) {
    __threadfence_system();
    volatile uint64_t* vptr = reinterpret_cast<volatile uint64_t*>(dst);
    *vptr = value;
}

static __device__ __forceinline__ uint32_t
musa_load_acquire_32(const volatile void* src) {
    __threadfence_system();
    volatile const uint32_t* vptr = reinterpret_cast<volatile const uint32_t*>(src);
    return *vptr;
}

static __device__ __forceinline__ uint64_t
musa_load_acquire_64(const volatile void* src) {
    __threadfence_system();
    volatile const uint64_t* vptr = reinterpret_cast<volatile const uint64_t*>(src);
    return *vptr;
}

// ---------------------------------------------------------------------------
// atomic_add_release
//
// IMPORTANT: Do NOT use atomicAdd_system / atomicCAS_system — they cause the
// MUSA compiler (clang-14 based) to enter an infinite SelectionDAG loop,
// producing 1.6GB+ of debug output and never completing compilation.
//
// Instead, use block-scope atomicAdd + __threadfence_system() for the same
// effect: the atomic provides the read-modify-write, and the system-scope
// fence ensures visibility across GPUs/nodes.
// ---------------------------------------------------------------------------
static __device__ __forceinline__ void musa_atomic_add_release(void* addr,
                                                               uint64_t value) {
    atomicAdd(reinterpret_cast<unsigned long long*>(addr),
              static_cast<unsigned long long>(value));
    __threadfence_system();
}

// ---------------------------------------------------------------------------
// atomic_load_acquire
//
// No ld.acquire.sys.global in MUSA.  Use fence + volatile_load pattern.
// ---------------------------------------------------------------------------
static __device__ __forceinline__ uint64_t
musa_atomic_load_acquire(void* addr) {
    __threadfence_system();
    // Use volatile_load for compiler ordering guarantee
    volatile uint64_t* vptr = reinterpret_cast<volatile uint64_t*>(addr);
    return *vptr;
}

// ---------------------------------------------------------------------------
// atomic_cas_acquire
//
// Use block-scope atomicCAS + __threadfence_system() for acquire semantics.
// See atomic_add_release comment for why we avoid atomicCAS_system.
// ---------------------------------------------------------------------------
static __device__ __forceinline__ uint32_t
musa_atomic_cas_acquire(void* addr, uint32_t expected, uint32_t desired) {
    unsigned int old = atomicCAS(
        reinterpret_cast<unsigned int*>(addr),
        static_cast<unsigned int>(expected),
        static_cast<unsigned int>(desired));
    __threadfence_system();
    return old;
}

// ---------------------------------------------------------------------------
// mmio_write64 / mmio_write32
//
// For MMIO writes (e.g., RDMA doorbell), we need to bypass L1 cache and
// ensure the store is visible to the PCIe device.  MUSA provides __stcg()
// which stores at the L2/global cache level (bypasses L1), equivalent to
// CUDA's st.global.cg / L1::no_allocate.
//
// Fallback: volatile_store + __threadfence_system() if __stcg is unavailable.
// ---------------------------------------------------------------------------
static __device__ __forceinline__ void musa_mmio_write64(volatile void* addr,
                                                         uint64_t value) {
#if MUSA_HAS_STCG
    // __stcg bypasses L1, stores to L2/global level
    __stcg(reinterpret_cast<uint64_t*>(const_cast<void*>(addr)), value);
#else
    volatile uint64_t* vptr = reinterpret_cast<volatile uint64_t*>(addr);
    *vptr = value;
#endif
    __threadfence_system();
}

static __device__ __forceinline__ void musa_mmio_write32(volatile void* addr,
                                                         uint32_t value) {
#if MUSA_HAS_STCG
    __stcg(reinterpret_cast<uint32_t*>(const_cast<void*>(addr)), value);
#else
    volatile uint32_t* vptr = reinterpret_cast<volatile uint32_t*>(addr);
    *vptr = value;
#endif
    __threadfence_system();
}

// ---------------------------------------------------------------------------
// fence_acq_rel
// ---------------------------------------------------------------------------
static __device__ __forceinline__ void musa_fence_acq_rel() {
    __threadfence_system();
}

// ---------------------------------------------------------------------------
// spin_wait_eq / spin_wait_ne
//
// __musa_nanosleep is an undocumented compiler builtin that may not be
// available on all archs.  Use clock64() backoff as fallback (same pattern
// as MT-DeepEP's timeout_check).
// ---------------------------------------------------------------------------
static __device__ __forceinline__ void musa_spin_wait_eq(volatile void* addr,
                                                         uint32_t expected) {
    uint32_t value;
    do {
        __threadfence_system();
        volatile uint32_t* vptr = reinterpret_cast<volatile uint32_t*>(addr);
        value = *vptr;
        if (value == expected) break;
        // Brief backoff — yield the warp slot
#if __MUSA_ARCH__ >= 310
        __musa_nanosleep(100);
#else
        // Fallback: busy-wait with no sleep
#endif
    } while (true);
}

static __device__ __forceinline__ void musa_spin_wait_ne(volatile void* addr,
                                                         uint32_t value) {
    uint32_t loaded;
    do {
        __threadfence_system();
        volatile uint32_t* vptr = reinterpret_cast<volatile uint32_t*>(addr);
        loaded = *vptr;
        if (loaded != value) break;
#if __MUSA_ARCH__ >= 310
        __musa_nanosleep(100);
#endif
    } while (true);
}

// ---------------------------------------------------------------------------
// memcpy_async (placeholder — same as CUDA version)
// ---------------------------------------------------------------------------
static __device__ __forceinline__ void musa_memcpy_async(void* dst,
                                                         const void* src,
                                                         size_t len) {
    musa_byte_copy(dst, src, len);
}

// ---------------------------------------------------------------------------
// DeviceOps singleton & platform struct
// ---------------------------------------------------------------------------
static __device__ DeviceOps musa_device_ops = {
    musa_store_release,     musa_load_acquire,      musa_store_relaxed,
    musa_load_relaxed,      musa_store_release_32,  musa_store_release_64,
    musa_load_acquire_32,   musa_load_acquire_64,   musa_atomic_add_release,
    musa_atomic_load_acquire, musa_atomic_cas_acquire, musa_mmio_write64,
    musa_mmio_write32,      musa_fence_acq_rel,     musa_spin_wait_eq,
    musa_spin_wait_ne,      musa_memcpy_async};

struct MusaPlatform {
    static __device__ __forceinline__ DeviceOps* getOps() {
        return &musa_device_ops;
    }
};

static __device__ __forceinline__ DeviceOps* musaDeviceOps() {
    return MusaPlatform::getOps();
}

}  // namespace mooncake::tent::device::musa_platform
