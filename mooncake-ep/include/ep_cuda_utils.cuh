#pragma once

#include <mooncake_ep_exception.cuh>

__device__ __forceinline__ void trap() { asm("trap;"); }

__device__ __forceinline__ void memory_fence() {
    asm volatile("fence.acq_rel.sys;" ::: "memory");
}

__device__ __forceinline__ void memory_fence_gpu() {
    asm volatile("fence.acq_rel.gpu;" ::: "memory");
}

__device__ __forceinline__ void memory_fence_cta() {
    asm volatile("fence.acq_rel.cta;" ::: "memory");
}

__device__ __forceinline__ void st_relaxed_sys_global(const int *ptr, int val) {
    asm volatile("st.relaxed.sys.global.s32 [%0], %1;" ::"l"(ptr), "r"(val)
                 : "memory");
}

__device__ __forceinline__ void st_release_sys_global(const int *ptr, int val) {
    asm volatile("st.release.sys.global.s32 [%0], %1;" ::"l"(ptr), "r"(val)
                 : "memory");
}

__device__ __forceinline__ void st_release_cta(const int *ptr, int val) {
    asm volatile("st.release.cta.s32 [%0], %1;" ::"l"(ptr), "r"(val)
                 : "memory");
}

__device__ __forceinline__ int ld_acquire_sys_global(const int *ptr) {
    int ret;
    asm volatile("ld.acquire.sys.global.s32 %0, [%1];" : "=r"(ret) : "l"(ptr));
    return ret;
}

__device__ __forceinline__ uint64_t ld_acquire_sys_global(const uint64_t *ptr) {
    uint64_t ret;
    asm volatile("ld.acquire.sys.global.u64 %0, [%1];" : "=l"(ret) : "l"(ptr));
    return ret;
}

__device__ __forceinline__ int ld_acquire_global(const int *ptr) {
    int ret;
    asm volatile("ld.acquire.gpu.global.s32 %0, [%1];" : "=r"(ret) : "l"(ptr));
    return ret;
}

__device__ __forceinline__ int atomic_add_release_sys_global(const int *ptr,
                                                             int value) {
    int ret;
    asm volatile("atom.add.release.sys.global.s32 %0, [%1], %2;"
                 : "=r"(ret)
                 : "l"(ptr), "r"(value));
    return ret;
}

__device__ __forceinline__ int atomic_add_release_global(const int *ptr,
                                                         int value) {
    int ret;
    asm volatile("atom.add.release.gpu.global.s32 %0, [%1], %2;"
                 : "=r"(ret)
                 : "l"(ptr), "r"(value));
    return ret;
}

__device__ __forceinline__ int ld_acquire_cta(const int *ptr) {
    int ret;
    asm volatile("ld.acquire.cta.s32 %0, [%1];" : "=r"(ret) : "l"(ptr));
    return ret;
}

__device__ __forceinline__ uint8_t ld_na_relaxed(const uint8_t *ptr) {
    uint16_t ret;
    asm volatile("ld.relaxed.gpu.global.L1::no_allocate.b8 %0, [%1];"
                 : "=h"(ret)
                 : "l"(ptr));
    return static_cast<uint8_t>(ret);
}

__device__ __forceinline__ uint16_t ld_na_relaxed(const uint16_t *ptr) {
    uint16_t ret;
    asm volatile("ld.relaxed.gpu.global.L1::no_allocate.b16 %0, [%1];"
                 : "=h"(ret)
                 : "l"(ptr));
    return ret;
}

__device__ __forceinline__ uint32_t ld_na_relaxed(const uint32_t *ptr) {
    uint32_t ret;
    asm volatile("ld.relaxed.gpu.global.L1::no_allocate.b32 %0, [%1];"
                 : "=r"(ret)
                 : "l"(ptr));
    return ret;
}

__device__ __forceinline__ uint64_t ld_na_relaxed(const uint64_t *ptr) {
    uint64_t ret;
    asm volatile("ld.relaxed.gpu.global.L1::no_allocate.b64 %0, [%1];"
                 : "=l"(ret)
                 : "l"(ptr));
    return ret;
}

__device__ __forceinline__ int ld_volatile_global(const int *ptr) {
    int ret;
    asm volatile("ld.volatile.global.s32 %0, [%1];" : "=r"(ret) : "l"(ptr));
    return ret;
}

__device__ __forceinline__ float ld_volatile_global(const float *ptr) {
    float ret;
    asm volatile("ld.volatile.global.f32 %0, [%1];" : "=f"(ret) : "l"(ptr));
    return ret;
}

__device__ __forceinline__ int64_t ld_volatile_global(const int64_t *ptr) {
    int64_t ret;
    asm volatile("ld.volatile.global.s64 %0, [%1];" : "=l"(ret) : "l"(ptr));
    return ret;
}

__device__ __forceinline__ int64_t ld_volatile_global(const uint64_t *ptr) {
    int64_t ret;
    asm volatile("ld.volatile.global.u64 %0, [%1];" : "=l"(ret) : "l"(ptr));
    return ret;
}

#ifndef DISABLE_AGGRESSIVE_PTX_INSTRS
#define LD_NC_FUNC "ld.global.nc.L1::no_allocate.L2::256B"
#else
#define LD_NC_FUNC "ld.volatile.global"
#endif

// `ld.global.nc.L1::no_allocate` will be translated into
// `LDG.E.NA.[width].CONSTANT` in SASS
template <typename dtype_t>
__device__ __forceinline__ dtype_t ld_nc_global(const dtype_t *ptr) {
    auto ret = ld_nc_global(
        reinterpret_cast<const typename VecInt<sizeof(dtype_t)>::vec_t *>(ptr));
    return *reinterpret_cast<dtype_t *>(&ret);
}

template <>
__device__ __forceinline__ uint8_t ld_nc_global(const uint8_t *ptr) {
    uint16_t ret;
    // NOTES: we must use `uint16_t` as inline ASM does not support 8-bit
    // constraint letter (`h` below means unsigned 16-bit)
    asm volatile(LD_NC_FUNC ".u8 %0, [%1];" : "=h"(ret) : "l"(ptr));
    return static_cast<uint8_t>(ret);
}

template <>
__device__ __forceinline__ int ld_nc_global(const int *ptr) {
    int ret;
    asm volatile(LD_NC_FUNC ".s32 %0, [%1];" : "=r"(ret) : "l"(ptr));
    return ret;
}

template <>
__device__ __forceinline__ int64_t ld_nc_global(const int64_t *ptr) {
    int64_t ret;
    asm volatile(LD_NC_FUNC ".s64 %0, [%1];" : "=l"(ret) : "l"(ptr));
    return ret;
}

template <>
__device__ __forceinline__ float ld_nc_global(const float *ptr) {
    float ret;
    asm volatile(LD_NC_FUNC ".f32 %0, [%1];" : "=f"(ret) : "l"(ptr));
    return ret;
}

template <>
__device__ __forceinline__ int2 ld_nc_global(const int2 *ptr) {
    int2 ret;
    asm volatile(LD_NC_FUNC ".v2.s32 {%0, %1}, [%2];"
                 : "=r"(ret.x), "=r"(ret.y)
                 : "l"(ptr));
    return ret;
}

template <>
__device__ __forceinline__ int4 ld_nc_global(const int4 *ptr) {
    int4 ret;
    asm volatile(LD_NC_FUNC ".v4.s32 {%0, %1, %2, %3}, [%4];"
                 : "=r"(ret.x), "=r"(ret.y), "=r"(ret.z), "=r"(ret.w)
                 : "l"(ptr));
    return ret;
}

__device__ __forceinline__ void st_na_relaxed(const uint8_t *ptr, uint8_t val) {
    asm volatile("st.relaxed.gpu.global.L1::no_allocate.b8 [%0], %1;"
                 :
                 : "l"(ptr), "h"(static_cast<uint16_t>(val)));
}

__device__ __forceinline__ void st_na_relaxed(const uint16_t *ptr,
                                              uint16_t val) {
    asm volatile("st.relaxed.gpu.global.L1::no_allocate.b16 [%0], %1;"
                 :
                 : "l"(ptr), "h"(val));
}

__device__ __forceinline__ void st_na_relaxed(const uint32_t *ptr,
                                              uint32_t val) {
    asm volatile("st.relaxed.gpu.global.L1::no_allocate.b32 [%0], %1;"
                 :
                 : "l"(ptr), "r"(val));
}

__device__ __forceinline__ void st_na_relaxed(const int *ptr, int val) {
    asm volatile("st.relaxed.gpu.global.L1::no_allocate.b32 [%0], %1;"
                 :
                 : "l"(ptr), "r"(val));
}

__device__ __forceinline__ void st_na_relaxed(const int4 *ptr, int4 val) {
    asm volatile(
        "st.relaxed.gpu.global.L1::no_allocate.v4.s32 [%0], {%1, %2, %3, %4};"
        :
        : "l"(ptr), "r"(val.x), "r"(val.y), "r"(val.z), "r"(val.w));
}

__device__ __forceinline__ void st_na_release(const int *ptr, int val) {
    asm volatile("st.release.gpu.global.L1::no_allocate.b32 [%0], %1;"
                 :
                 : "l"(ptr), "r"(val));
}

__device__ __forceinline__ void st_na_release(const uint32_t *ptr,
                                              uint32_t val) {
    asm volatile("st.release.gpu.global.L1::no_allocate.b32 [%0], %1;"
                 :
                 : "l"(ptr), "r"(val));
}

__device__ __forceinline__ void st_na_release(const uint64_t *ptr,
                                              uint64_t val) {
    asm volatile("st.release.gpu.global.L1::no_allocate.b64 [%0], %1;"
                 :
                 : "l"(ptr), "l"(val));
}

// `st.global.L1::no_allocate` will be translated into `ST.E.NA.[width]` in SASS
#ifndef DISABLE_AGGRESSIVE_PTX_INSTRS
#define ST_NA_FUNC "st.global.L1::no_allocate"
#else
#define ST_NA_FUNC "st.global"
#endif

template <typename dtype_t>
__device__ __forceinline__ void st_na_global(const dtype_t *ptr,
                                             const dtype_t &value) {
    st_na_global(
        reinterpret_cast<const typename VecInt<sizeof(dtype_t)>::vec_t *>(ptr),
        *reinterpret_cast<const typename VecInt<sizeof(dtype_t)>::vec_t *>(
            &value));
}

template <>
__device__ __forceinline__ void st_na_global(const int *ptr, const int &value) {
    asm volatile(ST_NA_FUNC ".s32 [%0], %1;" ::"l"(ptr), "r"(value));
}

template <>
__device__ __forceinline__ void st_na_global(const int64_t *ptr,
                                             const int64_t &value) {
    asm volatile(ST_NA_FUNC ".s64 [%0], %1;" ::"l"(ptr), "l"(value));
}

template <>
__device__ __forceinline__ void st_na_global(const float *ptr,
                                             const float &value) {
    asm volatile(ST_NA_FUNC ".f32 [%0], %1;" ::"l"(ptr), "f"(value));
}

template <>
__device__ __forceinline__ void st_na_global(const int4 *ptr,
                                             const int4 &value) {
    asm volatile(ST_NA_FUNC ".v4.s32 [%0], {%1, %2, %3, %4};" ::"l"(ptr),
                 "r"(value.x), "r"(value.y), "r"(value.z), "r"(value.w));
}

__device__ __forceinline__ void fence_view_async_shared() {
    asm volatile("fence.proxy.async.shared::cta; \n" ::);
}

__device__ __forceinline__ void fence_barrier_init() {
    asm volatile("fence.mbarrier_init.release.cluster; \n" ::);
}

__device__ __forceinline__ void mbarrier_init(uint64_t *mbar_ptr,
                                              uint32_t arrive_count) {
    auto mbar_int_ptr =
        static_cast<uint32_t>(__cvta_generic_to_shared(mbar_ptr));
    asm volatile("mbarrier.init.shared::cta.b64 [%1], %0;" ::"r"(arrive_count),
                 "r"(mbar_int_ptr));
}

__device__ __forceinline__ void mbarrier_wait(uint64_t *mbar_ptr,
                                              uint32_t &phase) {
    auto mbar_int_ptr =
        static_cast<uint32_t>(__cvta_generic_to_shared(mbar_ptr));
    asm volatile(
        "{\n\t"
        ".reg .pred       P1; \n\t"
        "LAB_WAIT: \n\t"
        "mbarrier.try_wait.parity.shared::cta.b64 P1, [%0], %1, %2; \n\t"
        "@P1 bra DONE; \n\t"
        "bra     LAB_WAIT; \n\t"
        "DONE: \n\t"
        "}" ::"r"(mbar_int_ptr),
        "r"(phase), "r"(0x989680));
    phase ^= 1;
}

__device__ __forceinline__ void mbarrier_arrive_and_expect_tx(
    uint64_t *mbar_ptr, int num_bytes) {
    auto mbar_int_ptr =
        static_cast<uint32_t>(__cvta_generic_to_shared(mbar_ptr));
    asm volatile(
        "mbarrier.arrive.expect_tx.shared::cta.b64 _, [%1], %0; \n\t" ::"r"(
            num_bytes),
        "r"(mbar_int_ptr));
}

__device__ __forceinline__ void tma_store_fence() {
    asm volatile("fence.proxy.async.shared::cta;");
}

constexpr uint64_t kEvictFirst = 0x12f0000000000000;
constexpr uint64_t kEvictNormal = 0x1000000000000000;

__device__ __forceinline__ void tma_load_1d(const void *smem_ptr,
                                            const void *gmem_ptr,
                                            uint64_t *mbar_ptr, int num_bytes,
                                            bool evict_first = true) {
    auto mbar_int_ptr =
        static_cast<uint32_t>(__cvta_generic_to_shared(mbar_ptr));
    auto smem_int_ptr =
        static_cast<uint32_t>(__cvta_generic_to_shared(smem_ptr));
    const auto cache_hint = evict_first ? kEvictFirst : kEvictNormal;
    asm volatile(
        "cp.async.bulk.shared::cluster.global.mbarrier::complete_tx::bytes.L2::"
        "cache_hint [%0], [%1], %2, [%3], %4;\n" ::"r"(smem_int_ptr),
        "l"(gmem_ptr), "r"(num_bytes), "r"(mbar_int_ptr), "l"(cache_hint)
        : "memory");
}

__device__ __forceinline__ void tma_store_1d(const void *smem_ptr,
                                             const void *gmem_ptr,
                                             int num_bytes,
                                             bool evict_first = true) {
    auto smem_int_ptr =
        static_cast<uint32_t>(__cvta_generic_to_shared(smem_ptr));
    const auto cache_hint = evict_first ? kEvictFirst : kEvictNormal;
    asm volatile(
        "cp.async.bulk.global.shared::cta.bulk_group.L2::cache_hint [%0], "
        "[%1], %2, %3;\n" ::"l"(gmem_ptr),
        "r"(smem_int_ptr), "r"(num_bytes), "l"(cache_hint)
        : "memory");
    asm volatile("cp.async.bulk.commit_group;");
}

template <int N = 0>
__device__ __forceinline__ void tma_store_wait() {
    asm volatile("cp.async.bulk.wait_group.read %0;" ::"n"(N) : "memory");
}

__forceinline__ __device__ int get_lane_id() {
    int lane_id;
    asm("mov.s32 %0, %laneid;" : "=r"(lane_id));
    return lane_id;
}
