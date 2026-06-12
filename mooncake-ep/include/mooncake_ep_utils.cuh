#pragma once

#include <mooncake_ep_device.h>

#define UNROLLED_WARP_COPY(UNROLL_FACTOR, LANE_ID, N, DST, SRC, LD_FUNC,      \
                           ST_FUNC)                                           \
    {                                                                         \
        constexpr int kLoopStride = 32 * (UNROLL_FACTOR);                     \
        typename std::remove_reference<decltype(LD_FUNC((SRC) + 0))>::type    \
            unrolled_values[(UNROLL_FACTOR)];                                 \
        auto __src = (SRC);                                                   \
        auto __dst = (DST);                                                   \
        for (int __i = (LANE_ID); __i < ((N) / kLoopStride) * kLoopStride;    \
             __i += kLoopStride) {                                            \
            _Pragma("unroll") for (int __j = 0; __j < (UNROLL_FACTOR); ++__j) \
                unrolled_values[__j] = LD_FUNC(__src + __i + __j * 32);       \
            _Pragma("unroll") for (int __j = 0; __j < (UNROLL_FACTOR); ++__j) \
                ST_FUNC(__dst + __i + __j * 32, unrolled_values[__j]);        \
        }                                                                     \
        for (int __i = ((N) / kLoopStride) * kLoopStride + (LANE_ID);         \
             __i < (N); __i += 32)                                            \
            ST_FUNC(__dst + __i, LD_FUNC(__src + __i));                       \
    }

namespace mooncake {

template <int kBytes>
struct VecInt {};
template <>
struct VecInt<1> {
    using vec_t = int8_t;
};
template <>
struct VecInt<2> {
    using vec_t = int16_t;
};
template <>
struct VecInt<4> {
    using vec_t = int;
};
template <>
struct VecInt<8> {
    using vec_t = int64_t;
};
template <>
struct VecInt<16> {
    using vec_t = int4;
};

// ---- TMA / mbarrier helpers (CUDA only) ----
#ifndef MOONCAKE_EP_USE_MUSA

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

#endif // MOONCAKE_EP_USE_MUSA

template <typename dtype_t>
__host__ __device__ dtype_t cell_div(dtype_t a, dtype_t b) {
    return (a + b - 1) / b;
}

template <typename dtype_t>
__host__ __device__ dtype_t align(dtype_t a, dtype_t b) {
    return cell_div<dtype_t>(a, b) * b;
}

__forceinline__ __device__ void get_channel_task_range(int num_tokens,
                                                       int num_sms, int sm_id,
                                                       int &token_start_idx,
                                                       int &token_end_idx) {
    int num_tokens_per_sm = cell_div(num_tokens, num_sms);
    token_start_idx = min(num_tokens_per_sm * sm_id, num_tokens);
    token_end_idx = min(token_start_idx + num_tokens_per_sm, num_tokens);
}

template <typename dtype_a_t, typename dtype_b_t>
__device__ __forceinline__ dtype_b_t pack2(const dtype_a_t &x,
                                           const dtype_a_t &y) {
    EP_STATIC_ASSERT(sizeof(dtype_a_t) * 2 == sizeof(dtype_b_t),
                     "Invalid dtypes");
    dtype_b_t packed;
    auto unpacked_ptr = reinterpret_cast<dtype_a_t *>(&packed);
    unpacked_ptr[0] = x, unpacked_ptr[1] = y;
    return packed;
}

template <typename dtype_a_t, typename dtype_b_t>
__device__ __forceinline__ void unpack2(const dtype_b_t &packed, dtype_a_t &x,
                                        dtype_a_t &y) {
    EP_STATIC_ASSERT(sizeof(dtype_a_t) * 2 == sizeof(dtype_b_t),
                     "Invalid dtypes");
    auto unpacked_ptr = reinterpret_cast<const dtype_a_t *>(&packed);
    x = unpacked_ptr[0], y = unpacked_ptr[1];
}

template <typename dtype_t>
__device__ __forceinline__ dtype_t broadcast(dtype_t &ptr, int src_lane_idx) {
    EP_STATIC_ASSERT(sizeof(dtype_t) % sizeof(int) == 0, "");
    auto send_int_values = reinterpret_cast<int *>(&ptr);
    int recv_int_values[sizeof(dtype_t) / sizeof(int)];
#pragma unroll
    for (int i = 0; i < sizeof(dtype_t) / sizeof(int); ++i)
        recv_int_values[i] =
            __shfl_sync(0xffffffff, send_int_values[i], src_lane_idx);
    return *reinterpret_cast<dtype_t *>(recv_int_values);
}

__forceinline__ __device__ int warp_reduce_sum(int value) {
    value += __shfl_xor_sync(0xffffffff, value, 16);
    value += __shfl_xor_sync(0xffffffff, value, 8);
    value += __shfl_xor_sync(0xffffffff, value, 4);
    value += __shfl_xor_sync(0xffffffff, value, 2);
    value += __shfl_xor_sync(0xffffffff, value, 1);
    return value;
}

__forceinline__ __device__ float half_warp_reduce_max(float value) {
    auto mask = __activemask();
    // The mask be in `{0xffffffff, 0xffff}`
    value = max(value, __shfl_xor_sync(mask, value, 8));
    value = max(value, __shfl_xor_sync(mask, value, 4));
    value = max(value, __shfl_xor_sync(mask, value, 2));
    value = max(value, __shfl_xor_sync(mask, value, 1));
    return value;
}

}  // namespace mooncake
