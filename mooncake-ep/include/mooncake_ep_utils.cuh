#pragma once

#include <mooncake_ep_exception.cuh>

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

// VecInt must be defined before vendor utils (which use it in templates).
// It is also used by shared functions below.
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

#ifdef MOONCAKE_EP_USE_MUSA
#include "ep_musa_utils.cuh"
#else
#include "ep_cuda_utils.cuh"
#endif

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

template <int kNumRanks>
__forceinline__ __device__ void move_fifo_slots(int &head) {
    head = (head + kNumRanks) % NUM_MAX_FIFO_SLOTS;
}

template <int kNumRanks>
__device__ __forceinline__ bool not_finished(int *task, int expected) {
    auto result = false;
    auto lane_id = threadIdx.x % 32;
    if (lane_id < kNumRanks)
        result = ld_volatile_global(task + lane_id) != expected;
    return __any_sync(0xffffffff, result);
}

template <int kNumRanks>
__forceinline__ __device__ void timeout_check(int **task_fifo_ptrs, int head,
                                              int rank, int expected,
                                              int tag = 0) {
    auto start_time = clock64();
    while (not_finished<kNumRanks>(task_fifo_ptrs[rank] + head, expected)) {
        if (clock64() - start_time > NUM_TIMEOUT_CYCLES and threadIdx.x == 0) {
            printf("Timeout check failed: %d (rank = %d)\n", tag, rank);
            trap();
        }
    }
}

template <int kNumRanks>
__forceinline__ __device__ void barrier_device(int **task_fifo_ptrs, int head,
                                               int rank, int tag = 0) {
    auto thread_id = static_cast<int>(threadIdx.x);
    EP_DEVICE_ASSERT(kNumRanks <= 32);

    if (thread_id < kNumRanks) {
        atomicAdd_system(task_fifo_ptrs[rank] + head + thread_id,
                         FINISHED_SUM_TAG);
        memory_fence();
        atomicSub_system(task_fifo_ptrs[thread_id] + head + rank,
                         FINISHED_SUM_TAG);
    }
    timeout_check<kNumRanks>(task_fifo_ptrs, head, rank, 0, tag);
}

}  // namespace mooncake
