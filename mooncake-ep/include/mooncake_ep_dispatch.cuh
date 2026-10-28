#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_exception.cuh>
#include <mooncake_ep_utils.cuh>
#include <transport/device/comm_device.cuh>

namespace mooncake {

using mooncake::device::CommCtx;
using mooncake::device::make_comm_ctx;
using mooncake::device::mc_atomic_add_release;
using mooncake::device::mc_bar_sync;
using mooncake::device::mc_grid_sync;
using mooncake::device::mc_ld_acquire;
using mooncake::device::mc_ld_nc;
using mooncake::device::mc_ld_nc_f32;
using mooncake::device::mc_ld_nc_s32;
using mooncake::device::mc_rdma_put;
using mooncake::device::mc_red_add;
using mooncake::device::mc_route_put;
using mooncake::device::mc_st_na;
using mooncake::device::mc_st_release;
#ifdef MOONCAKE_EP_USE_MUSA
using mooncake::device::mc_atomic_add_relaxed;
using mooncake::device::mc_fence_noflush;
#endif
using mooncake::device::mc_fence;
using mooncake::device::mc_fence_barrier_fence;

struct DispatchKernelArgs {
    void* packed_recv_x;
    float* packed_recv_x_scales;
    int* packed_recv_src_info;
    int64_t* packed_recv_layout_range;
    int* packed_recv_count;
    int32_t* active_ranks;
    void* mxa_buffer;
    int* rdma_send_signal_buffer;
    int* rdma_recv_signal_buffer;
    void* rdma_send_data_buffer;
    void* rdma_recv_data_buffer;
    void* raddrs;
    void* rkeys;
    void* qp_devctxs;
    const int32_t* nvlink_available;
    void* const* ipc_peer_ptrs;
    const void* x;
    const int64_t* topk_idx;
    int* atomic_counter_per_expert;
    int* atomic_finish_counter_per_expert;
    int* next_clean_buffer;
    int num_tokens;
    int num_max_dispatch_tokens_per_rank;
    int num_topk;
    int num_experts;
    int rank;
    int num_ranks;
    int64_t timeout_ticks;
    int phases;
    int active_qps_per_rank;
};

static_assert(std::is_trivially_copyable<DispatchKernelArgs>::value,
              "DispatchKernelArgs must be trivially copyable");

template <bool kUseFP8, int kNumWarpGroups, int kNumWarpsPerGroup, int kHidden>
__device__ __forceinline__ void dispatch_kernel_impl(
    const DispatchKernelArgs& args) {
    void* packed_recv_x = args.packed_recv_x;
    float* packed_recv_x_scales = args.packed_recv_x_scales;
    int* packed_recv_src_info = args.packed_recv_src_info;
    int64_t* packed_recv_layout_range = args.packed_recv_layout_range;
    int* packed_recv_count = args.packed_recv_count;
    int32_t* active_ranks = args.active_ranks;
    void* mxa_buffer = args.mxa_buffer;
    int* rdma_send_signal_buffer = args.rdma_send_signal_buffer;
    int* rdma_recv_signal_buffer = args.rdma_recv_signal_buffer;
    void* rdma_send_data_buffer = args.rdma_send_data_buffer;
    void* rdma_recv_data_buffer = args.rdma_recv_data_buffer;
    void* raddrs = args.raddrs;
    void* rkeys = args.rkeys;
    void* qp_devctxs = args.qp_devctxs;
    const int32_t* nvlink_available = args.nvlink_available;
    void* const* ipc_peer_ptrs = args.ipc_peer_ptrs;
    const void* x = args.x;
    const int64_t* topk_idx = args.topk_idx;
    int* atomic_counter_per_expert = args.atomic_counter_per_expert;
    int* atomic_finish_counter_per_expert =
        args.atomic_finish_counter_per_expert;
    int* next_clean_buffer = args.next_clean_buffer;
    int num_tokens = args.num_tokens;
    int num_max_dispatch_tokens_per_rank =
        args.num_max_dispatch_tokens_per_rank;
    int num_topk = args.num_topk;
    int num_experts = args.num_experts;
    int rank = args.rank;
    int num_ranks = args.num_ranks;
    int64_t timeout_ticks = args.timeout_ticks;
    int phases = args.phases;
    int active_qps_per_rank = args.active_qps_per_rank;

    const auto sm_id = static_cast<int>(blockIdx.x);
    const auto thread_id = static_cast<int>(threadIdx.x);
    const auto warp_id = thread_id / 32, lane_id = get_lane_id();
    const auto num_sms = static_cast<int>(gridDim.x);
    const auto num_warps = kNumWarpGroups * kNumWarpsPerGroup;
    const auto num_local_experts = num_experts / num_ranks;
    const auto warp_group_id = warp_id / kNumWarpsPerGroup;
    const auto sub_warp_id = warp_id % kNumWarpsPerGroup;
    const auto responsible_expert_idx = sm_id * kNumWarpGroups + warp_group_id;
#if defined(MOONCAKE_EP_USE_MUSA) || defined(MOONCAKE_EP_USE_MACA)
    // C500 reports 64-thread hardware warps. Do not split the last hardware
    // warp by assigning only the final 32-thread pseudo-warp to count work.
    // Reserve one full warp group from the data path, but write counts from a
    // single 32-thread lane group to avoid duplicate per-expert increments.
    const bool is_count_warp = warp_group_id == kNumWarpGroups - 1;
    const bool is_count_worker = is_count_warp && sub_warp_id == 0;
    const bool is_data_warp = warp_group_id < kNumWarpGroups - 1;
    const int num_send_threads = (kNumWarpGroups - 1) * kNumWarpsPerGroup * 32;
#else
    const bool is_count_warp = warp_id == num_warps - 1;
    const bool is_count_worker = is_count_warp;
    const bool is_data_warp = warp_id < num_warps - 1;
    const int num_send_threads = (num_warps - 1) * 32;
#endif

    // FP8 staffs
    constexpr int kNumPerChannels = 128;
    constexpr float kFP8Margin = 1e-4, kFP8Amax = 448,
                    kFP8AmaxInv = 1.0f / 448.0f;
    const int num_scales = kHidden / kNumPerChannels;
    const size_t hidden_bytes =
        kHidden * (kUseFP8 ? sizeof(ep_fp8_storage_t) : EP_BF16_SIZE);
    const size_t hidden_int4 = hidden_bytes / sizeof(int4);

    // Message package: hidden data, FP8 scales, index at source
    // NOTES: currently we have 3 reserved int fields for future use
    using vec_t = typename std::conditional<kUseFP8, int2, int4>::type;
    const size_t num_bytes_per_msg =
        sizeof(int4) + (kUseFP8 ? (kHidden + num_scales * sizeof(float))
                                : (kHidden * EP_BF16_SIZE));
    const size_t num_int4_per_msg = num_bytes_per_msg / sizeof(int4);
    EP_DEVICE_ASSERT(num_bytes_per_msg % sizeof(int4) == 0);

    // Communication context — platform dispatch is inside comm_device.cuh
    const CommCtx comm_ctx =
        make_comm_ctx(mxa_buffer, nvlink_available, ipc_peer_ptrs, raddrs,
                      rkeys, qp_devctxs, rdma_send_signal_buffer,
                      rdma_recv_signal_buffer, rank, num_ranks, MAX_QP_COUNT);
    const size_t num_qp_per_rank = MAX_QP_COUNT / num_ranks;

    // Sending phase
    if ((phases & LOW_LATENCY_SEND_PHASE) == 0) goto LOW_LATENCY_DISPATCH_RECV;

    // Expert counts
    __shared__ int shared_num_tokens_sent_per_expert[kNumWarpGroups];

    // There are 2 kinds of execution lanes in this part:
    // 1. Data lanes for FP8 cast and sending top-k tokens.
    // 2. Count lanes for reading `topk_idx` and per-expert token counts.
    // Non-CUDA backends reserve a full warp group for the count path. This
    // keeps the final group out of the data path when MUSA uses five groups.
    if (is_data_warp) {
        constexpr int kNumElemsPerRead = sizeof(int4) / EP_BF16_SIZE;
        EP_DEVICE_ASSERT(kHidden % kNumElemsPerRead == 0);
        EP_STATIC_ASSERT(kNumElemsPerRead * 32 % kNumPerChannels == 0,
                         "Invalid vectorization");
        const auto num_threads = num_send_threads;
        const size_t hidden_bf16_int4 = kHidden / kNumElemsPerRead;

        for (int token_idx = sm_id; token_idx < num_tokens;
             token_idx += num_sms) {
            const auto x_int4 =
                reinterpret_cast<const int4*>(x) + token_idx * hidden_bf16_int4;
            const auto rdma_x_src_idx = reinterpret_cast<int*>(
                reinterpret_cast<uint8_t*>(rdma_send_data_buffer) +
                token_idx * num_bytes_per_msg);
            const auto rdma_x_vec = reinterpret_cast<vec_t*>(
                reinterpret_cast<uint8_t*>(rdma_x_src_idx) + sizeof(int4));
            const auto rdma_x_scales = reinterpret_cast<float*>(
                reinterpret_cast<uint8_t*>(rdma_x_vec) + hidden_bytes);

            // Overlap top-k index read and source token index write
            auto dst_expert_idx =
                warp_id < num_topk
                    ? static_cast<int>(
                          __ldg(topk_idx + token_idx * num_topk + warp_id))
                    : -1;
            thread_id == 0 ? (*rdma_x_src_idx = token_idx) : 0;

// FP8 cast
#pragma unroll
            for (int i = thread_id; i < hidden_bf16_int4; i += num_threads) {
                // Read
                auto int4_value = __ldg(x_int4 + i);

                if (kUseFP8) {
                    // Calculate local amax
                    auto bf16_values =
                        reinterpret_cast<nv_bfloat16*>(&int4_value);
                    float fp32_values[kNumElemsPerRead];
                    float amax = kFP8Margin, scale, scale_inv;
#pragma unroll
                    for (int j = 0; j < kNumElemsPerRead; ++j) {
                        fp32_values[j] = __bfloat162float(bf16_values[j]);
                        amax = fmaxf(amax, fabsf(fp32_values[j]));
                    }

                    // Reduce amax and scale
                    EP_STATIC_ASSERT(
                        kNumElemsPerRead * 32 / kNumPerChannels == 2,
                        "Invalid vectorization");
                    amax = half_warp_reduce_max(amax), scale = kFP8Amax / amax,
                    scale_inv = amax * kFP8AmaxInv;
                    if (lane_id == 0 or lane_id == 16)
                        rdma_x_scales[i * kNumElemsPerRead / 128] = scale_inv;

                    // Cast into send buffer
                    vec_t int2_value;
                    auto fp8x2_values =
                        reinterpret_cast<ep_fp8x2_storage_t*>(&int2_value);
#pragma unroll
                    for (int j = 0; j < kNumElemsPerRead; j += 2) {
                        float2 fp32x2 = {fp32_values[j] * scale,
                                         fp32_values[j + 1] * scale};
                        fp8x2_values[j / 2] = ep_cvt_float2_to_fp8x2(fp32x2);
                    }
                    rdma_x_vec[i] = int2_value;
                } else {
                    // Reinterpret-cast is for C++14 compatibility
                    rdma_x_vec[i] = *reinterpret_cast<vec_t*>(&int4_value);
                }
            }
            mc_bar_sync(1, num_threads);

            // Issue sends
            if (dst_expert_idx >= 0) {
                int slot_idx =
                    lane_id == 0
                        ? atomicAdd(atomic_counter_per_expert + dst_expert_idx,
                                    1)
                        : 0;
                slot_idx = __shfl_sync(0xffffffff, slot_idx, 0);
                const auto dst_rank = dst_expert_idx / num_local_experts;
                const auto dst_expert_local_idx =
                    dst_expert_idx % num_local_experts;
                const auto src_ptr =
                    reinterpret_cast<const void*>(rdma_x_src_idx);
                const auto dst_ptr = reinterpret_cast<void*>(
                    reinterpret_cast<uint64_t>(rdma_recv_data_buffer) +
                    dst_expert_local_idx * num_ranks *
                        num_max_dispatch_tokens_per_rank * num_bytes_per_msg +
                    rank * num_max_dispatch_tokens_per_rank *
                        num_bytes_per_msg +
                    slot_idx * num_bytes_per_msg);

                void* write_dst = mc_route_put(comm_ctx, dst_rank, dst_ptr);
                if (write_dst != nullptr) {
                    // Local or P2P path — warp-cooperative copy
                    const auto* src_int4_ptr =
                        reinterpret_cast<const int4*>(src_ptr);
                    const auto* dst_int4_ptr =
                        reinterpret_cast<int4*>(write_dst);
#ifndef MOONCAKE_EP_USE_MUSA
                    mc_fence();
#endif
                    UNROLLED_WARP_COPY(8, lane_id, num_int4_per_msg,
                                       dst_int4_ptr, src_int4_ptr, mc_ld_nc,
                                       mc_st_na);
#ifndef MOONCAKE_EP_USE_MUSA
                    mc_fence();
#endif
                } else {
                    // IBGDA path — send directly from source buffer
                    mc_fence();
#if defined(MOONCAKE_EP_USE_MACA) || defined(MOONCAKE_EP_USE_MUSA)
                    // MACA and MUSA execute two 32-lane sub-warps in one
                    // hardware warp; serialize posts sharing one QP.
                    for (int half = 0; half < 2; ++half) {
                        if ((sub_warp_id & 1) == half) {
#endif
                            mc_rdma_put(comm_ctx,
                                        ep_qp_channel(dst_expert_local_idx,
                                                      num_qp_per_rank,
                                                      active_qps_per_rank),
                                        dst_rank, num_qp_per_rank, src_ptr,
                                        dst_ptr, num_bytes_per_msg, lane_id);
#if defined(MOONCAKE_EP_USE_MACA) || defined(MOONCAKE_EP_USE_MUSA)
                        }
                        __syncwarp();
                    }
#endif
                }

                // Increase counter after finishing
                __syncwarp();
#ifdef MOONCAKE_EP_USE_MUSA
                mc_fence_noflush();
#endif
                lane_id == 0
                    ?
#ifdef MOONCAKE_EP_USE_MUSA
                    mc_atomic_add_relaxed(
                        atomic_finish_counter_per_expert + dst_expert_idx, 1)
                    : 0;
#else
                    mc_atomic_add_release(
                        atomic_finish_counter_per_expert + dst_expert_idx, 1)
                    : 0;
#endif
            }
        }
    } else if (is_count_warp) {
#ifdef MOONCAKE_EP_SPLIT_SEND_RECV
        // Participate in __syncthreads() barriers from data warps.
        // Each token iteration in the send loop above calls
        // __syncthreads() once; the count path must match.
        for (int token_idx = sm_id; token_idx < num_tokens;
             token_idx += num_sms) {
            __syncthreads();
        }
#endif
    }
    if (is_count_worker) {
        EP_DEVICE_ASSERT(num_sms > 1);
        if (sm_id == 0) {
// The first SM is also responsible for cleaning the next buffer
#pragma unroll
            for (int i = lane_id; i < num_experts; i += 32)
                next_clean_buffer[i] = 0;

            // Notify before executing `int_p`
            __syncwarp();
#ifdef MOONCAKE_EP_USE_MUSA
            // Publish the cleared buffer once before any local ready tag.
            mc_fence();
            __syncwarp();
#endif
#pragma unroll
            for (int i = lane_id; i < num_experts; i += 32)
#ifdef MOONCAKE_EP_USE_MUSA
                mc_atomic_add_relaxed(atomic_finish_counter_per_expert + i,
                                      FINISHED_SUM_TAG);
#else
                mc_atomic_add_release(atomic_finish_counter_per_expert + i,
                                      FINISHED_SUM_TAG);
#endif
        }

        // This SM should be responsible for some destination experts, read
        // `topk_idx` for them
        int expert_count[kNumWarpGroups] = {0};
        const auto expert_begin_idx = sm_id * kNumWarpGroups;
        const auto expert_end_idx =
            min(expert_begin_idx + kNumWarpGroups, num_experts);

// Per lane count
#pragma unroll 8
        for (int i = lane_id; i < num_tokens * num_topk; i += 32) {
            auto idx = static_cast<int>(__ldg(topk_idx + i));
            if (idx >= expert_begin_idx and idx < expert_end_idx)
                expert_count[idx - expert_begin_idx]++;
        }

// Warp reduce
#pragma unroll
        for (int i = expert_begin_idx; i < expert_end_idx; ++i) {
            auto sum = warp_reduce_sum(expert_count[i - expert_begin_idx]);
            if (lane_id == 0) {
                shared_num_tokens_sent_per_expert[i - expert_begin_idx] = sum;
#ifdef MOONCAKE_EP_USE_MUSA
                // Local completion accounting; payload publication is fenced
                // by the expert's signal sender after the count reaches its
                // tag.
                mc_atomic_add_relaxed(atomic_finish_counter_per_expert + i,
                                      FINISHED_SUM_TAG - sum);
#else
                mc_atomic_add_release(atomic_finish_counter_per_expert + i,
                                      FINISHED_SUM_TAG - sum);
#endif
            }
        }
    }
#ifdef MOONCAKE_EP_USE_MUSA
    __syncthreads();
#else
    mc_fence_barrier_fence();
#endif

    // Issue count sends
    if (responsible_expert_idx < num_experts and sub_warp_id == 0 and
        lane_id == 0) {
        const auto dst_rank = responsible_expert_idx / num_local_experts;
        const auto dst_expert_local_idx =
            responsible_expert_idx % num_local_experts;
        const auto num_tokens_sent =
            shared_num_tokens_sent_per_expert[responsible_expert_idx -
                                              sm_id * kNumWarpGroups];

        // Wait local sends issued and send expert counts
#if defined(MOONCAKE_EP_USE_MACA) || defined(MOONCAKE_EP_USE_MUSA)
        while (
            atomicAdd(atomic_finish_counter_per_expert + responsible_expert_idx,
                      0) != FINISHED_SUM_TAG * 2) {
        }
#else
        while (mc_ld_acquire(atomic_finish_counter_per_expert +
                             responsible_expert_idx) != FINISHED_SUM_TAG * 2) {
        }
#endif
        if (dst_rank != rank) {
#ifdef MOONCAKE_EP_USE_MUSA
            mc_fence();
#endif
            int* signal_ptr = rdma_recv_signal_buffer +
                              dst_expert_local_idx * num_ranks + rank;
            mc_red_add(comm_ctx, dst_rank,
                       ep_qp_channel(dst_expert_local_idx, num_qp_per_rank,
                                     active_qps_per_rank),
                       num_qp_per_rank, signal_ptr,
                       static_cast<int32_t>(-num_tokens_sent - 1));
        } else {
            mc_st_release(rdma_recv_signal_buffer +
                              dst_expert_local_idx * num_ranks + rank,
                          -num_tokens_sent - 1);
        }

        // Clean workspace for next use
        atomic_counter_per_expert[responsible_expert_idx] = 0;
        atomic_finish_counter_per_expert[responsible_expert_idx] = 0;

        // Clean `packed_recv_count`
        if (dst_rank == 0) packed_recv_count[dst_expert_local_idx] = 0;
    }
    __syncwarp();

// Receiving phase
LOW_LATENCY_DISPATCH_RECV:
    if ((phases & LOW_LATENCY_RECV_PHASE) == 0) return;

    // For send-and-recv kernels, we need a grid sync for making
    // `packed_recv_count` visible
    if (phases & LOW_LATENCY_SEND_PHASE) mc_grid_sync();

    // Receiving and packing
    if (responsible_expert_idx < num_experts) {
        const auto src_rank = responsible_expert_idx / num_local_experts;
        const auto local_expert_idx =
            responsible_expert_idx % num_local_experts;
        const auto rdma_recv_x_uint8 =
            reinterpret_cast<uint8_t*>(rdma_recv_data_buffer) +
            local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank *
                num_bytes_per_msg +
            src_rank * num_max_dispatch_tokens_per_rank * num_bytes_per_msg;
        const auto recv_x_int4 = reinterpret_cast<int4*>(packed_recv_x) +
                                 local_expert_idx * num_ranks *
                                     num_max_dispatch_tokens_per_rank *
                                     hidden_int4;
        const auto recv_x_scales =
            packed_recv_x_scales + local_expert_idx * num_ranks *
                                       num_max_dispatch_tokens_per_rank *
                                       num_scales;
        const auto recv_src_info =
            packed_recv_src_info +
            local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank;
        const auto recv_range =
            packed_recv_layout_range + local_expert_idx * num_ranks;

        // Shared between sub-warps in warp groups
        __shared__ int shared_num_recv_tokens[kNumWarpGroups],
            shared_recv_token_begin_idx[kNumWarpGroups];

        // Wait tokens to arrive
        // NOTES: using sub-warp 1 to overlap with sub-warp 0
        int num_recv_tokens, recv_token_begin_idx;
        EP_STATIC_ASSERT(kNumWarpsPerGroup > 1,
                         "Requires more than one warp per group");
        if (sub_warp_id == 1 and lane_id == 0) {
            unsigned long long start_time = clock64();
            while ((num_recv_tokens = mc_ld_acquire(
                        rdma_recv_signal_buffer + local_expert_idx * num_ranks +
                        src_rank)) == 0) {
                unsigned long long end_time = clock64();
                if (timeout_ticks != -1 &&
                    end_time - start_time > timeout_ticks) {
                    active_ranks[src_rank] = 0;
                }
                if (!active_ranks[src_rank]) {
                    num_recv_tokens = -1;
                    break;
                }
            }
            num_recv_tokens = -num_recv_tokens - 1;
            recv_token_begin_idx = atomicAdd(
                packed_recv_count + local_expert_idx, num_recv_tokens);
            shared_num_recv_tokens[warp_group_id] = num_recv_tokens;
            shared_recv_token_begin_idx[warp_group_id] = recv_token_begin_idx;
            recv_range[src_rank] =
                pack2<int, int64_t>(num_recv_tokens, recv_token_begin_idx);
        }
        mc_bar_sync(warp_group_id + 2, kNumWarpsPerGroup * 32);
        num_recv_tokens = shared_num_recv_tokens[warp_group_id];
        recv_token_begin_idx = shared_recv_token_begin_idx[warp_group_id];

        // Copy tokens
        EP_DEVICE_ASSERT(num_scales <= 64);
        mc_fence();
        for (int i = sub_warp_id; i < num_recv_tokens; i += kNumWarpsPerGroup) {
            // Copy source info
            const auto src_src_idx = reinterpret_cast<int*>(
                rdma_recv_x_uint8 + i * num_bytes_per_msg);
            if (lane_id == 0)
                recv_src_info[recv_token_begin_idx + i] =
                    mc_ld_nc_s32(src_src_idx);
            __syncwarp();

            // Copy data
            // NOTES: only 2 load iterations for 7K hidden with 7 unrolls
            const auto src_data = reinterpret_cast<int4*>(
                reinterpret_cast<uint8_t*>(src_src_idx) + sizeof(int4));
            const auto dst_data =
                recv_x_int4 + (recv_token_begin_idx + i) * hidden_int4;
#ifndef MOONCAKE_EP_USE_MUSA
            mc_fence();
#endif
            UNROLLED_WARP_COPY(7, lane_id, hidden_int4, dst_data, src_data,
                               mc_ld_nc, mc_st_na);

            // Copy scales
            if (kUseFP8) {
                const auto src_scales = reinterpret_cast<float*>(
                    reinterpret_cast<uint8_t*>(src_data) + hidden_bytes);
                const auto dst_scales = reinterpret_cast<float*>(
                    recv_x_scales + recv_token_begin_idx + i);
                const auto scale_stride =
                    num_ranks * num_max_dispatch_tokens_per_rank;
                auto scale_0 = lane_id < num_scales
                                   ? mc_ld_nc_f32(src_scales + lane_id)
                                   : 0;
                auto scale_1 = (lane_id + 32) < num_scales
                                   ? mc_ld_nc_f32(src_scales + lane_id + 32)
                                   : 0;
                lane_id < num_scales
                    ? dst_scales[lane_id * scale_stride] = scale_0
                    : 0.0f;
                (lane_id + 32) < num_scales
                    ? dst_scales[(lane_id + 32) * scale_stride] = scale_1
                    : 0.0f;
            }
        }
    } else {
        mc_bar_sync(warp_group_id + 2, kNumWarpsPerGroup * 32);
    }
}

template <bool kUseFP8, int kNumWarpGroups, int kNumWarpsPerGroup, int kHidden>
__global__ EP_LAUNCH_BOUNDS(kNumWarpGroups* kNumWarpsPerGroup * 32,
                            1) void dispatch(const DispatchKernelArgs args) {
    dispatch_kernel_impl<kUseFP8, kNumWarpGroups, kNumWarpsPerGroup, kHidden>(
        args);
}

}  // namespace mooncake
