// clang-format off

#include <cstdio>

#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_exception.cuh>
#include <mooncake_ep_launch.cuh>
#include <transport/device/comm_device.cuh>
#include <mooncake_ep_utils.cuh>

namespace mooncake {

using mooncake::device::CommCtx;
using mooncake::device::make_comm_ctx;
using mooncake::device::mc_route_put;
using mooncake::device::mc_rdma_put;
using mooncake::device::mc_signal;
using mooncake::device::mc_red_add;
using mooncake::device::mc_bar_sync;
using mooncake::device::mc_grid_sync;
using mooncake::device::mc_ld_nc;
using mooncake::device::mc_ld_nc_s32;
using mooncake::device::mc_ld_nc_f32;
using mooncake::device::mc_st_na;
using mooncake::device::mc_ld_acquire;
using mooncake::device::mc_st_release;
using mooncake::device::mc_atomic_add_release;
using mooncake::device::mc_fence;
using mooncake::device::mc_fence_barrier_fence;

__global__ void mark_phase_ack_kernel(void* mxa_buffer,
                                      const int32_t* nvlink_available,
                                      void* const* ipc_peer_ptrs,
                                      int* ack_buffer, int rank,
                                      int num_ranks, int epoch) {
    const CommCtx comm_ctx = make_comm_ctx(
        mxa_buffer, nvlink_available, ipc_peer_ptrs, nullptr, nullptr, nullptr,
        ack_buffer, ack_buffer, rank, num_ranks, MAX_QP_COUNT, 0);

    for (int peer = static_cast<int>(threadIdx.x); peer < num_ranks;
         peer += static_cast<int>(blockDim.x)) {
        if (peer == rank) {
            mc_st_release(ack_buffer + rank, epoch);
        } else {
            void* dst = mc_route_put(comm_ctx, peer, ack_buffer + rank);
            if (dst != nullptr)
                mc_st_release(reinterpret_cast<int*>(dst), epoch);
        }
    }
}

__global__ void wait_phase_ack_kernel(int* ack_buffer, int rank, int num_ranks,
                                      int epoch, int64_t timeout_ticks) {
    for (int peer = static_cast<int>(threadIdx.x); peer < num_ranks;
         peer += static_cast<int>(blockDim.x)) {
        if (peer == rank)
            continue;

        int64_t start_time = static_cast<int64_t>(clock64());
        while (mc_ld_acquire(ack_buffer + peer) < epoch) {
            int64_t end_time = static_cast<int64_t>(clock64());
            if (timeout_ticks != -1 && end_time - start_time > timeout_ticks)
                return;
        }
    }
}

__global__ void mark_and_wait_phase_ack_kernel(
        void* mxa_buffer, const int32_t* nvlink_available,
        void* const* ipc_peer_ptrs, int* ack_buffer, int rank, int num_ranks,
        int epoch, int64_t timeout_ticks) {
    const CommCtx comm_ctx = make_comm_ctx(
        mxa_buffer, nvlink_available, ipc_peer_ptrs, nullptr, nullptr, nullptr,
        ack_buffer, ack_buffer, rank, num_ranks, MAX_QP_COUNT, 0);

    for (int peer = static_cast<int>(threadIdx.x); peer < num_ranks;
         peer += static_cast<int>(blockDim.x)) {
        if (peer == rank) {
            mc_st_release(ack_buffer + rank, epoch);
        } else {
            void* dst = mc_route_put(comm_ctx, peer, ack_buffer + rank);
            if (dst != nullptr)
                mc_st_release(reinterpret_cast<int*>(dst), epoch);
        }
    }

    __syncthreads();

    for (int peer = static_cast<int>(threadIdx.x); peer < num_ranks;
         peer += static_cast<int>(blockDim.x)) {
        if (peer == rank)
            continue;

        int64_t start_time = static_cast<int64_t>(clock64());
        while (mc_ld_acquire(ack_buffer + peer) < epoch) {
            int64_t end_time = static_cast<int64_t>(clock64());
            if (timeout_ticks != -1 && end_time - start_time > timeout_ticks)
                return;
        }
    }
}

void mark_phase_ack(void* mxa_buffer, const int32_t* nvlink_available,
                    void* const* ipc_peer_ptrs, int* ack_buffer, int rank,
                    int num_ranks, int epoch, cudaStream_t stream) {
    SETUP_LAUNCH_CONFIG(1, 32, stream);
    LAUNCH_KERNEL(&cfg, mark_phase_ack_kernel, mxa_buffer, nvlink_available,
                  ipc_peer_ptrs, ack_buffer, rank, num_ranks, epoch);
}

void wait_phase_ack(int* ack_buffer, int rank, int num_ranks, int epoch,
                    cudaStream_t stream, int64_t timeout_ticks) {
    SETUP_LAUNCH_CONFIG(1, 32, stream);
    LAUNCH_KERNEL(&cfg, wait_phase_ack_kernel, ack_buffer, rank, num_ranks,
                  epoch, timeout_ticks);
}

void mark_and_wait_phase_ack(void* mxa_buffer,
                             const int32_t* nvlink_available,
                             void* const* ipc_peer_ptrs, int* ack_buffer,
                             int rank, int num_ranks, int epoch,
                             cudaStream_t stream, int64_t timeout_ticks) {
    SETUP_LAUNCH_CONFIG(1, 32, stream);
    LAUNCH_KERNEL(&cfg, mark_and_wait_phase_ack_kernel, mxa_buffer,
                  nvlink_available, ipc_peer_ptrs, ack_buffer, rank, num_ranks,
                  epoch, timeout_ticks);
}

template <bool kUseFP8, int kNumWarpGroups, int kNumWarpsPerGroup, int kHidden>
__global__ EP_LAUNCH_BOUNDS(kNumWarpGroups * kNumWarpsPerGroup * kEpWarpSize, 1) void
dispatch(void* packed_recv_x, float* packed_recv_x_scales,
         int* packed_recv_src_info, int64_t* packed_recv_layout_range,
         int* packed_recv_count, int32_t* active_ranks,
         void* mxa_buffer,
         int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
         void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
         void* cuda_counter_buffer, void* cuda_data_buffer,
         void* raddrs, void* rkeys, void* qp_devctxs,
         const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
         const void* x, const int64_t* topk_idx,
         int* atomic_counter_per_expert, int* atomic_finish_counter_per_expert,
         int* next_clean_buffer,
         int num_tokens, int num_max_dispatch_tokens_per_rank,
         int num_topk, int num_experts, int rank, int num_ranks,
         int force_rdma_data, int poll_rdma_put, int rdma_write_signal,
         int active_qps_per_peer,
         int64_t timeout_ticks,
         int phases) {
    const auto sm_id = static_cast<int>(blockIdx.x);
    const auto thread_id = static_cast<int>(threadIdx.x);
    const auto warp_id = thread_id / kEpWarpSize, lane_id = get_lane_id();
    const auto num_sms = static_cast<int>(gridDim.x);
    const auto num_warps = kNumWarpGroups * kNumWarpsPerGroup;
    const auto num_local_experts = num_experts / num_ranks;
    const auto warp_group_id = warp_id / kNumWarpsPerGroup;
    const auto sub_warp_id = warp_id % kNumWarpsPerGroup;
    const auto responsible_expert_idx = sm_id * kNumWarpGroups + warp_group_id;

    // FP8 staffs
    constexpr int kNumPerChannels = 128;
    constexpr float kFP8Margin = 1e-4, kFP8Amax = 448, kFP8AmaxInv = 1.0f / 448.0f;
    const int num_scales = kHidden / kNumPerChannels;
    const size_t hidden_bytes = kHidden * (kUseFP8 ? sizeof(ep_fp8_storage_t) : EP_BF16_SIZE);
    const size_t hidden_int4 = hidden_bytes / sizeof(int4);

    // Message package: hidden data, FP8 scales, index at source
    // NOTES: currently we have 3 reserved int fields for future use
    using vec_t = typename std::conditional<kUseFP8, int2, int4>::type;
    const size_t num_bytes_per_msg = sizeof(int4) + (kUseFP8 ? (kHidden + num_scales * sizeof(float)) : (kHidden * EP_BF16_SIZE));
    const size_t num_int4_per_msg = num_bytes_per_msg / sizeof(int4);
    EP_DEVICE_ASSERT(num_bytes_per_msg % sizeof(int4) == 0);

    // Communication context — platform dispatch is inside comm_device.cuh
    const CommCtx comm_ctx = make_comm_ctx(
        mxa_buffer, nvlink_available, ipc_peer_ptrs,
        raddrs, rkeys, qp_devctxs,
        rdma_send_signal_buffer, rdma_recv_signal_buffer,
        rank, num_ranks, MAX_QP_COUNT, force_rdma_data, poll_rdma_put,
        rdma_write_signal);
    const size_t num_qp_per_rank = active_qps_per_peer;

    // Sending phase
    if ((phases & LOW_LATENCY_SEND_PHASE) == 0)
        goto LOW_LATENCY_DISPATCH_RECV;

    // Expert counts
    __shared__ int shared_num_tokens_sent_per_expert[kNumWarpGroups];

    // There are 2 kinds of warps in this part:
    // 1. The first-kind warps for FP8 cast and sending top-k tokens
    // 2. The last warp for reading `topk_idx` and count for per-expert information
    if (warp_id < num_warps - 1) {
        constexpr int kNumElemsPerRead = sizeof(int4) / EP_BF16_SIZE;
        EP_DEVICE_ASSERT(kHidden % kNumElemsPerRead == 0);
        EP_STATIC_ASSERT(kNumElemsPerRead * 32 % kNumPerChannels == 0, "Invalid vectorization");
        const auto num_threads = (num_warps - 1) * kEpWarpSize;
        const size_t hidden_bf16_int4 = kHidden / kNumElemsPerRead;

        for (int token_idx = sm_id; token_idx < num_tokens; token_idx += num_sms) {
            const auto x_int4 = reinterpret_cast<const int4*>(x) + token_idx * hidden_bf16_int4;
            const auto rdma_x_src_idx = reinterpret_cast<int*>(reinterpret_cast<uint8_t*>(rdma_send_data_buffer) + token_idx * num_bytes_per_msg);
            const auto rdma_x_vec = reinterpret_cast<vec_t*>(reinterpret_cast<uint8_t*>(rdma_x_src_idx) + sizeof(int4));
            const auto rdma_x_scales = reinterpret_cast<float*>(reinterpret_cast<uint8_t*>(rdma_x_vec) + hidden_bytes);

            // Overlap top-k index read and source token index write
            auto dst_expert_idx = warp_id < num_topk ? static_cast<int>(__ldg(topk_idx + token_idx * num_topk + warp_id)) : -1;
            thread_id == 0 ? (*rdma_x_src_idx = token_idx) : 0;

            // FP8 cast
            #pragma unroll
            for (int i = thread_id; i < hidden_bf16_int4; i += num_threads) {
                    // Read
                    auto int4_value = __ldg(x_int4 + i);

                    if (kUseFP8) {
                        // Calculate local amax
                        auto bf16_values = reinterpret_cast<nv_bfloat16*>(&int4_value);
                        float fp32_values[kNumElemsPerRead];
                        float amax = kFP8Margin, scale, scale_inv;
                        #pragma unroll
                        for (int j = 0; j < kNumElemsPerRead; ++ j) {
                            fp32_values[j] = __bfloat162float(bf16_values[j]);
                            amax = fmaxf(amax, fabsf(fp32_values[j]));
                        }

                        // Reduce amax and scale
                        EP_STATIC_ASSERT(kNumElemsPerRead * 32 / kNumPerChannels == 2, "Invalid vectorization");
                        amax = half_warp_reduce_max(amax), scale = kFP8Amax / amax, scale_inv = amax * kFP8AmaxInv;
                        if (lane_id == 0 or lane_id == 16)
                            rdma_x_scales[i * kNumElemsPerRead / 128] = scale_inv;

                        // Cast into send buffer
                        vec_t int2_value;
                        auto fp8x2_values = reinterpret_cast<ep_fp8x2_storage_t*>(&int2_value);
                        #pragma unroll
                        for (int j = 0; j < kNumElemsPerRead; j += 2) {
                            float2 fp32x2 = {fp32_values[j] * scale, fp32_values[j + 1] * scale};
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
                int slot_idx = lane_id == 0 ? atomicAdd(atomic_counter_per_expert + dst_expert_idx, 1) : 0;
                slot_idx = __shfl_sync(kEpWarpMask, slot_idx, 0);
                const auto dst_rank = dst_expert_idx / num_local_experts;
                const auto dst_expert_local_idx = dst_expert_idx % num_local_experts;
                const auto src_ptr = reinterpret_cast<const void*>(rdma_x_src_idx);
                const auto dst_ptr = reinterpret_cast<void*>(
                    reinterpret_cast<uint64_t>(rdma_recv_data_buffer) +
                    dst_expert_local_idx * num_ranks * num_max_dispatch_tokens_per_rank * num_bytes_per_msg +
                    rank * num_max_dispatch_tokens_per_rank * num_bytes_per_msg +
                    slot_idx * num_bytes_per_msg);

                void* write_dst = mc_route_put(comm_ctx, dst_rank, dst_ptr);
                if (write_dst != nullptr) {
                    // Local or P2P path — warp-cooperative copy
                    const auto* src_int4_ptr = reinterpret_cast<const int4*>(src_ptr);
                    const auto* dst_int4_ptr = reinterpret_cast<int4*>(write_dst);
                    mc_fence();
                    UNROLLED_WARP_COPY(8, lane_id, num_int4_per_msg, dst_int4_ptr, src_int4_ptr, mc_ld_nc, mc_st_na);
                    mc_fence();
                } else {
                    // IBGDA path — send directly from source buffer
                    __syncwarp();
                    mc_fence();
                    mc_rdma_put(comm_ctx, dst_expert_local_idx % num_qp_per_rank, dst_rank, num_qp_per_rank,
                                      src_ptr, dst_ptr, num_bytes_per_msg, lane_id);
                }

                // Increase counter after finishing
                __syncwarp();
                lane_id == 0 ? mc_atomic_add_release(atomic_finish_counter_per_expert + dst_expert_idx, 1) : 0;
            }
        }
    } else if (warp_id == num_warps - 1) {
#if defined(MOONCAKE_EP_USE_MUSA) || defined(MOONCAKE_EP_USE_MACA)
        // Participate in __syncthreads() barriers from data warps.
        // Each token iteration in the send loop above calls
        // __syncthreads() once; the count warp must match.
        for (int token_idx = sm_id; token_idx < num_tokens; token_idx += num_sms) {
            __syncthreads();
        }
#endif
        EP_DEVICE_ASSERT(num_sms > 1);
        if (sm_id == 0) {
            // The first SM is also responsible for cleaning the next buffer
            #pragma unroll
            for (int i = lane_id; i < num_experts; i += kEpWarpSize)
                next_clean_buffer[i] = 0;

            // Notify before executing `int_p`
            __syncwarp();
            #pragma unroll
            for (int i = lane_id; i < num_experts; i += kEpWarpSize)
                mc_atomic_add_release(atomic_finish_counter_per_expert + i, FINISHED_SUM_TAG);
        }

        // This SM should be responsible for some destination experts, read `topk_idx` for them
        int expert_count[kNumWarpGroups] = {0};
        const auto expert_begin_idx = sm_id * kNumWarpGroups;
        const auto expert_end_idx = min(expert_begin_idx + kNumWarpGroups, num_experts);

        // Per lane count
        #pragma unroll 8
        for (int i = lane_id; i < num_tokens * num_topk; i += kEpWarpSize) {
            auto idx = static_cast<int>(__ldg(topk_idx + i));
            if (idx >= expert_begin_idx and idx < expert_end_idx)
                expert_count[idx - expert_begin_idx] ++;
        }

        // Warp reduce
        #pragma unroll
        for (int i = expert_begin_idx; i < expert_end_idx; ++ i) {
            auto sum = warp_reduce_sum(expert_count[i - expert_begin_idx]);
            if (lane_id == 0) {
                shared_num_tokens_sent_per_expert[i - expert_begin_idx] = sum;
                mc_atomic_add_release(atomic_finish_counter_per_expert + i, FINISHED_SUM_TAG - sum);
            }
        }
    }
    mc_fence_barrier_fence();

    // Issue count sends
    if (responsible_expert_idx < num_experts and sub_warp_id == 0 and lane_id == 0) {
        const auto dst_rank = responsible_expert_idx / num_local_experts;
        const auto dst_expert_local_idx = responsible_expert_idx % num_local_experts;
        const auto num_tokens_sent = shared_num_tokens_sent_per_expert[responsible_expert_idx - sm_id * kNumWarpGroups];

        // Wait local sends issued and send expert counts
        while (mc_ld_acquire(atomic_finish_counter_per_expert + responsible_expert_idx) != FINISHED_SUM_TAG * 2);
        if (dst_rank != rank) {
            int* signal_ptr = rdma_recv_signal_buffer + dst_expert_local_idx * num_ranks + rank;
            if (force_rdma_data && num_tokens_sent > 0) {
                printf("[EP dispatch count send] rank=%d expert=%d dst_rank=%d dst_local=%d src_rank=%d count=%d sig_off_words=%lld write_signal=%d\n",
                       rank, responsible_expert_idx, dst_rank,
                       dst_expert_local_idx, rank, num_tokens_sent,
                       static_cast<long long>(signal_ptr - reinterpret_cast<int*>(mxa_buffer)),
                       rdma_write_signal);
            }
            mc_red_add(comm_ctx, dst_rank, dst_expert_local_idx % num_qp_per_rank, num_qp_per_rank,
                       signal_ptr, static_cast<int32_t>(-num_tokens_sent - 1));
        } else {
            mc_st_release(rdma_recv_signal_buffer + dst_expert_local_idx * num_ranks + rank, -num_tokens_sent - 1);
        }

        // Clean workspace for next use
        atomic_counter_per_expert[responsible_expert_idx] = 0;
        atomic_finish_counter_per_expert[responsible_expert_idx] = 0;

        // Clean `packed_recv_count`
        if (dst_rank == 0)
            packed_recv_count[dst_expert_local_idx] = 0;
    }
    __syncwarp();

    // Receiving phase
    LOW_LATENCY_DISPATCH_RECV:
    if ((phases & LOW_LATENCY_RECV_PHASE) == 0)
        return;

    // For send-and-recv kernels, we need a grid sync for making `packed_recv_count` visible
    if (phases & LOW_LATENCY_SEND_PHASE)
        mc_grid_sync();

    // Receiving and packing
    if (responsible_expert_idx < num_experts) {
        const auto src_rank = responsible_expert_idx / num_local_experts;
        const auto local_expert_idx = responsible_expert_idx % num_local_experts;
        const auto rdma_recv_x_uint8 = reinterpret_cast<uint8_t*>(rdma_recv_data_buffer) +
                local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank * num_bytes_per_msg +
                src_rank * num_max_dispatch_tokens_per_rank * num_bytes_per_msg;
        const auto recv_x_int4 = reinterpret_cast<int4*>(packed_recv_x) +
                local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank * hidden_int4;
        const auto recv_x_scales = packed_recv_x_scales + local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank * num_scales;
        const auto recv_src_info = packed_recv_src_info + local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank;
        const auto recv_range = packed_recv_layout_range + local_expert_idx * num_ranks;

        // Shared between sub-warps in warp groups
        __shared__ int shared_num_recv_tokens[kNumWarpGroups], shared_recv_token_begin_idx[kNumWarpGroups];

        // Wait tokens to arrive
        // NOTES: using sub-warp 1 to overlap with sub-warp 0
        int num_recv_tokens, recv_token_begin_idx;
        EP_STATIC_ASSERT(kNumWarpsPerGroup > 1, "Requires more than one warp per group");
        if (sub_warp_id == 1 and lane_id == 0) {
            unsigned long long start_time = clock64();
            while ((num_recv_tokens = mc_ld_acquire(rdma_recv_signal_buffer + local_expert_idx * num_ranks + src_rank)) == 0) {
                unsigned long long end_time = clock64();
                if (timeout_ticks != -1 && end_time - start_time > timeout_ticks) {
                    active_ranks[src_rank] = 0;
                }
                if (!active_ranks[src_rank]) {
                    num_recv_tokens = -1;
                    break;
                }
            }
            num_recv_tokens = -num_recv_tokens - 1;
            recv_token_begin_idx = atomicAdd(packed_recv_count + local_expert_idx, num_recv_tokens);
            shared_num_recv_tokens[warp_group_id] = num_recv_tokens;
            shared_recv_token_begin_idx[warp_group_id] = recv_token_begin_idx;
            recv_range[src_rank] = pack2<int, int64_t>(num_recv_tokens, recv_token_begin_idx);
        }
        mc_bar_sync(warp_group_id + 2, kNumWarpsPerGroup * kEpWarpSize);
        num_recv_tokens = shared_num_recv_tokens[warp_group_id];
        recv_token_begin_idx = shared_recv_token_begin_idx[warp_group_id];

        // Copy tokens
        EP_DEVICE_ASSERT(num_scales <= 64);
        mc_fence();
        for (int i = sub_warp_id; i < num_recv_tokens; i += kNumWarpsPerGroup) {
            // Copy source info
            const auto src_src_idx = reinterpret_cast<int*>(rdma_recv_x_uint8 + i * num_bytes_per_msg);
            if (lane_id == 0)
                recv_src_info[recv_token_begin_idx + i] = mc_ld_nc_s32(src_src_idx);
            __syncwarp();

            // Copy data
            // NOTES: only 2 load iterations for 7K hidden with 7 unrolls
            const auto src_data = reinterpret_cast<int4*>(reinterpret_cast<uint8_t*>(src_src_idx) + sizeof(int4));
            const auto dst_data = recv_x_int4 + (recv_token_begin_idx + i) * hidden_int4;
            mc_fence();
            UNROLLED_WARP_COPY(7, lane_id, hidden_int4, dst_data, src_data, mc_ld_nc, mc_st_na);

            // Copy scales
            if (kUseFP8) {
                const auto src_scales = reinterpret_cast<float*>(reinterpret_cast<uint8_t*>(src_data) + hidden_bytes);
                const auto dst_scales = reinterpret_cast<float*>(recv_x_scales + recv_token_begin_idx + i);
                const auto scale_stride = num_ranks * num_max_dispatch_tokens_per_rank;
                auto scale_0 = lane_id < num_scales ? mc_ld_nc_f32(src_scales + lane_id) : 0;
                auto scale_1 = (lane_id + 32) < num_scales ? mc_ld_nc_f32(src_scales + lane_id + 32) : 0;
                lane_id < num_scales ? dst_scales[lane_id * scale_stride] = scale_0 : 0.0f;
                (lane_id + 32) < num_scales ? dst_scales[(lane_id + 32) * scale_stride] = scale_1 : 0.0f;
            }
        }
    } else {
        mc_bar_sync(warp_group_id + 2, kNumWarpsPerGroup * kEpWarpSize);
    }
}

void dispatch(void* packed_recv_x, float* packed_recv_x_scales,
              int* packed_recv_src_info, int64_t* packed_recv_layout_range,
              int* packed_recv_count, int32_t* active_ranks,
              void* mxa_buffer,
              int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
              void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
              void* cuda_counter_buffer, void* cuda_data_buffer,
              void* raddrs, void* rkeys, void* qp_devctxs,
              const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
              const void* x, const int64_t* topk_idx,
              int* next_clean_buffer,
              int num_tokens, int hidden, int num_max_dispatch_tokens_per_rank,
              int num_topk, int num_experts, int rank, int num_ranks,
              bool use_fp8, int force_rdma_data, int poll_rdma_put,
              int rdma_write_signal, int active_qps_per_peer, void* workspace,
              cudaStream_t stream, int64_t timeout_ticks, int phases) {
    constexpr int kNumMaxTopK = 11;
#if defined(MOONCAKE_EP_USE_MACA)
    // C500 uses 64-thread hardware waves. Use 15 waves per CTA to keep the
    // block within 1024 threads while preserving enough send waves for top-k.
    constexpr int kNumWarpsPerGroup = 3;
    constexpr int kNumWarpGroups = 5;
#elif defined(MOONCAKE_EP_USE_MUSA)
    constexpr int kNumWarpsPerGroup = 4;
    // MT S5000 benefits from slightly more CTAs while keeping enough warps for top-k<=11.
    constexpr int kNumWarpGroups = 5;
#else
    constexpr int kNumWarpsPerGroup = 4;
    constexpr int kNumWarpGroups = 8;
#endif
    EP_STATIC_ASSERT(kNumMaxTopK + 1 <= kNumWarpGroups * kNumWarpsPerGroup, "Too many top-k selections");

    const auto num_warps = kNumWarpGroups * kNumWarpsPerGroup;
    const auto num_sms = cell_div(num_experts, kNumWarpGroups);
    EP_HOST_ASSERT(num_topk <= kNumMaxTopK);
#if defined(MOONCAKE_EP_USE_MACA)
    EP_HOST_ASSERT(!use_fp8);
#endif

    // Workspace checks
    auto atomic_counter_per_expert = reinterpret_cast<int*>(workspace);
    auto atomic_finish_counter_per_expert = atomic_counter_per_expert + num_experts;
    EP_HOST_ASSERT(num_experts * sizeof(int) * 2 <= NUM_WORKSPACE_BYTES);

#define DISPATCH_LAUNCH_CASE(hidden) { \
auto dispatch_func = use_fp8 ? dispatch<true, kNumWarpGroups, kNumWarpsPerGroup, hidden> : \
                               dispatch<false, kNumWarpGroups, kNumWarpsPerGroup, hidden>; \
LAUNCH_KERNEL(&cfg, dispatch_func, \
              packed_recv_x, packed_recv_x_scales, \
              packed_recv_src_info, packed_recv_layout_range, \
              packed_recv_count, active_ranks, \
              mxa_buffer, \
              rdma_send_signal_buffer, rdma_recv_signal_buffer, \
              rdma_send_data_buffer, rdma_recv_data_buffer, \
              cuda_counter_buffer, cuda_data_buffer, \
              raddrs, rkeys, qp_devctxs, \
              nvlink_available, ipc_peer_ptrs, \
              x, topk_idx, \
              atomic_counter_per_expert, atomic_finish_counter_per_expert, \
              next_clean_buffer, \
              num_tokens, num_max_dispatch_tokens_per_rank, \
              num_topk, num_experts, rank, num_ranks, force_rdma_data, \
              poll_rdma_put, rdma_write_signal, active_qps_per_peer, \
              timeout_ticks, phases); } break

    SETUP_LAUNCH_CONFIG(num_sms, num_warps * kEpWarpSize, stream);
    SWITCH_HIDDEN(DISPATCH_LAUNCH_CASE);
#undef DISPATCH_LAUNCH_CASE
}

template <int kNumWarpGroups, int kNumWarpsPerGroup, int kHidden, int kNumMaxTopk>
__global__ EP_LAUNCH_BOUNDS(kNumWarpGroups * kNumWarpsPerGroup * kEpWarpSize, 1) void
combine(void* combined_x, int32_t* active_ranks,
        void* mxa_buffer,
        int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
        void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
        void* cuda_counter_buffer, void* cuda_data_buffer,
        void* raddrs, void* rkeys, void* qp_devctxs,
        const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
        const void* x, const int64_t* topk_idx, const float* topk_weights,
        const int* src_info, const int64_t* layout_range,
        int* next_clean_buffer,
        int* atomic_clean_flag,
        int num_combined_tokens, int hidden, int num_topk,
        int num_max_dispatch_tokens_per_rank,
        int num_experts, int rank, int num_ranks, int force_rdma_data,
        int poll_rdma_put, int rdma_write_signal, int active_qps_per_peer,
        int64_t timeout_ticks,
        int phases, bool zero_copy) {
    const auto sm_id = static_cast<int>(blockIdx.x);
    const auto num_sms = static_cast<int>(gridDim.x);
    const auto thread_id = static_cast<int>(threadIdx.x);
    const auto num_threads = static_cast<int>(blockDim.x);
    const auto warp_id = thread_id / kEpWarpSize, lane_id = get_lane_id();
    const auto num_local_experts = num_experts / num_ranks;
    const auto warp_group_id = warp_id / kNumWarpsPerGroup;
    const auto sub_warp_id = warp_id % kNumWarpsPerGroup;
    const auto responsible_expert_idx = sm_id * kNumWarpGroups + warp_group_id;

    // Data type staffs
    constexpr int kNumElemsPerInt4 = sizeof(int4) / EP_BF16_SIZE;
    const size_t hidden_bf16_int4 = kHidden / kNumElemsPerInt4;

    // Message package
    constexpr size_t num_bytes_per_slot = kHidden * EP_BF16_SIZE;
    EP_STATIC_ASSERT(num_bytes_per_slot % sizeof(int4) == 0, "Invalid vectorization");

    // Communication context — platform dispatch is inside comm_device.cuh
    const CommCtx comm_ctx = make_comm_ctx(
        mxa_buffer, nvlink_available, ipc_peer_ptrs,
        raddrs, rkeys, qp_devctxs,
        rdma_send_signal_buffer, rdma_recv_signal_buffer,
        rank, num_ranks, MAX_QP_COUNT, force_rdma_data, poll_rdma_put,
        rdma_write_signal);
    const size_t num_qp_per_rank = active_qps_per_peer;

    // Sending phase
    if ((phases & LOW_LATENCY_SEND_PHASE) == 0)
        goto LOW_LATENCY_COMBINE_RECV;

    // Clean up next buffer
    if (sm_id == 0 and warp_group_id == 0 and sub_warp_id == 0) {
        #pragma unroll
        for (int i = lane_id; i < num_experts; i += kEpWarpSize)
            next_clean_buffer[i] = 0;

        // Notify before executing `int_p`
        __syncwarp();
        if (lane_id == 0)
            mc_atomic_add_release(atomic_clean_flag, num_experts);
    }

    // Issue IBGDA sends
    if (responsible_expert_idx < num_experts) {
        const auto dst_rank = responsible_expert_idx / num_local_experts;
        const auto local_expert_idx = responsible_expert_idx % num_local_experts;
        const auto global_expert_idx = rank * num_local_experts + local_expert_idx;
        const auto layout = __ldg(layout_range + local_expert_idx * num_ranks + dst_rank);
        const auto local_x = reinterpret_cast<const int4*>(x) +
                local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank * hidden_bf16_int4;
        const auto local_src_info = src_info + local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank;
        const auto rdma_send_x_vec = reinterpret_cast<uint8_t*>(rdma_send_data_buffer) +
                local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank * num_bytes_per_slot;

        // Unpack layout
        int offset, num_tokens_to_send;
        unpack2(layout, num_tokens_to_send, offset);

        // Issue IBGDA send
        for (int token_idx = offset + sub_warp_id; token_idx < offset + num_tokens_to_send; token_idx += kNumWarpsPerGroup) {
            const auto x_int4 = local_x + token_idx * hidden_bf16_int4;
            const auto rdma_send_type_row = reinterpret_cast<int*>(rdma_send_x_vec + token_idx * num_bytes_per_slot);
            const auto rdma_send_x_vec_row = reinterpret_cast<uint8_t*>(rdma_send_type_row);

            // Copy directly to local rank, or copy to buffer and issue RDMA
            auto src_idx = __ldg(local_src_info + token_idx);
            const auto buf_ptr = reinterpret_cast<void*>(rdma_send_x_vec_row);
            const auto dst_ptr = reinterpret_cast<void*>(
                reinterpret_cast<uint64_t>(rdma_recv_data_buffer) +
                (global_expert_idx * num_max_dispatch_tokens_per_rank + src_idx) * num_bytes_per_slot);

            void* write_dst = mc_route_put(comm_ctx, dst_rank, dst_ptr);
            if (write_dst != nullptr) {
                // Local or P2P path — warp-cooperative copy
                const auto dst_int4_ptr = reinterpret_cast<int4*>(write_dst);
                UNROLLED_WARP_COPY(7, lane_id, hidden_bf16_int4, dst_int4_ptr, x_int4, mc_ld_nc, mc_st_na);
                mc_fence();
            } else {
                // IBGDA path — stage to send buffer then RDMA write
                const auto buf_int4_ptr = reinterpret_cast<int4*>(buf_ptr);
                if (not zero_copy)
                    UNROLLED_WARP_COPY(7, lane_id, hidden_bf16_int4, buf_int4_ptr, x_int4, mc_ld_nc, mc_st_na);
                __syncwarp();
                mc_fence();
                mc_rdma_put(comm_ctx, local_expert_idx % num_qp_per_rank, dst_rank, num_qp_per_rank,
                                  buf_ptr, dst_ptr, num_bytes_per_slot, lane_id);
            }
        }
        // Put finishing flag
        EP_STATIC_ASSERT(kNumWarpsPerGroup > 1, "Requires more than one warp per group");
        mc_bar_sync(warp_group_id + 1, kNumWarpsPerGroup * kEpWarpSize);
        if (sub_warp_id == 1 and lane_id == 0) {
            while (mc_ld_acquire(atomic_clean_flag) == 0);
            if (dst_rank != rank) {
                int* signal_ptr = rdma_recv_signal_buffer + global_expert_idx;
                mc_signal(comm_ctx, dst_rank, local_expert_idx % num_qp_per_rank, num_qp_per_rank, signal_ptr, 1);
            } else {
                mc_st_release(rdma_recv_signal_buffer + global_expert_idx, 1);
            }
            mc_atomic_add_release(atomic_clean_flag, -1);
        }
        __syncwarp();
    } else {
        mc_bar_sync(warp_group_id + 1, kNumWarpsPerGroup * kEpWarpSize);
    }

    // Receiving phase
    LOW_LATENCY_COMBINE_RECV:
    if ((phases & LOW_LATENCY_RECV_PHASE) == 0)
        return;

    // Wait all ranks to arrive
    if (responsible_expert_idx < num_experts) {
        const auto src_rank = responsible_expert_idx / num_local_experts;
        EP_STATIC_ASSERT(kNumWarpsPerGroup > 1, "Invalid number of warps per group");
        if (sub_warp_id == 0 and lane_id == 0) {
            unsigned long long start_time = clock64();
            while (mc_ld_acquire(rdma_recv_signal_buffer + responsible_expert_idx) == 0) {
                unsigned long long end_time = clock64();
                if (timeout_ticks != -1 && end_time - start_time > timeout_ticks) {
                    active_ranks[src_rank] = 0;
                }
                if (!active_ranks[src_rank]) {
                    break;
                }
            }
        }
    }
#if defined(MOONCAKE_EP_USE_MUSA) || defined(MOONCAKE_EP_USE_MACA)
    // mc_grid_sync() is a no-op on MUSA; use a block-wide fence/barrier before
    // reduction so threads see peer writes.
    __syncthreads();
    mc_fence();
    __syncthreads();
#else
    mc_grid_sync();
#endif

    // Reduce tokens with FP8 cast
    EP_DEVICE_ASSERT(num_topk <= kEpWarpSize and hidden_bf16_int4 <= num_threads);
    EP_STATIC_ASSERT(kHidden % (kEpWarpSize * kNumElemsPerInt4) == 0, "Invalid vectorization");
    if (thread_id < hidden_bf16_int4) {
        for (int token_idx = sm_id; token_idx < num_combined_tokens; token_idx += num_sms) {
            mc_fence();
            // Read top-k indices and weights
            int reg_topk_idx[kNumMaxTopk];
            float reg_topk_weights[kNumMaxTopk];
            #pragma unroll
            for (int i = 0; i < num_topk; ++ i) {
                reg_topk_idx[i] = static_cast<int>(__ldg(topk_idx + token_idx * num_topk + i));
                reg_topk_weights[i] = __ldg(topk_weights + token_idx * num_topk + i);
            }

            float combined_values[kNumElemsPerInt4] = {0.0f};
            #pragma unroll
            for (int i = 0; i < num_topk; ++ i) if (reg_topk_idx[i] >= 0) {
                // Read from sources
                auto rdma_buffer_type = reinterpret_cast<const int*>(reinterpret_cast<uint8_t*>(rdma_recv_data_buffer) + (reg_topk_idx[i] * num_max_dispatch_tokens_per_rank + token_idx) * num_bytes_per_slot);
                auto rdma_buffer_row = reinterpret_cast<const uint8_t*>(rdma_buffer_type);

                // Reduce
                auto x_vec = mc_ld_nc(reinterpret_cast<const int4*>(rdma_buffer_row) + thread_id);
                const auto x_bf16 = reinterpret_cast<nv_bfloat16*>(&x_vec);
                #pragma unroll
                for (int j = 0; j < kNumElemsPerInt4; ++ j)
                    combined_values[j] += __bfloat162float(x_bf16[j]) * reg_topk_weights[i];
            }

            // Write results
            int4& combined_int4 = *reinterpret_cast<int4*>(combined_values);
            auto combined_bf16 = reinterpret_cast<nv_bfloat16*>(&combined_values);
            #pragma unroll
            for (int j = 0; j < kNumElemsPerInt4; ++ j)
                combined_bf16[j] = __float2bfloat16(combined_values[j]);
            (reinterpret_cast<int4*>(combined_x) + token_idx * hidden_bf16_int4)[thread_id] = combined_int4;
        }
    }
}

void combine(void* combined_x, int32_t* active_ranks,
             void* mxa_buffer,
             int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
             void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
             void* cuda_counter_buffer, void* cuda_data_buffer,
             void* raddrs, void* rkeys, void* qp_devctxs,
             const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
             const void* x, const int64_t* topk_idx, const float* topk_weights,
             const int* src_info, const int64_t* layout_range,
             int* next_clean_buffer,
             int num_combined_tokens, int hidden, int num_max_dispatch_tokens_per_rank,
             int num_topk, int num_experts, int rank, int num_ranks,
             int force_rdma_data, int poll_rdma_put, int rdma_write_signal,
             int active_qps_per_peer, void* workspace, cudaStream_t stream,
             int64_t timeout_ticks, int phases, bool zero_copy) {
#if defined(MOONCAKE_EP_USE_MACA)
    constexpr int kNumWarpsPerGroup = 3;
    constexpr int kNumWarpGroups = 5;
#else
    constexpr int kNumWarpsPerGroup = 4;
    constexpr int kNumWarpGroups = 8;
#endif
    constexpr int kNumMaxTopk = 11;

    const auto num_warps = kNumWarpGroups * kNumWarpsPerGroup;
    const auto num_sms = cell_div(num_experts, kNumWarpGroups);

    // Check workspace
    auto atomic_clean_flag = reinterpret_cast<int*>(workspace);
    EP_HOST_ASSERT(sizeof(int) <= NUM_WORKSPACE_BYTES);
    EP_HOST_ASSERT(num_topk <= kNumMaxTopk);

#define COMBINE_LAUNCH_CASE(hidden) { \
auto combine_func = combine<kNumWarpGroups, kNumWarpsPerGroup, hidden, kNumMaxTopk>; \
LAUNCH_KERNEL(&cfg, combine_func, \
              combined_x, active_ranks, \
              mxa_buffer, \
              rdma_send_signal_buffer, rdma_recv_signal_buffer, \
              rdma_send_data_buffer, rdma_recv_data_buffer, \
              cuda_counter_buffer, cuda_data_buffer, \
              raddrs, rkeys, qp_devctxs, \
              nvlink_available, ipc_peer_ptrs, \
              x, topk_idx, topk_weights, src_info, layout_range, \
              next_clean_buffer, \
              atomic_clean_flag, \
              num_combined_tokens, hidden, num_topk, \
              num_max_dispatch_tokens_per_rank, \
              num_experts, rank, num_ranks, force_rdma_data, poll_rdma_put, \
              rdma_write_signal, active_qps_per_peer, timeout_ticks, phases, \
              zero_copy); } break

    SETUP_LAUNCH_CONFIG(num_sms, num_warps * kEpWarpSize, stream);
    SWITCH_HIDDEN(COMBINE_LAUNCH_CASE);
#undef COMBINE_LAUNCH_CASE
}

__global__ void debug_rdma_put_probe_kernel(void* mxa_buffer, void* raddrs,
                                            void* rkeys, void* qp_devctxs,
                                            int rank, int dst_rank,
                                            int qps_per_rank,
                                            uint64_t dst_byte_offset,
                                            uint64_t src_byte_offset,
                                            uint64_t value,
                                            uint32_t nbytes,
                                            uint64_t* local_source,
                                            int poll_completion) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    *local_source = value;
    mc_fence();

    CommCtx ctx{};
    ctx.rank = rank;
    ctx.force_rdma_data = 1;
    ctx.poll_rdma_put = poll_completion;
    ctx.rdma_write_signal = 0;
    ctx.p2p.local_base = mxa_buffer;
    ctx.p2p.available = nullptr;
    ctx.p2p.peer_ptrs = nullptr;
    ctx.ibgda.qp_devctxs = reinterpret_cast<mlx5gda_qp_devctx*>(qp_devctxs);
    ctx.ibgda.raddrs = reinterpret_cast<const uint64_t*>(raddrs);
    ctx.ibgda.rkeys = reinterpret_cast<const uint32_t*>(rkeys);
    ctx.ibgda.local_atomic_base = mxa_buffer;
    ctx.ibgda.remote_atomic_base = mxa_buffer;

    uint64_t recv_raddr = ctx.ibgda.raddrs[dst_rank] + dst_byte_offset;
    int qp_idx = dst_rank * qps_per_rank;
    mlx5gda_qp_devctx* qp = ctx.ibgda.qp_devctxs + qp_idx;
    printf("[EP debug rdma probe] rank=%d dst=%d qpr=%d off=%llu "
           "src_off=%llu rbase=0x%llx raddr=0x%llx value=0x%llx "
           "source=0x%llx qp_idx=%d qpn=%u laddr=0x%llx lkey=0x%x "
           "rkey=0x%x self_base=0x%llx bytes=%u\n",
           rank, dst_rank, qps_per_rank,
           static_cast<unsigned long long>(dst_byte_offset),
           static_cast<unsigned long long>(src_byte_offset),
           static_cast<unsigned long long>(ctx.ibgda.raddrs[dst_rank]),
           static_cast<unsigned long long>(recv_raddr),
           static_cast<unsigned long long>(value),
           static_cast<unsigned long long>(*local_source),
           qp_idx, qp->qpn,
           static_cast<unsigned long long>(
               reinterpret_cast<uint64_t>(local_source)),
           ctx.ibgda.rkeys[rank], ctx.ibgda.rkeys[dst_rank],
           static_cast<unsigned long long>(ctx.ibgda.raddrs[rank]), nbytes);
    mc_ibgda_put(ctx.ibgda, 0, dst_rank, rank, qps_per_rank, local_source,
                 recv_raddr, nbytes, poll_completion != 0);
}

__global__ void debug_rdma_multi_put_probe_kernel(
    void* mxa_buffer, void* raddrs, void* rkeys, void* qp_devctxs, int rank,
    int dst_rank, int qps_per_rank, uint64_t dst_byte_offset,
    uint64_t dst_stride, uint64_t src_byte_offset, uint64_t src_stride,
    uint32_t nbytes, int nputs, int poll_completion, int delay_iters,
    int channel_offset, int warmup_puts, uint64_t warmup_dst_offset,
    uint64_t warmup_src_offset, int pre_post_delay_iters,
    int prewrite_source) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;

    CommCtx ctx{};
    ctx.rank = rank;
    ctx.force_rdma_data = 1;
    ctx.poll_rdma_put = poll_completion;
    ctx.rdma_write_signal = 0;
    ctx.p2p.local_base = mxa_buffer;
    ctx.p2p.available = nullptr;
    ctx.p2p.peer_ptrs = nullptr;
    ctx.ibgda.qp_devctxs = reinterpret_cast<mlx5gda_qp_devctx*>(qp_devctxs);
    ctx.ibgda.raddrs = reinterpret_cast<const uint64_t*>(raddrs);
    ctx.ibgda.rkeys = reinterpret_cast<const uint32_t*>(rkeys);
    ctx.ibgda.local_atomic_base = mxa_buffer;
    ctx.ibgda.remote_atomic_base = mxa_buffer;

    int total_puts = warmup_puts + nputs;
    for (int i = 0; i < total_puts; ++i) {
        bool warmup = i < warmup_puts;
        int logical_i = warmup ? i : i - warmup_puts;
        uint64_t cur_src_off =
            warmup ? warmup_src_offset + static_cast<uint64_t>(logical_i) *
                                             src_stride
                   : src_byte_offset + static_cast<uint64_t>(logical_i) *
                                           src_stride;
        uint64_t cur_dst_off =
            warmup ? warmup_dst_offset + static_cast<uint64_t>(logical_i) *
                                             dst_stride
                   : dst_byte_offset + static_cast<uint64_t>(logical_i) *
                                           dst_stride;
        auto* local_source = reinterpret_cast<uint64_t*>(
            reinterpret_cast<uintptr_t>(mxa_buffer) + cur_src_off);
        uint64_t value = (rank == 0 ? 0xabc0000000000000ULL
                                    : 0xdef0000000000000ULL) |
                         static_cast<uint64_t>(logical_i);
        if (!prewrite_source) {
            *local_source = value;
            mc_fence();
        }
        for (int spin = 0; spin < pre_post_delay_iters; ++spin) {
            __threadfence_system();
        }

        int channel = (i + channel_offset) % qps_per_rank;
        uint64_t recv_raddr = ctx.ibgda.raddrs[dst_rank] + cur_dst_off;
        if (i < 4 || (warmup_puts > 0 && i < warmup_puts + 4)) {
            int qp_idx = dst_rank * qps_per_rank + channel;
            mlx5gda_qp_devctx* qp = ctx.ibgda.qp_devctxs + qp_idx;
            printf("[EP debug multi put] rank=%d dst=%d i=%d warmup=%d ch=%d qpr=%d "
                   "src_off=%llu dst_off=%llu bytes=%u qpn=%u source=0x%llx\n",
                   rank, dst_rank, i, warmup ? 1 : 0, channel, qps_per_rank,
                   static_cast<unsigned long long>(cur_src_off),
                   static_cast<unsigned long long>(cur_dst_off),
                   nbytes, qp->qpn,
                   static_cast<unsigned long long>(*local_source));
        }
        mc_ibgda_put(ctx.ibgda, channel, dst_rank, rank, qps_per_rank,
                     local_source, recv_raddr, nbytes,
                     poll_completion != 0);
        for (int spin = 0; spin < delay_iters; ++spin) {
            __threadfence_system();
        }
    }
}

__global__ void debug_fill_multi_put_sources_kernel(
    void* mxa_buffer, int rank, uint64_t src_byte_offset, uint64_t src_stride,
    int nputs) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    for (int i = 0; i < nputs; ++i) {
        auto* local_source = reinterpret_cast<uint64_t*>(
            reinterpret_cast<uintptr_t>(mxa_buffer) + src_byte_offset +
            static_cast<uint64_t>(i) * src_stride);
        *local_source = ((rank == 0 ? 0xabc0000000000000ULL
                                    : 0xdef0000000000000ULL) |
                         static_cast<uint64_t>(i));
    }
    mc_fence();
}

void debug_rdma_put_probe(void* mxa_buffer, void* raddrs, void* rkeys,
                          void* qp_devctxs, int rank, int num_ranks,
                          int qps_per_rank, int dst_rank,
                          uint64_t dst_byte_offset, uint64_t src_byte_offset,
                          uint64_t value,
                          uint32_t nbytes, uint64_t* local_source,
                          int poll_completion, cudaStream_t stream) {
    (void)num_ranks;
    SETUP_LAUNCH_CONFIG(1, 1, stream);
    LAUNCH_KERNEL(&cfg, debug_rdma_put_probe_kernel, mxa_buffer, raddrs, rkeys,
                  qp_devctxs, rank, dst_rank, qps_per_rank, dst_byte_offset,
                  src_byte_offset, value, nbytes, local_source,
                  poll_completion);
}

void debug_rdma_multi_put_probe(
    void* mxa_buffer, void* raddrs, void* rkeys, void* qp_devctxs, int rank,
    int num_ranks, int qps_per_rank, int dst_rank, uint64_t dst_byte_offset,
    uint64_t dst_stride, uint64_t src_byte_offset, uint64_t src_stride,
    uint32_t nbytes, int nputs, int poll_completion, int delay_iters,
    int channel_offset, int warmup_puts, uint64_t warmup_dst_offset,
    uint64_t warmup_src_offset, int pre_post_delay_iters, int prewrite_source,
    cudaStream_t stream) {
    (void)num_ranks;
    SETUP_LAUNCH_CONFIG(1, 1, stream);
    LAUNCH_KERNEL(&cfg, debug_rdma_multi_put_probe_kernel, mxa_buffer, raddrs,
                  rkeys, qp_devctxs, rank, dst_rank, qps_per_rank,
                  dst_byte_offset, dst_stride, src_byte_offset, src_stride,
                  nbytes, nputs, poll_completion, delay_iters, channel_offset,
                  warmup_puts, warmup_dst_offset, warmup_src_offset,
                  pre_post_delay_iters, prewrite_source);
}

void debug_fill_multi_put_sources(void* mxa_buffer, int rank,
                                  uint64_t src_byte_offset,
                                  uint64_t src_stride, int nputs,
                                  cudaStream_t stream) {
    SETUP_LAUNCH_CONFIG(1, 1, stream);
    LAUNCH_KERNEL(&cfg, debug_fill_multi_put_sources_kernel, mxa_buffer, rank,
                  src_byte_offset, src_stride, nputs);
}

} // namespace mooncake
