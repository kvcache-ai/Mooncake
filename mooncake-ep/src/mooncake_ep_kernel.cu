// clang-format off

#include <cstdio>

#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_dispatch.cuh>
#include <mooncake_ep_exception.cuh>
#include <mooncake_ep_launch.cuh>
#include <transport/device/comm_device.cuh>
#include <mooncake_ep_utils.cuh>

#if !defined(MOONCAKE_EP_USE_MUSA) && !defined(MOONCAKE_EP_USE_MACA)
#include <jit/dispatch_jit.h>
#endif

namespace mooncake {

using mooncake::device::CommCtx;
using mooncake::device::make_comm_ctx;
using mooncake::device::mc_route_put;
using mooncake::device::mc_rdma_put;
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
#ifdef MOONCAKE_EP_USE_MUSA
using mooncake::device::mc_atomic_add_relaxed;
using mooncake::device::mc_fence_noflush;
#endif
using mooncake::device::mc_fence;
using mooncake::device::mc_fence_barrier_fence;

__global__ void mark_phase_ack_kernel(void* mxa_buffer,
                                      const int32_t* nvlink_available,
                                      void* const* ipc_peer_ptrs,
                                      int* ack_buffer, int rank,
                                      int num_ranks, int epoch) {
    const CommCtx comm_ctx = make_comm_ctx(
        mxa_buffer, nvlink_available, ipc_peer_ptrs, nullptr, nullptr, nullptr,
        ack_buffer, ack_buffer, rank, num_ranks, MAX_QP_COUNT);

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
        ack_buffer, ack_buffer, rank, num_ranks, MAX_QP_COUNT);

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

void dispatch(void* packed_recv_x, float* packed_recv_x_scales,
              int* packed_recv_src_info, int64_t* packed_recv_layout_range,
              int* packed_recv_count, int32_t* active_ranks,
              void* mxa_buffer,
              int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
              void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
              void* raddrs, void* rkeys, void* qp_devctxs,
              const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
              const void* x, const int64_t* topk_idx,
              int* next_clean_buffer,
              int num_tokens, int hidden, int num_max_dispatch_tokens_per_rank,
              int num_topk, int num_experts, int rank, int num_ranks, bool use_fp8,
              void* workspace, cudaStream_t stream,
              int64_t timeout_ticks, int phases, int active_qps_per_rank) {
    constexpr int kNumMaxTopK = 17;
    constexpr int kNumWarpsPerGroup = 4;
    int num_warp_groups = 8;
#ifdef MOONCAKE_EP_USE_MUSA
    cudaDeviceProp device_prop{};
    int device = 0;
    CUDA_CHECK(cudaGetDevice(&device));
    CUDA_CHECK(cudaGetDeviceProperties(&device_prop, device));
    num_warp_groups = cell_div(num_experts, device_prop.multiProcessorCount);
    // Select the group count from the device shape. The S5000's 56 SMs select
    // six groups for 288 experts and five groups for the 256-expert shape.
    // Five is the minimum valid value for the kNumMaxTopK capacity check.
    num_warp_groups = max(5, min(8, num_warp_groups));
#endif
    EP_HOST_ASSERT(kNumMaxTopK + 1 <= num_warp_groups * kNumWarpsPerGroup &&
                   "Too many top-k selections");

    const auto num_sms = max(2, cell_div(num_experts, num_warp_groups));
    EP_HOST_ASSERT(num_topk <= kNumMaxTopK);

    // Workspace checks
    auto atomic_counter_per_expert = reinterpret_cast<int*>(workspace);
    auto atomic_finish_counter_per_expert = atomic_counter_per_expert + num_experts;
    EP_HOST_ASSERT(num_experts * sizeof(int) * 2 <= NUM_WORKSPACE_BYTES);

    const DispatchKernelArgs args {
        packed_recv_x,
        packed_recv_x_scales,
        packed_recv_src_info,
        packed_recv_layout_range,
        packed_recv_count,
        active_ranks,
        mxa_buffer,
        rdma_send_signal_buffer,
        rdma_recv_signal_buffer,
        rdma_send_data_buffer,
        rdma_recv_data_buffer,
        raddrs,
        rkeys,
        qp_devctxs,
        nvlink_available,
        ipc_peer_ptrs,
        x,
        topk_idx,
        atomic_counter_per_expert,
        atomic_finish_counter_per_expert,
        next_clean_buffer,
        num_tokens,
        num_max_dispatch_tokens_per_rank,
        num_topk,
        num_experts,
        rank,
        num_ranks,
        timeout_ticks,
        phases,
        active_qps_per_rank,
    };

#if !defined(MOONCAKE_EP_USE_MUSA) && !defined(MOONCAKE_EP_USE_MACA)
    jit::launch_dispatch_jit(hidden, use_fp8, num_warp_groups,
                             kNumWarpsPerGroup, num_sms, args, stream);
#else // !defined(MOONCAKE_EP_USE_MUSA) && !defined(MOONCAKE_EP_USE_MACA)
#define DISPATCH_LAUNCH_GROUP(hidden, groups) case groups: { \
constexpr int kNumWarpGroups = groups; \
auto dispatch_func = use_fp8 ? dispatch<true, kNumWarpGroups, kNumWarpsPerGroup, hidden> : \
                               dispatch<false, kNumWarpGroups, kNumWarpsPerGroup, hidden>; \
LAUNCH_KERNEL(&cfg, dispatch_func, args); } break

#define DISPATCH_LAUNCH_CASE(hidden) { \
switch (num_warp_groups) { \
DISPATCH_LAUNCH_GROUP(hidden, 3); \
DISPATCH_LAUNCH_GROUP(hidden, 4); \
DISPATCH_LAUNCH_GROUP(hidden, 5); \
DISPATCH_LAUNCH_GROUP(hidden, 6); \
DISPATCH_LAUNCH_GROUP(hidden, 7); \
DISPATCH_LAUNCH_GROUP(hidden, 8); \
default: EP_HOST_ASSERT(false && "Unsupported dispatch warp-group count"); \
} } break

    SETUP_LAUNCH_CONFIG(num_sms, num_warp_groups * kNumWarpsPerGroup * 32, stream);
    SWITCH_HIDDEN(DISPATCH_LAUNCH_CASE);
#undef DISPATCH_LAUNCH_CASE
#undef DISPATCH_LAUNCH_GROUP
#endif // !defined(MOONCAKE_EP_USE_MUSA) && !defined(MOONCAKE_EP_USE_MACA)
}

template <int kNumWarpGroups, int kNumWarpsPerGroup, int kHidden, int kNumMaxTopk>
__global__ EP_LAUNCH_BOUNDS(kNumWarpGroups * kNumWarpsPerGroup * 32, 1) void
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
        int num_experts, int rank, int num_ranks,
        int64_t timeout_ticks,
        int phases, bool zero_copy, int active_qps_per_rank) {
    const auto sm_id = static_cast<int>(blockIdx.x);
    const auto num_sms = static_cast<int>(gridDim.x);
    const auto thread_id = static_cast<int>(threadIdx.x);
    const auto num_threads = static_cast<int>(blockDim.x);
    const auto warp_id = thread_id / 32, lane_id = get_lane_id();
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
        rank, num_ranks, MAX_QP_COUNT);
    const size_t num_qp_per_rank = MAX_QP_COUNT / num_ranks;

    // Sending phase
    if ((phases & LOW_LATENCY_SEND_PHASE) == 0)
        goto LOW_LATENCY_COMBINE_RECV;

    // Clean up next buffer
    if (sm_id == 0 and warp_group_id == 0 and sub_warp_id == 0) {
        #pragma unroll
        for (int i = lane_id; i < num_experts; i += 32)
            next_clean_buffer[i] = 0;

        // Notify before executing `int_p`
        __syncwarp();
#ifdef MOONCAKE_EP_USE_MUSA
        mc_fence();
        __syncwarp();
        if (lane_id == 0)
            mc_atomic_add_relaxed(atomic_clean_flag, num_experts);
#else
        if (lane_id == 0)
            mc_atomic_add_release(atomic_clean_flag, num_experts);
#endif
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
#ifndef MOONCAKE_EP_USE_MUSA
                mc_fence();
#endif
            } else {
                // IBGDA path — stage to send buffer then RDMA write
                const auto buf_int4_ptr = reinterpret_cast<int4*>(buf_ptr);
                if (not zero_copy)
                    UNROLLED_WARP_COPY(7, lane_id, hidden_bf16_int4, buf_int4_ptr, x_int4, mc_ld_nc, mc_st_na);
                __syncwarp();
#if defined(MOONCAKE_EP_USE_MACA) || defined(MOONCAKE_EP_USE_MUSA)
                // Match dispatch publication and avoid concurrent posts from
                // the two 32-lane sub-warps sharing one QP.
                mc_fence();
                for (int half = 0; half < 2; ++half) {
                    if ((sub_warp_id & 1) == half) {
#endif
                mc_rdma_put(comm_ctx,
                            ep_qp_channel(local_expert_idx, num_qp_per_rank,
                                          active_qps_per_rank),
                            dst_rank, num_qp_per_rank, buf_ptr, dst_ptr,
                            num_bytes_per_slot, lane_id);
#if defined(MOONCAKE_EP_USE_MACA) || defined(MOONCAKE_EP_USE_MUSA)
                    }
                    __syncwarp();
                }
#endif
            }
        }
        // Put finishing flag
        EP_STATIC_ASSERT(kNumWarpsPerGroup > 1, "Requires more than one warp per group");
#ifdef MOONCAKE_EP_USE_MUSA
        // Publish all local/P2P payload stores once for this CTA before any
        // expert completion word is made observable by a peer.
        mc_fence_barrier_fence();
#else
        mc_bar_sync(warp_group_id + 1, kNumWarpsPerGroup * 32);
#endif
        if (sub_warp_id == 1 and lane_id == 0) {
#ifdef MOONCAKE_EP_USE_MUSA
            while (atomicAdd(atomic_clean_flag, 0) == 0);
#else
            while (mc_ld_acquire(atomic_clean_flag) == 0);
#endif
            if (dst_rank != rank) {
                int* signal_ptr = rdma_recv_signal_buffer + global_expert_idx;
                mc_signal(comm_ctx, dst_rank,
                          ep_qp_channel(local_expert_idx, num_qp_per_rank,
                                        active_qps_per_rank),
                          num_qp_per_rank, signal_ptr, 1);
            } else {
                mc_st_release(rdma_recv_signal_buffer + global_expert_idx, 1);
            }
#ifdef MOONCAKE_EP_USE_MUSA
            // The peer signal already publishes the payload. This local tally
            // is reused after the current kernel completes on the same stream.
            mc_atomic_add_relaxed(atomic_clean_flag, -1);
#else
            mc_atomic_add_release(atomic_clean_flag, -1);
#endif
        }
        __syncwarp();
    } else {
#ifdef MOONCAKE_EP_USE_MUSA
        mc_fence_barrier_fence();
#else
        mc_bar_sync(warp_group_id + 1, kNumWarpsPerGroup * 32);
#endif
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
#ifdef MOONCAKE_EP_SPLIT_SEND_RECV
    // mc_grid_sync() is a no-op on split-kernel platforms; use a block-wide
    // fence/barrier before reduction so threads see peer writes.
    __syncthreads();
#ifndef MOONCAKE_EP_USE_MUSA
    mc_fence();
#endif
    __syncthreads();
#else
    mc_grid_sync();
#endif

    // Reduce tokens with FP8 cast
    EP_DEVICE_ASSERT(num_topk <= 32 and hidden_bf16_int4 <= num_threads);
    EP_STATIC_ASSERT(kHidden % (32 * kNumElemsPerInt4) == 0, "Invalid vectorization");
#ifdef MOONCAKE_EP_USE_MUSA
    // A completion word is stable for the entire combine. When the expert
    // count is smaller than the token/expert fanout, acquire every expert
    // once per reduction CTA instead of repeatedly acquiring the same words
    // for every token.
    const bool wait_all_experts =
        num_experts <= num_combined_tokens * num_topk;
    if (wait_all_experts) {
        const int entry_idx = thread_id;
        const int token_offset = entry_idx / num_topk;
        const int topk_offset = entry_idx % num_topk;
        const int token_idx = sm_id + token_offset * num_sms;
        if (token_idx < num_combined_tokens) {
            const int expert_idx = static_cast<int>(
                __ldg(topk_idx + token_idx * num_topk + topk_offset));
            if (expert_idx >= 0) {
                const int expert_src_rank = expert_idx / num_local_experts;
                const unsigned long long start_time = clock64();
                while (mc_ld_acquire(rdma_recv_signal_buffer + expert_idx) == 0 &&
                       active_ranks[expert_src_rank]) {
                    const unsigned long long end_time = clock64();
                    if (timeout_ticks != -1 && end_time - start_time > timeout_ticks)
                        active_ranks[expert_src_rank] = 0;
                }
            }
        }
    }
    __syncthreads();
    for (int token_idx = sm_id; token_idx < num_combined_tokens; token_idx += num_sms) {
        // CUDA's cooperative grid barrier above orders an expert-owning block
        // before every token-reduction block. MUSA has no grid barrier, so
        // the reduction block waits only on the experts selected by its token.
        if (!wait_all_experts && thread_id == 0) {
            #pragma unroll
            for (int i = 0; i < num_topk; ++i) {
                const int expert_idx =
                    static_cast<int>(__ldg(topk_idx + token_idx * num_topk + i));
                if (expert_idx < 0)
                    continue;
                const int expert_src_rank = expert_idx / num_local_experts;
                const unsigned long long start_time = clock64();
                while (mc_ld_acquire(rdma_recv_signal_buffer + expert_idx) == 0 &&
                       active_ranks[expert_src_rank]) {
                    const unsigned long long end_time = clock64();
                    if (timeout_ticks != -1 && end_time - start_time > timeout_ticks)
                        active_ranks[expert_src_rank] = 0;
                }
            }
        }
        __syncthreads();
        if (thread_id < hidden_bf16_int4) {
#else
    if (thread_id < hidden_bf16_int4) {
        for (int token_idx = sm_id; token_idx < num_combined_tokens; token_idx += num_sms) {
#endif
#ifndef MOONCAKE_EP_USE_MUSA
            mc_fence();
#endif
            // Read top-k indices and weights
            int reg_topk_idx[kNumMaxTopk];
            float reg_topk_weights[kNumMaxTopk];
            #pragma unroll
            for (int i = 0; i < num_topk; ++ i) {
                reg_topk_idx[i] = static_cast<int>(__ldg(topk_idx + token_idx * num_topk + i));
                reg_topk_weights[i] = __ldg(topk_weights + token_idx * num_topk + i);
            }

#ifdef MOONCAKE_EP_USE_MUSA
            float2 combined_values[kNumElemsPerInt4 / 2] = {};
#else
            float combined_values[kNumElemsPerInt4] = {0.0f};
#endif
            #pragma unroll
            for (int i = 0; i < num_topk; ++ i) if (reg_topk_idx[i] >= 0) {
                // Skip experts on inactive ranks (timed out during combine recv)
                int expert_src_rank = reg_topk_idx[i] / num_local_experts;
                if (!active_ranks[expert_src_rank])
                    continue;
                // Read from sources
                auto rdma_buffer_type = reinterpret_cast<const int*>(reinterpret_cast<uint8_t*>(rdma_recv_data_buffer) + (reg_topk_idx[i] * num_max_dispatch_tokens_per_rank + token_idx) * num_bytes_per_slot);
                auto rdma_buffer_row = reinterpret_cast<const uint8_t*>(rdma_buffer_type);

                // Reduce
                auto x_vec = mc_ld_nc(reinterpret_cast<const int4*>(rdma_buffer_row) + thread_id);
#ifdef MOONCAKE_EP_USE_MUSA
                const auto x_bf162 = reinterpret_cast<nv_bfloat162*>(&x_vec);
                #pragma unroll
                for (int j = 0; j < kNumElemsPerInt4 / 2; ++j) {
                    const float2 values = {__bfloat162float(x_bf162[j].x),
                                           __bfloat162float(x_bf162[j].y)};
                    combined_values[j].x += values.x * reg_topk_weights[i];
                    combined_values[j].y += values.y * reg_topk_weights[i];
                }
#else
                const auto x_bf16 = reinterpret_cast<nv_bfloat16*>(&x_vec);
                #pragma unroll
                for (int j = 0; j < kNumElemsPerInt4; ++ j)
                    combined_values[j] += __bfloat162float(x_bf16[j]) * reg_topk_weights[i];
#endif
            }

            // Write results
#ifdef MOONCAKE_EP_USE_MUSA
            int4 combined_int4;
            auto combined_bf162 = reinterpret_cast<nv_bfloat162*>(&combined_int4);
            #pragma unroll
            for (int j = 0; j < kNumElemsPerInt4 / 2; ++j)
                combined_bf162[j] = __floats2bfloat162_rn(combined_values[j].x,
                                                           combined_values[j].y);
#else
            int4& combined_int4 = *reinterpret_cast<int4*>(combined_values);
            auto combined_bf16 = reinterpret_cast<nv_bfloat16*>(&combined_values);
            #pragma unroll
            for (int j = 0; j < kNumElemsPerInt4; ++ j)
                combined_bf16[j] = __float2bfloat16(combined_values[j]);
#endif
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
             void* workspace, cudaStream_t stream,
             int64_t timeout_ticks, int phases, bool zero_copy,
             int active_qps_per_rank) {
    constexpr int kNumWarpsPerGroup = 4;
    constexpr int kNumWarpGroups = 8;
    constexpr int kNumMaxTopk = 17;

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
              num_experts, rank, num_ranks, \
              timeout_ticks, phases, zero_copy, active_qps_per_rank); } break

    SETUP_LAUNCH_CONFIG(num_sms, num_warps * 32, stream);
    SWITCH_HIDDEN(COMBINE_LAUNCH_CASE);
#undef COMBINE_LAUNCH_CASE
}

} // namespace mooncake
