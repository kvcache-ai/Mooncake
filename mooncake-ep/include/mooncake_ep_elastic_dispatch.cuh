// Elastic dispatch kernel — adapted from DeepEP V2 dispatch.cuh.
//
// Two warp roles:
//   1. Notify warps (0..kNumNotifyWarps-1): count tokens per expert/rank,
//      do full-grid reduction, compute prefix sums, write slot indices.
//   2. Dispatch warps (kNumNotifyWarps..): read pre-computed slot indices,
//      copy data to remote buffers via Device API.
//
// Differences from DeepEP V2:
//   - No TMA → UNROLLED_WARP_COPY with shared memory staging.
//   - No GIN → Device API (mc_route_put, mc_rdma_put, mc_signal).
//   - No mbarrier → __syncwarp.
//   - No ptx::named_barrier → mc_bar_sync.
//   - No cudaTriggerProgrammaticLaunchCompletion.
//   - No scaleout (single-node only, kIsScaleupNVLink = true).
//   - No team_t (ncclTeamTagLsa/ncclTeamTagWorld).
//   - No SF packs (kNumSFPacks = 0).
#pragma once

#include <mooncake_ep_barrier.cuh>
#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_exception.cuh>
#include <mooncake_ep_layout.cuh>
#include <mooncake_ep_utils.cuh>
#include <transport/device/comm_device.cuh>

namespace mooncake {
namespace elastic_dispatch {

using mooncake::device::CommCtx;
using mooncake::device::make_comm_ctx;
using mooncake::device::mc_route_put;
using mooncake::device::mc_rdma_put;
using mooncake::device::mc_signal;
using mooncake::device::mc_bar_sync;
using mooncake::device::mc_grid_sync;
using mooncake::device::mc_ld_nc;
using mooncake::device::mc_st_na;
using mooncake::device::mc_ld_acquire;
using mooncake::device::mc_st_release;
using mooncake::device::mc_atomic_add_release;

template <bool kReuseSlotIndices, int kNumNotifyWarps, int kNumDispatchWarps,
          int kNumRanks, int kNumHiddenBytes,
          int kExpertAlignment,
          int64_t kNumTimeoutCycles,
          int kNumNotifyThreads = kNumNotifyWarps * 32,
          int kNumDispatchThreads = kNumDispatchWarps * 32,
          int kNumThreads = kNumNotifyThreads + kNumDispatchThreads>
__global__ void __launch_bounds__(kNumThreads, 1)
dispatch_impl(
    void* x, int64_t* topk_idx, float* topk_weights,
    int64_t* copied_topk_idx,
    int* cumulative_local_expert_recv_stats,
    int* psum_num_recv_tokens_per_rank,
    int* psum_num_recv_tokens_per_expert,
    int* dst_buffer_slot_idx,
    int num_tokens,
    int num_topk,
    int num_experts,
    int num_max_tokens_per_rank,
    void* mxa_buffer, void* raddrs, void* rkeys, void* qp_devctxs,
    const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
    int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
    void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
    void* buffer, void* workspace,
    int rank_idx, int num_ranks, int num_sms) {
    const int num_experts_per_rank = num_experts / kNumRanks;
    EP_DEVICE_ASSERT(num_experts % kNumRanks == 0);
    EP_STATIC_ASSERT(kNumNotifyWarps % 4 == 0, "Invalid warpgroup size");

    // Utils
    const auto sm_idx = static_cast<int>(blockIdx.x);
    const auto thread_idx = static_cast<int>(threadIdx.x);
    const auto warp_idx = thread_idx / 32;
    const auto lane_idx = get_lane_id();

    // Workspace
    const auto workspace_layout =
        layout::WorkspaceLayout(workspace, num_ranks, num_experts);

    // Shared memory
    extern __shared__ __align__(16) int8_t smem[];
    const int num_smem_bytes_for_notify =
        kNumNotifyThreads > 0
            ? align<int>(kNumRanks + num_experts, kNumNotifyThreads) *
                  static_cast<int>(sizeof(int))
            : 0;

    // Named barrier index for notify warps
    constexpr int kNotifyBarrierIndex = 1;

    // Communication context
    const CommCtx comm_ctx = make_comm_ctx(
        mxa_buffer, nvlink_available, ipc_peer_ptrs, raddrs, rkeys,
        qp_devctxs, rdma_send_signal_buffer, rdma_recv_signal_buffer, rank_idx,
        num_ranks, MAX_QP_COUNT);
    const size_t num_qp_per_rank = MAX_QP_COUNT / num_ranks;

    // Barrier at start
    barrier::gpu_barrier<kNumRanks, kNumThreads, kNumTimeoutCycles,
                         EP_BARRIER_TAG_DISPATCH_0, false, false>(
        comm_ctx, workspace_layout, rank_idx, sm_idx, thread_idx, num_sms);

    // =====================================================================
    // Notify warps: count tokens, reduce, compute prefix sums
    // =====================================================================
    if (warp_idx < kNumNotifyWarps) {
        const int num_aligned_elems = num_smem_bytes_for_notify / sizeof(int);
        const auto rank_expert_count = advance_ptr<int>(smem, 0);

        int* rank_count = rank_expert_count;
        int* expert_count = rank_expert_count + kNumRanks;

        // Clean initial counts
#pragma unroll
        for (int i = 0; i < num_aligned_elems / kNumNotifyThreads; ++i)
            rank_expert_count[i * kNumNotifyThreads + thread_idx] = 0;
        mc_bar_sync(kNotifyBarrierIndex, kNumNotifyThreads);

        // Atomic add on shared memory
        EP_DEVICE_ASSERT(num_topk <= 32);
        const auto global_warp_idx = warp_idx * num_sms + sm_idx;
        for (int i = global_warp_idx; i < num_tokens;
             i += kNumNotifyWarps * num_sms) {
            const auto dst_expert_idx =
                lane_idx < num_topk
                    ? static_cast<int>(
                          __ldg(topk_idx + i * num_topk + lane_idx))
                    : -1;
            if (dst_expert_idx >= 0)
                atomicAdd_block(expert_count + dst_expert_idx, 1);

            const auto dst_rank_idx =
                dst_expert_idx >= 0
                    ? dst_expert_idx / num_experts_per_rank
                    : -1;
            if (deduplicate(dst_rank_idx, lane_idx) && dst_rank_idx >= 0)
                atomicAdd_block(rank_count + dst_rank_idx, 1);
        }
        mc_bar_sync(kNotifyBarrierIndex, kNumNotifyThreads);

        // Full-grid reduction: each SM writes its count to global workspace
#pragma unroll
        for (int i = thread_idx; i < kNumRanks + num_experts;
             i += kNumNotifyThreads) {
            const int64_t counter =
                (1ll << 32ll) | rank_expert_count[i];
            mc_atomic_add_release(
                reinterpret_cast<int*>(
                    workspace_layout.get_notify_reduction_workspace_ptr() +
                    i),
                static_cast<int>(counter));
        }

        // SM 0 does the remaining work
        if (sm_idx == 0) {
            // Wait all SMs' arrival and reduce
#pragma unroll
            for (int i = thread_idx; i < kNumRanks + num_experts;
                 i += kNumNotifyThreads) {
                barrier::timeout_while<kNumTimeoutCycles>(
                    true, [=](const bool& is_last_check) {
                        const auto status = ld_volatile_global(
                            workspace_layout
                                .get_notify_reduction_workspace_ptr() +
                            i);
                        if ((status >> 32) == num_sms) {
                            const auto encoded = encode_decode_positive(
                                static_cast<int>(status & 0xffffffffll));
                            rank_expert_count[i] = encoded;

                            // Write to send buffer for peer access
                            workspace_layout
                                .get_rank_expert_count_ptr<true>()[i] =
                                encoded;

                            // Clean for next usage
                            workspace_layout
                                .get_notify_reduction_workspace_ptr()[i] = 0;
                            return true;
                        }

                        if (is_last_check) {
                            printf(
                                "Mooncake EP notify (GPU reduction) timeout, "
                                "rank: %d/%d, thread: %d, status: %d | %d, "
                                "expected: %d\n",
                                rank_idx, kNumRanks, thread_idx,
                                static_cast<int>(status >> 32),
                                static_cast<int>(status & 0xffffffff),
                                num_sms);
                        }
                        return false;
                    });
            }
            mc_bar_sync(kNotifyBarrierIndex, kNumNotifyThreads);

            // Issue rank count writes to peers
            for (int i = thread_idx; i < kNumRanks;
                 i += kNumNotifyThreads) {
                const auto dst_rank_counter =
                    workspace_layout.get_rank_count_ptr<false>() + rank_idx;
                if (i == rank_idx) {
                    mc_st_release(
                        reinterpret_cast<int*>(dst_rank_counter),
                        static_cast<int>(rank_count[i]));
                } else {
                    void* peer_ptr = mc_route_put(
                        comm_ctx, i,
                        const_cast<int*>(dst_rank_counter));
                    if (peer_ptr != nullptr) {
                        mc_st_release(static_cast<int*>(peer_ptr),
                                      static_cast<int>(rank_count[i]));
                    } else {
                        mc_signal(comm_ctx, i, 0, 1,
                                  const_cast<int*>(dst_rank_counter),
                                  static_cast<int>(rank_count[i]));
                    }
                }
            }
            __syncwarp();

            // Issue expert count writes to peers (NVLink per-element)
            for (int i = thread_idx; i < num_experts;
                 i += kNumNotifyThreads) {
                const auto idx =
                    num_experts_per_rank * rank_idx +
                    (i % num_experts_per_rank);
                const auto dst_expert_counter =
                    workspace_layout.get_expert_count_ptr<false>() + idx;
                const auto dst_rank = i / num_experts_per_rank;
                if (dst_rank == rank_idx) {
                    mc_st_release(
                        reinterpret_cast<int*>(dst_expert_counter),
                        static_cast<int>(expert_count[i]));
                } else {
                    void* peer_ptr = mc_route_put(
                        comm_ctx, dst_rank,
                        const_cast<int*>(dst_expert_counter));
                    if (peer_ptr != nullptr) {
                        mc_st_release(static_cast<int*>(peer_ptr),
                                      static_cast<int>(expert_count[i]));
                    } else {
                        mc_signal(comm_ctx, dst_rank, 0, 1,
                                  const_cast<int*>(dst_expert_counter),
                                  static_cast<int>(expert_count[i]));
                    }
                }
            }

            mc_bar_sync(kNotifyBarrierIndex, kNumNotifyThreads);

            // Wait for rank and expert count from peers
            const auto start_clock = clock64();
            for (int i = thread_idx; i < kNumRanks + num_experts;
                 i += kNumNotifyThreads) {
                barrier::timeout_while<kNumTimeoutCycles>(
                    [=](const bool& is_last_check) {
                        const auto count = static_cast<int>(
                            ld_volatile_global(
                                workspace_layout
                                    .get_rank_expert_count_ptr<false>() +
                                i));
                        const auto decoded =
                            encode_decode_positive(count);
                        if (is_decoded_positive_ready(decoded)) {
                            workspace_layout
                                .get_rank_expert_count_ptr<false>()[i] = 0;
                            rank_expert_count[i] = decoded;
                            return true;
                        }

                        if (is_last_check)
                            printf(
                                "Mooncake EP notify timeout, rank: %d, "
                                "thread: %d, count: %d\n",
                                rank_idx, i, decoded);
                        return false;
                    },
                    start_clock);
            }
            mc_bar_sync(kNotifyBarrierIndex, kNumNotifyThreads);

            // Reduce expert count and add stats
            for (int i = thread_idx; i < num_experts_per_rank;
                 i += kNumNotifyThreads) {
                int sum = 0;
#pragma unroll
                for (int j = 0; j < kNumRanks; ++j)
                    sum += expert_count[j * num_experts_per_rank + i];
                expert_count[i] = align(sum, kExpertAlignment);

                if (cumulative_local_expert_recv_stats != nullptr)
                    atomicAdd(cumulative_local_expert_recv_stats + i, sum);
            }
            mc_bar_sync(kNotifyBarrierIndex, kNumNotifyThreads);

            // Prefix sum by warps
            const auto do_psum = [=](const int* count, int* out, const int n,
                                     const int is_exclusive) {
                int psum = 0;
#pragma unroll
                for (int i = 0; i < cell_div(n + is_exclusive, 32); ++i) {
                    const auto idx = i * 32 + lane_idx;
                    const auto mem_idx = idx - is_exclusive;
                    const auto value =
                        (0 <= mem_idx && mem_idx < n) ? count[mem_idx] : 0;
                    const auto sum =
                        psum + warp_inclusive_sum(value, lane_idx);

                    if (idx < n + is_exclusive) out[idx] = sum;
                    psum = exchange(sum, 31);
                }
            };
            if (warp_idx == 0) {
                do_psum(rank_count, psum_num_recv_tokens_per_rank, kNumRanks,
                        0);
            } else if (warp_idx == 1) {
                do_psum(expert_count, psum_num_recv_tokens_per_expert,
                        num_experts_per_rank, 1);
            }
        }
    }
    // =====================================================================
    // Dispatch warps: copy data to remote buffers
    // =====================================================================
    else {
        const int dispatch_warp_idx = warp_idx - kNumNotifyWarps;

        // Buffer layouts
        const auto token_layout = layout::TokenLayout(
            kNumHiddenBytes, 0, num_topk, true);
        const auto smem_buffer = layout::BufferLayout(
            token_layout, kNumDispatchWarps, 1,
            advance_ptr<int>(smem, num_smem_bytes_for_notify));
        const auto tma_buffer =
            smem_buffer.get_rank_buffer(dispatch_warp_idx)
                .get_token_buffer(0);
        auto recv_buffer = layout::BufferLayout(
            token_layout, kNumRanks, num_max_tokens_per_rank, buffer);
        auto send_buffer = layout::BufferLayout(
            token_layout, 1, num_max_tokens_per_rank,
            recv_buffer.get_buffer_end_ptr());
        recv_buffer = recv_buffer.get_rank_buffer(rank_idx);

        // Iterate all tokens
        const auto token_start = dispatch_warp_idx * num_sms + sm_idx;
        const auto token_stride = kNumDispatchWarps * num_sms;
        for (int token_idx = token_start; token_idx < num_tokens;
             token_idx += token_stride) {
            const auto token_i64_idx = static_cast<int64_t>(token_idx);

            // Load hidden data into shared memory (warp-cooperative)
            {
                const auto src_ptr = reinterpret_cast<const int4*>(
                    advance_ptr(x, token_i64_idx * kNumHiddenBytes));
                const auto dst_ptr =
                    reinterpret_cast<int4*>(tma_buffer.get_hidden_ptr());
                const int num_int4 = kNumHiddenBytes / sizeof(int4);
                UNROLLED_WARP_COPY(4, lane_idx, num_int4, dst_ptr, src_ptr,
                                   mc_ld_nc, mc_st_na);
            }
            __syncwarp();

            // Load top-k indices and weights
            EP_DEVICE_ASSERT(num_topk <= 32);
            int stored_dst_rank_idx = -1;
            if (lane_idx < num_topk) {
                const auto uncasted_dst_expert_idx =
                    __ldg(topk_idx + token_idx * num_topk + lane_idx);
                const auto dst_expert_idx =
                    static_cast<int>(uncasted_dst_expert_idx);
                stored_dst_rank_idx =
                    dst_expert_idx >= 0
                        ? dst_expert_idx / num_experts_per_rank
                        : -1;
                tma_buffer.get_topk_idx_ptr()[lane_idx] = dst_expert_idx;
                if (topk_weights != nullptr)
                    tma_buffer.get_topk_weights_ptr()[lane_idx] =
                        __ldg(topk_weights +
                              token_idx * num_topk + lane_idx);
                if (copied_topk_idx != nullptr)
                    copied_topk_idx[token_idx * num_topk + lane_idx] =
                        uncasted_dst_expert_idx;
            }
            __syncwarp();

            // Add source metadata
            if (elect_one_sync())
                *tma_buffer.get_src_token_global_idx_ptr() =
                    rank_idx * num_max_tokens_per_rank + token_idx;
            __syncwarp();

            // Deduplicate ranks and assign slots
            int stored_dst_slot_idx = -1;
            if constexpr (kReuseSlotIndices) {
                if (lane_idx < num_topk)
                    stored_dst_slot_idx =
                        __ldg(dst_buffer_slot_idx +
                              token_idx * num_topk + lane_idx);
                stored_dst_slot_idx =
                    stored_dst_slot_idx >= 0
                        ? (stored_dst_slot_idx -
                           rank_idx * num_max_tokens_per_rank)
                        : -1;
            } else {
                if (deduplicate(stored_dst_rank_idx, lane_idx) &&
                    stored_dst_rank_idx >= 0)
                    stored_dst_slot_idx = atomicAdd(
                        workspace_layout.get_atomic_sender_counter() +
                            stored_dst_rank_idx,
                        1);
                if (lane_idx < num_topk) {
                    const auto value =
                        stored_dst_slot_idx >= 0
                            ? rank_idx * num_max_tokens_per_rank +
                                  stored_dst_slot_idx
                            : -1;
                    dst_buffer_slot_idx[token_idx * num_topk + lane_idx] =
                        value;
                }
            }
            __syncwarp();

            // Copy to remote buffer
            if (stored_dst_slot_idx >= 0) {
                const auto dst_ptr = recv_buffer
                                         .get_token_buffer(
                                             stored_dst_slot_idx)
                                         .get_base_ptr();
                void* write_dst =
                    mc_route_put(comm_ctx, stored_dst_rank_idx, dst_ptr);
                if (write_dst != nullptr) {
                    // P2P path: warp-cooperative copy
                    const auto src_int4_ptr =
                        reinterpret_cast<const int4*>(
                            tma_buffer.get_base_ptr());
                    const auto dst_int4_ptr =
                        reinterpret_cast<int4*>(write_dst);
                    const int num_int4 =
                        token_layout.get_num_bytes() / sizeof(int4);
                    UNROLLED_WARP_COPY(4, lane_idx, num_int4, dst_int4_ptr,
                                       src_int4_ptr, mc_ld_nc, mc_st_na);
                } else {
                    // RDMA path: copy to send buffer then issue RDMA
                    auto send_buffer_ptr =
                        send_buffer.get_token_buffer(token_idx)
                            .get_base_ptr();
                    const auto src_int4_ptr =
                        reinterpret_cast<const int4*>(
                            tma_buffer.get_base_ptr());
                    const auto buf_int4_ptr =
                        reinterpret_cast<int4*>(send_buffer_ptr);
                    const int num_int4 =
                        token_layout.get_num_bytes() / sizeof(int4);
                    UNROLLED_WARP_COPY(4, lane_idx, num_int4, buf_int4_ptr,
                                       src_int4_ptr, mc_ld_nc, mc_st_na);
                    __syncwarp();
                    mc_rdma_put(comm_ctx,
                                stored_dst_rank_idx % num_qp_per_rank,
                                stored_dst_rank_idx, num_qp_per_rank,
                                send_buffer_ptr, dst_ptr,
                                token_layout.get_num_bytes(), lane_idx);
                }
            }
        }
    }

    // Barrier to ensure data arrival
    barrier::gpu_barrier<kNumRanks, kNumThreads, kNumTimeoutCycles,
                         EP_BARRIER_TAG_DISPATCH_1, true, true>(
        comm_ctx, workspace_layout, rank_idx, sm_idx, thread_idx, num_sms);

    // Clean atomic counters
    EP_STATIC_ASSERT(kNumRanks <= kNumThreads, "Insufficient threads");
    if (!kReuseSlotIndices && sm_idx == 0 && thread_idx < kNumRanks)
        workspace_layout.get_atomic_sender_counter()[thread_idx] = 0;
}

}  // namespace elastic_dispatch
}  // namespace mooncake
