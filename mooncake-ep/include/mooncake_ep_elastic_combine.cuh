// Elastic combine kernel — adapted from DeepEP V2 combine.cuh.
//
// Reads reduced tokens from the local tensor, optionally does local reduction
// (when multiple top-k sources map to the same token), and writes results to
// remote buffers via Device API.
//
// Differences from DeepEP V2:
//   - No TMA → UNROLLED_WARP_COPY with shared memory staging.
//   - No GIN → Device API (mc_route_put, mc_rdma_put).
//   - No mbarrier → __syncwarp.
//   - No scaleout (single-node only, kIsScaleupNVLink = true).
//   - No team_t (ncclTeamTagLsa/ncclTeamTagWorld).
//   - No expanded layout / multiple reduction (kUseExpandedLayout=false,
//     kAllowMultipleReduction=false).
#pragma once

#include <mooncake_ep_barrier.cuh>
#include <mooncake_ep_combine_utils.cuh>
#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_exception.cuh>
#include <mooncake_ep_layout.cuh>
#include <mooncake_ep_utils.cuh>
#include <transport/device/comm_device.cuh>

namespace mooncake {
namespace elastic_combine {

using mooncake::device::CommCtx;
using mooncake::device::make_comm_ctx;
using mooncake::device::mc_route_put;
using mooncake::device::mc_rdma_put;
using mooncake::device::mc_bar_sync;
using mooncake::device::mc_grid_sync;
using mooncake::device::mc_ld_nc;
using mooncake::device::mc_st_na;
using mooncake::device::mc_ld_acquire;
using mooncake::device::mc_st_release;
using mooncake::device::mc_atomic_add_release;

template <int kNumWarps, int kNumRanks, int kHidden,
          int64_t kNumTimeoutCycles,
          int kNumThreads = kNumWarps * 32,
          int kNumHiddenBytes = kHidden * sizeof(nv_bfloat16)>
__global__ void __launch_bounds__(kNumThreads, 1)
combine_impl(
    nv_bfloat16* x, float* topk_weights, int* src_metadata,
    int* psum_num_recv_tokens_per_rank,
    void* mxa_buffer, void* raddrs, void* rkeys, void* qp_devctxs,
    const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
    int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
    void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
    void* buffer, void* workspace,
    int rank_idx, int num_ranks, int num_sms,
    int num_reduced_tokens,
    int num_topk,
    int num_experts,
    int num_max_tokens_per_rank) {
    // Utils
    const auto sm_idx = static_cast<int>(blockIdx.x);
    const auto thread_idx = static_cast<int>(threadIdx.x);
    const auto warp_idx = (thread_idx / 32 + rank_idx) % kNumWarps;
    const auto lane_idx = get_lane_id();
    const auto global_warp_idx = warp_idx * num_sms + sm_idx;

    // Read actual token count if without CPU sync
    if (num_reduced_tokens == num_max_tokens_per_rank * kNumRanks)
        num_reduced_tokens =
            __ldg(psum_num_recv_tokens_per_rank + kNumRanks - 1);

    // Buffer layouts
    extern __shared__ __align__(16) int8_t smem[];
    const auto token_layout =
        layout::TokenLayout(kNumHiddenBytes, 0, num_topk, false);
    const auto tma_buffer =
        layout::BufferLayout(token_layout, kNumWarps, 1, smem)
            .get_rank_buffer(warp_idx)
            .get_token_buffer(0);
    const auto recv_buffer = layout::BufferLayout(
        token_layout, num_topk, num_max_tokens_per_rank, buffer);
    const auto send_buffer = layout::BufferLayout(
        token_layout, kNumRanks, num_max_tokens_per_rank,
        recv_buffer.get_buffer_end_ptr());

    // Communication context
    const CommCtx comm_ctx = make_comm_ctx(
        mxa_buffer, nvlink_available, ipc_peer_ptrs, raddrs, rkeys,
        qp_devctxs, rdma_send_signal_buffer, rdma_recv_signal_buffer, rank_idx,
        num_ranks, MAX_QP_COUNT);
    const size_t num_qp_per_rank = MAX_QP_COUNT / num_ranks;

    // Workspace
    const auto workspace_layout =
        layout::WorkspaceLayout(workspace, num_ranks, num_experts);

    // Barrier to ensure remote buffer is available
    barrier::gpu_barrier<kNumRanks, kNumThreads, kNumTimeoutCycles,
                         EP_BARRIER_TAG_COMBINE_0, false, false>(
        comm_ctx, workspace_layout, rank_idx, sm_idx, thread_idx, num_sms);

    // Write into remote buffers
    int num_tokens_per_warp =
        cell_div(num_reduced_tokens, num_sms * kNumWarps);
    const int token_start_idx = num_tokens_per_warp * global_warp_idx;
    const int token_end_idx =
        min(token_start_idx + num_tokens_per_warp, num_reduced_tokens);
    for (int i = token_start_idx; i < token_end_idx; ++i) {
        // Read source metadata
        const int metadata_stride = 2 + num_topk;
        const int src_token_idx =
            __ldg(src_metadata + i * metadata_stride) % num_max_tokens_per_rank;
        const int src_rank_topk_idx =
            __ldg(src_metadata + i * metadata_stride + 1);
        const int src_rank_idx = src_rank_topk_idx / num_topk;
        const int src_topk_idx = src_rank_topk_idx % num_topk;

        // Determine destination: P2P or RDMA
        void* write_dst = mc_route_put(
            comm_ctx, src_rank_idx,
            recv_buffer.get_rank_buffer(src_topk_idx)
                .get_token_buffer(src_token_idx)
                .get_base_ptr());
        const bool nvlink_bypass = (write_dst != nullptr);

        // Master token buffer (destination)
        layout::TokenLayout master_token_buffer = [&]() {
            if (nvlink_bypass) {
                auto tb = recv_buffer.get_rank_buffer(src_topk_idx)
                              .get_token_buffer(src_token_idx);
                tb.set_base_ptr(write_dst);
                return tb;
            }
            return send_buffer.get_rank_buffer(src_rank_idx)
                .get_token_buffer(src_token_idx);
        }();

        // Load hidden data into shared memory
        {
            const auto src_ptr = reinterpret_cast<const int4*>(
                advance_ptr(x, static_cast<int64_t>(i) * kNumHiddenBytes));
            const auto dst_ptr =
                reinterpret_cast<int4*>(tma_buffer.get_base_ptr());
            const int num_int4 = kNumHiddenBytes / sizeof(int4);
            UNROLLED_WARP_COPY(4, lane_idx, num_int4, dst_ptr, src_ptr,
                               mc_ld_nc, mc_st_na);
        }
        __syncwarp();

        // Write to destination
        if (nvlink_bypass) {
            // P2P: direct warp-cooperative copy
            const auto src_int4_ptr =
                reinterpret_cast<const int4*>(tma_buffer.get_base_ptr());
            const auto dst_int4_ptr =
                reinterpret_cast<int4*>(master_token_buffer.get_base_ptr());
            const int num_int4 = kNumHiddenBytes / sizeof(int4);
            UNROLLED_WARP_COPY(4, lane_idx, num_int4, dst_int4_ptr,
                               src_int4_ptr, mc_ld_nc, mc_st_na);
        } else {
            // RDMA: copy to send buffer, then issue RDMA write
            const auto src_int4_ptr =
                reinterpret_cast<const int4*>(tma_buffer.get_base_ptr());
            const auto buf_int4_ptr =
                reinterpret_cast<int4*>(master_token_buffer.get_base_ptr());
            const int num_int4 = kNumHiddenBytes / sizeof(int4);
            UNROLLED_WARP_COPY(4, lane_idx, num_int4, buf_int4_ptr,
                               src_int4_ptr, mc_ld_nc, mc_st_na);
            __syncwarp();
            mc_rdma_put(
                comm_ctx, src_rank_idx % num_qp_per_rank, src_rank_idx,
                num_qp_per_rank, master_token_buffer.get_base_ptr(),
                recv_buffer.get_rank_buffer(src_topk_idx)
                    .get_token_buffer(src_token_idx)
                    .get_base_ptr(),
                kNumHiddenBytes, lane_idx);
        }

        // Write topk weights
        if (topk_weights != nullptr && lane_idx < num_topk) {
            const float value =
                __ldg(topk_weights + (i * num_topk + lane_idx));
            master_token_buffer.get_topk_weights_ptr()[lane_idx] = value;
        }
        __syncwarp();
    }

    // Final barrier to ensure data arrival
    barrier::gpu_barrier<kNumRanks, kNumThreads, kNumTimeoutCycles,
                         EP_BARRIER_TAG_COMBINE_1, true, true>(
        comm_ctx, workspace_layout, rank_idx, sm_idx, thread_idx, num_sms);
}

}  // namespace elastic_combine
}  // namespace mooncake
