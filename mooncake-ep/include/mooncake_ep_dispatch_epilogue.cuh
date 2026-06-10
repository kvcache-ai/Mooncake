// Dispatch copy epilogue — adapted from DeepEP V2 dispatch_copy_epilogue.cuh.
//
// Copies received tokens from the communication buffer to output tensors
// (recv_x, recv_sf, recv_topk_idx, recv_topk_weights, recv_src_metadata).
//
// Differences from DeepEP V2:
//   - TMA load/store → UNROLLED_WARP_COPY with shared memory staging.
//   - No cudaGridDependencySynchronize / cudaTriggerProgrammaticLaunchCompletion.
//   - No mbarrier (synchronous warp-cooperative copy).
//   - No channel/linked-list logic (single-node only).
//   - No scaleout ranks (kNumScaleoutRanks = 1).
#pragma once

#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_exception.cuh>
#include <mooncake_ep_layout.cuh>
#include <mooncake_ep_utils.cuh>
#include <transport/device/comm_device.cuh>

namespace mooncake {
namespace dispatch_epilogue {

using mooncake::device::mc_ld_nc;
using mooncake::device::mc_st_na;

template <bool kDoExpand, int kNumWarps, int kNumRanks,
          int kNumHiddenBytes,
          int kNumThreads = kNumWarps * 32>
__global__ void __launch_bounds__(kNumThreads, 1)
dispatch_copy_epilogue_impl(
    void* buffer, void* workspace,
    int* psum_num_recv_tokens_per_rank,
    int* psum_num_recv_tokens_per_expert,
    void* recv_x, void* recv_sf,
    int64_t* recv_topk_idx, float* recv_topk_weights,
    int* recv_src_metadata,
    int num_recv_tokens,
    int num_sf_packs,
    int recv_sf_token_stride, int recv_sf_hidden_stride,
    int num_topk,
    int num_experts,
    int num_max_tokens_per_rank,
    int rank_idx,
    int num_sms) {
    // Utils
    const auto sm_idx = static_cast<int>(blockIdx.x);
    const auto thread_idx = static_cast<int>(threadIdx.x);
    const auto warp_idx = thread_idx / 32;
    const auto lane_idx = get_lane_id();
    const auto global_warp_idx = warp_idx * num_sms + sm_idx;

    // Expert range for this rank
    const int num_experts_per_rank = num_experts / kNumRanks;
    const auto expert_start_idx = num_experts_per_rank * rank_idx;
    const auto expert_end_idx = num_experts_per_rank * (rank_idx + 1);

    // Buffer layouts
    extern __shared__ __align__(16) int8_t smem[];
    const auto token_layout = layout::TokenLayout(
        kNumHiddenBytes, num_sf_packs * sizeof(float), num_topk, true);
    const auto scaleup_buffer = layout::BufferLayout(
        token_layout, kNumRanks, num_max_tokens_per_rank, buffer);

    // Shared memory staging buffer (one per warp)
    const auto smem_per_warp =
        align<int>(token_layout.get_num_bytes(), 16);
    auto warp_smem = smem + warp_idx * smem_per_warp;

    // For no CPU sync case, read actual token count from GPU tensor
    if (num_recv_tokens == num_max_tokens_per_rank * kNumRanks)
        num_recv_tokens = psum_num_recv_tokens_per_rank[kNumRanks - 1];

    // Current rank tracking
    int current_rank_idx = -1, stored_psum_num_recv_tokens;
    int current_rank_start = 0, current_rank_end = 0;

#pragma unroll
    for (int i = global_warp_idx; i < num_recv_tokens;
         i += kNumWarps * num_sms) {
        // Find which rank this token belongs to
        while (i >= current_rank_end) {
            current_rank_idx += 1;
            EP_DEVICE_ASSERT(current_rank_idx < kNumRanks);
            const auto stored_lane_idx = current_rank_idx % 32;
            if (stored_lane_idx == 0 &&
                current_rank_idx + lane_idx < kNumRanks)
                stored_psum_num_recv_tokens =
                    psum_num_recv_tokens_per_rank[current_rank_idx +
                                                  lane_idx];
            current_rank_start = current_rank_end;
            current_rank_end =
                exchange(stored_psum_num_recv_tokens, stored_lane_idx);
        }
        const auto buffer_token =
            scaleup_buffer.get_rank_buffer(current_rank_idx)
                .get_token_buffer(i - current_rank_start);

        // Copy from buffer to shared memory (warp-cooperative)
        {
            const auto src_ptr =
                static_cast<const int4*>(buffer_token.get_base_ptr());
            const auto dst_ptr = reinterpret_cast<int4*>(warp_smem);
            const int num_int4 =
                token_layout.get_num_bytes() / sizeof(int4);
            UNROLLED_WARP_COPY(4, lane_idx, num_int4, dst_ptr, src_ptr,
                               mc_ld_nc, mc_st_na);
        }
        __syncwarp();

        // Read target expert indices from shared memory
        EP_DEVICE_ASSERT(num_topk <= 32);
        int dst_expert_idx = -1;
        if (lane_idx < num_topk) {
            const auto smem_token = layout::TokenLayout(
                kNumHiddenBytes, num_sf_packs * sizeof(float), num_topk,
                true, warp_smem);
            dst_expert_idx = smem_token.get_topk_idx_ptr()[lane_idx];
        }
        __syncwarp();

        // Validate and remap expert index
        const auto in_range =
            expert_start_idx <= dst_expert_idx &&
            dst_expert_idx < expert_end_idx;
        const auto master_src_topk_idx =
            get_master_lane_idx(gather(in_range));
        dst_expert_idx =
            in_range ? dst_expert_idx - expert_start_idx : -1;
        EP_DEVICE_ASSERT(deduplicate(dst_expert_idx, lane_idx) ||
                         dst_expert_idx == -1);
        if (!kDoExpand && lane_idx < num_topk)
            recv_topk_idx[i * num_topk + lane_idx] =
                static_cast<int64_t>(dst_expert_idx);
        __syncwarp();

        // Calculate target tensor index
        int dst_tensor_idx = -1;
        if (!kDoExpand && elect_one_sync()) {
            dst_tensor_idx = i;
        } else if (kDoExpand && dst_expert_idx >= 0) {
            dst_tensor_idx =
                atomicAdd(psum_num_recv_tokens_per_expert + dst_expert_idx,
                          1);
        }
        __syncwarp();

        // Copy hidden data from shared memory to output
        if (kDoExpand ? (dst_tensor_idx >= 0) : elect_one_sync()) {
            const auto smem_token = layout::TokenLayout(
                kNumHiddenBytes, num_sf_packs * sizeof(float), num_topk,
                true, warp_smem);
            const auto src_hidden =
                static_cast<const int4*>(smem_token.get_hidden_ptr());
            const auto dst_hidden = reinterpret_cast<int4*>(
                advance_ptr(recv_x,
                            static_cast<int64_t>(dst_tensor_idx) *
                                kNumHiddenBytes));
            const int num_hidden_int4 =
                kNumHiddenBytes / sizeof(int4);
            UNROLLED_WARP_COPY(4, lane_idx, num_hidden_int4, dst_hidden,
                               src_hidden, mc_ld_nc, mc_st_na);
        }
        __syncwarp();

        // Copy SF (scale factors)
        if (num_sf_packs > 0) {
            const auto smem_token = layout::TokenLayout(
                kNumHiddenBytes, num_sf_packs * sizeof(float), num_topk,
                true, warp_smem);
            const auto smem_sf =
                static_cast<const float*>(smem_token.get_sf_ptr());

            const auto recv_sf_token_stride_i64 =
                static_cast<int64_t>(recv_sf_token_stride);
            const auto recv_sf_hidden_stride_i64 =
                static_cast<int64_t>(recv_sf_hidden_stride);

            auto mask =
                kDoExpand ? gather(dst_tensor_idx >= 0) : 1u;
            while (mask) {
                const int valid_lane_idx = __ffs(mask) - 1;
                const auto gmem_dst = advance_ptr<float>(
                    recv_sf,
                    exchange(dst_tensor_idx, valid_lane_idx) *
                        (recv_sf_token_stride_i64 * sizeof(float)));
                for (int k = lane_idx; k < num_sf_packs; k += 32)
                    gmem_dst[k * recv_sf_hidden_stride_i64] =
                        smem_sf[k];
                mask ^= 1u << valid_lane_idx;
            }
        }

        // Copy top-k weights
        if (kDoExpand && recv_topk_weights != nullptr &&
            dst_tensor_idx >= 0) {
            const auto smem_token = layout::TokenLayout(
                kNumHiddenBytes, num_sf_packs * sizeof(float), num_topk,
                true, warp_smem);
            recv_topk_weights[dst_tensor_idx] =
                smem_token.get_topk_weights_ptr()[lane_idx];
        } else if (!kDoExpand && recv_topk_weights != nullptr &&
                   lane_idx < num_topk) {
            const auto smem_token = layout::TokenLayout(
                kNumHiddenBytes, num_sf_packs * sizeof(float), num_topk,
                true, warp_smem);
            recv_topk_weights[i * num_topk + lane_idx] =
                smem_token.get_topk_weights_ptr()[lane_idx];
        }
        __syncwarp();

        // Write source metadata
        const int metadata_stride = 2 + num_topk;
        if (elect_one_sync()) {
            const auto smem_token = layout::TokenLayout(
                kNumHiddenBytes, num_sf_packs * sizeof(float), num_topk,
                true, warp_smem);
            recv_src_metadata[i * metadata_stride + 0] =
                *smem_token.get_src_token_global_idx_ptr();
            recv_src_metadata[i * metadata_stride + 1] =
                current_rank_idx * num_topk + master_src_topk_idx;
        }
        __syncwarp();

        // Write reduction source indices (expand mode)
        if (kDoExpand && lane_idx < num_topk)
            recv_src_metadata[i * metadata_stride + 2 + lane_idx] =
                dst_tensor_idx;
        __syncwarp();
    }
}

}  // namespace dispatch_epilogue
}  // namespace mooncake
