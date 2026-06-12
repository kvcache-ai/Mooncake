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
using mooncake::device::mc_ld_generic;
using mooncake::device::mc_st_generic;

template <bool kDoExpand, bool kUseFP8, int kNumWarps, int kNumRanks,
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
    // The elastic dispatch communication buffer always stores BF16 hidden
    // data plus metadata.  `num_sf_packs` here describes the optional FP8
    // output scale tensor, not extra bytes inside the communication token.
    const auto token_layout = layout::TokenLayout(
        kNumHiddenBytes, 0, num_topk, true);
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
                               mc_ld_nc, mc_st_generic);
        }
        __syncwarp();

        // Read target expert indices from shared memory
        EP_DEVICE_ASSERT(num_topk <= 32);
        int dst_expert_idx = -1;
        if (lane_idx < num_topk) {
            const auto smem_token = layout::TokenLayout(
                kNumHiddenBytes, 0, num_topk, true, warp_smem);
            dst_expert_idx = smem_token.get_topk_idx_ptr()[lane_idx];
        }
        __syncwarp();

        // Save global expert index before remapping
        const int global_dst_expert_idx = dst_expert_idx;
        const auto in_range =
            expert_start_idx <= dst_expert_idx &&
            dst_expert_idx < expert_end_idx;
        const auto master_src_topk_idx =
            get_master_lane_idx(gather(in_range));
        const int local_expert_idx =
            in_range ? dst_expert_idx - expert_start_idx : -1;
        dst_expert_idx = local_expert_idx;
        // TODO: re-enable after verifying dispatch kernel correctly
        // deduplicates by expert (not just by rank).
        // EP_DEVICE_ASSERT(deduplicate(dst_expert_idx, lane_idx) ||
        //                  dst_expert_idx == -1);
        if (!kDoExpand && lane_idx < num_topk && recv_topk_idx != nullptr)
            recv_topk_idx[i * num_topk + lane_idx] =
                static_cast<int64_t>(in_range ? global_dst_expert_idx : -1);
        __syncwarp();

        // Calculate target tensor indices (positions within expert buffers).
        // A single received token may target multiple local experts when
        // several top-k entries map to this rank.  Each valid lane allocates
        // one expert-local slot; below we iterate those lanes and perform one
        // full-warp cooperative copy per destination.
        int dst_tensor_idx = -1;
        if (dst_expert_idx >= 0) {
            dst_tensor_idx =
                atomicAdd(psum_num_recv_tokens_per_expert + dst_expert_idx,
                          1);
        }
        unsigned copy_mask = gather(dst_tensor_idx >= 0);
        __syncwarp();

        // Copy hidden data from shared memory to output (packed layout), once
        // for every local expert selected by this token.
        while (copy_mask) {
            const int copy_lane = __ffs(copy_mask) - 1;
            const int copy_dst_expert_idx = __shfl_sync(
                0xffffffff, dst_expert_idx, copy_lane);
            const int copy_dst_tensor_idx = __shfl_sync(
                0xffffffff, dst_tensor_idx, copy_lane);
            const auto smem_token = layout::TokenLayout(
                kNumHiddenBytes, 0, num_topk, true, warp_smem);
            const auto src_hidden =
                static_cast<const int4*>(smem_token.get_hidden_ptr());
            // Packed layout: [num_local_experts][num_ranks * max_tokens][hidden]
            const int64_t expert_stride =
                static_cast<int64_t>(num_max_tokens_per_rank) * kNumRanks;
            const int64_t flat_output_idx =
                static_cast<int64_t>(copy_dst_expert_idx) * expert_stride +
                static_cast<int64_t>(copy_dst_tensor_idx);
            if constexpr (!kUseFP8) {
                const auto dst_hidden = reinterpret_cast<int4*>(
                    advance_ptr(recv_x, flat_output_idx * kNumHiddenBytes));
                const int num_hidden_int4 =
                    kNumHiddenBytes / sizeof(int4);
                UNROLLED_WARP_COPY(4, lane_idx, num_hidden_int4, dst_hidden,
                                   src_hidden, mc_ld_generic, mc_st_na);
            } else {
                constexpr int kNumElemsPerInt4 =
                    sizeof(int4) / sizeof(nv_bfloat16);
                constexpr int kNumElemsPerFP8Pack =
                    sizeof(int2) / sizeof(__nv_fp8_storage_t);
                EP_STATIC_ASSERT(kNumElemsPerInt4 == kNumElemsPerFP8Pack,
                                 "Invalid FP8 pack size");
                constexpr int kHiddenElems =
                    kNumHiddenBytes / sizeof(nv_bfloat16);
                EP_STATIC_ASSERT(kHiddenElems % 128 == 0,
                                 "FP8 requires 128-wide scale groups");
                const int num_hidden_int4 = kNumHiddenBytes / sizeof(int4);
                const auto dst_fp8 = reinterpret_cast<int2*>(
                    advance_ptr(recv_x,
                                flat_output_idx *
                                    static_cast<int64_t>(kHiddenElems) *
                                    sizeof(__nv_fp8_storage_t)));
                const int total_tokens = num_max_tokens_per_rank * kNumRanks;
                auto* dst_scales = advance_ptr<float>(
                    recv_sf,
                    (static_cast<int64_t>(copy_dst_expert_idx) * total_tokens *
                         num_sf_packs +
                     copy_dst_tensor_idx) *
                        sizeof(float));
                for (int vec_idx = lane_idx; vec_idx < num_hidden_int4;
                     vec_idx += 32) {
                    const int4 src_value = mc_ld_generic(src_hidden + vec_idx);
                    const auto src_bf16 =
                        reinterpret_cast<const nv_bfloat16*>(&src_value);
                    float fp32_values[kNumElemsPerInt4];
                    float amax = 1e-4f;
#pragma unroll
                    for (int e = 0; e < kNumElemsPerInt4; ++e) {
                        fp32_values[e] = __bfloat162float(src_bf16[e]);
                        amax = fmaxf(amax, fabsf(fp32_values[e]));
                    }
                    amax = half_warp_reduce_max(amax);
                    const float scale = 448.0f / amax;
                    if ((lane_idx & 15) == 0) {
                        const int scale_idx = vec_idx / 16;
                        dst_scales[static_cast<int64_t>(scale_idx) *
                                   total_tokens] = amax / 448.0f;
                    }

                    int2 packed;
                    auto fp8x2 =
                        reinterpret_cast<__nv_fp8x2_storage_t*>(&packed);
#pragma unroll
                    for (int e = 0; e < kNumElemsPerInt4; e += 2) {
                        const float2 fp32x2 = {fp32_values[e] * scale,
                                               fp32_values[e + 1] * scale};
                        fp8x2[e / 2] = __nv_cvt_float2_to_fp8x2(
                            fp32x2, __NV_SATFINITE, __NV_E4M3);
                    }
                    dst_fp8[vec_idx] = packed;
                }
            }

            // Write source metadata (packed layout, stride=2).
            // [0] = src_token_global_idx (rank * max_tokens + token_idx)
            // [1] = src_rank_topk_idx (rank * num_topk + topk_idx)
            if (elect_one_sync()) {
                const int64_t flat_idx =
                    static_cast<int64_t>(copy_dst_expert_idx) * expert_stride +
                    static_cast<int64_t>(copy_dst_tensor_idx);
                recv_src_metadata[flat_idx * 2 + 0] =
                    *smem_token.get_src_token_global_idx_ptr();
                recv_src_metadata[flat_idx * 2 + 1] =
                    current_rank_idx * num_topk + copy_lane;
            }
            copy_mask ^= 1u << copy_lane;
            __syncwarp();
        }
        __syncwarp();

        // FP8 scales are generated together with the FP8 hidden output above.

        // Copy top-k weights
        if (recv_topk_weights != nullptr && dst_tensor_idx >= 0) {
            const auto smem_token = layout::TokenLayout(
                kNumHiddenBytes, 0, num_topk, true, warp_smem);
            recv_topk_weights[dst_tensor_idx] =
                smem_token.get_topk_weights_ptr()[lane_idx];
        }
        __syncwarp();

        __syncwarp();
    }
}

}  // namespace dispatch_epilogue
}  // namespace mooncake
