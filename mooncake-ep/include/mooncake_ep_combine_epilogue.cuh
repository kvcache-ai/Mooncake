// Combine reduce epilogue — adapted from DeepEP V2 combine_reduce_epilogue.cuh.
//
// Post-combine reduction: reads from the communication buffer, reduces
// multiple contributions (from different top-k sources), and writes the
// final combined result to the output tensor.
//
// Differences from DeepEP V2:
//   - TMA load/store → UNROLLED_WARP_COPY with shared memory staging.
//   - No cudaGridDependencySynchronize (no PDL).
//   - No mbarrier.
//   - No scaleout ranks (kNumScaleoutRanks = 1).
//   - ptx::* → mooncake::*.
#pragma once

#include <mooncake_ep_combine_utils.cuh>
#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_exception.cuh>
#include <mooncake_ep_layout.cuh>
#include <mooncake_ep_utils.cuh>
#include <transport/device/comm_device.cuh>

namespace mooncake {
namespace combine_epilogue {

using mooncake::device::mc_ld_nc;
using mooncake::device::mc_st_na;

using mooncake::combine::CombineVecTraits;
using mooncake::combine::combine_reduce;
using mooncake::combine::compute_topk_slots;
using mooncake::combine::get_num_tokens_in_layout;
using mooncake::combine::use_rank_layout;

template <bool kUseExpandedLayout, bool kAllowMultipleReduction,
          int kNumWarps, int kNumRanks, int kHidden,
          int kNumThreads = kNumWarps * 32,
          int kNumHiddenBytes = kHidden * sizeof(nv_bfloat16)>
__global__ void __launch_bounds__(kNumThreads, 1)
combine_reduce_epilogue_impl(
    nv_bfloat16* combined_x, float* combined_topk_weights,
    int64_t* combined_topk_idx, void* recv_buffer, void* bias_0,
    void* bias_1, int num_combined_tokens,
    int num_topk,
    int num_experts,
    int num_max_tokens_per_rank,
    int rank_idx,
    int num_sms) {
    const int num_experts_per_rank = num_experts / kNumRanks;
    EP_DEVICE_ASSERT(num_experts % kNumRanks == 0);

    // Compute layout strategy at runtime
    const bool use_rank_layout_val =
        kAllowMultipleReduction ? (kNumRanks <= num_topk) : false;
    const int num_tokens_in_layout =
        use_rank_layout_val ? kNumRanks : num_topk;

    // Utils
    const auto sm_idx = static_cast<int>(blockIdx.x);
    const auto warp_idx = threadIdx.x / 32;
    const auto lane_idx = get_lane_id();
    const auto global_warp_idx =
        warp_idx * num_sms + sm_idx;

    // Buffer layouts
    extern __shared__ __align__(16) int8_t smem[];
    const auto comm_token_layout =
        layout::TokenLayout(kNumHiddenBytes, 0, num_topk, false);
    const auto comm_buffer = layout::BufferLayout(
        comm_token_layout, num_tokens_in_layout, num_max_tokens_per_rank,
        recv_buffer);

    // Output buffer
    const auto output_token_layout =
        layout::TokenLayout(kNumHiddenBytes, 0, 0, false);
    const auto output_buffer = layout::BufferLayout(
        output_token_layout, 1, num_combined_tokens, combined_x);

    // Shared memory staging (one per warp)
    const auto smem_per_warp =
        align<int>(kNumHiddenBytes, 16);
    auto warp_smem = smem + warp_idx * smem_per_warp;

    // Bias buffers
    const auto bias_0_buffer = layout::BufferLayout(
        output_token_layout, 1, num_combined_tokens, bias_0);
    const auto bias_1_buffer = layout::BufferLayout(
        output_token_layout, 1, num_combined_tokens, bias_1);

    // Read from buffers and reduce
    for (int token_idx = global_warp_idx; token_idx < num_combined_tokens;
         token_idx += kNumWarps * num_sms) {
        // Preprocess top-k indices
        int stored_dst_rank_idx = -1, stored_dst_expert_idx = -1;
        EP_DEVICE_ASSERT(num_topk <= 32);
        if (lane_idx < num_topk) {
            stored_dst_expert_idx = static_cast<int>(
                combined_topk_idx[token_idx * num_topk + lane_idx]);
            stored_dst_rank_idx =
                stored_dst_expert_idx >= 0
                    ? stored_dst_expert_idx / num_experts_per_rank
                    : -1;
        }
        __syncwarp();

        // Determine deduplication strategy
        const auto [should_deduplicate, deduplicate_key] =
            [&]() -> std::pair<bool, int> {
            if constexpr (kUseExpandedLayout &&
                          !kAllowMultipleReduction) {
                return {false, 0};
            } else {
                return {true, stored_dst_rank_idx};
            }
        }();

        auto reduce_valid_mask =
            should_deduplicate
                ? gather(deduplicate(deduplicate_key, lane_idx) &&
                         stored_dst_rank_idx >= 0)
                : gather(stored_dst_rank_idx >= 0);
        int topk_slot_idx[EP_NUM_MAX_RANKS];  // max(kNumRanks, num_topk) <= 32
        compute_topk_slots<EP_NUM_MAX_RANKS>(
            topk_slot_idx, reduce_valid_mask,
            [=](const int& idx) {
                return use_rank_layout_val
                           ? exchange(stored_dst_rank_idx, idx)
                           : idx;
            });

        // Reduce using combine_reduce
        using combine_vec_t =
            typename CombineVecTraits<kHidden * sizeof(nv_bfloat16)>::vec_t;
        constexpr int kHiddenVec =
            kHidden * sizeof(nv_bfloat16) / sizeof(combine_vec_t);
        constexpr int kUnrollFactor = 4;  // Fixed for SM90

        combine_reduce<kHiddenVec, kUnrollFactor, EP_NUM_MAX_RANKS>(
            lane_idx, topk_slot_idx,
            static_cast<combine_vec_t*>(static_cast<void*>(warp_smem)),
            /* Get source base */
            [=](const int& slot_idx) {
                return static_cast<combine_vec_t*>(
                    comm_buffer.get_rank_buffer(slot_idx)
                        .get_token_buffer(token_idx)
                        .get_base_ptr());
            },
            /* Wait buffer release */
            [=]() { __syncwarp(); },
            /* num_expected_topk */
            num_tokens_in_layout,
            /* Bias 0 */
            bias_0 == nullptr
                ? nullptr
                : static_cast<combine_vec_t*>(
                      bias_0_buffer.get_token_buffer(token_idx)
                          .get_base_ptr()),
            /* Bias 1 */
            bias_1 == nullptr
                ? nullptr
                : static_cast<combine_vec_t*>(
                      bias_1_buffer.get_token_buffer(token_idx)
                          .get_base_ptr()));
        __syncwarp();

        // Copy reduced result from shared memory to output
        {
            const auto src_ptr =
                reinterpret_cast<const int4*>(warp_smem);
            const auto dst_ptr = reinterpret_cast<int4*>(
                output_buffer.get_token_buffer(token_idx)
                    .get_base_ptr());
            const int num_int4 = kNumHiddenBytes / sizeof(int4);
            UNROLLED_WARP_COPY(4, lane_idx, num_int4, dst_ptr, src_ptr,
                               mc_ld_nc, mc_st_na);
        }
        __syncwarp();

        // Write top-k weights
        if (combined_topk_weights != nullptr) {
            const auto master_lane_idx =
                get_master_lane_idx(match(stored_dst_rank_idx));
            if (lane_idx < num_topk) {
                float value = 0;
                if (stored_dst_rank_idx >= 0) {
                    const auto dst_ptr =
                        comm_buffer
                            .get_rank_buffer(use_rank_layout_val
                                                 ? stored_dst_rank_idx
                                                 : master_lane_idx)
                            .get_token_buffer(token_idx)
                            .get_topk_weights_ptr() +
                        lane_idx;
                    value = *dst_ptr;
                }
                combined_topk_weights[token_idx * num_topk + lane_idx] =
                    value;
            }
            __syncwarp();
        }
    }
}

}  // namespace combine_epilogue
}  // namespace mooncake
