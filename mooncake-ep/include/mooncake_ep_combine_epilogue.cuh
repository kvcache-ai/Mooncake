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
        if (lane_idx < num_topk && combined_topk_idx != nullptr) {
            stored_dst_expert_idx = static_cast<int>(
                combined_topk_idx[token_idx * num_topk + lane_idx]);
            stored_dst_rank_idx =
                stored_dst_expert_idx >= 0
                    ? stored_dst_expert_idx / num_experts_per_rank
                    : -1;
        }
        __syncwarp();

        // Determine deduplication strategy.
        // Deduplication is only needed when using rank layout (where
        // multiple topk slots map to the same rank buffer entry).  When
        // using topk layout, each lane maps to a separate buffer slot.
        const auto [should_deduplicate, deduplicate_key] =
            [&]() -> std::pair<bool, int> {
            if constexpr (kUseExpandedLayout &&
                          !kAllowMultipleReduction) {
                return {false, 0};
            } else if (!use_rank_layout_val) {
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

        // Weighted reduce on the source rank.  The expert rank does not have
        // the source token's top-k weights, so elastic_combine only returns raw
        // expert outputs.  Apply local topk_weights here while reducing the
        // top-k slots for each original token.
        const auto dst_ptr = reinterpret_cast<int4*>(
            output_buffer.get_token_buffer(token_idx).get_base_ptr());
        const int num_int4 = kNumHiddenBytes / sizeof(int4);
        for (int vec_idx = lane_idx; vec_idx < num_int4; vec_idx += 32) {
            float accum[sizeof(int4) / sizeof(nv_bfloat16)] = {0.0f};
#pragma unroll
            for (int k = 0; k < EP_NUM_MAX_RANKS; ++k) {
                if (k >= num_tokens_in_layout) break;
                const int slot_idx = topk_slot_idx[k];
                if (slot_idx < 0) continue;
                const auto src_ptr = reinterpret_cast<const int4*>(
                    comm_buffer.get_rank_buffer(slot_idx)
                        .get_token_buffer(token_idx)
                        .get_base_ptr());
                const int4 value = mc_ld_nc(src_ptr + vec_idx);
                const auto value_bf16 =
                    reinterpret_cast<const nv_bfloat16*>(&value);
                const float weight =
                    combined_topk_weights == nullptr
                        ? 1.0f
                        : __ldg(combined_topk_weights +
                                static_cast<int64_t>(token_idx) * num_topk +
                                slot_idx);
#pragma unroll
                for (int e = 0; e < sizeof(int4) / sizeof(nv_bfloat16); ++e)
                    accum[e] += __bfloat162float(value_bf16[e]) * weight;
            }

            int4 out_value;
            auto out_bf16 = reinterpret_cast<nv_bfloat16*>(&out_value);
#pragma unroll
            for (int e = 0; e < sizeof(int4) / sizeof(nv_bfloat16); ++e)
                out_bf16[e] = __float2bfloat16(accum[e]);
            mc_st_na(dst_ptr + vec_idx, out_value);
        }
        __syncwarp();

        // `combined_topk_weights` is used as input weights in this port.
    }
}

}  // namespace combine_epilogue
}  // namespace mooncake
