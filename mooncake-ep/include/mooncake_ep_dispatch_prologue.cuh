// Deterministic dispatch prologue — adapted from DeepEP V2
// dispatch_deterministic_prologue.cuh.
//
// Pre-computes dst_buffer_slot_idx[num_tokens * num_topk] so that the main
// dispatch kernel can use deterministic slot assignments instead of atomicAdd.
//
// Algorithm:
//   1. Each warp scans its token region, counting tokens per rank.
//   2. Block-level reduction → global prefix sum via grid sync.
//   3. Second pass: assign deterministic slot indices.
//
// Differences from DeepEP V2:
//   - cooperative_groups::this_grid().sync() → mc_grid_sync().
//   - ptx::* → mooncake::* (match, deduplicate, exchange, gather, etc.).
//   - math::* → mooncake::* (advance_ptr, cell_div).
#pragma once

#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_exception.cuh>
#include <mooncake_ep_utils.cuh>
#include <transport/device/comm_device.cuh>

namespace mooncake {
namespace dispatch_prologue {

using mooncake::device::mc_grid_sync;

template <int kNumWarps, int kNumRanks,
          int kNumThreads = kNumWarps * 32>
__global__ void __launch_bounds__(kNumThreads, 1)
dispatch_deterministic_prologue_impl(
    const int64_t* topk_idx,
    int* rank_count_buffer,
    int* dst_buffer_slot_idx,
    int num_tokens,
    int num_topk,
    int num_experts,
    int num_max_tokens_per_rank,
    int rank_idx,
    int num_sms) {
    const int num_experts_per_rank = num_experts / kNumRanks;
    EP_DEVICE_ASSERT(num_experts % kNumRanks == 0);

    // Utils
    const auto sm_idx = static_cast<int>(blockIdx.x);
    const auto thread_idx = static_cast<int>(threadIdx.x);
    const auto warp_idx = thread_idx / 32;
    const auto lane_idx = get_lane_id();
    const auto global_warp_idx = sm_idx * kNumWarps + warp_idx;

    // Token region for this warp
    const auto num_tokens_per_warp =
        cell_div(num_tokens, num_sms * kNumWarps);
    const auto start_token_idx = global_warp_idx * num_tokens_per_warp;
    const auto end_token_idx =
        min(start_token_idx + num_tokens_per_warp, num_tokens);

    // Group configs: each warp handles kNumTokensPerGroup tokens concurrently
    const int num_tokens_per_group = 32 / num_topk;
    const auto token_idx_offset = lane_idx / num_topk;
    const unsigned token_mask =
        ((1u << num_topk) - 1) << (token_idx_offset * num_topk);
    EP_DEVICE_ASSERT(num_topk <= 32);

    // Shared memory for reduction
    extern __shared__ int8_t smem[];
    const auto rank_count_global_psum = advance_ptr<int>(smem, 0);
    const auto rank_count_warp_sum = advance_ptr<int>(
        rank_count_global_psum,
        (kNumRanks + warp_idx * kNumRanks) * sizeof(int));
    const auto rank_count_warp_psum = advance_ptr<int>(
        rank_count_warp_sum, kNumWarps * kNumRanks * sizeof(int));

    // Initialize to zero
    for (int i = thread_idx; i < kNumRanks * (1 + 2 * kNumWarps);
         i += kNumThreads)
        reinterpret_cast<int*>(smem)[i] = 0;
    __syncthreads();

    // Helper lambdas
    const auto map_expert_to_rank_idx = [&](int expert_idx) {
        return expert_idx >= 0 ? expert_idx / num_experts_per_rank : -1;
    };
    const auto is_unique = [&](int rank_val) {
        return ((match(rank_val) & token_mask) >> lane_idx) == 1;
    };
    const auto count_ones_before = [&](unsigned mask_val, int bit_idx) {
        return __popc(mask_val & ((1u << bit_idx) - 1));
    };
    const auto get_other_rank_count_warp_sum = [&](int other_warp_idx) {
        return advance_ptr<int>(
            rank_count_warp_sum,
            (other_warp_idx - warp_idx) * kNumRanks * sizeof(int));
    };

    // --- Pass 1: each warp scans tokens, counts per rank ---
    for (int i = start_token_idx; i < end_token_idx;
         i += num_tokens_per_group) {
        const auto token_idx = i + token_idx_offset;
        const auto is_active =
            lane_idx < num_topk * num_tokens_per_group &&
            token_idx < end_token_idx;
        const int expert_idx =
            is_active
                ? static_cast<int>(__ldg(topk_idx + i * num_topk + lane_idx))
                : -1;
        const auto rank_val = map_expert_to_rank_idx(expert_idx);

        // Deduplicate: avoid duplicate messages to same rank
        const auto deduped_rank = is_unique(rank_val) ? rank_val : -1;
        const auto rank_mask = match(deduped_rank);

        // Largest lane index sends the count
        if ((rank_mask >> lane_idx) == 1 && deduped_rank >= 0)
            rank_count_warp_sum[deduped_rank] += __popc(rank_mask);
    }
    __syncthreads();

    // --- Block sum → global ---
    for (int r = thread_idx; r < kNumRanks; r += kNumThreads) {
        int block_sum = 0;
        for (int i = 0; i < kNumWarps; i++)
            block_sum += get_other_rank_count_warp_sum(i)[r];
        rank_count_buffer[sm_idx * kNumRanks + r] = block_sum;
    }
    mc_grid_sync();

    // --- Prefix sum before current SM ---
    for (int r = lane_idx; r < kNumRanks; r += 32) {
        int count = 0;
        for (int i = warp_idx; i < sm_idx; i += kNumWarps)
            count += rank_count_buffer[i * kNumRanks + r];
        atomicAdd_block(rank_count_global_psum + r, count);
    }
    __syncthreads();

    // --- Each warp's prefix sum ---
    for (int r = lane_idx; r < kNumRanks; r += 32) {
        int count = rank_count_global_psum[r];
        for (int i = 0; i < warp_idx; i++)
            count += get_other_rank_count_warp_sum(i)[r];
        rank_count_warp_psum[r] = count;
    }
    __syncwarp();

    // --- Pass 2: assign deterministic slot indices ---
    for (int i = start_token_idx; i < end_token_idx;
         i += num_tokens_per_group) {
        const auto token_idx = i + token_idx_offset;
        const auto is_active =
            lane_idx < num_topk * num_tokens_per_group &&
            token_idx < end_token_idx;
        const int expert_idx =
            is_active
                ? static_cast<int>(__ldg(topk_idx + i * num_topk + lane_idx))
                : -1;
        const auto rank_val = map_expert_to_rank_idx(expert_idx);

        const auto deduped_rank = is_unique(rank_val) ? rank_val : -1;
        const auto rank_mask = match(deduped_rank);

        // Store slot index
        const auto stored_slot =
            deduped_rank >= 0
                ? rank_count_warp_psum[deduped_rank] +
                      count_ones_before(rank_mask, lane_idx)
                : -1;
        const auto value =
            stored_slot >= 0
                ? rank_idx * num_max_tokens_per_rank + stored_slot
                : -1;
        if (is_active) dst_buffer_slot_idx[i * num_topk + lane_idx] = value;

        // Advance prefix sum
        if ((rank_mask >> lane_idx) == 1 && deduped_rank >= 0)
            rank_count_warp_psum[deduped_rank] += __popc(rank_mask);
        __syncwarp();
    }
}

}  // namespace dispatch_prologue
}  // namespace mooncake
