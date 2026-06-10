// Combine reduction utilities — adapted from DeepEP V2 combine_utils.cuh.
//
// Provides:
//   CombineVecTraits     — vector type selection by hidden size
//   compute_topk_slots   — sort valid top-k indices to front
//   combine_reduce       — core reduction: read from multiple source buffers,
//                          reduce (BF16 hadd or FP32 accumulate), write to
//                          shared memory
//
// Differences from DeepEP V2:
//   - No TMA waits (replaced with __syncwarp).
//   - No longlong4_t (SM100+ only; we target SM90 with int4).
//   - ptx::ldg → __ldg, ptx::ldg_with_gez_pred → mooncake::ldg_with_gez_pred.
//   - ptx::accumulate → mooncake::accumulate.
#pragma once

#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_exception.cuh>
#include <mooncake_ep_utils.cuh>

namespace mooncake {
namespace combine {

// ---------------------------------------------------------------------------
// Layout helpers
// ---------------------------------------------------------------------------

template <bool kAllowMultipleReduction, int kNumRanks, int kNumTopk>
constexpr bool use_rank_layout() {
    if constexpr (!kAllowMultipleReduction) return false;
    return kNumRanks <= kNumTopk;
}

template <bool kAllowMultipleReduction, int kNumRanks, int kNumTopk>
constexpr int get_num_tokens_in_layout() {
    return use_rank_layout<kAllowMultipleReduction, kNumRanks, kNumTopk>()
               ? kNumRanks
               : kNumTopk;
}

// ---------------------------------------------------------------------------
// CombineVecTraits — vector type for combine loads/stores
// ---------------------------------------------------------------------------

template <int kHiddenBytes>
struct CombineVecTraits {
    // On SM90 (Hopper), use int4 (16 bytes).  SM100+ would use longlong4_t.
    using vec_t = int4;
};

// ---------------------------------------------------------------------------
// compute_topk_slots — sort valid top-k indices to the front
// ---------------------------------------------------------------------------

template <int kNumValidTopk, typename fetch_func_t>
__device__ __forceinline__ void compute_topk_slots(
    int (&topk_slot_idx)[kNumValidTopk], uint32_t mask,
    const fetch_func_t& fetch_func) {
#pragma unroll
    for (int k = 0; k < kNumValidTopk; ++k) {
        const int lowest_idx = __ffs(mask) - 1;
        const auto fetched = fetch_func(lowest_idx);
        mask &= mask - 1;
        topk_slot_idx[k] = lowest_idx >= 0 ? fetched : -1;
    }
}

// ---------------------------------------------------------------------------
// combine_reduce — core BF16 reduction kernel
//
// Reads from multiple source buffers (one per valid top-k slot), reduces
// using BF16 hadd (fast path for 1-2 sources) or FP32 accumulate (general
// path), and writes results to shared memory.
// ---------------------------------------------------------------------------

template <int kHiddenVec, int kUnrollFactor, int kNumValidTopk,
          typename vec_t,
          typename get_src_buffer_ptr_func_t,
          typename wait_buffer_func_t>
__device__ __forceinline__ void combine_reduce(
    const int& lane_idx, int (&topk_slot_idx)[kNumValidTopk],
    vec_t* dst_buffer_ptr,
    const get_src_buffer_ptr_func_t& get_src_buffer_ptr_func,
    const wait_buffer_func_t& wait_buffer_func,
    int num_expected_topk,
    vec_t* bias_0 = nullptr, vec_t* bias_1 = nullptr) {
    constexpr int kNumElemsPerVec = sizeof(vec_t) / sizeof(nv_bfloat16);
    EP_STATIC_ASSERT(kNumElemsPerVec % 2 == 0, "Invalid number of elements");
    EP_STATIC_ASSERT(kHiddenVec % (kUnrollFactor * 32) == 0,
                     "Invalid unrolling");

    // Use BF16 hadd fast path when: no bias AND at most 2 sources
    const bool enable_hadd_bypass =
        (bias_0 == nullptr && bias_1 == nullptr) &&
        (kNumValidTopk <= 2 || topk_slot_idx[2] < 0);
    EP_STATIC_ASSERT(kNumValidTopk > 0, "Invalid top-k");

    if (enable_hadd_bypass) {
#pragma unroll 1
        for (int i = 0; i < kHiddenVec / (kUnrollFactor * 32); ++i) {
            // Read values from slot 0
            const auto slot_0 = topk_slot_idx[0];
            const auto src_base_ptr_0 = get_src_buffer_ptr_func(slot_0);
            vec_t values_0[kUnrollFactor] = {};
#pragma unroll
            for (int j = 0; j < kUnrollFactor; ++j) {
                values_0[j] = ldg_with_gez_pred(
                    src_base_ptr_0 +
                        (i * (kUnrollFactor * 32) + j * 32 + lane_idx),
                    slot_0);
            }

            // Read values from slot 1
            vec_t values_1[kUnrollFactor] = {};
            const auto slot_1 =
                kNumValidTopk == 1 ? -1 : topk_slot_idx[1];
            const auto src_base_ptr_1 = get_src_buffer_ptr_func(slot_1);
#pragma unroll
            for (int j = 0; j < kUnrollFactor; ++j) {
                values_1[j] = ldg_with_gez_pred(
                    src_base_ptr_1 +
                        (i * (kUnrollFactor * 32) + j * 32 + lane_idx),
                    slot_1);
            }

            // Wait buffer releases before first write
            if (i == 0) wait_buffer_func();

            // BF16 hadd reduction into shared memory
            const auto bf162_view_0 =
                reinterpret_cast<__nv_bfloat162*>(values_0);
            const auto bf162_view_1 =
                reinterpret_cast<__nv_bfloat162*>(values_1);
#pragma unroll
            for (int j = 0; j < kUnrollFactor; ++j) {
#pragma unroll
                for (int l = 0; l < kNumElemsPerVec / 2; ++l)
                    bf162_view_0[j * (kNumElemsPerVec / 2) + l] +=
                        bf162_view_1[j * (kNumElemsPerVec / 2) + l];
                dst_buffer_ptr[i * (kUnrollFactor * 32) + j * 32 + lane_idx] =
                    values_0[j];
            }
        }
    } else {
#pragma unroll 1
        for (int i = 0; i < kHiddenVec / (kUnrollFactor * 32); ++i) {
            // FP32 accumulate buffer
            float2 reduced[kUnrollFactor * kNumElemsPerVec / 2] = {};

            // Add bias
            const auto add_bias = [&](const vec_t* base_ptr) {
                vec_t values[kUnrollFactor];
#pragma unroll
                for (int j = 0; j < kUnrollFactor; ++j)
                    values[j] = __ldg(base_ptr +
                                      i * (kUnrollFactor * 32) + j * 32 +
                                      lane_idx);

                const auto bf162_view =
                    reinterpret_cast<__nv_bfloat162*>(values);
#pragma unroll
                for (int j = 0; j < kUnrollFactor * kNumElemsPerVec / 2; ++j)
                    accumulate(reduced[j], bf162_view[j]);
            };
            if (bias_0 != nullptr) add_bias(bias_0);
            if (bias_1 != nullptr) add_bias(bias_1);

            // Reduce from each valid top-k slot
#pragma unroll
            for (int k = 0; k < kNumValidTopk; ++k) {
                if (k >= num_expected_topk && topk_slot_idx[k] < 0) break;

                const auto src_base_ptr =
                    get_src_buffer_ptr_func(topk_slot_idx[k]);
                vec_t values[kUnrollFactor] = {};
#pragma unroll
                for (int j = 0; j < kUnrollFactor; ++j) {
                    values[j] = ldg_with_gez_pred(
                        src_base_ptr +
                            (i * (kUnrollFactor * 32) + j * 32 + lane_idx),
                        topk_slot_idx[k]);
                }

                const auto bf162_view =
                    reinterpret_cast<__nv_bfloat162*>(values);
#pragma unroll
                for (int j = 0; j < kUnrollFactor * kNumElemsPerVec / 2; ++j)
                    accumulate(reduced[j], bf162_view[j]);
            }

            // Wait buffer releases before first write
            if (i == 0) wait_buffer_func();

            // Cast FP32 → BF16 and write to shared memory
#pragma unroll
            for (int j = 0; j < kUnrollFactor; ++j) {
                vec_t casted_value;
                auto bf162_view =
                    reinterpret_cast<__nv_bfloat162*>(&casted_value);
#pragma unroll
                for (int l = 0; l < kNumElemsPerVec / 2; ++l)
                    bf162_view[l] = __float22bfloat162_rn(
                        reduced[j * (kNumElemsPerVec / 2) + l]);
                dst_buffer_ptr[i * (kUnrollFactor * 32) + j * 32 + lane_idx] =
                    casted_value;
            }
        }
    }
}

}  // namespace combine
}  // namespace mooncake
