// Buffer layout utilities — adapted from DeepEP V2 layout.cuh.
//
// WorkspaceLayout: fixed-layout control buffer (barrier counters, notify
//   reduction workspace, rank/expert count buffers, atomic sender counters).
// TokenLayout: per-token memory layout within communication buffers.
// BufferLayout: multi-rank multi-token buffer addressing.
//
// Differences from DeepEP V2:
//   - No mbarrier (synchronous warp-cooperative copy instead of TMA).
//   - No linked-list / channel / scaleout fields (single-node only).
//   - No kWithMBarrier template parameter.
//   - Adapted to Mooncake's NUM_WORKSPACE_BYTES (32 MiB).
#pragma once

#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_exception.cuh>
#include <mooncake_ep_utils.cuh>

namespace mooncake {
namespace layout {

// ===========================================================================
// WorkspaceLayout
// ===========================================================================

struct WorkspaceLayout {
    void* workspace;
    int num_ranks;
    int num_experts;
    int num_experts_per_rank;

    static constexpr int kNumMaxRanks = EP_NUM_MAX_RANKS;
    static constexpr int kNumMaxExperts = EP_NUM_MAX_EXPERTS;
    static constexpr int kNumMaxExpertsPerRank = EP_NUM_MAX_EXPERTS_PER_RANK;
    // Barrier storage:
    //   - uint64_t counter
    //   - two phases of per-source-rank int slots
    //
    // DeepEP V2 uses NCCL GIN symmetric memory plus remote RED atomics to
    // accumulate a single counter per phase.  Mooncake's Device API currently
    // routes P2P through CUDA IPC peer mappings, where ordinary stores work but
    // remote atomics/reductions are not a safe portability assumption.  Use one
    // slot per source rank so barrier signaling only needs release stores.
    static constexpr int64_t kNumBarrierSignalBytes =
        sizeof(uint64_t) + 2 * kNumMaxRanks * sizeof(int);

    __forceinline__ __device__ __host__ WorkspaceLayout(
        void* workspace, int num_ranks, int num_experts)
        : workspace(workspace),
          num_ranks(num_ranks),
          num_experts(num_experts) {
        num_experts_per_rank = num_experts / num_ranks;
        EP_DEVICE_ASSERT(num_experts % num_ranks == 0);
        EP_DEVICE_ASSERT(num_ranks <= kNumMaxRanks);
        EP_DEVICE_ASSERT(num_experts <= kNumMaxExperts);
        EP_DEVICE_ASSERT(num_experts_per_rank <= kNumMaxExpertsPerRank);
    }

    static int64_t get_num_bytes() {
        int64_t num_bytes = 0;

        // Barrier signals (16 bytes)
        num_bytes += kNumBarrierSignalBytes;

        // Notify reduction workspace: (max_ranks + max_experts) * sizeof(int64_t)
        num_bytes += (kNumMaxRanks + kNumMaxExperts) * sizeof(int64_t);

        // Rank/expert count send buffer: (max_ranks + max_experts) * sizeof(int64_t)
        num_bytes += (kNumMaxRanks + kNumMaxExperts) * sizeof(int64_t);

        // Rank/expert count recv buffer: (max_ranks + max_experts) * sizeof(int64_t)
        num_bytes += (kNumMaxRanks + kNumMaxExperts) * sizeof(int64_t);

        // Atomic sender counter: max_ranks * sizeof(int)
        num_bytes += kNumMaxRanks * sizeof(int);

        // Existing: atomic_counter_per_expert + atomic_finish_counter_per_expert
        // + atomic_clean_flag (handled separately in MooncakeEpBuffer)

        // Align to 32 bytes for LDG.256
        return align<int64_t>(num_bytes, 32);
    }

    // --- Barrier signals ---

    __forceinline__ __device__ __host__ uint64_t*
    get_barrier_counter_ptr() const {
        return static_cast<uint64_t*>(workspace);
    }

    __forceinline__ __device__ __host__ int* get_barrier_signal_ptr(
        int phase) const {
        return advance_ptr<int>(workspace,
                                sizeof(uint64_t) +
                                    phase * kNumMaxRanks * sizeof(int));
    }

    __forceinline__ __device__ __host__ int* get_barrier_signal_slot_ptr(
        int phase, int src_rank) const {
        return get_barrier_signal_ptr(phase) + src_rank;
    }

    // --- Notify reduction workspace ---

    __forceinline__ __device__ __host__ int64_t*
    get_notify_reduction_workspace_ptr() const {
        return advance_ptr<int64_t>(workspace, kNumBarrierSignalBytes);
    }

    // --- Rank/expert count buffers (send side) ---

    template <bool kIsSendBuffer>
    __forceinline__ __device__ __host__ int64_t*
    get_rank_expert_count_ptr() const {
        const auto base_ptr = advance_ptr<int64_t>(
            get_notify_reduction_workspace_ptr(),
            (kNumMaxRanks + kNumMaxExperts) * sizeof(int64_t));
        return base_ptr + (kIsSendBuffer ? 0 : kNumMaxRanks + kNumMaxExperts);
    }

    template <bool kIsSendBuffer>
    __forceinline__ __device__ __host__ int64_t* get_rank_count_ptr() const {
        return get_rank_expert_count_ptr<kIsSendBuffer>();
    }

    template <bool kIsSendBuffer>
    __forceinline__ __device__ __host__ int64_t* get_expert_count_ptr() const {
        return get_rank_expert_count_ptr<kIsSendBuffer>() + num_ranks;
    }

    // --- Atomic sender counter ---

    __forceinline__ __device__ __host__ int*
    get_atomic_sender_counter() const {
        return advance_ptr<int>(
            get_rank_expert_count_ptr<true>(),
            2 * (kNumMaxRanks + kNumMaxExperts) * sizeof(int64_t));
    }
};

// ===========================================================================
// TokenLayout
// ===========================================================================

struct TokenLayout {
    int num_hidden_bytes;
    int num_sf_bytes;
    bool with_metadata;
    int num_topk;
    int num_metadata_bytes;
    void* base;

    __forceinline__ __device__ __host__ TokenLayout(
        int num_hidden_bytes, int num_sf_bytes, int num_topk,
        bool with_metadata, void* base = nullptr)
        : num_hidden_bytes(num_hidden_bytes),
          num_sf_bytes(num_sf_bytes),
          with_metadata(with_metadata),
          num_topk(num_topk),
          num_metadata_bytes(
              num_topk * (sizeof(int) + sizeof(float)) +
              (with_metadata ? (1 + num_topk) * sizeof(int) : 0)),
          base(base) {
        EP_STATIC_ASSERT(sizeof(int) == sizeof(float), "Invalid size assumption");
    }

    // Alignment granularity for buffer layout (no TMA, use 16B for LDG.128).
    static constexpr int kAlignBytes = 16;

    __forceinline__ __device__ __host__ int64_t get_num_bytes() const {
        return align<int64_t>(num_hidden_bytes, kAlignBytes) +
               align<int64_t>(num_sf_bytes, kAlignBytes) +
               align<int64_t>(num_metadata_bytes, kAlignBytes);
    }

    __forceinline__ __device__ __host__ void* get_base_ptr() const {
        return base;
    }

    __forceinline__ __device__ __host__ void set_base_ptr(void* ptr) {
        base = ptr;
    }

    __forceinline__ __device__ __host__ void* get_hidden_ptr() const {
        return get_base_ptr();
    }

    __forceinline__ __device__ __host__ void* get_sf_ptr() const {
        return advance_ptr(base, align(num_hidden_bytes, kAlignBytes));
    }

    __forceinline__ __device__ __host__ int* get_metadata_ptr() const {
        return advance_ptr<int>(get_sf_ptr(), align(num_sf_bytes, kAlignBytes));
    }

    __forceinline__ __device__ __host__ int* get_topk_idx_ptr() const {
        return get_metadata_ptr();
    }

    __forceinline__ __device__ __host__ float* get_topk_weights_ptr() const {
        return advance_ptr<float>(get_metadata_ptr(), num_topk * sizeof(int));
    }

    __forceinline__ __device__ __host__ int*
    get_src_token_global_idx_ptr() const {
        return advance_ptr<int>(get_topk_weights_ptr(),
                                num_topk * sizeof(float));
    }
};

// ===========================================================================
// BufferLayout
// ===========================================================================

struct BufferLayout {
    TokenLayout token_layout;
    int num_ranks;
    int num_max_tokens_per_rank;
    void* base;

    __forceinline__ __device__ __host__ BufferLayout(
        const TokenLayout& token_layout, int num_ranks,
        int num_max_tokens_per_rank, void* base = nullptr)
        : token_layout(token_layout),
          num_ranks(num_ranks),
          num_max_tokens_per_rank(num_max_tokens_per_rank),
          base(base) {}

    __forceinline__ __device__ __host__ int64_t get_num_bytes_per_token()
        const {
        return token_layout.get_num_bytes();
    }

    __forceinline__ __device__ __host__ int64_t get_num_bytes_per_rank() const {
        return num_max_tokens_per_rank * get_num_bytes_per_token();
    }

    __forceinline__ __device__ __host__ int64_t get_num_bytes() const {
        return get_num_bytes_per_rank() * num_ranks;
    }

    __forceinline__ __device__ __host__ void* get_buffer_end_ptr() const {
        return advance_ptr(base, get_num_bytes());
    }

    __forceinline__ __device__ __host__ BufferLayout get_rank_buffer(
        int rank_idx) const {
        return BufferLayout(token_layout, 1, num_max_tokens_per_rank,
                            static_cast<int8_t*>(base) +
                                get_num_bytes_per_rank() * rank_idx);
    }

    __forceinline__ __device__ __host__ TokenLayout get_token_buffer(
        int token_idx, bool global = false) const {
        EP_DEVICE_ASSERT(num_ranks == 1 || global);
        return TokenLayout(
            token_layout.num_hidden_bytes, token_layout.num_sf_bytes,
            token_layout.num_topk, token_layout.with_metadata,
            static_cast<int8_t*>(base) +
                token_layout.get_num_bytes() * token_idx);
    }
};

}  // namespace layout
}  // namespace mooncake
