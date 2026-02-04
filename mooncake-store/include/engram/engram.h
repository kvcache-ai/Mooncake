#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "client_buffer.hpp"
#include "engram/engram_config.h"
#include "pyclient.h"

namespace mooncake {
namespace engram {

/**
 * Engram conditional memory module (C++ implementation).
 *
 * Integrates N-gram hash mapping, Mooncake Store-backed embedding lookup,
 * gating, and ShortConv. Requires Mooncake Store for forward (no local-only
 * mode).
 */
class Engram {
   public:
    Engram(int layer_id, const EngramConfig& config,
           const BackboneConfig& backbone_cfg,
           std::shared_ptr<PyClient> store = nullptr);

    ~Engram() = default;

    /**
     * Return required workspace size in bytes for forward(B, L).
     * Caller may allocate once and pass to forward() to avoid per-call allocation.
     */
    size_t get_forward_workspace_size(int B, int L) const;

    /**
     * Forward pass.
     * @param hidden_states [B, L, hc_mult, D] input hidden states
     * @param input_ids [B, L] token IDs
     * @param output [B, L, hc_mult, D] output (pre-allocated, registered
     * buffer)
     * @param workspace Optional pre-allocated buffer (size >=
     * get_forward_workspace_size(B,L)); if nullptr or too small, allocates
     * internally
     * @param workspace_size Size of workspace in bytes
     * @return 0 on success, negative on error
     */
    int forward(const void* hidden_states, size_t hidden_states_size,
                const std::vector<std::vector<int64_t>>& input_ids,
                void* output, size_t output_size, void* workspace = nullptr,
                size_t workspace_size = 0) const;

    /**
     * Populate Store with embedding tensors.
     * @param embedding_buffers Pre-allocated buffers for each head [N_h, D]
     * @param buffer_sizes Size in bytes for each buffer
     * @return 0 on success, negative on error
     */
    int populate_store_from_buffers(const std::vector<void*>& embedding_buffers,
                                    const std::vector<size_t>& buffer_sizes);

   private:
    int layer_id_;
    EngramConfig config_;
    BackboneConfig backbone_cfg_;
    std::shared_ptr<PyClient> store_;

    // --- Hash mapping state (inlined from NgramHashMapping) ---
    std::vector<int64_t> vocab_size_per_ngram_;
    int max_ngram_size_;
    int n_embed_per_ngram_;
    int n_head_per_ngram_;
    int64_t pad_id_;
    std::vector<int> layer_ids_;
    int tokenizer_vocab_size_;
    std::unordered_map<int, std::vector<int64_t>> layer_multipliers_;
    std::unordered_map<int, std::vector<std::vector<int64_t>>>
        vocab_size_across_layers_;

    // --- Embedding state (inlined from MooncakeEngramEmbedding) ---
    std::vector<int64_t> list_of_N_;
    int embed_D_;
    std::vector<std::string> embed_keys_;

    // Projection weights (for gating and value)
    std::vector<std::vector<float>> key_proj_weights_;
    std::vector<float> value_proj_weights_;

    // Hash mapping helpers
    static bool is_prime(int64_t n);
    static int64_t find_next_prime(int64_t start,
                                   std::unordered_set<int64_t>& seen);
    std::vector<std::vector<std::vector<int64_t>>> hash_(
        const std::vector<std::vector<int64_t>>& input_ids, int layer_id) const;

    // Embedding lookup / populate
    int embedding_lookup(
        const std::vector<std::vector<std::vector<int64_t>>>& hash_ids,
        void* output_buffer, size_t output_size) const;
    int embedding_populate_from_tensors(
        const std::vector<void*>& embedding_data,
        const std::vector<size_t>& sizes);

    // Gating and ShortConv
    void compute_gates_and_fuse(const float* embeddings,
                                const float* hidden_states, int B, int L,
                                float* output) const;
    void apply_short_conv(const float* input, int B, int L,
                          float* output) const;
};

}  // namespace engram
}  // namespace mooncake
