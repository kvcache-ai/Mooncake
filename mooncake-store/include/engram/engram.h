#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
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
 * Engram storage/query helper (C++ implementation).
 *
 * Integrates N-gram hash mapping with Mooncake Store-backed embedding storage
 * and lookup. The original model-side forward/gating/ShortConv logic is
 * intentionally kept out of Mooncake; this class only owns data placement and
 * query-time embedding retrieval.
 */
class Engram {
   public:
    Engram(int layer_id, const EngramConfig& config,
           const BackboneConfig& backbone_cfg,
           std::shared_ptr<PyClient> store = nullptr);

    ~Engram();

    /**
     * Return workspace size in bytes for embedding table buffers (zero-copy get).
     * When workspace includes this size, query_embeddings() uses batch_get_into
     * for zero-copy RDMA transfer.
     */
    size_t get_embedding_tables_workspace_size() const;

    /**
     * Return the workspace size in bytes required by query_embeddings(B, L).
     * The query path only needs temporary per-head scratch buffers large enough
     * for the requested rows in the current batch, so this can be much smaller
     * than the full embedding-table footprint.
     */
    size_t get_query_workspace_size(int B, int L) const;

    /** Timing breakdown for query_embeddings() (ms). */
    struct QueryTiming {
        double hash_ms = 0;
        double store_read_ms = 0;   /* total embedding_lookup */
        /* embedding_lookup breakdown (subset of store_read) */
        double emb_register_ms = 0;
        double emb_batch_query_ms = 0;
        double emb_get_into_range_ms = 0;
        double emb_scatter_ms = 0;
        double emb_unregister_ms = 0;
        double emb_prep_ms = 0;     /* unique_idx + range_merge */
        double emb_batch_get_buffer_ms = 0;  /* when using batch_get_buffer path */
        double emb_lookup_ms = 0;   /* memcpy from tables to output */
        double emb_total_internal_ms = 0;  /* wall time inside embedding_lookup (for verification) */
        double emb_gap_ms = 0;      /* unaccounted: internal - sum(phases), for debugging */
        double emb_setup_ms = 0;    /* t_embed_start to t_register_start */
    };

    /**
     * Query embedding rows for a batch of token IDs.
     * @param input_ids [B, L] token IDs
     * @param output [B, L, num_heads, embed_D] output buffer
     * @param workspace Optional pre-allocated buffer (size >=
     * get_query_workspace_size(B,L)); if nullptr or too small, falls back to
     * batch_get_buffer()
     * @param workspace_size Size of workspace in bytes
     * @return 0 on success, negative on error
     */
    int query_embeddings(const std::vector<std::vector<int64_t>>& input_ids,
                         void* output, size_t output_size,
                         void* workspace = nullptr,
                         size_t workspace_size = 0,
                         QueryTiming* out_timing = nullptr) const;

    /**
     * Hash input IDs into per-head embedding row indices.
     * @return [B, L, num_heads]
     */
    std::vector<std::vector<std::vector<int64_t>>> hash_input_ids(
        const std::vector<std::vector<int64_t>>& input_ids) const;

    std::vector<int64_t> get_table_vocab_sizes() const;
    std::vector<std::string> get_store_keys() const;
    int get_num_heads() const;
    int get_embedding_dim() const;
    /**
     * Remove all head tables owned by this Engram layer from Mooncake Store.
     * Missing keys are ignored. Returns the number of removed tables on
     * success, or a negative error code on failure.
     */
    int remove_from_store(bool force = false);

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

    // Optional internal scratch workspace reused across queries when the
    // caller does not provide one. This avoids per-query register/unregister
    // overhead while keeping ownership/lifetime inside Engram.
    mutable std::mutex internal_workspace_mu_;
    mutable std::vector<char> internal_workspace_;
    mutable std::vector<void*> internal_table_buffers_;
    mutable std::vector<size_t> internal_table_sizes_;
    mutable bool internal_workspace_registered_ = false;
    mutable std::mutex query_result_cache_mu_;
    mutable std::vector<QueryResult> cached_query_results_;
    mutable bool query_result_cache_valid_ = false;

    // Hash mapping helpers
    static bool is_prime(int64_t n);
    static int64_t find_next_prime(int64_t start,
                                   std::unordered_set<int64_t>& seen);
    std::vector<std::vector<std::vector<int64_t>>> hash_(
        const std::vector<std::vector<int64_t>>& input_ids, int layer_id) const;
    std::vector<size_t> get_query_table_buffer_sizes(int B, int L) const;
    int ensure_internal_workspace(int B, int L) const;
    void release_internal_workspace() const;
    std::vector<tl::expected<QueryResult, ErrorCode>> get_query_results() const;
    void invalidate_query_result_cache() const;

    // Embedding lookup / populate
    // table_buffers/sizes: optional for zero-copy; when valid, use batch_get_into
    // emb_timing: optional; when non-null, filled with per-phase breakdown
    int embedding_lookup(
        const std::vector<std::vector<std::vector<int64_t>>>& hash_ids,
        void* output_buffer, size_t output_size,
        const std::vector<void*>* table_buffers = nullptr,
        const std::vector<size_t>* table_sizes = nullptr,
        bool buffers_are_pre_registered = false,
        QueryTiming* emb_timing = nullptr) const;
    int embedding_populate_from_tensors(
        const std::vector<void*>& embedding_data,
        const std::vector<size_t>& sizes);
};

}  // namespace engram
}  // namespace mooncake
