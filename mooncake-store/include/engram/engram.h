#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
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
                         size_t workspace_size = 0) const;

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
    std::string local_agent_name_;
    std::string store_protocol_;

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

    // Consumer-side descriptor ring used by the generic SG submit path.
    mutable std::mutex request_ring_mu_;
    mutable std::condition_variable request_ring_cv_;
    mutable std::vector<char> request_ring_workspace_;
    mutable std::vector<void*> request_ring_buffers_;
    mutable std::vector<AllocatedBuffer::Descriptor> request_ring_descriptors_;
    mutable std::vector<bool> request_ring_in_use_;
    mutable std::unordered_map<uint64_t, size_t> request_ring_request_to_slot_;
    mutable bool request_ring_registered_ = false;
    mutable size_t request_ring_slot_size_ = 0;
    mutable size_t request_ring_slot_count_ = 0;

    // Producer-side staging buffers used by remote gather requests. The
    // consumer keeps using the normal query workspace; only the producer needs
    // temporary registered buffers for WRITE-back.
    std::mutex remote_gather_workspace_mu_;
    std::vector<char> remote_gather_workspace_;
    std::vector<void*> remote_gather_buffers_;
    std::vector<size_t> remote_gather_buffer_sizes_;
    bool remote_gather_workspace_registered_ = false;

    std::mutex remote_descriptor_workspace_mu_;
    std::vector<char> remote_descriptor_workspace_;
    bool remote_descriptor_workspace_registered_ = false;

    std::mutex remote_gather_service_mu_;
    std::thread remote_gather_service_thread_;
    std::atomic<bool> remote_gather_service_running_{false};
    mutable std::atomic<uint64_t> remote_gather_request_seq_{1};

    struct LookupRowRef {
        int64_t idx = 0;
        size_t output_offset = 0;
    };

    struct LookupPackedRange {
        int64_t start = 0;
        int64_t end = 0;
        size_t buffer_offset = 0;
        size_t ref_begin = 0;
        size_t ref_end = 0;
    };

    // Hash mapping helpers
    static bool is_prime(int64_t n);
    static int64_t find_next_prime(int64_t start,
                                   std::unordered_set<int64_t>& seen);
    void hash_flat_(
        const std::vector<std::vector<int64_t>>& input_ids, int layer_id,
        std::vector<int64_t>& flat_hash_ids,
        std::vector<std::vector<LookupRowRef>>* head_refs = nullptr) const;
    std::vector<size_t> get_query_table_buffer_sizes(int B, int L) const;
    int ensure_internal_workspace(int B, int L) const;
    void release_internal_workspace() const;
    std::vector<tl::expected<QueryResult, ErrorCode>> get_query_results() const;
    void invalidate_query_result_cache() const;
    bool remote_gather_enabled() const;
    int ensure_request_ring(size_t min_slot_size = 0) const;
    void release_request_ring() const;
    int ensure_remote_gather_workspace(const std::vector<size_t>& head_sizes);
    int ensure_remote_descriptor_workspace(size_t required_bytes);
    void release_remote_descriptor_workspace();
    void release_remote_gather_workspace();
    void ensure_remote_gather_service();
    void stop_remote_gather_service();
    void remote_gather_service_loop();

    // Embedding lookup / populate
    // table_buffers/sizes: optional for zero-copy; when valid, use batch_get_into
    int embedding_lookup(
        const std::vector<int64_t>& flat_hash_ids, int B, int L,
        void* output_buffer, size_t output_size,
        const std::vector<void*>* table_buffers = nullptr,
        const std::vector<size_t>* table_sizes = nullptr,
        bool buffers_are_pre_registered = false,
        std::vector<std::vector<LookupRowRef>>* prepared_head_refs = nullptr) const;
    int embedding_populate_from_tensors(
        const std::vector<void*>& embedding_data,
        const std::vector<size_t>& sizes);
};

}  // namespace engram
}  // namespace mooncake
