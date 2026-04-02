#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "engram/engram_config.h"
#include "pyclient.h"

namespace mooncake {
namespace engram {

/**
 * Mooncake backend for Engram embedding tables.
 *
 * This class intentionally owns only storage-side concerns:
 * - per-head table naming/layout in Mooncake Store
 * - batch populate / remove
 * - row-id based embedding lookup
 *
 * It does not implement tokenizer compression, N-gram hashing, routing,
 * gating, convolution, or any other model-side Engram logic.
 */
class Engram {
   public:
    Engram(int layer_id, const EngramConfig& config,
           std::shared_ptr<PyClient> store = nullptr);

    ~Engram() = default;

    /**
     * Lookup embedding rows for a batch of precomputed row IDs.
     * @param row_ids [B, L, H] precomputed row IDs, where H == num_heads
     * @param output [B, L, H, D] output buffer
     * @param output_size Size of output buffer in bytes
     * @return 0 on success, negative on error
     */
    int lookup_rows(
        const std::vector<std::vector<std::vector<int64_t>>>& row_ids,
        void* output, size_t output_size) const;

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
     * Populate Store with per-head embedding tensors.
     * @param embedding_buffers Buffers for each head [N_h, D]
     * @param buffer_sizes Size in bytes for each buffer
     * @return 0 on success, negative on error
     */
    int populate(const std::vector<void*>& embedding_buffers,
                 const std::vector<size_t>& buffer_sizes);

   private:
    struct LookupRowRef {
        int64_t idx = 0;
        size_t output_offset = 0;
    };

    std::shared_ptr<PyClient> store_;
    std::vector<int64_t> table_vocab_sizes_;
    int embedding_dim_;
    std::vector<std::string> embed_keys_;

    mutable std::mutex query_result_cache_mu_;
    mutable std::vector<QueryResult> cached_query_results_;
    mutable bool query_result_cache_valid_ = false;

    bool copy_cached_query_results(
        std::vector<tl::expected<QueryResult, ErrorCode>>& query_results) const;
    void update_query_result_cache(
        const std::vector<tl::expected<QueryResult, ErrorCode>>& query_results)
        const;
    std::vector<tl::expected<QueryResult, ErrorCode>> get_query_results() const;
    void invalidate_query_result_cache() const;

    int prepare_lookup_rows(
        const std::vector<std::vector<std::vector<int64_t>>>& row_ids,
        int& B, int& L,
        std::vector<std::vector<LookupRowRef>>& head_refs) const;

    void resolve_local_tables(
        const std::vector<tl::expected<QueryResult, ErrorCode>>& query_results,
        std::vector<const float*>& tables, size_t row_bytes) const;
    bool fetch_missing_tables(std::vector<const float*>& tables,
                              size_t row_bytes) const;
    void materialize_output(
        const std::vector<std::vector<LookupRowRef>>& head_refs,
        const std::vector<const float*>& tables, float* output,
        size_t row_bytes) const;
    int lookup_rows_flat(
        const std::vector<std::vector<LookupRowRef>>& head_refs, int B, int L,
        void* output_buffer, size_t output_size) const;
};

}  // namespace engram
}  // namespace mooncake
