#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "engram/engram_store_config.h"
#include "replica.h"

namespace mooncake {

class PyClient;

namespace engram {

/**
 * Mooncake backend for all layers of a model's Engram embedding tables.
 *
 * This class intentionally owns only storage-side concerns:
 * - per-head table naming/layout in Mooncake Store
 * - batch populate / remove
 * - row-id based embedding lookup
 *
 * It does not implement tokenizer compression, N-gram hashing, routing,
 * gating, convolution, or any other model-side Engram logic.
 */
class EngramStore {
   public:
    struct LookupRequest {
        int layer_id;
        const int64_t* row_ids;
        int batch_size;
        int sequence_length;
        void* output;
        size_t output_size;
    };

    EngramStore(const std::map<int, EngramStoreConfig>& layers,
                std::shared_ptr<PyClient> store = nullptr);

    ~EngramStore() = default;

    // Bind immutable caller-owned tables before lookup, without a Store client.
    // The caller must keep these buffers alive until this EngramStore is
    // destroyed.
    int bind_local(int layer_id, const std::vector<const void*>& buffers,
                   const std::vector<size_t>& sizes);

    /**
     * Lookup embedding rows for a batch of precomputed row IDs.
     * @param row_ids [B, L, H] precomputed row IDs, where H == num_heads
     * Store-backed lookup requires registered output; local lookup does not.
     * @param output [B, L, H, row_bytes] contiguous byte output buffer
     * @param output_size Size of output buffer in bytes
     * @return 0 on success, negative on error
     */
    int lookup_into(int layer_id, const int64_t* row_ids, int B, int L,
                    void* output, size_t output_size) const;

    /**
     * Lookup several layers through one Store ranged-read submission.
     * Each output must be registered for Store-backed lookup.
     */
    int lookup_many_into(const std::vector<LookupRequest>& requests) const;

    /**
     * Lookup several layers into raw Store-registered addresses. This variant
     * is Store-backed only and leaves output contents undefined on failure.
     */
    int lookup_many_into_registered(
        const std::vector<LookupRequest>& requests) const;

    std::vector<int> get_layer_ids() const;
    std::vector<int64_t> get_table_vocab_sizes(int layer_id) const;
    std::vector<std::string> get_store_keys(int layer_id) const;
    int get_num_heads(int layer_id) const;
    int get_row_bytes(int layer_id) const;

    /**
     * Remove all head tables owned by the selected layer from Mooncake
     * Store. Missing keys are ignored. Returns the number of removed tables on
     * success, or a negative error code on failure.
     */
    int remove_from_store(int layer_id, bool force = false);

    /**
     * Populate Store with per-head embedding tensors.
     * @param embedding_buffers Byte buffers for each head [N_h, row_bytes]
     * @param buffer_sizes Size in bytes for each buffer
     * @return 0 on success, negative on error
     */
    int populate(int layer_id, const std::vector<void*>& embedding_buffers,
                 const std::vector<size_t>& buffer_sizes,
                 const ReplicateConfig& config = ReplicateConfig{});

   private:
    std::shared_ptr<PyClient> store_;
    struct QueryCacheEntry;
    struct Layer {
        EngramStoreConfig config;
        std::vector<std::string> keys;
        std::vector<const void*> local_tables;
    };
    const Layer& get_layer(int layer_id) const;
    std::shared_ptr<const QueryCacheEntry> get_query_cache(
        const std::vector<int>& layer_ids,
        const std::vector<std::string>& keys) const;
    void invalidate_query_cache(int layer_id) const;
    int lookup_many_into_impl(const std::vector<LookupRequest>& requests,
                              bool clear_outputs_on_failure) const;
    std::map<int, Layer> layers_;
    mutable std::mutex query_cache_mutex_;
    mutable std::map<int, std::shared_ptr<QueryCacheEntry>> query_cache_;
    mutable std::shared_ptr<QueryCacheEntry> multi_layer_query_cache_;
};

}  // namespace engram
}  // namespace mooncake
