#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "engram/engram_store_config.h"

namespace mooncake {

class PyClient;

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
class EngramStore {
   public:
    EngramStore(int layer_id, const EngramStoreConfig& config,
                std::shared_ptr<PyClient> store = nullptr);

    ~EngramStore() = default;

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

    /**
     * Fast path for contiguous row-id buffers with shape [B, L, H].
     */
    int lookup_rows_contiguous(const int64_t* row_ids, int B, int L,
                               void* output, size_t output_size) const;

    std::vector<int64_t> get_table_vocab_sizes() const;
    std::vector<std::string> get_store_keys() const;
    int get_num_heads() const;
    int get_embedding_dim() const;

    /**
     * Remove all head tables owned by this EngramStore layer from Mooncake
     * Store. Missing keys are ignored. Returns the number of removed tables on
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
    int lookup_rows_flat(const int64_t* row_ids, int B, int L, void* output,
                         size_t output_size) const;

    std::shared_ptr<PyClient> store_;
    std::vector<int64_t> table_vocab_sizes_;
    int embedding_dim_;
    std::vector<std::string> embed_keys_;
};

}  // namespace engram
}  // namespace mooncake
