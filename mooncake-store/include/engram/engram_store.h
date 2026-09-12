#pragma once

#include <cstdint>
#include <map>
#include <memory>
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
    struct Layer {
        EngramStoreConfig config;
        std::vector<std::string> keys;
        std::vector<const void*> local_tables;
    };
    const Layer& get_layer(int layer_id) const;
    std::map<int, Layer> layers_;
};

}  // namespace engram
}  // namespace mooncake
