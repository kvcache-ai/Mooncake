#pragma once

#include <cstdint>
#include <vector>

namespace mooncake {
namespace engram {

/**
 * Physical table layout for Mooncake's EngramStore backend.
 *
 * Mooncake does not derive table sizes from tokenizer/hash/model config. The
 * caller must provide the final per-head table sizes and the per-row embedding
 * width it wants Mooncake to store and query.
 */
struct EngramStoreConfig {
    std::vector<int64_t> table_vocab_sizes = {1024};
    int embedding_dim = 64;
};

}  // namespace engram
}  // namespace mooncake
