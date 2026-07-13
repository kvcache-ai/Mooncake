#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "ylt/struct_pack.hpp"

namespace mooncake {

// Optional per-block metadata supplied by cache-aware clients. Mooncake only
// transports these fields; empty values mean that the field is unavailable.
struct StoreEventInfo {
    std::string model_name;
    uint32_t block_size{0};
    std::string block_hash;
    std::string parent_block_hash;
    std::vector<uint32_t> token_ids;

    YLT_REFL(StoreEventInfo, model_name, block_size, block_hash,
             parent_block_hash, token_ids);
};

}  // namespace mooncake
