#pragma once

#include "nvme_kv/key_codec.h"

namespace mooncake {

// Device addresses are allocated, not hashed. ID zero is reserved for absence.
inline NvmeKvPhysicalKey EncodeSharedNvmeKvPhysicalKey(uint64_t allocation_id,
                                                       uint64_t part = 0) {
    NvmeKvPhysicalKey key{};
    for (size_t i = 0; i < 8; ++i) {
        key[i] = static_cast<uint8_t>(allocation_id >> (56 - i * 8));
        key[i + 8] = static_cast<uint8_t>(part >> (56 - i * 8));
    }
    return key;
}

}  // namespace mooncake
