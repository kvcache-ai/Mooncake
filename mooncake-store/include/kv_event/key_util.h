#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace mooncake {

// Parses object keys encoded as decimal or 0x-prefixed hex u64 hashes.
inline std::optional<uint64_t> ParseSeqHashFromObjectKey(
    const std::string& object_key) {
    if (object_key.empty()) {
        return std::nullopt;
    }
    try {
        size_t idx = 0;
        if (object_key.size() >= 2 &&
            (object_key[0] == '0' &&
             (object_key[1] == 'x' || object_key[1] == 'X'))) {
            uint64_t value = std::stoull(object_key, &idx, 16);
            if (idx == object_key.size()) {
                return value;
            }
            return std::nullopt;
        }
        uint64_t value = std::stoull(object_key, &idx, 10);
        if (idx == object_key.size()) {
            return value;
        }
    } catch (const std::exception&) {
        return std::nullopt;
    }
    return std::nullopt;
}

}  // namespace mooncake
