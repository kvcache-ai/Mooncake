#pragma once

#include <charconv>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace mooncake {

// Parses object keys encoded as decimal or 0x-prefixed hex u64 hashes.
inline std::optional<uint64_t> ParseSeqHashFromObjectKey(
    const std::string& object_key) {
    if (object_key.empty()) {
        return std::nullopt;
    }
    std::string_view encoded = object_key;
    int base = 10;
    if (encoded.size() >= 2 && encoded[0] == '0' &&
        (encoded[1] == 'x' || encoded[1] == 'X')) {
        encoded.remove_prefix(2);
        base = 16;
    }
    if (encoded.empty()) {
        return std::nullopt;
    }

    uint64_t value = 0;
    const auto [end, error] = std::from_chars(
        encoded.data(), encoded.data() + encoded.size(), value, base);
    if (error != std::errc{} || end != encoded.data() + encoded.size()) {
        return std::nullopt;
    }
    return value;
}

}  // namespace mooncake
