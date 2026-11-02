#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

inline constexpr uint32_t kKvcsDefaultMaxKeySize = 256;
inline constexpr uint32_t kKvcsMaxKeySize = 1024;

inline bool IsKvcsKeyStringSafe(std::string_view key,
                                size_t max_key_size = kKvcsDefaultMaxKeySize) {
    return !key.empty() && key.size() <= max_key_size &&
           key.find('\0') == std::string_view::npos;
}

// Provider namespaces are configured per SDK driver. Encode the logical key
// into one printable, injective provider key. Hex avoids NUL ambiguity without
// a provider-specific escaping rule.
inline ObjectKey EncodeKvcsKey(std::string_view logical_key) {
    std::string encoded = "mc1:";
    encoded.reserve(encoded.size() + logical_key.size() * 2);
    auto append_hex = [&encoded](std::string_view value) {
        constexpr char kHexDigits[] = "0123456789abcdef";
        for (unsigned char byte : value) {
            encoded.push_back(kHexDigits[byte >> 4]);
            encoded.push_back(kHexDigits[byte & 0x0f]);
        }
    };
    append_hex(logical_key);
    return encoded;
}

struct KvcsIoRequest {
    ObjectKey logical_key;
    std::vector<Slice> slices;
    uint64_t size = 0;
};

using KvcsIoResults = std::vector<tl::expected<void, ErrorCode>>;

struct KvcsMountpointStat {
    uint32_t index = 0;
    std::string id;
    uint64_t total_bytes = 0;
    uint64_t used_bytes = 0;
};

/**
 * The SDK-facing boundary used by the KVCS backend.
 * Implementations use the public SDK C API; Mooncake does not access EFC
 * internals.
 */
class KvcsDriver {
   public:
    virtual ~KvcsDriver() = default;

    virtual tl::expected<void, ErrorCode> Init() = 0;
    virtual uint64_t MaxValueSize() const = 0;
    virtual uint32_t MaxKeySize() const { return kKvcsDefaultMaxKeySize; }
    virtual tl::expected<std::vector<KvcsMountpointStat>, ErrorCode>
    GetMountpointStats() {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }

    virtual KvcsIoResults BatchPut(std::span<const KvcsIoRequest> requests) = 0;
    virtual KvcsIoResults BatchGet(std::span<const KvcsIoRequest> requests) = 0;
    virtual KvcsIoResults BatchDelete(
        std::span<const ObjectKey> logical_keys) = 0;
};

}  // namespace mooncake
