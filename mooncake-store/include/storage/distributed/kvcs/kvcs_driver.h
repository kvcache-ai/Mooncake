#pragma once

#include <chrono>
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

// Provider namespaces are configured per SDK driver, while Mooncake can
// multiplex several tenants through one driver.  Encode both components into
// a printable, injective key before crossing the provider boundary.  Hex
// avoids delimiter and NUL ambiguity without imposing a provider-specific
// escaping rule on logical keys.
inline ObjectKey EncodeKvcsKey(std::string_view tenant_id,
                               std::string_view logical_key) {
    std::string encoded = "mc1:";
    encoded.reserve(encoded.size() +
                    (tenant_id.size() + logical_key.size()) * 2 + 1);
    auto append_hex = [&encoded](std::string_view value) {
        constexpr char kHexDigits[] = "0123456789abcdef";
        for (unsigned char byte : value) {
            encoded.push_back(kHexDigits[byte >> 4]);
            encoded.push_back(kHexDigits[byte & 0x0f]);
        }
    };
    append_hex(tenant_id);
    encoded.push_back('.');
    append_hex(logical_key);
    return encoded;
}

struct KvcsPutRequest {
    ObjectKey logical_key;
    std::vector<Slice> slices;
    uint64_t size = 0;
};

struct KvcsGetRequest {
    ObjectKey logical_key;
    std::vector<Slice> slices;
    uint64_t size = 0;
};

using KvcsPutResults = std::vector<tl::expected<void, ErrorCode>>;
using KvcsGetResults = std::vector<tl::expected<void, ErrorCode>>;
using KvcsDeleteResults = std::vector<tl::expected<void, ErrorCode>>;
using KvcsDriverQueryResults = std::vector<tl::expected<void, ErrorCode>>;

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

    virtual KvcsPutResults BatchPut(
        std::span<const KvcsPutRequest> requests) = 0;
    virtual KvcsDriverQueryResults BatchQuery(
        std::span<const ObjectKey> logical_keys) = 0;
    virtual KvcsDriverQueryResults BatchQueryUntil(
        std::span<const ObjectKey> logical_keys,
        std::chrono::steady_clock::time_point deadline) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return KvcsDriverQueryResults(
                logical_keys.size(),
                tl::make_unexpected(ErrorCode::RPC_TIMEOUT));
        }
        return BatchQuery(logical_keys);
    }
    virtual KvcsGetResults BatchGet(
        std::span<const KvcsGetRequest> requests) = 0;
    virtual KvcsDeleteResults BatchDelete(
        std::span<const ObjectKey> logical_keys) = 0;
};

}  // namespace mooncake
