#pragma once

#include <sys/uio.h>

#include <span>
#include <string>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct KeyInfo {
    // Decoded Mooncake logical key, not a provider-specific physical key.
    std::string logical_key;
    size_t size = 0;
};

/**
 * @brief Adapts object storage services to the distributed backend's
 * key-value I/O contract.
 *
 * All key parameters are opaque Mooncake logical keys and may contain
 * arbitrary bytes. Implementations own the reversible mapping between logical
 * keys and their configured physical object namespace, including prefixing,
 * encoding, and provider-specific validation.
 */
class ObjectStorageAdapter {
   public:
    virtual ~ObjectStorageAdapter() = default;

    virtual tl::expected<void, ErrorCode> Put(const std::string& logical_key,
                                              std::span<const char> data) = 0;

    // Atomic multi-region write: one commit and one result.
    virtual tl::expected<void, ErrorCode> PutV(const std::string& logical_key,
                                               const iovec* iov,
                                               int iovcnt) = 0;

    virtual tl::expected<size_t, ErrorCode> Get(const std::string& logical_key,
                                                void* buf, size_t len) = 0;

    virtual tl::expected<bool, ErrorCode> Exists(
        const std::string& logical_key) = 0;

    // Pagination is an implementation detail. Returns decoded logical keys
    // from the adapter's configured physical namespace.
    virtual tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() = 0;

    virtual tl::expected<void, ErrorCode> Init() = 0;
    virtual const char* GetName() const = 0;
};

}  // namespace mooncake
