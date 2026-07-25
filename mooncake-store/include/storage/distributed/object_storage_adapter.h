#pragma once

#include <sys/uio.h>

#include <span>
#include <string>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct KeyInfo {
    std::string key;
    size_t size = 0;
};

/**
 * @brief Adapts object storage services to the distributed backend's
 * key-value I/O contract.
 */
class ObjectStorageAdapter {
   public:
    virtual ~ObjectStorageAdapter() = default;

    virtual tl::expected<void, ErrorCode> Put(const std::string& key,
                                              std::span<const char> data) = 0;

    // Atomic multi-region write: one commit and one result.
    virtual tl::expected<void, ErrorCode> PutV(const std::string& key,
                                               const iovec* iov,
                                               int iovcnt) = 0;

    virtual tl::expected<size_t, ErrorCode> Get(const std::string& key,
                                                void* buf, size_t len) = 0;

    virtual tl::expected<bool, ErrorCode> Exists(const std::string& key) = 0;

    // Pagination is an implementation detail; callers receive all matching
    // keys and their sizes.
    virtual tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() = 0;

    virtual tl::expected<void, ErrorCode> Init() = 0;
    virtual const char* GetName() const = 0;
};

}  // namespace mooncake
