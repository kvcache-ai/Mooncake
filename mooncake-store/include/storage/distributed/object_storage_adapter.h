#pragma once

#include <sys/types.h>
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

struct ObjectPutRequest {
    std::string logical_key;
    const iovec* iov = nullptr;
    int iovcnt = 0;
};

struct ObjectGetRequest {
    std::string logical_key;
    void* buffer = nullptr;
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

    // Atomic, synchronous multi-region write: one commit and one result.
    // Iovecs and payloads must remain valid and unchanged until return.
    virtual tl::expected<void, ErrorCode> PutV(const std::string& logical_key,
                                               const iovec* iov,
                                               int iovcnt) = 0;

    virtual tl::expected<size_t, ErrorCode> Get(const std::string& logical_key,
                                                void* buf, size_t len) = 0;

    // Read exactly len bytes starting at offset. Implementations must reject
    // short or mismatched ranges. An error may leave buf partially written.
    // Optional so existing whole-object adapters remain source compatible.
    virtual tl::expected<size_t, ErrorCode> GetRange(
        const std::string& logical_key, void* buf, size_t len, off_t offset) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }

    // Batch methods may be overridden by concurrent adapters. The default
    // implementation preserves compatibility by executing requests serially.
    // Return exactly one result per request, in the same order. Referenced
    // buffers and iovec arrays must remain valid until the synchronous batch
    // call returns. Upload descriptors and payloads must remain unchanged.
    virtual std::vector<tl::expected<void, ErrorCode>> PutBatch(
        const std::vector<ObjectPutRequest>& requests) {
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(requests.size());
        for (const auto& request : requests) {
            results.emplace_back(
                PutV(request.logical_key, request.iov, request.iovcnt));
        }
        return results;
    }

    virtual std::vector<tl::expected<size_t, ErrorCode>> GetBatch(
        const std::vector<ObjectGetRequest>& requests) {
        std::vector<tl::expected<size_t, ErrorCode>> results;
        results.reserve(requests.size());
        for (const auto& request : requests) {
            results.emplace_back(
                Get(request.logical_key, request.buffer, request.size));
        }
        return results;
    }

    virtual tl::expected<bool, ErrorCode> Exists(
        const std::string& logical_key) = 0;

    virtual tl::expected<void, ErrorCode> Delete(
        const std::string& logical_key) = 0;

    // Pagination is an implementation detail. Returns decoded logical keys
    // from the adapter's configured physical namespace.
    virtual tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() = 0;

    virtual tl::expected<void, ErrorCode> Init() = 0;

    // Performs an end-to-end service check after Init(). Implementations may
    // create a temporary object but must make a best effort to clean it up.
    // Optional for source compatibility. Enabling health checks on an adapter
    // without an override fails explicitly instead of reporting a false
    // success.
    virtual tl::expected<void, ErrorCode> CheckHealth() {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    virtual const char* GetName() const = 0;

    // True only when Put never retries an ambiguous physical upload. A fresh
    // retry could materialize after DELETE even if a later PUT succeeds.
    // Required by the immutable-attempt backend protocol. Existing
    // whole-object offload and batch operations keep their own retry policy.
    virtual bool SupportsSinglePutAttempt() const { return false; }

    // Separate capability: a single-attempt Put does not imply that PutV
    // avoids retries. Includes the empty-object path through Put, if any.
    virtual bool SupportsSinglePutVAttempt() const { return false; }
};

}  // namespace mooncake
