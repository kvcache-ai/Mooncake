#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "storage/distributed/object_storage_adapter.h"

namespace mooncake {

struct ObjectStorageBackendConfig {
    uint64_t max_object_bytes{256ULL * 1024 * 1024};
    uint64_t max_staging_bytes{1024ULL * 1024 * 1024};
};

// Synchronous I/O for a caller-selected immutable object name. The caller owns
// name allocation, upload claims/retries, leases and deletion safety; this
// component does not publish replicas or retry an ambiguous PUT.
// The adapter must be initialized and support concurrent calls if shared.
// Writes require SupportsSinglePutAttempt(); retrying adapters are rejected.
class ObjectStorageBackend {
   public:
    // Copy between caller memory (possibly device memory) and host staging.
    // Return only when copying is complete; false reports TRANSFER_FAIL.
    using Copy = std::function<bool(void*, const void*, size_t)>;

    // Throws std::invalid_argument for a null adapter or zero limits.
    ObjectStorageBackend(ObjectStorageBackendConfig config,
                         std::shared_ptr<ObjectStorageAdapter> adapter);

    // Gather a full object before issuing one PUT. upload_started is reset on
    // entry and set just before invoking Put: true means PUT may have begun,
    // including when the adapter returns an error.
    ErrorCode Write(const std::string& object_key, uint64_t object_size,
                    const std::vector<Slice>& slices, const Copy& to_host,
                    bool* upload_started = nullptr);

    // Coalesce continuous object ranges across destination slice boundaries,
    // then scatter using at most min(4 MiB, max_staging_bytes) of
    // staging. Errors may leave destination slices partially written.
    ErrorCode Read(const std::string& object_key, uint64_t object_size,
                   const std::vector<Slice>& slices, uint64_t offset,
                   const Copy& from_host);

   private:
    const ObjectStorageBackendConfig config_;
    const std::shared_ptr<ObjectStorageAdapter> adapter_;
    std::atomic<uint64_t> staging_bytes_{0};
};

}  // namespace mooncake
