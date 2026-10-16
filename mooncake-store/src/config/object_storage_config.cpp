#include "config/object_storage_config.h"

#include <stdexcept>
#include <limits>
#include <ylt/struct_pack.hpp>

namespace mooncake {

bool ObjectStorageConfig::Validate() const {
    return !enabled ||
           (provider == "oss" && !endpoint.empty() && !bucket.empty() &&
            !region.empty() && !prefix.empty() && max_object_bytes > 0 &&
            max_pending_bytes >= max_object_bytes && max_pending_objects > 0 &&
            maintenance_batch_size > 0 && request_timeout_ms > 0 &&
            request_timeout_ms <=
                static_cast<uint64_t>(std::numeric_limits<long>::max()) &&
            max_object_bytes <=
                static_cast<uint64_t>(std::numeric_limits<size_t>::max()) &&
            (write_mode == "sync" || write_mode == "async" ||
             write_mode == "offload_on_evict"));
}

ObjectStorageWriteMode ObjectStorageConfig::WriteMode() const {
    if (write_mode == "sync") return ObjectStorageWriteMode::SYNC;
    if (write_mode == "async") return ObjectStorageWriteMode::ASYNC;
    if (write_mode == "offload_on_evict")
        return ObjectStorageWriteMode::OFFLOAD_ON_EVICT;
    throw std::invalid_argument("Invalid object_storage_write_mode");
}

std::string ObjectStorageConfig::NamespaceId() const {
    // Compare the complete namespace rather than a collision-prone hash.
    const auto bytes = struct_pack::serialize(provider, endpoint, bucket,
                                              region, prefix, path_style);
    return {bytes.begin(), bytes.end()};
}

}  // namespace mooncake
