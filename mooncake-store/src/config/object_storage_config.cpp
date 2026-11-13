#include "config/object_storage_config.h"
#include <ylt/struct_pack.hpp>

namespace mooncake {
bool ObjectStorageConfig::Validate() const {
    return !enabled || ((target.provider == "s3" || target.provider == "oss") &&
                        !target.endpoint.empty() && !target.bucket.empty() &&
                        !target.prefix.empty() && max_object_bytes &&
                        max_staging_bytes && max_pending_records &&
                        maintenance_batch_size && cleanup_interval_ms);
}
std::string ObjectStorageConfig::NamespaceId() const {
    auto bytes = struct_pack::serialize(target);
    return {bytes.begin(), bytes.end()};
}
}  // namespace mooncake
