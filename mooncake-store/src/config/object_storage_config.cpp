#include "config/object_storage_config.h"
#include <ylt/struct_pack.hpp>

namespace mooncake {
bool ObjectStorageConfig::Validate() const {
    return !enabled ||
           ((target.provider == "s3" || target.provider == "oss") &&
            !target.endpoint.empty() && !target.bucket.empty() &&
            !target.region.empty() && !target.prefix.empty() &&
            max_object_bytes && max_staging_bytes && max_pending_records &&
            maintenance_batch_size && cleanup_interval_ms && max_gc_records &&
            unknown_upload_retention_ms && gc_scan_interval_ms &&
            gc_min_object_age_ms &&
            cleanup_max_backoff_ms >= cleanup_interval_ms &&
            cleanup_max_backoff_ms <= 365ULL * 24 * 60 * 60 * 1000 &&
            unknown_upload_retention_ms <= 365ULL * 24 * 60 * 60 * 1000 &&
            gc_scan_interval_ms <= 365ULL * 24 * 60 * 60 * 1000 &&
            gc_min_object_age_ms <= 365ULL * 24 * 60 * 60 * 1000);
}
std::string ObjectStorageConfig::NamespaceId() const {
    auto bytes =
        struct_pack::serialize<struct_pack::sp_config::DISABLE_TYPE_INFO>(
            target);
    return {bytes.begin(), bytes.end()};
}
std::string ObjectStorageGcPrefix(const std::string& cluster_id) {
    constexpr char hex[] = "0123456789abcdef";
    std::string prefix = "mc-gc-v1-";
    for (unsigned char c : cluster_id) {
        prefix += hex[c >> 4];
        prefix += hex[c & 15];
    }
    return prefix + "-";
}
}  // namespace mooncake
