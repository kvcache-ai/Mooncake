#include "config/bucket_backend_config.h"

#include <glog/logging.h>

#include <string>
#include <vector>

#include "ascii_string.h"
#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

bool BucketBackendConfig::Validate() const {
    if (bucket_keys_limit <= 0) {
        LOG(ERROR) << "BucketBackendConfig: bucket_keys_limit must > 0";
        return false;
    }
    if (bucket_size_limit <= 0) {
        LOG(ERROR) << "BucketBackendConfig: bucket_size_limit must > 0";
        return false;
    }
    // Deliberately stricter than the scalar max_total_size, where <= 0 means
    // "unlimited": the per-disk list is position-aligned, so a 0 in it is far
    // more likely a typo that silently unlimits one disk than an intent to
    // limit some disks and not others.
    for (size_t i = 0; i < max_total_size_per_disk.size(); ++i) {
        if (max_total_size_per_disk[i] <= 0) {
            LOG(ERROR) << "BucketBackendConfig: max_total_size_per_disk[" << i
                       << "] must > 0, got " << max_total_size_per_disk[i];
            return false;
        }
    }
    return true;
}

BucketBackendConfig BucketBackendConfig::FromEnvironment() {
    BucketBackendConfig config;
    using Variables = BucketBackendEnvironmentVariables;

    config.bucket_keys_limit =
        Environ::ReadOr(Variables::MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT,
                        config.bucket_keys_limit);

    config.bucket_size_limit =
        Environ::ReadOr(Variables::MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES,
                        config.bucket_size_limit);

    config.max_total_size = Environ::ReadOr(
        Variables::MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE,
        Environ::ReadOr(Variables::MOONCAKE_BUCKET_MAX_TOTAL_SIZE,
                        config.max_total_size));

    config.max_physical_bytes =
        Environ::ReadOr(Variables::MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES,
                        config.max_physical_bytes);

    config.disk_scan_cache_ms =
        Environ::ReadOr(Variables::MOONCAKE_OFFLOAD_BUCKET_DISK_SCAN_CACHE_MS,
                        config.disk_scan_cache_ms);

    // Per-disk quota list, positionally aligned with the storage path comma
    // list. Because the alignment is positional, a single unparsable entry
    // cannot simply be skipped: that would shift every later disk onto its
    // neighbour's quota. Drop the whole list instead and fall back to the
    // scalar quota, loudly.
    const std::string max_total_size_list = Environ::ReadOr(
        Variables::MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE_LIST, std::string{});
    for (const auto& entry : SplitCommaList(max_total_size_list)) {
        try {
            config.max_total_size_per_disk.push_back(
                static_cast<int64_t>(std::stoll(entry)));
        } catch (const std::exception& e) {
            LOG(ERROR) << "MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE_LIST='"
                       << max_total_size_list << "' has an unparsable entry '"
                       << entry << "' (" << e.what()
                       << "); ignoring the whole list and falling back to "
                          "MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE.";
            config.max_total_size_per_disk.clear();
            break;
        }
    }

    const std::string policy = Environ::ReadOr(
        Variables::MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY,
        Environ::ReadOr(Variables::MOONCAKE_BUCKET_EVICTION_POLICY,
                        std::string{"fifo"}));
    if (policy == "fifo") {
        config.eviction_policy = BucketEvictionPolicy::FIFO;
    } else if (policy == "lru") {
        config.eviction_policy = BucketEvictionPolicy::LRU;
    } else {
        config.eviction_policy = BucketEvictionPolicy::NONE;
    }

    return config;
}

}  // namespace mooncake
