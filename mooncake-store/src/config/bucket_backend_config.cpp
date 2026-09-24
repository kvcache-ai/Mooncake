#include "config/bucket_backend_config.h"

#include <glog/logging.h>

#include <charconv>
#include <string>
#include <string_view>
#include <system_error>
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

tl::expected<BucketBackendConfig, ErrorCode>
BucketBackendConfig::FromEnvironment() {
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
    // list. Skipping an empty or unparsable entry would shift every later disk
    // onto its neighbour's quota, and falling back to the scalar quota would
    // silently ignore what the operator configured, so either is a startup
    // error.
    const std::string max_total_size_list = Environ::ReadOr(
        Variables::MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE_LIST, std::string{});
    if (!TrimAsciiWhitespace(max_total_size_list).empty()) {
        const auto entries =
            SplitAsciiList(max_total_size_list, ',', /*keep_empty=*/true);
        for (size_t i = 0; i < entries.size(); ++i) {
            const std::string_view entry = entries[i];
            int64_t parsed = 0;
            const auto [unconsumed, ec] = std::from_chars(
                entry.data(), entry.data() + entry.size(), parsed);
            // An empty entry fails here too; trailing garbage such as "100x"
            // is a typo, not a quota.
            if (ec != std::errc{} ||
                unconsumed != entry.data() + entry.size()) {
                LOG(ERROR) << "MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE_LIST='"
                           << max_total_size_list << "' has an "
                           << (entry.empty() ? "empty" : "unparsable")
                           << " entry at position " << i << " ('" << entry
                           << "'); every disk needs exactly one quota.";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            config.max_total_size_per_disk.push_back(parsed);
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
