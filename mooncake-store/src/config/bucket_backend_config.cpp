#include "config/bucket_backend_config.h"

#include <glog/logging.h>

#include <cstdlib>
#include <string>

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

bool BucketBackendConfig::Validate() const {
    if (write_io_workers < 0 || write_io_backoff_ms <= 0 ||
        (write_io_workers > 0 && write_sub_batch_max_iovecs < 4)) {
        LOG(ERROR)
            << "BucketBackendConfig: invalid I/O workers or sub batch size";
        return false;
    }
    if (bucket_keys_limit <= 0) {
        LOG(ERROR) << "BucketBackendConfig: bucket_keys_limit must > 0";
        return false;
    }
    if (bucket_size_limit <= 0) {
        LOG(ERROR) << "BucketBackendConfig: bucket_size_limit must > 0";
        return false;
    }
    return true;
}

BucketBackendConfig BucketBackendConfig::FromEnvironment() {
    BucketBackendConfig config;
    using Variables = BucketBackendEnvironmentVariables;

    // Unlike legacy limits, invalid explicitly configured I/O controls must
    // fail validation rather than silently falling back to defaults.
    const auto read_io = [](const auto& variable, int64_t fallback) {
        if (!std::getenv(variable.name)) return fallback;
        return Environ::Read(variable).value_or(-1);
    };
    config.write_io_workers = read_io(
        Variables::MOONCAKE_OFFLOAD_WRITE_IO_WORKERS, config.write_io_workers);
    config.write_sub_batch_max_iovecs =
        read_io(Variables::MOONCAKE_OFFLOAD_WRITE_SUB_BATCH_MAX_IOVECS,
                config.write_sub_batch_max_iovecs);
    config.write_io_backoff_ms =
        read_io(Variables::MOONCAKE_OFFLOAD_WRITE_IO_BACKOFF_MS,
                config.write_io_backoff_ms);

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
