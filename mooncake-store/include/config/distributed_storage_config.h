#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace mooncake {

struct DistributedStorageConfig {
    std::string fsdir = "/mnt/3fs/mooncake";
    std::string fs_adapter_type = "hf3fs";
    std::string allocator_type = "shard";
    bool enable_health_check = false;
    int shard_count = 64;
    uint64_t shard_capacity = 4ULL * 1024 * 1024 * 1024;
    uint64_t bucket_capacity = 1ULL * 1024 * 1024 * 1024;
    int64_t max_bucket_count = 64;
    uint64_t alignment = 4096;
    bool single_tenant = true;
    bool eviction_enabled = true;
    double eviction_high_watermark = 0.9;
    double eviction_low_watermark = 0.7;
    std::chrono::seconds deferred_free_duration{30};
    std::chrono::seconds eviction_check_interval{5};
    std::string object_storage_config_path;
    bool enable_parallel_query = false;
    uint32_t provider_query_timeout_ms = 50;

    bool UsesObjectStorage() const {
        return fs_adapter_type == "oss" || fs_adapter_type == "kvcs-standard" ||
               fs_adapter_type == "kvcs-lowlevel";
    }
    bool UsesKvcs() const {
        return fs_adapter_type == "kvcs-standard" ||
               fs_adapter_type == "kvcs-lowlevel";
    }

    bool Validate() const;
    bool ValidateForAllocator() const;
    static DistributedStorageConfig FromEnvironment();
    std::string FormatStr() const;
};

}  // namespace mooncake
