#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include "storage/distributed/global_allocator_interface.h"

namespace mooncake {

struct DistributedStorageConfig {
    std::string fsdir = "/mnt/3fs/mooncake";
    std::string fs_adapter_type = "hf3fs";
    bool enable_health_check = false;
    int shard_count = 64;
    uint64_t shard_capacity = 4ULL * 1024 * 1024 * 1024;
    uint64_t alignment = 4096;
    bool single_tenant = true;
    bool eviction_enabled = true;
    double eviction_high_watermark = 0.9;
    double eviction_low_watermark = 0.7;
    std::chrono::seconds deferred_free_duration{30};
    std::chrono::seconds eviction_check_interval{5};

    DfsAllocatorType allocator_type = DfsAllocatorType::SHARD;
    uint64_t bucket_capacity = 256ULL * 1024 * 1024;
    int64_t max_bucket_count = 256;
    bool allocator_type_valid = true;

    bool Validate() const;
    bool ValidateForAllocator() const;
    bool ValidateForBucketAllocator() const;
    static DistributedStorageConfig FromEnvironment();
    std::string FormatStr() const;
};

}  // namespace mooncake
