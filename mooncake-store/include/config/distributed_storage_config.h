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
    // Client-side POSIX adapter only: read and write DFS files with O_DIRECT,
    // zero-padding each object to `alignment` inside its aligned_size. There
    // is no page-cache fallback: backend Init fails if the filesystem cannot
    // serve O_DIRECT at `alignment`.
    bool posix_direct_io = false;
    // With posix_direct_io: fsync() each O_DIRECT file once per BatchWrite,
    // after the batch's writes to it, and fail those writes if fsync fails.
    // Ignored without posix_direct_io.
    bool posix_direct_io_fsync = false;
    bool single_tenant = true;
    bool eviction_enabled = true;
    double eviction_high_watermark = 0.9;
    double eviction_low_watermark = 0.7;
    std::chrono::seconds deferred_free_duration{30};
    std::chrono::seconds eviction_check_interval{5};
    std::string object_storage_config_path;

    bool UsesObjectStorage() const {
        return fs_adapter_type == "oss" || fs_adapter_type == "s3" ||
               fs_adapter_type == "kvcs-lowlevel";
    }
    bool UsesKvcs() const { return fs_adapter_type == "kvcs-lowlevel"; }

    bool Validate() const;
    bool ValidateForAllocator() const;
    static DistributedStorageConfig FromEnvironment();
    std::string FormatStr() const;
};

}  // namespace mooncake
