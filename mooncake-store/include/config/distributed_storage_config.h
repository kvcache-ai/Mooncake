#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace mooncake {

struct DistributedStorageConfig {
    // The WAL is compacted after reaching this configurable threshold.
    static constexpr uint64_t kDefaultMetadataWalCompactionThresholdBytes =
        64ULL * 1024 * 1024;
    // Bound the configurable threshold so recovery cannot be delayed
    // indefinitely by an unreasonably large WAL setting.
    static constexpr uint64_t kMaxAllowedMetadataWalCompactionThresholdBytes =
        512ULL * 1024 * 1024;

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
    std::chrono::seconds metadata_checkpoint_interval{300};
    uint64_t metadata_wal_compaction_threshold_bytes =
        kDefaultMetadataWalCompactionThresholdBytes;
    // Master-only namespace included in allocator sidecar fingerprints.
    std::string metadata_namespace;

    bool Validate() const;
    bool ValidateForAllocator() const;
    static DistributedStorageConfig FromEnvironment();
    std::string FormatStr() const;
};

}  // namespace mooncake
