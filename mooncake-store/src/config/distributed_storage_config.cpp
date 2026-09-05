#include "config/distributed_storage_config.h"

#include <glog/logging.h>
#include <chrono>
#include <filesystem>
#include <limits>
#include <sstream>

#include "environ.h"
#include "environment_variables.h"
#include "storage/distributed/bucket_entry_layout.h"

namespace mooncake {

std::optional<DfsAllocatorType> ParseDfsAllocatorType(std::string_view name) {
    if (name == "shard" || name == "SHARD") return DfsAllocatorType::SHARD;
    if (name == "bucket" || name == "BUCKET") return DfsAllocatorType::BUCKET;
    return std::nullopt;
}

const char* ToString(DfsAllocatorType type) {
    switch (type) {
        case DfsAllocatorType::SHARD:
            return "shard";
        case DfsAllocatorType::BUCKET:
            return "bucket";
    }
    return "unknown";
}

bool DistributedStorageConfig::Validate() const {
    if (fsdir.empty()) {
        LOG(ERROR) << "DistributedStorageConfig: fsdir is empty";
        return false;
    }
    if (!std::filesystem::path(fsdir).is_absolute()) {
        LOG(ERROR)
            << "DistributedStorageConfig: fsdir must be an absolute path: "
            << fsdir;
        return false;
    }
    if (fs_adapter_type != "hf3fs" && fs_adapter_type != "posix") {
        LOG(ERROR) << "DistributedStorageConfig: unsupported fs_adapter_type: "
                   << fs_adapter_type;
        return false;
    }
    if (shard_count <= 0) {
        LOG(ERROR) << "DistributedStorageConfig: shard_count must > 0";
        return false;
    }
    if (shard_capacity == 0) {
        LOG(ERROR) << "DistributedStorageConfig: shard_capacity must > 0";
        return false;
    }
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        LOG(ERROR) << "DistributedStorageConfig: alignment must be power of 2";
        return false;
    }
    if (shard_capacity % alignment != 0) {
        LOG(ERROR) << "DistributedStorageConfig: shard_capacity must align";
        return false;
    }
    if (!single_tenant) {
        LOG(ERROR) << "DistributedStorageConfig: Currently, DFS requires "
                      "single_tenant=true";
        return false;
    }
    return true;
}

bool DistributedStorageConfig::ValidateForAllocator() const {
    if (!Validate()) return false;

    if (eviction_low_watermark < 0.0 || eviction_low_watermark > 1.0 ||
        eviction_high_watermark < 0.0 || eviction_high_watermark > 1.0 ||
        eviction_low_watermark >= eviction_high_watermark) {
        LOG(ERROR) << "DistributedStorageConfig: eviction watermarks must "
                      "satisfy 0 <= low < high <= 1, low="
                   << eviction_low_watermark
                   << ", high=" << eviction_high_watermark;
        return false;
    }
    if (deferred_free_duration.count() < 0) {
        LOG(ERROR) << "DistributedStorageConfig: deferred_free_duration must "
                      "be non-negative, seconds="
                   << deferred_free_duration.count();
        return false;
    }
    if (eviction_enabled && eviction_check_interval.count() <= 0) {
        LOG(ERROR) << "DistributedStorageConfig: eviction_check_interval must "
                      "be positive when eviction is enabled, seconds="
                   << eviction_check_interval.count();
        return false;
    }
    return true;
}

bool DistributedStorageConfig::ValidateForBucketAllocator() const {
    if (!ValidateForAllocator()) return false;
    if (!allocator_type_valid) {
        LOG(ERROR) << "DistributedStorageConfig: invalid allocator type";
        return false;
    }
    if (bucket_capacity == 0 ||
        bucket_capacity <= BucketEntryLayout::kHeaderSize) {
        LOG(ERROR) << "DistributedStorageConfig: bucket_capacity is too small, "
                   << "bucket_capacity=" << bucket_capacity;
        return false;
    }
    if (bucket_capacity % alignment != 0) {
        LOG(ERROR) << "DistributedStorageConfig: bucket_capacity must align";
        return false;
    }
    if (max_bucket_count <= 0 || max_bucket_count > kMaxBucketId) {
        LOG(ERROR) << "DistributedStorageConfig: max_bucket_count must be in "
                      "[1, "
                   << kMaxBucketId << "], max_bucket_count="
                   << max_bucket_count;
        return false;
    }
    if (static_cast<uint64_t>(max_bucket_count) >
        std::numeric_limits<uint64_t>::max() / bucket_capacity) {
        LOG(ERROR) << "DistributedStorageConfig: bucket capacity overflow";
        return false;
    }
    if (batch_read_threads < 1 ||
        batch_read_threads > kMaxDfsBatchReadThreads) {
        LOG(ERROR) << "DistributedStorageConfig: batch_read_threads must be in "
                      "[1, "
                   << kMaxDfsBatchReadThreads
                   << "], batch_read_threads=" << batch_read_threads;
        return false;
    }
    return true;
}

DistributedStorageConfig DistributedStorageConfig::FromEnvironment() {
    DistributedStorageConfig config;
    using Variables = DistributedStorageEnvironmentVariables;

    const auto legacy_root_dir =
        Environ::ReadOr(Variables::MOONCAKE_DISTRIBUTED_ROOT_DIR, config.fsdir);
    config.fsdir =
        Environ::ReadOr(Variables::MOONCAKE_DFS_ROOT_DIR, legacy_root_dir);
    if (!std::filesystem::path(config.fsdir).is_absolute()) {
        config.fsdir = std::filesystem::absolute(config.fsdir).string();
    }

    const auto legacy_fs_adapter = Environ::ReadOr(
        Variables::MOONCAKE_DISTRIBUTED_FS_TYPE, config.fs_adapter_type);
    config.fs_adapter_type =
        Environ::ReadOr(Variables::MOONCAKE_DFS_FS_ADAPTER, legacy_fs_adapter);
    config.enable_health_check =
        Environ::ReadOr(Variables::MOONCAKE_DISTRIBUTED_HEALTH_CHECK,
                        config.enable_health_check);
    config.shard_count = Environ::ReadOr(Variables::MOONCAKE_DFS_SHARD_COUNT,
                                         config.shard_count);
    config.shard_capacity = Environ::ReadOr(
        Variables::MOONCAKE_DFS_SHARD_CAPACITY, config.shard_capacity);
    config.alignment =
        Environ::ReadOr(Variables::MOONCAKE_DFS_ALIGNMENT, config.alignment);
    config.single_tenant = Environ::ReadOr(
        Variables::MOONCAKE_DFS_SINGLE_TENANT, config.single_tenant);
    config.eviction_enabled = Environ::ReadOr(
        Variables::MOONCAKE_DFS_EVICTION_ENABLED, config.eviction_enabled);

    // GetDouble silently falls back for an empty value; ReadOr<double> emits a
    // warning. Keep the existing diagnostics while this refactor is
    // behavior-preserving.
    config.eviction_high_watermark =
        Environ::GetDouble(Variables::MOONCAKE_DFS_EVICTION_HIGH_WATERMARK.name,
                           config.eviction_high_watermark);
    config.eviction_low_watermark =
        Environ::GetDouble(Variables::MOONCAKE_DFS_EVICTION_LOW_WATERMARK.name,
                           config.eviction_low_watermark);
    config.deferred_free_duration = std::chrono::seconds(Environ::ReadOr(
        Variables::MOONCAKE_DFS_DEFERRED_FREE_SECONDS,
        static_cast<int>(config.deferred_free_duration.count())));
    config.eviction_check_interval = std::chrono::seconds(Environ::ReadOr(
        Variables::MOONCAKE_DFS_EVICTION_CHECK_INTERVAL,
        static_cast<int>(config.eviction_check_interval.count())));

    const std::string allocator_type_name = Environ::ReadOr(
        Variables::MOONCAKE_DFS_ALLOCATOR_TYPE,
        std::string(ToString(config.allocator_type)));
    if (auto parsed = ParseDfsAllocatorType(allocator_type_name)) {
        config.allocator_type = *parsed;
    } else {
        LOG(ERROR) << "Unknown MOONCAKE_DFS_ALLOCATOR_TYPE '"
                   << allocator_type_name << "', expected 'shard' or 'bucket'";
        config.allocator_type_valid = false;
    }
    config.bucket_capacity = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BUCKET_CAPACITY, config.bucket_capacity);
    config.max_bucket_count = Environ::ReadOr(
        Variables::MOONCAKE_DFS_MAX_BUCKET_COUNT,
        static_cast<int>(config.max_bucket_count));
    config.batch_read_threads = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BATCH_READ_THREADS,
        config.batch_read_threads);
    config.batch_read_merge_enabled = Environ::ReadOr(
        Variables::MOONCAKE_DFS_BATCH_READ_MERGE_ENABLED,
        config.batch_read_merge_enabled);
    config.direct_read_enabled = Environ::ReadOr(
        Variables::MOONCAKE_DFS_DIRECT_READ_ENABLED,
        config.direct_read_enabled);
    return config;
}

std::string DistributedStorageConfig::FormatStr() const {
    std::ostringstream oss;
    oss << "fsdir=" << fsdir << ", fs_adapter_type=" << fs_adapter_type
        << ", enable_health_check=" << enable_health_check
        << ", shard_count=" << shard_count
        << ", shard_capacity=" << shard_capacity << ", alignment=" << alignment
        << ", single_tenant=" << single_tenant
        << ", eviction_enabled=" << eviction_enabled
        << ", eviction_high_watermark=" << eviction_high_watermark
        << ", eviction_low_watermark=" << eviction_low_watermark
        << ", deferred_free_seconds=" << deferred_free_duration.count()
        << ", eviction_check_interval_seconds="
        << eviction_check_interval.count()
        << ", allocator_type=" << ToString(allocator_type)
        << ", bucket_capacity=" << bucket_capacity
        << ", max_bucket_count=" << max_bucket_count
        << ", batch_read_threads=" << batch_read_threads
        << ", batch_read_merge_enabled=" << batch_read_merge_enabled
        << ", direct_read_enabled=" << direct_read_enabled;
    return oss.str();
}

}  // namespace mooncake
