#include "config/distributed_storage_config.h"

#include <glog/logging.h>
#include <chrono>
#include <filesystem>
#include <limits>
#include <sstream>
#include <string_view>

#include "environ.h"
#include "environment_variables.h"
#include "storage/distributed/dfs_allocator_interface.h"

namespace mooncake {

namespace {

// Watermarks silently fall back for an empty value; other invalid values warn
// through GetTypedOr.
double ReadWatermark(const Environ& env,
                     const EnvironmentVariable<double>& variable,
                     double default_value) {
    const auto raw = env.Get(variable.name);
    if (!raw.has_value() || raw->empty()) {
        return default_value;
    }
    return env.GetTypedOr(variable, default_value);
}

}  // namespace

std::optional<DfsAllocatorType> ParseDfsAllocatorType(std::string_view name) {
    if (name == "shard") return DfsAllocatorType::SHARD;
    if (name == "bucket") return DfsAllocatorType::BUCKET;
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
    if (fs_adapter_type != "hf3fs" && fs_adapter_type != "posix" &&
        fs_adapter_type != "oss") {
        LOG(ERROR) << "DistributedStorageConfig: unsupported fs_adapter_type: "
                   << fs_adapter_type;
        return false;
    }
    if (fs_adapter_type == "oss") {
        return true;
    }
    const auto parsed_allocator = ParseDfsAllocatorType(allocator_type);
    if (!parsed_allocator) {
        LOG(ERROR) << "DistributedStorageConfig: unsupported allocator_type: "
                   << allocator_type;
        return false;
    }
    // alignment is shared by both allocators.
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        LOG(ERROR) << "DistributedStorageConfig: alignment must be power of 2";
        return false;
    }
    if (*parsed_allocator == DfsAllocatorType::SHARD) {
        if (shard_count <= 0) {
            LOG(ERROR) << "DistributedStorageConfig: shard_count must > 0";
            return false;
        }
        if (shard_capacity == 0) {
            LOG(ERROR) << "DistributedStorageConfig: shard_capacity must > 0";
            return false;
        }
        if (shard_capacity % alignment != 0) {
            LOG(ERROR) << "DistributedStorageConfig: shard_capacity must align";
            return false;
        }
    }
    if (*parsed_allocator == DfsAllocatorType::BUCKET &&
        (bucket_capacity == 0 || bucket_capacity % alignment != 0 ||
         bucket_capacity >
             static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
         max_bucket_count <= 0 ||
         bucket_capacity > std::numeric_limits<uint64_t>::max() /
                               static_cast<uint64_t>(max_bucket_count))) {
        LOG(ERROR) << "DistributedStorageConfig: invalid bucket capacity/count";
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
    if (fs_adapter_type == "oss") {
        LOG(ERROR) << "DistributedStorageConfig: DFS allocator requires a "
                      "filesystem adapter";
        return false;
    }

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

DistributedStorageConfig DistributedStorageConfig::FromEnvironment(
    const Environ& env) {
    DistributedStorageConfig config;
    using Variables = CommonEnvironmentVariables::DistributedStorage;

    const auto legacy_root_dir =
        env.GetTypedOr(Variables::MOONCAKE_DISTRIBUTED_ROOT_DIR, config.fsdir);
    config.fsdir =
        env.GetTypedOr(Variables::MOONCAKE_DFS_ROOT_DIR, legacy_root_dir);
    if (!std::filesystem::path(config.fsdir).is_absolute()) {
        config.fsdir = std::filesystem::absolute(config.fsdir).string();
    }

    const auto legacy_fs_adapter = env.GetTypedOr(
        Variables::MOONCAKE_DISTRIBUTED_FS_TYPE, config.fs_adapter_type);
    config.fs_adapter_type =
        env.GetTypedOr(Variables::MOONCAKE_DFS_FS_ADAPTER, legacy_fs_adapter);
    config.allocator_type = env.GetTypedOr(Variables::MOONCAKE_DFS_ALLOCATOR,
                                           config.allocator_type);
    config.enable_health_check =
        env.GetTypedOr(Variables::MOONCAKE_DISTRIBUTED_HEALTH_CHECK,
                       config.enable_health_check);
    config.shard_count =
        env.GetTypedOr(Variables::MOONCAKE_DFS_SHARD_COUNT, config.shard_count);
    config.shard_capacity = env.GetTypedOr(
        Variables::MOONCAKE_DFS_SHARD_CAPACITY, config.shard_capacity);
    config.bucket_capacity = env.GetTypedOr(
        Variables::MOONCAKE_DFS_BUCKET_CAPACITY, config.bucket_capacity);
    config.max_bucket_count = env.GetTypedOr(
        Variables::MOONCAKE_DFS_MAX_BUCKET_COUNT, config.max_bucket_count);
    config.alignment =
        env.GetTypedOr(Variables::MOONCAKE_DFS_ALIGNMENT, config.alignment);
    config.single_tenant = env.GetTypedOr(Variables::MOONCAKE_DFS_SINGLE_TENANT,
                                          config.single_tenant);
    config.eviction_enabled = env.GetTypedOr(
        Variables::MOONCAKE_DFS_EVICTION_ENABLED, config.eviction_enabled);

    config.eviction_high_watermark =
        ReadWatermark(env, Variables::MOONCAKE_DFS_EVICTION_HIGH_WATERMARK,
                      config.eviction_high_watermark);
    config.eviction_low_watermark =
        ReadWatermark(env, Variables::MOONCAKE_DFS_EVICTION_LOW_WATERMARK,
                      config.eviction_low_watermark);
    config.deferred_free_duration = std::chrono::seconds(env.GetTypedOr(
        Variables::MOONCAKE_DFS_DEFERRED_FREE_SECONDS,
        static_cast<int>(config.deferred_free_duration.count())));
    config.eviction_check_interval = std::chrono::seconds(env.GetTypedOr(
        Variables::MOONCAKE_DFS_EVICTION_CHECK_INTERVAL,
        static_cast<int>(config.eviction_check_interval.count())));
    return config;
}

std::string DistributedStorageConfig::FormatStr() const {
    std::ostringstream oss;
    oss << "fsdir=" << fsdir << ", fs_adapter_type=" << fs_adapter_type
        << ", allocator_type=" << allocator_type
        << ", enable_health_check=" << enable_health_check
        << ", shard_count=" << shard_count
        << ", shard_capacity=" << shard_capacity
        << ", bucket_capacity=" << bucket_capacity
        << ", max_bucket_count=" << max_bucket_count
        << ", alignment=" << alignment << ", single_tenant=" << single_tenant
        << ", eviction_enabled=" << eviction_enabled
        << ", eviction_high_watermark=" << eviction_high_watermark
        << ", eviction_low_watermark=" << eviction_low_watermark
        << ", deferred_free_seconds=" << deferred_free_duration.count()
        << ", eviction_check_interval_seconds="
        << eviction_check_interval.count();
    return oss.str();
}

}  // namespace mooncake
