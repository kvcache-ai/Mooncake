#include "config/distributed_storage_config.h"

#include <glog/logging.h>
#include <chrono>
#include <filesystem>
#include <sstream>
#include <string_view>
#include <unordered_set>

#include "ascii_string.h"
#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

namespace {

std::vector<std::string> ParseRootDirs(std::string_view value) {
    std::vector<std::string> roots;
    size_t begin = 0;
    for (size_t end = value.find(',', begin); end != std::string_view::npos;
         end = value.find(',', begin)) {
        const std::string_view root = value.substr(begin, end - begin);
        roots.emplace_back(TrimAsciiWhitespace(root));
        begin = end + 1;
    }
    roots.emplace_back(TrimAsciiWhitespace(value.substr(begin)));
    return roots;
}

}  // namespace

bool DistributedStorageConfig::Validate() const {
    if (root_dirs.empty()) {
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
    }
    if (fs_adapter_type != "hf3fs" && fs_adapter_type != "posix") {
        LOG(ERROR) << "DistributedStorageConfig: unsupported fs_adapter_type: "
                   << fs_adapter_type;
        return false;
    }
    if (!root_dirs.empty()) {
        if (fs_adapter_type != "posix") {
            LOG(ERROR) << "DistributedStorageConfig: multiple DFS roots are "
                          "supported only by the posix adapter";
            return false;
        }

        std::unordered_set<std::string> canonical_roots;
        for (const auto& root : root_dirs) {
            const std::filesystem::path root_path(root);
            if (root.empty() || !root_path.is_absolute()) {
                LOG(ERROR) << "DistributedStorageConfig: every DFS root must "
                              "be a non-empty absolute path: "
                           << root;
                return false;
            }

            std::error_code ec;
            const auto canonical_root =
                std::filesystem::canonical(root_path, ec);
            if (ec || !std::filesystem::is_directory(canonical_root, ec)) {
                LOG(ERROR) << "DistributedStorageConfig: DFS root must be an "
                              "existing directory: "
                           << root;
                return false;
            }
            if (!canonical_roots.insert(canonical_root.string()).second) {
                LOG(ERROR) << "DistributedStorageConfig: duplicate DFS root: "
                           << root;
                return false;
            }
        }
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

const std::string& DistributedStorageConfig::RootForShard(
    size_t shard_idx) const {
    if (root_dirs.empty()) return fsdir;
    return root_dirs[shard_idx % root_dirs.size()];
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

DistributedStorageConfig DistributedStorageConfig::FromEnvironment() {
    DistributedStorageConfig config;
    using Variables = DistributedStorageEnvironmentVariables;

    const auto legacy_root_dir =
        Environ::ReadOr(Variables::MOONCAKE_DISTRIBUTED_ROOT_DIR, config.fsdir);
    config.fsdir =
        Environ::ReadOr(Variables::MOONCAKE_DFS_ROOT_DIR, legacy_root_dir);
    if (const auto roots = Environ::Read(Variables::MOONCAKE_DFS_ROOT_DIRS)) {
        config.root_dirs = ParseRootDirs(*roots);
    } else if (!std::filesystem::path(config.fsdir).is_absolute()) {
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
    return config;
}

std::string DistributedStorageConfig::FormatStr() const {
    std::ostringstream oss;
    oss << "fsdir=" << fsdir;
    if (!root_dirs.empty()) {
        oss << ", root_dirs=[";
        for (size_t i = 0; i < root_dirs.size(); ++i) {
            if (i != 0) oss << ',';
            oss << root_dirs[i];
        }
        oss << ']';
    }
    oss << ", fs_adapter_type=" << fs_adapter_type
        << ", enable_health_check=" << enable_health_check
        << ", shard_count=" << shard_count
        << ", shard_capacity=" << shard_capacity << ", alignment=" << alignment
        << ", single_tenant=" << single_tenant
        << ", eviction_enabled=" << eviction_enabled
        << ", eviction_high_watermark=" << eviction_high_watermark
        << ", eviction_low_watermark=" << eviction_low_watermark
        << ", deferred_free_seconds=" << deferred_free_duration.count()
        << ", eviction_check_interval_seconds="
        << eviction_check_interval.count();
    return oss.str();
}

}  // namespace mooncake
