#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "config/distributed_storage_config.h"

namespace mooncake {

namespace {

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        const char* value = std::getenv(name);
        if (value != nullptr) {
            original_ = value;
        }
        unsetenv(name);
    }

    ~ScopedEnvVar() {
        if (original_.has_value()) {
            setenv(name_.c_str(), original_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

    void Set(const char* value) { setenv(name_.c_str(), value, 1); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

struct DistributedStorageEnvironment {
    ScopedEnvVar root_dir{"MOONCAKE_DFS_ROOT_DIR"};
    ScopedEnvVar legacy_root_dir{"MOONCAKE_DISTRIBUTED_ROOT_DIR"};
    ScopedEnvVar fs_adapter{"MOONCAKE_DFS_FS_ADAPTER"};
    ScopedEnvVar legacy_fs_adapter{"MOONCAKE_DISTRIBUTED_FS_TYPE"};
    ScopedEnvVar health_check{"MOONCAKE_DISTRIBUTED_HEALTH_CHECK"};
    ScopedEnvVar shard_count{"MOONCAKE_DFS_SHARD_COUNT"};
    ScopedEnvVar shard_capacity{"MOONCAKE_DFS_SHARD_CAPACITY"};
    ScopedEnvVar alignment{"MOONCAKE_DFS_ALIGNMENT"};
    ScopedEnvVar single_tenant{"MOONCAKE_DFS_SINGLE_TENANT"};
    ScopedEnvVar eviction_enabled{"MOONCAKE_DFS_EVICTION_ENABLED"};
    ScopedEnvVar eviction_high_watermark{
        "MOONCAKE_DFS_EVICTION_HIGH_WATERMARK"};
    ScopedEnvVar eviction_low_watermark{"MOONCAKE_DFS_EVICTION_LOW_WATERMARK"};
    ScopedEnvVar deferred_free_seconds{"MOONCAKE_DFS_DEFERRED_FREE_SECONDS"};
    ScopedEnvVar eviction_check_interval{
        "MOONCAKE_DFS_EVICTION_CHECK_INTERVAL"};
};

void ExpectDefaultConfig(const DistributedStorageConfig& config) {
    EXPECT_EQ(config.fsdir, "/mnt/3fs/mooncake");
    EXPECT_EQ(config.fs_adapter_type, "hf3fs");
    EXPECT_FALSE(config.enable_health_check);
    EXPECT_EQ(config.shard_count, 64);
    EXPECT_EQ(config.shard_capacity, 4ULL * 1024 * 1024 * 1024);
    EXPECT_EQ(config.alignment, 4096);
    EXPECT_TRUE(config.single_tenant);
    EXPECT_TRUE(config.eviction_enabled);
    EXPECT_DOUBLE_EQ(config.eviction_high_watermark, 0.9);
    EXPECT_DOUBLE_EQ(config.eviction_low_watermark, 0.7);
    EXPECT_EQ(config.deferred_free_duration, std::chrono::seconds(30));
    EXPECT_EQ(config.eviction_check_interval, std::chrono::seconds(5));
}

DistributedStorageConfig ValidConfig() {
    DistributedStorageConfig config;
    config.fsdir = "/tmp/mooncake-distributed-storage";
    config.fs_adapter_type = "posix";
    config.shard_count = 8;
    config.shard_capacity = 1024 * 1024;
    config.alignment = 4096;
    config.single_tenant = true;
    config.eviction_enabled = true;
    config.eviction_high_watermark = 0.85;
    config.eviction_low_watermark = 0.65;
    config.deferred_free_duration = std::chrono::seconds(12);
    config.eviction_check_interval = std::chrono::seconds(3);
    return config;
}

class DistributedStorageConfigTest : public ::testing::Test {
   protected:
    DistributedStorageEnvironment env;
};

TEST_F(DistributedStorageConfigTest, UsesDefaultsWhenEnvironmentIsUnset) {
    const auto config = DistributedStorageConfig::FromEnvironment();

    ExpectDefaultConfig(config);
    EXPECT_TRUE(config.Validate());
    EXPECT_TRUE(config.ValidateForAllocator());
}

TEST_F(DistributedStorageConfigTest, ReadsValidEnvironmentValues) {
    env.root_dir.Set("/tmp/mooncake-dfs");
    env.fs_adapter.Set("posix");
    env.health_check.Set("true");
    env.shard_count.Set("8");
    env.shard_capacity.Set("1048576");
    env.alignment.Set("4096");
    env.single_tenant.Set("1");
    env.eviction_enabled.Set("1");
    env.eviction_high_watermark.Set("0.85");
    env.eviction_low_watermark.Set("0.65");
    env.deferred_free_seconds.Set("12");
    env.eviction_check_interval.Set("3");

    const auto config = DistributedStorageConfig::FromEnvironment();

    EXPECT_EQ(config.fsdir, "/tmp/mooncake-dfs");
    EXPECT_EQ(config.fs_adapter_type, "posix");
    EXPECT_TRUE(config.enable_health_check);
    EXPECT_EQ(config.shard_count, 8);
    EXPECT_EQ(config.shard_capacity, 1048576);
    EXPECT_EQ(config.alignment, 4096);
    EXPECT_TRUE(config.single_tenant);
    EXPECT_TRUE(config.eviction_enabled);
    EXPECT_DOUBLE_EQ(config.eviction_high_watermark, 0.85);
    EXPECT_DOUBLE_EQ(config.eviction_low_watermark, 0.65);
    EXPECT_EQ(config.deferred_free_duration, std::chrono::seconds(12));
    EXPECT_EQ(config.eviction_check_interval, std::chrono::seconds(3));
    EXPECT_TRUE(config.Validate());
    EXPECT_TRUE(config.ValidateForAllocator());

    const std::string formatted = config.FormatStr();
    EXPECT_NE(formatted.find("fs_adapter_type=posix"), std::string::npos);
    EXPECT_NE(formatted.find("shard_count=8"), std::string::npos);
    EXPECT_NE(formatted.find("eviction_high_watermark=0.85"),
              std::string::npos);
}

TEST_F(DistributedStorageConfigTest, PreservesAliasPrecedence) {
    env.legacy_root_dir.Set("/tmp/legacy-dfs");
    env.legacy_fs_adapter.Set("posix");

    const auto legacy = DistributedStorageConfig::FromEnvironment();
    EXPECT_EQ(legacy.fsdir, "/tmp/legacy-dfs");
    EXPECT_EQ(legacy.fs_adapter_type, "posix");

    env.root_dir.Set("/tmp/preferred-dfs");
    env.fs_adapter.Set("hf3fs");

    const auto preferred = DistributedStorageConfig::FromEnvironment();
    EXPECT_EQ(preferred.fsdir, "/tmp/preferred-dfs");
    EXPECT_EQ(preferred.fs_adapter_type, "hf3fs");
}

TEST_F(DistributedStorageConfigTest, EmptyPreferredRootOverridesAlias) {
    env.legacy_root_dir.Set("/tmp/legacy-dfs");
    env.root_dir.Set("");

    EXPECT_THROW(DistributedStorageConfig::FromEnvironment(),
                 std::filesystem::filesystem_error);
}

TEST_F(DistributedStorageConfigTest, EmptyPreferredAdapterOverridesAlias) {
    env.legacy_fs_adapter.Set("posix");
    env.fs_adapter.Set("");

    const auto config = DistributedStorageConfig::FromEnvironment();

    EXPECT_TRUE(config.fs_adapter_type.empty());
    EXPECT_FALSE(config.Validate());
}

TEST_F(DistributedStorageConfigTest, ConvertsRelativeRootToAbsolutePath) {
    env.root_dir.Set("relative-dfs-root");

    const auto config = DistributedStorageConfig::FromEnvironment();

    EXPECT_EQ(config.fsdir,
              std::filesystem::absolute("relative-dfs-root").string());
}

TEST_F(DistributedStorageConfigTest,
       InvalidValuesUseDefaultsAndPreserveDiagnostics) {
    env.health_check.Set("invalid");
    env.shard_count.Set("invalid");
    env.shard_capacity.Set("-1");
    env.alignment.Set("18446744073709551616");
    env.single_tenant.Set("invalid");
    env.eviction_enabled.Set("invalid");
    env.eviction_high_watermark.Set("invalid");
    env.deferred_free_seconds.Set("invalid");
    env.eviction_check_interval.Set("invalid");

    ::testing::internal::CaptureStderr();
    const auto config = DistributedStorageConfig::FromEnvironment();
    const std::string logs = ::testing::internal::GetCapturedStderr();

    ExpectDefaultConfig(config);
    for (const char* name : {
             "MOONCAKE_DISTRIBUTED_HEALTH_CHECK",
             "MOONCAKE_DFS_SHARD_COUNT",
             "MOONCAKE_DFS_SHARD_CAPACITY",
             "MOONCAKE_DFS_ALIGNMENT",
             "MOONCAKE_DFS_SINGLE_TENANT",
             "MOONCAKE_DFS_EVICTION_ENABLED",
             "MOONCAKE_DFS_EVICTION_HIGH_WATERMARK",
             "MOONCAKE_DFS_DEFERRED_FREE_SECONDS",
             "MOONCAKE_DFS_EVICTION_CHECK_INTERVAL",
         }) {
        EXPECT_NE(logs.find(name), std::string::npos) << name;
    }
}

TEST_F(DistributedStorageConfigTest,
       EmptyWatermarksUseDefaultsWithoutDiagnostics) {
    env.eviction_high_watermark.Set("");
    env.eviction_low_watermark.Set("");

    ::testing::internal::CaptureStderr();
    const auto config = DistributedStorageConfig::FromEnvironment();
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_DOUBLE_EQ(config.eviction_high_watermark, 0.9);
    EXPECT_DOUBLE_EQ(config.eviction_low_watermark, 0.7);
    EXPECT_TRUE(logs.empty());
}

TEST(DistributedStorageConfigValidationTest, RejectsInvalidBaseSettings) {
    using Mutation =
        std::pair<const char*, std::function<void(DistributedStorageConfig&)>>;
    const std::vector<Mutation> mutations{
        {"empty root", [](auto& config) { config.fsdir.clear(); }},
        {"relative root", [](auto& config) { config.fsdir = "relative/path"; }},
        {"unsupported adapter",
         [](auto& config) { config.fs_adapter_type = "unsupported"; }},
        {"zero shards", [](auto& config) { config.shard_count = 0; }},
        {"zero capacity", [](auto& config) { config.shard_capacity = 0; }},
        {"zero alignment", [](auto& config) { config.alignment = 0; }},
        {"non-power-of-two alignment",
         [](auto& config) { config.alignment = 3; }},
        {"unaligned capacity",
         [](auto& config) { config.shard_capacity += 1; }},
        {"multi tenant", [](auto& config) { config.single_tenant = false; }},
    };

    for (const auto& [name, mutate] : mutations) {
        SCOPED_TRACE(name);
        auto config = ValidConfig();
        mutate(config);
        EXPECT_FALSE(config.Validate());
    }
}

TEST(DistributedStorageConfigValidationTest, RejectsInvalidAllocatorSettings) {
    using Mutation =
        std::pair<const char*, std::function<void(DistributedStorageConfig&)>>;
    const std::vector<Mutation> mutations{
        {"negative low watermark",
         [](auto& config) { config.eviction_low_watermark = -0.1; }},
        {"high watermark above one",
         [](auto& config) { config.eviction_high_watermark = 1.1; }},
        {"unordered watermarks",
         [](auto& config) { config.eviction_low_watermark = 0.85; }},
        {"negative deferred free",
         [](auto& config) {
             config.deferred_free_duration = std::chrono::seconds(-1);
         }},
        {"zero eviction interval",
         [](auto& config) {
             config.eviction_check_interval = std::chrono::seconds(0);
         }},
    };

    for (const auto& [name, mutate] : mutations) {
        SCOPED_TRACE(name);
        auto config = ValidConfig();
        mutate(config);
        EXPECT_FALSE(config.ValidateForAllocator());
    }

    auto eviction_disabled = ValidConfig();
    eviction_disabled.eviction_enabled = false;
    eviction_disabled.eviction_check_interval = std::chrono::seconds(0);
    EXPECT_TRUE(eviction_disabled.ValidateForAllocator());
}

}  // namespace

}  // namespace mooncake
