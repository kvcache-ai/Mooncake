#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "config/distributed_storage_config.h"
#include "environ.h"

namespace mooncake {

namespace {

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
    DistributedStorageConfig Load() const {
        return DistributedStorageConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
};

TEST_F(DistributedStorageConfigTest, UsesDefaultsWhenEnvironmentIsUnset) {
    const auto config = Load();

    ExpectDefaultConfig(config);
    EXPECT_TRUE(config.Validate());
    EXPECT_TRUE(config.ValidateForAllocator());
}

TEST_F(DistributedStorageConfigTest, ReadsValidEnvironmentValues) {
    source_.Set("MOONCAKE_DFS_ROOT_DIR", "/tmp/mooncake-dfs");
    source_.Set("MOONCAKE_DFS_FS_ADAPTER", "posix");
    source_.Set("MOONCAKE_DISTRIBUTED_HEALTH_CHECK", "true");
    source_.Set("MOONCAKE_DFS_SHARD_COUNT", "8");
    source_.Set("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");
    source_.Set("MOONCAKE_DFS_ALIGNMENT", "4096");
    source_.Set("MOONCAKE_DFS_SINGLE_TENANT", "1");
    source_.Set("MOONCAKE_DFS_EVICTION_ENABLED", "1");
    source_.Set("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "0.85");
    source_.Set("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.65");
    source_.Set("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "12");
    source_.Set("MOONCAKE_DFS_EVICTION_CHECK_INTERVAL", "3");

    const auto config = Load();

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
    source_.Set("MOONCAKE_DISTRIBUTED_ROOT_DIR", "/tmp/legacy-dfs");
    source_.Set("MOONCAKE_DISTRIBUTED_FS_TYPE", "posix");

    const auto legacy = Load();
    EXPECT_EQ(legacy.fsdir, "/tmp/legacy-dfs");
    EXPECT_EQ(legacy.fs_adapter_type, "posix");

    source_.Set("MOONCAKE_DFS_ROOT_DIR", "/tmp/preferred-dfs");
    source_.Set("MOONCAKE_DFS_FS_ADAPTER", "hf3fs");

    const auto preferred = Load();
    EXPECT_EQ(preferred.fsdir, "/tmp/preferred-dfs");
    EXPECT_EQ(preferred.fs_adapter_type, "hf3fs");
}

TEST_F(DistributedStorageConfigTest, EmptyPreferredRootOverridesAlias) {
    source_.Set("MOONCAKE_DISTRIBUTED_ROOT_DIR", "/tmp/legacy-dfs");
    source_.Set("MOONCAKE_DFS_ROOT_DIR", "");

    EXPECT_THROW(Load(), std::filesystem::filesystem_error);
}

TEST_F(DistributedStorageConfigTest, EmptyPreferredAdapterOverridesAlias) {
    source_.Set("MOONCAKE_DISTRIBUTED_FS_TYPE", "posix");
    source_.Set("MOONCAKE_DFS_FS_ADAPTER", "");

    const auto config = Load();

    EXPECT_TRUE(config.fs_adapter_type.empty());
    EXPECT_FALSE(config.Validate());
}

TEST_F(DistributedStorageConfigTest, ConvertsRelativeRootToAbsolutePath) {
    source_.Set("MOONCAKE_DFS_ROOT_DIR", "relative-dfs-root");

    const auto config = Load();

    EXPECT_EQ(config.fsdir,
              std::filesystem::absolute("relative-dfs-root").string());
}

TEST_F(DistributedStorageConfigTest,
       InvalidValuesUseDefaultsAndPreserveDiagnostics) {
    source_.Set("MOONCAKE_DISTRIBUTED_HEALTH_CHECK", "invalid");
    source_.Set("MOONCAKE_DFS_SHARD_COUNT", "invalid");
    source_.Set("MOONCAKE_DFS_SHARD_CAPACITY", "-1");
    source_.Set("MOONCAKE_DFS_ALIGNMENT", "18446744073709551616");
    source_.Set("MOONCAKE_DFS_SINGLE_TENANT", "invalid");
    source_.Set("MOONCAKE_DFS_EVICTION_ENABLED", "invalid");
    source_.Set("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "invalid");
    source_.Set("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "invalid");
    source_.Set("MOONCAKE_DFS_EVICTION_CHECK_INTERVAL", "invalid");

    ::testing::internal::CaptureStderr();
    const auto config = Load();
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
    source_.Set("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "");
    source_.Set("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "");

    ::testing::internal::CaptureStderr();
    const auto config = Load();
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
