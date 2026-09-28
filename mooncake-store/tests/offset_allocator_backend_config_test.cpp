#include "config/offset_allocator_backend_config.h"

#include <gtest/gtest.h>

#include <cmath>
#include <cstdlib>
#include <optional>
#include <string>

namespace mooncake::test {
namespace {

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) {
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

struct OffsetAllocatorEnvironment {
    ScopedEnvVar policy{"MOONCAKE_OFFSET_EVICTION_POLICY"};
    ScopedEnvVar high_ratio{"MOONCAKE_OFFSET_HIGH_RATIO"};
    ScopedEnvVar low_ratio{"MOONCAKE_OFFSET_LOW_RATIO"};
    ScopedEnvVar max_nodes{"MOONCAKE_OFFSET_MAX_CAPACITY_NODES"};
    ScopedEnvVar max_evict{"MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD"};
    ScopedEnvVar persist_mode{"MOONCAKE_OFFSET_PERSIST_MODE"};
    ScopedEnvVar persist_interval{"MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS"};
    ScopedEnvVar record_crc{"MOONCAKE_OFFSET_RECORD_CRC"};

    void SetAll(const char* value) {
        policy.Set(value);
        high_ratio.Set(value);
        low_ratio.Set(value);
        max_nodes.Set(value);
        max_evict.Set(value);
        persist_mode.Set(value);
        persist_interval.Set(value);
        record_crc.Set(value);
    }
};

void ExpectDefaultOffsetAllocatorConfig(
    const OffsetAllocatorBackendConfig& config) {
    EXPECT_EQ(config.eviction_policy, OffsetEvictionPolicy::NONE);
    EXPECT_EQ(config.high_watermark_bytes, 0);
    EXPECT_EQ(config.low_watermark_bytes, 0);
    EXPECT_DOUBLE_EQ(config.high_ratio, 0.90);
    EXPECT_DOUBLE_EQ(config.low_ratio, 0.80);
    EXPECT_EQ(config.high_watermark_keys, 0);
    EXPECT_EQ(config.low_watermark_keys, 0);
    EXPECT_DOUBLE_EQ(config.keys_high_ratio, 0.90);
    EXPECT_DOUBLE_EQ(config.keys_low_ratio, 0.80);
    EXPECT_EQ(config.max_capacity_nodes, 0);
    EXPECT_EQ(config.max_evict_per_offload, 4096);
    EXPECT_EQ(config.fallback_evict_batch, 16);
    EXPECT_EQ(config.persist_mode, OffsetPersistMode::kDisabled);
    EXPECT_EQ(config.persist_interval_seconds, 60);
    EXPECT_TRUE(config.enable_record_crc);
}

class OffsetAllocatorEnvironmentTest : public ::testing::Test {
   protected:
    OffsetAllocatorEnvironment env;
};

TEST_F(OffsetAllocatorEnvironmentTest, KeepsDefaultsWhenVariablesAreUnset) {
    const auto config = OffsetAllocatorBackendConfig::FromEnvironment();
    ExpectDefaultOffsetAllocatorConfig(config);
}

TEST_F(OffsetAllocatorEnvironmentTest, ReadsValidValues) {
    env.policy.Set("FIFO");
    env.high_ratio.Set("0.75");
    env.low_ratio.Set("0.50");
    env.max_nodes.Set("123");
    env.max_evict.Set("17");
    env.persist_mode.Set("RELAXED");
    env.persist_interval.Set("10");
    env.record_crc.Set("false");

    const auto config = OffsetAllocatorBackendConfig::FromEnvironment();
    EXPECT_EQ(config.eviction_policy, OffsetEvictionPolicy::FIFO);
    EXPECT_DOUBLE_EQ(config.high_ratio, 0.75);
    EXPECT_DOUBLE_EQ(config.low_ratio, 0.50);
    EXPECT_DOUBLE_EQ(config.keys_high_ratio, 0.75);
    EXPECT_DOUBLE_EQ(config.keys_low_ratio, 0.50);
    EXPECT_EQ(config.max_capacity_nodes, 123);
    EXPECT_EQ(config.max_evict_per_offload, 17);
    EXPECT_EQ(config.persist_mode, OffsetPersistMode::kRelaxed);
    EXPECT_EQ(config.persist_interval_seconds, 10);
    EXPECT_FALSE(config.enable_record_crc);
}

TEST_F(OffsetAllocatorEnvironmentTest, PreservesLegacyRatioParsing) {
    env.high_ratio.Set("0.75suffix");

    const auto suffixed = OffsetAllocatorBackendConfig::FromEnvironment();
    EXPECT_DOUBLE_EQ(suffixed.high_ratio, 0.75);
    EXPECT_DOUBLE_EQ(suffixed.keys_high_ratio, 0.75);

    env.high_ratio.Set("nan");
    const auto nan = OffsetAllocatorBackendConfig::FromEnvironment();
    EXPECT_TRUE(std::isnan(nan.high_ratio));
    EXPECT_TRUE(std::isnan(nan.keys_high_ratio));
}

TEST_F(OffsetAllocatorEnvironmentTest, KeepsDefaultsForInvalidValues) {
    env.policy.Set("unknown");
    env.high_ratio.Set("not-a-ratio");
    env.low_ratio.Set("not-a-ratio");
    env.max_nodes.Set("not-an-integer");
    env.max_evict.Set("-1");
    env.persist_mode.Set("unknown");
    env.persist_interval.Set("not-an-integer");
    env.record_crc.Set("unknown");

    const auto config = OffsetAllocatorBackendConfig::FromEnvironment();
    ExpectDefaultOffsetAllocatorConfig(config);
}

TEST_F(OffsetAllocatorEnvironmentTest,
       KeepsDefaultRatioForWhitespacePrefixedInvalidValue) {
    env.high_ratio.Set(" invalid");

    const auto config = OffsetAllocatorBackendConfig::FromEnvironment();
    EXPECT_DOUBLE_EQ(config.high_ratio, 0.90);
    EXPECT_DOUBLE_EQ(config.keys_high_ratio, 0.90);
}

TEST_F(OffsetAllocatorEnvironmentTest, KeepsDefaultsForEmptyValues) {
    env.SetAll("");

    const auto config = OffsetAllocatorBackendConfig::FromEnvironment();
    ExpectDefaultOffsetAllocatorConfig(config);
}

TEST_F(OffsetAllocatorEnvironmentTest,
       PreservesDiagnosticsForUnparsableAndEmptyValues) {
    for (const char* value : {"invalid", ""}) {
        env.SetAll(value);
        testing::internal::CaptureStderr();
        const auto config = OffsetAllocatorBackendConfig::FromEnvironment();
        const std::string logs = testing::internal::GetCapturedStderr();

        ExpectDefaultOffsetAllocatorConfig(config);
        EXPECT_NE(logs.find("MOONCAKE_OFFSET_MAX_CAPACITY_NODES"),
                  std::string::npos);
        EXPECT_NE(logs.find("MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD"),
                  std::string::npos);
        EXPECT_NE(logs.find("MOONCAKE_OFFSET_PERSIST_MODE"), std::string::npos);
        EXPECT_NE(logs.find("MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS"),
                  std::string::npos);
        EXPECT_EQ(logs.find("MOONCAKE_OFFSET_EVICTION_POLICY"),
                  std::string::npos);
        EXPECT_EQ(logs.find("MOONCAKE_OFFSET_HIGH_RATIO"), std::string::npos);
        EXPECT_EQ(logs.find("MOONCAKE_OFFSET_LOW_RATIO"), std::string::npos);
        EXPECT_EQ(logs.find("MOONCAKE_OFFSET_RECORD_CRC"), std::string::npos);
    }
}

TEST_F(OffsetAllocatorEnvironmentTest,
       PreservesWarningForNonPositiveEvictionCap) {
    env.max_evict.Set("-1");
    testing::internal::CaptureStderr();
    const auto config = OffsetAllocatorBackendConfig::FromEnvironment();
    const std::string logs = testing::internal::GetCapturedStderr();

    ExpectDefaultOffsetAllocatorConfig(config);
    EXPECT_NE(logs.find("MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD=-1 is "
                        "non-positive"),
              std::string::npos);
}

TEST(OffsetAllocatorBackendConfigValidationTest, AcceptsDefaults) {
    EXPECT_TRUE(OffsetAllocatorBackendConfig{}.Validate());
}

TEST(OffsetAllocatorBackendConfigValidationTest, RejectsExistingInvalidCases) {
    auto expect_invalid = [](auto mutate) {
        OffsetAllocatorBackendConfig config;
        mutate(config);
        EXPECT_FALSE(config.Validate());
    };

    expect_invalid([](auto& config) {
        config.persist_mode = OffsetPersistMode::kRelaxed;
        config.persist_interval_seconds = 4;
    });
    expect_invalid([](auto& config) { config.high_ratio = 0.0; });
    expect_invalid([](auto& config) { config.high_ratio = 1.1; });
    expect_invalid([](auto& config) { config.low_ratio = 0.0; });
    expect_invalid([](auto& config) { config.low_ratio = config.high_ratio; });
    expect_invalid([](auto& config) { config.keys_high_ratio = 0.0; });
    expect_invalid([](auto& config) { config.keys_high_ratio = 1.1; });
    expect_invalid([](auto& config) { config.keys_low_ratio = 0.0; });
    expect_invalid(
        [](auto& config) { config.keys_low_ratio = config.keys_high_ratio; });
    expect_invalid([](auto& config) { config.max_evict_per_offload = 0; });
    expect_invalid([](auto& config) { config.fallback_evict_batch = 0; });
    expect_invalid([](auto& config) { config.max_capacity_nodes = -1; });
}

}  // namespace
}  // namespace mooncake::test
