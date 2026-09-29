#include "config/offset_allocator_backend_config.h"

#include <gtest/gtest.h>

#include <cmath>
#include <string>

#include "environ.h"

namespace mooncake::test {
namespace {

constexpr const char* kPolicy = "MOONCAKE_OFFSET_EVICTION_POLICY";
constexpr const char* kHighRatio = "MOONCAKE_OFFSET_HIGH_RATIO";
constexpr const char* kLowRatio = "MOONCAKE_OFFSET_LOW_RATIO";
constexpr const char* kMaxNodes = "MOONCAKE_OFFSET_MAX_CAPACITY_NODES";
constexpr const char* kMaxEvict = "MOONCAKE_OFFSET_MAX_EVICT_PER_OFFLOAD";
constexpr const char* kPersistMode = "MOONCAKE_OFFSET_PERSIST_MODE";
constexpr const char* kPersistInterval =
    "MOONCAKE_OFFSET_PERSIST_INTERVAL_SECONDS";
constexpr const char* kRecordCrc = "MOONCAKE_OFFSET_RECORD_CRC";

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
    OffsetAllocatorBackendConfig Load() const {
        return OffsetAllocatorBackendConfig::FromEnvironment(Environ(source_));
    }

    void SetAll(const char* value) {
        for (const char* name :
             {kPolicy, kHighRatio, kLowRatio, kMaxNodes, kMaxEvict,
              kPersistMode, kPersistInterval, kRecordCrc}) {
            source_.Set(name, value);
        }
    }

    MapEnvironSource source_;
};

TEST_F(OffsetAllocatorEnvironmentTest, KeepsDefaultsWhenVariablesAreUnset) {
    const auto config = Load();
    ExpectDefaultOffsetAllocatorConfig(config);
}

TEST_F(OffsetAllocatorEnvironmentTest, ReadsValidValues) {
    source_.Set(kPolicy, "FIFO");
    source_.Set(kHighRatio, "0.75");
    source_.Set(kLowRatio, "0.50");
    source_.Set(kMaxNodes, "123");
    source_.Set(kMaxEvict, "17");
    source_.Set(kPersistMode, "RELAXED");
    source_.Set(kPersistInterval, "10");
    source_.Set(kRecordCrc, "false");

    const auto config = Load();
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
    source_.Set(kHighRatio, "0.75suffix");

    const auto suffixed = Load();
    EXPECT_DOUBLE_EQ(suffixed.high_ratio, 0.75);
    EXPECT_DOUBLE_EQ(suffixed.keys_high_ratio, 0.75);

    source_.Set(kHighRatio, "nan");
    const auto nan = Load();
    EXPECT_TRUE(std::isnan(nan.high_ratio));
    EXPECT_TRUE(std::isnan(nan.keys_high_ratio));
}

TEST_F(OffsetAllocatorEnvironmentTest, KeepsDefaultsForInvalidValues) {
    source_.Set(kPolicy, "unknown");
    source_.Set(kHighRatio, "not-a-ratio");
    source_.Set(kLowRatio, "not-a-ratio");
    source_.Set(kMaxNodes, "not-an-integer");
    source_.Set(kMaxEvict, "-1");
    source_.Set(kPersistMode, "unknown");
    source_.Set(kPersistInterval, "not-an-integer");
    source_.Set(kRecordCrc, "unknown");

    const auto config = Load();
    ExpectDefaultOffsetAllocatorConfig(config);
}

TEST_F(OffsetAllocatorEnvironmentTest,
       KeepsDefaultRatioForWhitespacePrefixedInvalidValue) {
    source_.Set(kHighRatio, " invalid");

    const auto config = Load();
    EXPECT_DOUBLE_EQ(config.high_ratio, 0.90);
    EXPECT_DOUBLE_EQ(config.keys_high_ratio, 0.90);
}

TEST_F(OffsetAllocatorEnvironmentTest, KeepsDefaultsForEmptyValues) {
    SetAll("");

    const auto config = Load();
    ExpectDefaultOffsetAllocatorConfig(config);
}

TEST_F(OffsetAllocatorEnvironmentTest,
       PreservesDiagnosticsForUnparsableAndEmptyValues) {
    for (const char* value : {"invalid", ""}) {
        SetAll(value);
        testing::internal::CaptureStderr();
        const auto config = Load();
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
    source_.Set(kMaxEvict, "-1");
    testing::internal::CaptureStderr();
    const auto config = Load();
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
