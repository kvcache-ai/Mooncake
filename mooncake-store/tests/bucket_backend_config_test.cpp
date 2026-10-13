#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "config/bucket_backend_config.h"
#include "environ.h"

namespace mooncake {
namespace {

constexpr char kKeysLimit[] = "MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT";
constexpr char kSizeLimit[] = "MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES";
constexpr char kMaxTotalSize[] = "MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE";
constexpr char kLegacyMaxTotalSize[] = "MOONCAKE_BUCKET_MAX_TOTAL_SIZE";
constexpr char kMaxPhysicalBytes[] =
    "MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES";
constexpr char kDiskScanCacheMs[] =
    "MOONCAKE_OFFLOAD_BUCKET_DISK_SCAN_CACHE_MS";
constexpr char kEvictionPolicy[] = "MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY";
constexpr char kLegacyEvictionPolicy[] = "MOONCAKE_BUCKET_EVICTION_POLICY";

class BucketBackendConfigTest : public ::testing::Test {
   protected:
    BucketBackendConfig Load() const {
        return BucketBackendConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
};

TEST_F(BucketBackendConfigTest, UsesExistingDefaultsWhenEnvironmentIsUnset) {
    const auto config = Load();

    EXPECT_EQ(config.bucket_keys_limit, 500);
    EXPECT_EQ(config.bucket_size_limit, 256 * 1024 * 1024);
    EXPECT_EQ(config.max_total_size, 0);
    EXPECT_EQ(config.max_physical_bytes, 0);
    EXPECT_EQ(config.disk_scan_cache_ms, 500);
    EXPECT_EQ(config.eviction_policy, BucketEvictionPolicy::FIFO);
}

TEST_F(BucketBackendConfigTest, ReadsIndependentValidValues) {
    source_.Set(kKeysLimit, "1000");
    source_.Set(kSizeLimit, "536870912");
    source_.Set(kMaxTotalSize, "1073741824");
    source_.Set(kMaxPhysicalBytes, "2147483648");
    source_.Set(kDiskScanCacheMs, "250");
    source_.Set(kEvictionPolicy, "lru");

    const auto config = Load();

    EXPECT_EQ(config.bucket_keys_limit, 1000);
    EXPECT_EQ(config.bucket_size_limit, 536870912);
    EXPECT_EQ(config.max_total_size, 1073741824);
    EXPECT_EQ(config.max_physical_bytes, 2147483648);
    EXPECT_EQ(config.disk_scan_cache_ms, 250);
    EXPECT_EQ(config.eviction_policy, BucketEvictionPolicy::LRU);
}

TEST_F(BucketBackendConfigTest, PreservesAcceptedIntegerSyntax) {
    source_.Set(kKeysLimit, " +7 ");
    source_.Set(kSizeLimit, " -8 ");
    source_.Set(kMaxPhysicalBytes, "0");
    source_.Set(kDiskScanCacheMs, "-1");

    const auto config = Load();

    EXPECT_EQ(config.bucket_keys_limit, 7);
    EXPECT_EQ(config.bucket_size_limit, -8);
    EXPECT_EQ(config.max_physical_bytes, 0);
    EXPECT_EQ(config.disk_scan_cache_ms, -1);
}

TEST_F(BucketBackendConfigTest, InvalidIntegersUseIndividualDefaults) {
    for (const char* value :
         {"", "invalid", "1suffix", "9223372036854775808"}) {
        source_.Set(kKeysLimit, value);
        source_.Set(kSizeLimit, value);
        source_.Set(kMaxPhysicalBytes, value);
        source_.Set(kDiskScanCacheMs, value);
        ::testing::internal::CaptureStderr();

        const auto config = Load();
        const std::string logs = ::testing::internal::GetCapturedStderr();

        EXPECT_EQ(config.bucket_keys_limit, 500) << value;
        EXPECT_EQ(config.bucket_size_limit, 256 * 1024 * 1024) << value;
        EXPECT_EQ(config.max_physical_bytes, 0) << value;
        EXPECT_EQ(config.disk_scan_cache_ms, 500) << value;
        for (const char* name : {
                 "MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT",
                 "MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES",
                 "MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES",
                 "MOONCAKE_OFFLOAD_BUCKET_DISK_SCAN_CACHE_MS",
             }) {
            EXPECT_NE(logs.find(name), std::string::npos)
                << name << '=' << value;
        }
    }
}

TEST_F(BucketBackendConfigTest, PreferredTotalSizeOverridesLegacyAlias) {
    source_.Set(kLegacyMaxTotalSize, "111");
    EXPECT_EQ(Load().max_total_size, 111);

    source_.Set(kMaxTotalSize, "222");
    EXPECT_EQ(Load().max_total_size, 222);
}

TEST_F(BucketBackendConfigTest,
       InvalidPreferredTotalSizeFallsBackToLegacyAlias) {
    source_.Set(kLegacyMaxTotalSize, "111");
    for (const char* value : {"", "invalid", "9223372036854775808"}) {
        source_.Set(kMaxTotalSize, value);
        EXPECT_EQ(Load().max_total_size, 111) << value;
    }
}

TEST_F(BucketBackendConfigTest, PreservesTotalSizeAliasWarningOrder) {
    source_.Set(kLegacyMaxTotalSize, "invalid-legacy");
    source_.Set(kMaxTotalSize, "invalid-preferred");
    ::testing::internal::CaptureStderr();

    const auto config = Load();
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_EQ(config.max_total_size, 0);
    const size_t legacy = logs.find("MOONCAKE_BUCKET_MAX_TOTAL_SIZE");
    const size_t preferred =
        logs.find("MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE");
    ASSERT_NE(legacy, std::string::npos);
    ASSERT_NE(preferred, std::string::npos);
    EXPECT_LT(legacy, preferred);
}

TEST_F(BucketBackendConfigTest, PreservesEvictionPolicyAliasPrecedence) {
    source_.Set(kLegacyEvictionPolicy, "lru");
    EXPECT_EQ(Load().eviction_policy, BucketEvictionPolicy::LRU);

    source_.Set(kEvictionPolicy, "fifo");
    EXPECT_EQ(Load().eviction_policy, BucketEvictionPolicy::FIFO);
}

TEST_F(BucketBackendConfigTest, UnknownPolicyDisablesEvictionWithoutWarning) {
    source_.Set(kLegacyEvictionPolicy, "lru");
    for (const char* value : {"", "FIFO", "unknown"}) {
        source_.Set(kEvictionPolicy, value);
        ::testing::internal::CaptureStderr();

        const auto config = Load();
        const std::string logs = ::testing::internal::GetCapturedStderr();

        EXPECT_EQ(config.eviction_policy, BucketEvictionPolicy::NONE) << value;
        EXPECT_TRUE(logs.empty()) << value;
    }
}

TEST_F(BucketBackendConfigTest, ValidationPreservesExistingBounds) {
    BucketBackendConfig config;
    EXPECT_TRUE(config.Validate());

    config.bucket_keys_limit = 0;
    EXPECT_FALSE(config.Validate());

    config.bucket_keys_limit = 1;
    config.bucket_size_limit = 0;
    EXPECT_FALSE(config.Validate());

    config.bucket_size_limit = 1;
    config.max_total_size = -1;
    config.max_physical_bytes = -1;
    config.disk_scan_cache_ms = -1;
    EXPECT_TRUE(config.Validate());
}

TEST_F(BucketBackendConfigTest, EachConstructionReadsCurrentEnvironment) {
    source_.Set(kKeysLimit, "700");
    EXPECT_EQ(Load().bucket_keys_limit, 700);

    source_.Unset(kKeysLimit);
    EXPECT_EQ(Load().bucket_keys_limit, 500);
}

}  // namespace
}  // namespace mooncake
