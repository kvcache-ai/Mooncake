#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>

#include "config/bucket_backend_config.h"

namespace mooncake {
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
    void Unset() { unsetenv(name_.c_str()); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

struct BucketBackendEnvironment {
    ScopedEnvVar keys_limit{"MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT"};
    ScopedEnvVar size_limit{"MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES"};
    ScopedEnvVar max_total_size{"MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE"};
    ScopedEnvVar legacy_max_total_size{"MOONCAKE_BUCKET_MAX_TOTAL_SIZE"};
    ScopedEnvVar max_physical_bytes{
        "MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES"};
    ScopedEnvVar disk_scan_cache_ms{
        "MOONCAKE_OFFLOAD_BUCKET_DISK_SCAN_CACHE_MS"};
    ScopedEnvVar eviction_policy{"MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY"};
    ScopedEnvVar legacy_eviction_policy{"MOONCAKE_BUCKET_EVICTION_POLICY"};
};

class BucketBackendConfigTest : public ::testing::Test {
   protected:
    BucketBackendEnvironment env;
};

TEST_F(BucketBackendConfigTest, UsesExistingDefaultsWhenEnvironmentIsUnset) {
    const auto config = BucketBackendConfig::FromEnvironment();

    EXPECT_EQ(config.bucket_keys_limit, 500);
    EXPECT_EQ(config.bucket_size_limit, 256 * 1024 * 1024);
    EXPECT_EQ(config.max_total_size, 0);
    EXPECT_EQ(config.max_physical_bytes, 0);
    EXPECT_EQ(config.disk_scan_cache_ms, 500);
    EXPECT_EQ(config.eviction_policy, BucketEvictionPolicy::FIFO);
}

TEST_F(BucketBackendConfigTest, ReadsIndependentValidValues) {
    env.keys_limit.Set("1000");
    env.size_limit.Set("536870912");
    env.max_total_size.Set("1073741824");
    env.max_physical_bytes.Set("2147483648");
    env.disk_scan_cache_ms.Set("250");
    env.eviction_policy.Set("lru");

    const auto config = BucketBackendConfig::FromEnvironment();

    EXPECT_EQ(config.bucket_keys_limit, 1000);
    EXPECT_EQ(config.bucket_size_limit, 536870912);
    EXPECT_EQ(config.max_total_size, 1073741824);
    EXPECT_EQ(config.max_physical_bytes, 2147483648);
    EXPECT_EQ(config.disk_scan_cache_ms, 250);
    EXPECT_EQ(config.eviction_policy, BucketEvictionPolicy::LRU);
}

TEST_F(BucketBackendConfigTest, PreservesAcceptedIntegerSyntax) {
    env.keys_limit.Set(" +7 ");
    env.size_limit.Set(" -8 ");
    env.max_physical_bytes.Set("0");
    env.disk_scan_cache_ms.Set("-1");

    const auto config = BucketBackendConfig::FromEnvironment();

    EXPECT_EQ(config.bucket_keys_limit, 7);
    EXPECT_EQ(config.bucket_size_limit, -8);
    EXPECT_EQ(config.max_physical_bytes, 0);
    EXPECT_EQ(config.disk_scan_cache_ms, -1);
}

TEST_F(BucketBackendConfigTest, InvalidIntegersUseIndividualDefaults) {
    for (const char* value :
         {"", "invalid", "1suffix", "9223372036854775808"}) {
        env.keys_limit.Set(value);
        env.size_limit.Set(value);
        env.max_physical_bytes.Set(value);
        env.disk_scan_cache_ms.Set(value);
        ::testing::internal::CaptureStderr();

        const auto config = BucketBackendConfig::FromEnvironment();
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
    env.legacy_max_total_size.Set("111");
    EXPECT_EQ(BucketBackendConfig::FromEnvironment().max_total_size, 111);

    env.max_total_size.Set("222");
    EXPECT_EQ(BucketBackendConfig::FromEnvironment().max_total_size, 222);
}

TEST_F(BucketBackendConfigTest,
       InvalidPreferredTotalSizeFallsBackToLegacyAlias) {
    env.legacy_max_total_size.Set("111");
    for (const char* value : {"", "invalid", "9223372036854775808"}) {
        env.max_total_size.Set(value);
        EXPECT_EQ(BucketBackendConfig::FromEnvironment().max_total_size, 111)
            << value;
    }
}

TEST_F(BucketBackendConfigTest, PreservesTotalSizeAliasWarningOrder) {
    env.legacy_max_total_size.Set("invalid-legacy");
    env.max_total_size.Set("invalid-preferred");
    ::testing::internal::CaptureStderr();

    const auto config = BucketBackendConfig::FromEnvironment();
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
    env.legacy_eviction_policy.Set("lru");
    EXPECT_EQ(BucketBackendConfig::FromEnvironment().eviction_policy,
              BucketEvictionPolicy::LRU);

    env.eviction_policy.Set("fifo");
    EXPECT_EQ(BucketBackendConfig::FromEnvironment().eviction_policy,
              BucketEvictionPolicy::FIFO);
}

TEST_F(BucketBackendConfigTest, UnknownPolicyDisablesEvictionWithoutWarning) {
    env.legacy_eviction_policy.Set("lru");
    for (const char* value : {"", "FIFO", "unknown"}) {
        env.eviction_policy.Set(value);
        ::testing::internal::CaptureStderr();

        const auto config = BucketBackendConfig::FromEnvironment();
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
    env.keys_limit.Set("700");
    EXPECT_EQ(BucketBackendConfig::FromEnvironment().bucket_keys_limit, 700);

    env.keys_limit.Unset();
    EXPECT_EQ(BucketBackendConfig::FromEnvironment().bucket_keys_limit, 500);
}

}  // namespace
}  // namespace mooncake
