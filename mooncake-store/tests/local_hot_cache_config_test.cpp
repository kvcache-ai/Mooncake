#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "local_hot_cache.h"

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

    void Set(const char* value) { setenv(name_.c_str(), value, 1); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

struct LocalHotCacheEnvironment {
    ScopedEnvVar total_size{"MC_STORE_LOCAL_HOT_CACHE_SIZE"};
    ScopedEnvVar block_size{"MC_STORE_LOCAL_HOT_BLOCK_SIZE"};
    ScopedEnvVar use_shm{"MC_STORE_LOCAL_HOT_CACHE_USE_SHM"};
    ScopedEnvVar admission_threshold{"MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD"};
};

class LocalHotCacheConfigTest : public ::testing::Test {
   protected:
    LocalHotCacheEnvironment env;

    void SetUp() override {
        google::InitGoogleLogging("LocalHotCacheConfigTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(LocalHotCacheConfigTest, UsesExistingDefaultsWhenEnvironmentIsUnset) {
    const auto config = LocalHotCacheConfig::FromEnvironment();

    EXPECT_EQ(config.total_size_bytes, 0);
    EXPECT_EQ(config.block_size_bytes, 16 * 1024 * 1024);
    EXPECT_FALSE(config.use_shm);
    EXPECT_EQ(config.admission_threshold, 2);
}

TEST_F(LocalHotCacheConfigTest, ReadsValidValues) {
    env.total_size.Set("33554432");
    env.block_size.Set("4194304");
    env.use_shm.Set("1");
    env.admission_threshold.Set("5");

    const auto config = LocalHotCacheConfig::FromEnvironment();

    EXPECT_EQ(config.total_size_bytes, 32 * 1024 * 1024);
    EXPECT_EQ(config.block_size_bytes, 4 * 1024 * 1024);
    EXPECT_TRUE(config.use_shm);
    EXPECT_EQ(config.admission_threshold, 5);
}

TEST_F(LocalHotCacheConfigTest, DisabledCacheDoesNotReadDependentSettings) {
    env.total_size.Set("0");
    env.block_size.Set("4194304");
    env.use_shm.Set("1");
    env.admission_threshold.Set("5");

    const auto config = LocalHotCacheConfig::FromEnvironment();

    EXPECT_EQ(config.total_size_bytes, 0);
    EXPECT_EQ(config.block_size_bytes, 16 * 1024 * 1024);
    EXPECT_FALSE(config.use_shm);
    EXPECT_EQ(config.admission_threshold, 2);
}

TEST_F(LocalHotCacheConfigTest, InvalidCacheSizesDisableCache) {
    for (const char* value :
         {"", "0", "-1", "invalid", "18446744073709551616"}) {
        env.total_size.Set(value);
        EXPECT_EQ(LocalHotCacheConfig::FromEnvironment().total_size_bytes, 0)
            << value;
    }
}

TEST_F(LocalHotCacheConfigTest, InvalidBlockSizesUseDefault) {
    env.total_size.Set("33554432");

    for (const char* value :
         {"", "0", "-1", "invalid", "18446744073709551616"}) {
        env.block_size.Set(value);
        EXPECT_EQ(LocalHotCacheConfig::FromEnvironment().block_size_bytes,
                  16 * 1024 * 1024)
            << value;
    }
}

TEST_F(LocalHotCacheConfigTest, InvalidAdmissionThresholdsUseDefault) {
    env.total_size.Set("33554432");

    for (const char* value :
         {"", "0", "-1", "256", "invalid", "18446744073709551616"}) {
        env.admission_threshold.Set(value);
        EXPECT_EQ(LocalHotCacheConfig::FromEnvironment().admission_threshold, 2)
            << value;
    }
}

TEST_F(LocalHotCacheConfigTest, PreservesLegacyNumericPrefixParsing) {
    env.total_size.Set("33554432suffix");
    env.block_size.Set("4194304suffix");
    env.admission_threshold.Set("5suffix");

    const auto config = LocalHotCacheConfig::FromEnvironment();

    EXPECT_EQ(config.total_size_bytes, 32 * 1024 * 1024);
    EXPECT_EQ(config.block_size_bytes, 4 * 1024 * 1024);
    EXPECT_EQ(config.admission_threshold, 5);
}

TEST_F(LocalHotCacheConfigTest, SharedMemoryRequiresExactOne) {
    env.total_size.Set("33554432");

    for (const char* value : {"", "0", "true", "01", " 1"}) {
        env.use_shm.Set(value);
        EXPECT_FALSE(LocalHotCacheConfig::FromEnvironment().use_shm) << value;
    }

    env.use_shm.Set("1");
    EXPECT_TRUE(LocalHotCacheConfig::FromEnvironment().use_shm);
}

}  // namespace
}  // namespace mooncake
