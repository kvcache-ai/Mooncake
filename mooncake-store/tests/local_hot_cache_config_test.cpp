#include <glog/logging.h>
#include <gtest/gtest.h>

#include "environ.h"
#include "local_hot_cache.h"

namespace mooncake {
namespace {

class LocalHotCacheConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("LocalHotCacheConfigTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    LocalHotCacheConfig Load() const {
        return LocalHotCacheConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
};

TEST_F(LocalHotCacheConfigTest, UsesExistingDefaultsWhenEnvironmentIsUnset) {
    const auto config = Load();

    EXPECT_EQ(config.total_size_bytes, 0);
    EXPECT_EQ(config.block_size_bytes, 16 * 1024 * 1024);
    EXPECT_FALSE(config.use_shm);
    EXPECT_EQ(config.admission_threshold, 2);
}

TEST_F(LocalHotCacheConfigTest, ReadsValidValues) {
    source_.Set("MC_STORE_LOCAL_HOT_CACHE_SIZE", "33554432");
    source_.Set("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "4194304");
    source_.Set("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", "1");
    source_.Set("MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD", "5");

    const auto config = Load();

    EXPECT_EQ(config.total_size_bytes, 32 * 1024 * 1024);
    EXPECT_EQ(config.block_size_bytes, 4 * 1024 * 1024);
    EXPECT_TRUE(config.use_shm);
    EXPECT_EQ(config.admission_threshold, 5);
}

TEST_F(LocalHotCacheConfigTest, DisabledCacheDoesNotReadDependentSettings) {
    source_.Set("MC_STORE_LOCAL_HOT_CACHE_SIZE", "0");
    source_.Set("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "4194304");
    source_.Set("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", "1");
    source_.Set("MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD", "5");

    const auto config = Load();

    EXPECT_EQ(config.total_size_bytes, 0);
    EXPECT_EQ(config.block_size_bytes, 16 * 1024 * 1024);
    EXPECT_FALSE(config.use_shm);
    EXPECT_EQ(config.admission_threshold, 2);
}

TEST_F(LocalHotCacheConfigTest, InvalidCacheSizesDisableCache) {
    for (const char* value :
         {"", "0", "-1", "invalid", "18446744073709551616"}) {
        source_.Set("MC_STORE_LOCAL_HOT_CACHE_SIZE", value);
        EXPECT_EQ(Load().total_size_bytes, 0) << value;
    }
}

TEST_F(LocalHotCacheConfigTest, InvalidBlockSizesUseDefault) {
    source_.Set("MC_STORE_LOCAL_HOT_CACHE_SIZE", "33554432");

    for (const char* value :
         {"", "0", "-1", "invalid", "18446744073709551616"}) {
        source_.Set("MC_STORE_LOCAL_HOT_BLOCK_SIZE", value);
        EXPECT_EQ(Load().block_size_bytes, 16 * 1024 * 1024) << value;
    }
}

TEST_F(LocalHotCacheConfigTest, InvalidAdmissionThresholdsUseDefault) {
    source_.Set("MC_STORE_LOCAL_HOT_CACHE_SIZE", "33554432");

    for (const char* value :
         {"", "0", "-1", "256", "invalid", "18446744073709551616"}) {
        source_.Set("MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD", value);
        EXPECT_EQ(Load().admission_threshold, 2) << value;
    }
}

TEST_F(LocalHotCacheConfigTest, PreservesLegacyNumericPrefixParsing) {
    source_.Set("MC_STORE_LOCAL_HOT_CACHE_SIZE", "33554432suffix");
    source_.Set("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "4194304suffix");
    source_.Set("MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD", "5suffix");

    const auto config = Load();

    EXPECT_EQ(config.total_size_bytes, 32 * 1024 * 1024);
    EXPECT_EQ(config.block_size_bytes, 4 * 1024 * 1024);
    EXPECT_EQ(config.admission_threshold, 5);
}

TEST_F(LocalHotCacheConfigTest, SharedMemoryRequiresExactOne) {
    source_.Set("MC_STORE_LOCAL_HOT_CACHE_SIZE", "33554432");

    for (const char* value : {"", "0", "true", "01", " 1"}) {
        source_.Set("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", value);
        EXPECT_FALSE(Load().use_shm) << value;
    }

    source_.Set("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", "1");
    EXPECT_TRUE(Load().use_shm);
}

}  // namespace
}  // namespace mooncake
