#include <glog/logging.h>
#include <gtest/gtest.h>

#include <thread>

#include "allocator.h"
#include "storage_backend.h"
#include "file_storage.h"

namespace mooncake::test {

void SetEnv(const std::string& key, const std::string& value) {
    setenv(key.c_str(), value.c_str(), 1);
}

void UnsetEnv(const std::string& key) {
#ifdef _WIN32
    unsetenv(key.c_str());
#else
    unsetenv(key.c_str());
#endif
}

class FileStorageTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("FileStorageTest");
        FLAGS_logtostderr = true;
        UnsetEnv("FILE_STORAGE_PATH");
        UnsetEnv("LOCAL_BUFFER_SIZE_BYTES");
        UnsetEnv("BUCKET_ITERATOR_KEYS_LIMIT");
        UnsetEnv("BUCKET_KEYS_LIMIT");
        UnsetEnv("BUCKET_SIZE_LIMIT_BYTES");
        UnsetEnv("TOTAL_KEYS_LIMIT");
        UnsetEnv("TOTAL_SIZE_LIMIT_BYTES");
        UnsetEnv("HEARTBEAT_INTERVAL_SECONDS");
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(FileStorageTest, DefaultValuesWhenNoEnvSet) {
    auto config = FileStorageConfig::FromEnvironment();

    EXPECT_EQ(config.storage_filepath, "/data/file_storage");
    EXPECT_EQ(config.local_buffer_size, 1280 * 1024 * 1024);
    EXPECT_EQ(config.bucket_iterator_keys_limit, 20000);
    EXPECT_EQ(config.bucket_keys_limit, 500);
    EXPECT_EQ(config.bucket_size_limit, 256 * 1024 * 1024);
    EXPECT_EQ(config.total_keys_limit, 10'000'000);
    EXPECT_EQ(config.total_size_limit, 2ULL * 1024 * 1024 * 1024 * 1024);
    EXPECT_EQ(config.heartbeat_interval_seconds, 10u);
}

TEST_F(FileStorageTest, ReadStringFromEnv) {
    SetEnv("FILE_STORAGE_PATH", "/tmp/storage");

    auto config = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(config.storage_filepath, "/tmp/storage");
}

TEST_F(FileStorageTest, ReadInt64FromEnv) {
    SetEnv("LOCAL_BUFFER_SIZE_BYTES", "2147483648");  // 2GB
    SetEnv("BUCKET_KEYS_LIMIT", "1000");
    SetEnv("TOTAL_KEYS_LIMIT", "5000000");

    auto config = FileStorageConfig::FromEnvironment();

    EXPECT_EQ(config.local_buffer_size, 2147483648);
    EXPECT_EQ(config.bucket_keys_limit, 1000);
    EXPECT_EQ(config.total_keys_limit, 5000000);
}

TEST_F(FileStorageTest, ReadUint32FromEnv) {
    SetEnv("HEARTBEAT_INTERVAL_SECONDS", "5");

    auto config = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(config.heartbeat_interval_seconds, 5u);
}

TEST_F(FileStorageTest, InvalidIntValueUsesDefault) {
    SetEnv("BUCKET_KEYS_LIMIT", "abc");
    SetEnv("TOTAL_SIZE_LIMIT_BYTES", "sdfsdf");
    SetEnv("HEARTBEAT_INTERVAL_SECONDS", "-1");

    auto config = FileStorageConfig::FromEnvironment();

    EXPECT_EQ(config.bucket_keys_limit, 500);
    EXPECT_EQ(config.total_size_limit, 2ULL * 1024 * 1024 * 1024 * 1024);
    EXPECT_EQ(config.heartbeat_interval_seconds, 10u);
}

TEST_F(FileStorageTest, OutOfRangeValueUsesDefault) {
    SetEnv("HEARTBEAT_INTERVAL_SECONDS", "4294967296");  // > UINT32_MAX
    SetEnv("HEARTBEAT_INTERVAL_SECONDS", "-10");         // negative

    auto config = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(config.heartbeat_interval_seconds, 10u);  // fallback to default
}

TEST_F(FileStorageTest, EmptyEnvValueUsesDefault) {
    SetEnv("BUCKET_KEYS_LIMIT", "");  // empty string

    auto config = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(config.bucket_keys_limit, 500);  // fallback
}

TEST_F(FileStorageTest, ValidateSuccessWithValidConfig) {
    FileStorageConfig config;
    config.storage_filepath = "/valid/path";
    config.bucket_keys_limit = 100;
    config.bucket_size_limit = 100000;
    config.total_keys_limit = 1000000;
    config.total_size_limit = 1073741824;  // 1GB
    config.heartbeat_interval_seconds = 5;

    EXPECT_TRUE(config.Validate());
}

TEST_F(FileStorageTest, ValidateFailsOnEmptyStoragePath) {
    FileStorageConfig config;
    config.storage_filepath = "";
    EXPECT_FALSE(config.Validate());
}

TEST_F(FileStorageTest, ValidateFailsOnInvalidLimits) {
    FileStorageConfig config;
    config.storage_filepath = "/tmp";

    config.bucket_keys_limit = 0;
    EXPECT_FALSE(config.Validate());

    config.bucket_keys_limit = 1;
    config.bucket_size_limit = 0;
    EXPECT_FALSE(config.Validate());

    config.bucket_size_limit = 1;
    config.total_keys_limit = 0;
    EXPECT_FALSE(config.Validate());

    config.total_keys_limit = 1;
    config.total_size_limit = 0;
    EXPECT_FALSE(config.Validate());

    config.total_size_limit = 1;
    config.heartbeat_interval_seconds = 0;
    EXPECT_FALSE(config.Validate());
}

}  // namespace mooncake::test