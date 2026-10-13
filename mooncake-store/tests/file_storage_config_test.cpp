#include <glog/logging.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "environ.h"
#include "storage_backend.h"

namespace mooncake {

namespace {

void ExpectDefaultFileStorageConfig(const FileStorageConfig& config) {
    EXPECT_EQ(config.storage_backend_type, StorageBackendType::kBucket);
    EXPECT_EQ(config.storage_filepath, "/data/file_storage");
    EXPECT_EQ(config.local_buffer_size, 1280 * 1024 * 1024);
    EXPECT_EQ(config.pinned_restore_arena_size, 0);
    EXPECT_EQ(config.scanmeta_iterator_keys_limit, 20000);
    EXPECT_EQ(config.total_keys_limit, 10'000'000);
    EXPECT_EQ(config.total_size_limit, 2ULL * 1024 * 1024 * 1024 * 1024);
    EXPECT_EQ(config.heartbeat_interval_seconds, 10u);
    EXPECT_EQ(config.client_buffer_gc_interval_seconds, 1u);
    EXPECT_EQ(config.client_buffer_gc_ttl_ms, 5000u);
    EXPECT_FALSE(config.use_uring);
    EXPECT_FALSE(config.enable_dfs);
    EXPECT_TRUE(config.enable_disk_watermark_eviction);
    EXPECT_DOUBLE_EQ(config.disk_eviction_high_watermark_ratio, 0.90);
    EXPECT_DOUBLE_EQ(config.disk_eviction_low_watermark_ratio, 0.80);
}

class FileStorageConfigTest : public ::testing::Test {
   protected:
    MapEnvironSource source_;
    std::filesystem::path data_path;

    FileStorageConfig Load() const {
        return FileStorageConfig::FromEnvironment(Environ(source_));
    }

    void SetUp() override {
        google::InitGoogleLogging("FileStorageConfigTest");
        FLAGS_logtostderr = true;
        data_path =
            std::filesystem::current_path() / "file_storage_config_data";
        std::filesystem::create_directories(data_path);
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        std::error_code ec;
        std::filesystem::remove_all(data_path, ec);
    }
};

TEST_F(FileStorageConfigTest, DefaultValuesWhenNoEnvSet) {
    const auto config = Load();

    ExpectDefaultFileStorageConfig(config);
}

TEST_F(FileStorageConfigTest, ReadsValidValues) {
    source_.Set("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
                "distributed_storage_backend");
    source_.Set("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", "/tmp/storage");
    source_.Set("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", "2147483648");
    source_.Set("MC_STORE_PINNED_RESTORE_ARENA_SIZE_BYTES", "67108864");
    source_.Set("MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT", "12345");
    source_.Set("MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT", "5000000");
    source_.Set("MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES", "1099511627776");
    source_.Set("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "5");
    source_.Set("MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS", "7");
    source_.Set("MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS", "9000");
    source_.Set("MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION", "0");
    source_.Set("MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO", "0.75");
    source_.Set("MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO", "0.50");
    source_.Set("MOONCAKE_OFFLOAD_USE_URING", "1");

    const auto config = Load();
    EXPECT_EQ(config.storage_backend_type, StorageBackendType::kDistributed);
    EXPECT_FALSE(config.enable_dfs);
    EXPECT_EQ(config.storage_filepath, "/tmp/storage");
    EXPECT_EQ(config.local_buffer_size, 2147483648);
    EXPECT_EQ(config.pinned_restore_arena_size, 64 * 1024 * 1024);
    EXPECT_EQ(config.scanmeta_iterator_keys_limit, 12345);
    EXPECT_EQ(config.total_keys_limit, 5000000);
    EXPECT_EQ(config.total_size_limit, 1099511627776);
    EXPECT_EQ(config.heartbeat_interval_seconds, 5u);
    EXPECT_EQ(config.client_buffer_gc_interval_seconds, 7u);
    EXPECT_EQ(config.client_buffer_gc_ttl_ms, 9000u);
    EXPECT_FALSE(config.enable_disk_watermark_eviction);
    EXPECT_DOUBLE_EQ(config.disk_eviction_high_watermark_ratio, 0.75);
    EXPECT_DOUBLE_EQ(config.disk_eviction_low_watermark_ratio, 0.50);
    EXPECT_TRUE(config.use_uring);
}

TEST_F(FileStorageConfigTest, PreservesAliasPrecedence) {
    source_.Set("MOONCAKE_SCANMETA_ITERATOR_KEYS_LIMIT", "111");
    source_.Set("MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO", "0.77");
    source_.Set("MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO", "0.55");
    source_.Set("MOONCAKE_USE_URING", "true");

    const auto fallback = Load();
    EXPECT_EQ(fallback.scanmeta_iterator_keys_limit, 111);
    EXPECT_DOUBLE_EQ(fallback.disk_eviction_high_watermark_ratio, 0.77);
    EXPECT_DOUBLE_EQ(fallback.disk_eviction_low_watermark_ratio, 0.55);
    EXPECT_TRUE(fallback.use_uring);

    source_.Set("MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT", "222");
    source_.Set("MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO", "0.75");
    source_.Set("MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO", "0.50");
    source_.Set("MOONCAKE_OFFLOAD_USE_URING", "false");

    const auto preferred = Load();
    EXPECT_EQ(preferred.scanmeta_iterator_keys_limit, 222);
    EXPECT_DOUBLE_EQ(preferred.disk_eviction_high_watermark_ratio, 0.75);
    EXPECT_DOUBLE_EQ(preferred.disk_eviction_low_watermark_ratio, 0.50);
    EXPECT_FALSE(preferred.use_uring);
}

TEST_F(FileStorageConfigTest, PreservesEmptyPreferredAliasBehavior) {
    source_.Set("MOONCAKE_SCANMETA_ITERATOR_KEYS_LIMIT", "111");
    source_.Set("MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO", "0.77");
    source_.Set("MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO", "0.55");
    source_.Set("MOONCAKE_USE_URING", "true");
    source_.Set("MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT", "");
    source_.Set("MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO", "");
    source_.Set("MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO", "");
    source_.Set("MOONCAKE_OFFLOAD_USE_URING", "");

    const auto config = Load();
    EXPECT_EQ(config.scanmeta_iterator_keys_limit, 111);
    EXPECT_DOUBLE_EQ(config.disk_eviction_high_watermark_ratio, 0.77);
    EXPECT_DOUBLE_EQ(config.disk_eviction_low_watermark_ratio, 0.55);
    EXPECT_FALSE(config.use_uring);
}

TEST_F(FileStorageConfigTest, PreservesInvalidPreferredAliasBehavior) {
    source_.Set("MOONCAKE_SCANMETA_ITERATOR_KEYS_LIMIT", "111");
    source_.Set("MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO", "0.77");
    source_.Set("MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO", "0.55");
    source_.Set("MOONCAKE_USE_URING", "true");
    source_.Set("MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT", "invalid");
    source_.Set("MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO",
                "invalid");
    source_.Set("MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO",
                "invalid");
    source_.Set("MOONCAKE_OFFLOAD_USE_URING", "invalid");

    const auto config = Load();
    EXPECT_EQ(config.scanmeta_iterator_keys_limit, 111);
    EXPECT_DOUBLE_EQ(config.disk_eviction_high_watermark_ratio, 0.90);
    EXPECT_DOUBLE_EQ(config.disk_eviction_low_watermark_ratio, 0.80);
    EXPECT_FALSE(config.use_uring);
}

TEST_F(FileStorageConfigTest, InvalidValuesUseDefaultsAndPreserveDiagnostics) {
    source_.Set("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", "invalid");
    source_.Set("MC_STORE_PINNED_RESTORE_ARENA_SIZE_BYTES", "invalid");
    source_.Set("MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT", "invalid");
    source_.Set("MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT", "invalid");
    source_.Set("MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES", "invalid");
    source_.Set("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "invalid");
    source_.Set("MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS",
                "invalid");
    source_.Set("MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS", "invalid");
    source_.Set("MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION", "invalid");
    source_.Set("MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO",
                "0.75suffix");
    source_.Set("MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO", "nan");
    source_.Set("MOONCAKE_OFFLOAD_USE_URING", "invalid");

    ::testing::internal::CaptureStderr();
    const auto config = Load();
    const std::string logs = ::testing::internal::GetCapturedStderr();

    ExpectDefaultFileStorageConfig(config);
    for (const char* name : {
             "MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES",
             "MC_STORE_PINNED_RESTORE_ARENA_SIZE_BYTES",
             "MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT",
             "MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT",
             "MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES",
             "MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS",
             "MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS",
             "MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS",
         }) {
        EXPECT_NE(logs.find(name), std::string::npos) << name;
    }
    EXPECT_EQ(logs.find("MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION"),
              std::string::npos);
    EXPECT_EQ(logs.find("MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO"),
              std::string::npos);
    EXPECT_EQ(logs.find("MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO"),
              std::string::npos);
    EXPECT_EQ(logs.find("MOONCAKE_OFFLOAD_USE_URING"), std::string::npos);
}

TEST_F(FileStorageConfigTest, InvalidIntValueUsesDefault) {
    source_.Set("MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES", "sdfsdf");
    source_.Set("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "-1");

    const auto config = Load();

    EXPECT_EQ(config.total_size_limit, 2ULL * 1024 * 1024 * 1024 * 1024);
    EXPECT_EQ(config.heartbeat_interval_seconds, 10u);
    EXPECT_DOUBLE_EQ(config.disk_eviction_high_watermark_ratio, 0.90);
    EXPECT_DOUBLE_EQ(config.disk_eviction_low_watermark_ratio, 0.80);
}

TEST_F(FileStorageConfigTest, OutOfRangeValueUsesDefault) {
    source_.Set("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "4294967296");
    const auto too_large = Load();
    EXPECT_EQ(too_large.heartbeat_interval_seconds, 10u);

    source_.Set("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "-10");
    const auto negative = Load();
    EXPECT_EQ(negative.heartbeat_interval_seconds, 10u);
}

TEST_F(FileStorageConfigTest, ValidateSuccessWithValidConfig) {
    FileStorageConfig config;
    config.storage_filepath = data_path.string();
    config.total_keys_limit = 1000000;
    config.total_size_limit = 1073741824;
    config.heartbeat_interval_seconds = 5;

    EXPECT_TRUE(config.Validate());
}

TEST_F(FileStorageConfigTest, ValidateFailsOnInvalidStoragePath) {
    FileStorageConfig config;
    config.storage_filepath = "";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "   ";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "relative/path";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "./data";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "../data";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "/valid/../invalid";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "/path/./sub";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "/tmp/this_directory_does_not_exist_12345";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = data_path.string();
    EXPECT_TRUE(config.Validate());
}

TEST_F(FileStorageConfigTest, ValidateFailsOnInvalidLimits) {
    FileStorageConfig config;
    config.storage_filepath = "/tmp";

    config.total_keys_limit = 0;
    EXPECT_FALSE(config.Validate());

    config.total_keys_limit = 1;
    config.total_size_limit = 0;
    EXPECT_FALSE(config.Validate());

    config.total_size_limit = 1;
    config.pinned_restore_arena_size = -1;
    EXPECT_FALSE(config.Validate());

    config.pinned_restore_arena_size = 0;
    config.heartbeat_interval_seconds = 0;
    EXPECT_FALSE(config.Validate());

    config.heartbeat_interval_seconds = 1;
    config.disk_eviction_low_watermark_ratio = 0.9;
    config.disk_eviction_high_watermark_ratio = 0.8;
    EXPECT_FALSE(config.Validate());

    config.disk_eviction_low_watermark_ratio = 0.0;
    config.disk_eviction_high_watermark_ratio = 0.8;
    EXPECT_FALSE(config.Validate());

    config.disk_eviction_low_watermark_ratio = 0.8;
    config.disk_eviction_high_watermark_ratio = 1.1;
    EXPECT_FALSE(config.Validate());
}

}  // namespace

}  // namespace mooncake
