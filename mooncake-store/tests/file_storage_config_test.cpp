#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>

#include "storage_backend.h"

namespace mooncake {

namespace {

void SetEnv(const std::string& key, const std::string& value) {
    setenv(key.c_str(), value.c_str(), 1);
}

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

    void Set(const char* value) { setenv(name_.c_str(), value, 1); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

struct FileStorageEnvironment {
    ScopedEnvVar storage_backend_descriptor{
        "MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR"};
    ScopedEnvVar storage_path{"MOONCAKE_OFFLOAD_FILE_STORAGE_PATH"};
    ScopedEnvVar local_buffer_size{"MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES"};
    ScopedEnvVar pinned_restore_arena_size{
        "MC_STORE_PINNED_RESTORE_ARENA_SIZE_BYTES"};
    ScopedEnvVar scanmeta_iterator_keys_limit{
        "MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT"};
    ScopedEnvVar legacy_scanmeta_iterator_keys_limit{
        "MOONCAKE_SCANMETA_ITERATOR_KEYS_LIMIT"};
    ScopedEnvVar total_keys_limit{"MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT"};
    ScopedEnvVar total_size_limit{"MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES"};
    ScopedEnvVar heartbeat_interval{
        "MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS"};
    ScopedEnvVar client_buffer_gc_interval{
        "MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS"};
    ScopedEnvVar client_buffer_gc_ttl{
        "MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS"};
    ScopedEnvVar enable_disk_watermark_eviction{
        "MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION"};
    ScopedEnvVar disk_eviction_high_watermark_ratio{
        "MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO"};
    ScopedEnvVar legacy_disk_eviction_high_watermark_ratio{
        "MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO"};
    ScopedEnvVar disk_eviction_low_watermark_ratio{
        "MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO"};
    ScopedEnvVar legacy_disk_eviction_low_watermark_ratio{
        "MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO"};
    ScopedEnvVar use_uring{"MOONCAKE_OFFLOAD_USE_URING"};
    ScopedEnvVar legacy_use_uring{"MOONCAKE_USE_URING"};
};

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
    FileStorageEnvironment env;
    ScopedEnvVar bucket_keys_limit{"MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT"};
    ScopedEnvVar bucket_size_limit{"MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES"};
    std::filesystem::path data_path;

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
    const auto config = FileStorageConfig::FromEnvironment();
    const auto bucket_backend_config = BucketBackendConfig::FromEnvironment();

    ExpectDefaultFileStorageConfig(config);
    EXPECT_EQ(bucket_backend_config.bucket_keys_limit, 500);
    EXPECT_EQ(bucket_backend_config.bucket_size_limit, 256 * 1024 * 1024);
}

TEST_F(FileStorageConfigTest, ReadsValidValues) {
    env.storage_backend_descriptor.Set("distributed_storage_backend");
    env.storage_path.Set("/tmp/storage");
    env.local_buffer_size.Set("2147483648");
    env.pinned_restore_arena_size.Set("67108864");
    env.scanmeta_iterator_keys_limit.Set("12345");
    env.total_keys_limit.Set("5000000");
    env.total_size_limit.Set("1099511627776");
    env.heartbeat_interval.Set("5");
    env.client_buffer_gc_interval.Set("7");
    env.client_buffer_gc_ttl.Set("9000");
    env.enable_disk_watermark_eviction.Set("0");
    env.disk_eviction_high_watermark_ratio.Set("0.75");
    env.disk_eviction_low_watermark_ratio.Set("0.50");
    env.use_uring.Set("1");

    const auto config = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(config.storage_backend_type, StorageBackendType::kDistributed);
    EXPECT_TRUE(config.enable_dfs);
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

TEST_F(FileStorageConfigTest, ReadsBucketBackendValues) {
    bucket_keys_limit.Set("1000");
    bucket_size_limit.Set("536870912");

    const auto config = BucketBackendConfig::FromEnvironment();
    EXPECT_EQ(config.bucket_keys_limit, 1000);
    EXPECT_EQ(config.bucket_size_limit, 512 * 1024 * 1024);
}

TEST_F(FileStorageConfigTest, PreservesAliasPrecedence) {
    env.legacy_scanmeta_iterator_keys_limit.Set("111");
    env.legacy_disk_eviction_high_watermark_ratio.Set("0.77");
    env.legacy_disk_eviction_low_watermark_ratio.Set("0.55");
    env.legacy_use_uring.Set("true");

    const auto fallback = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(fallback.scanmeta_iterator_keys_limit, 111);
    EXPECT_DOUBLE_EQ(fallback.disk_eviction_high_watermark_ratio, 0.77);
    EXPECT_DOUBLE_EQ(fallback.disk_eviction_low_watermark_ratio, 0.55);
    EXPECT_TRUE(fallback.use_uring);

    env.scanmeta_iterator_keys_limit.Set("222");
    env.disk_eviction_high_watermark_ratio.Set("0.75");
    env.disk_eviction_low_watermark_ratio.Set("0.50");
    env.use_uring.Set("false");

    const auto preferred = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(preferred.scanmeta_iterator_keys_limit, 222);
    EXPECT_DOUBLE_EQ(preferred.disk_eviction_high_watermark_ratio, 0.75);
    EXPECT_DOUBLE_EQ(preferred.disk_eviction_low_watermark_ratio, 0.50);
    EXPECT_FALSE(preferred.use_uring);
}

TEST_F(FileStorageConfigTest, PreservesEmptyPreferredAliasBehavior) {
    env.legacy_scanmeta_iterator_keys_limit.Set("111");
    env.legacy_disk_eviction_high_watermark_ratio.Set("0.77");
    env.legacy_disk_eviction_low_watermark_ratio.Set("0.55");
    env.legacy_use_uring.Set("true");
    env.scanmeta_iterator_keys_limit.Set("");
    env.disk_eviction_high_watermark_ratio.Set("");
    env.disk_eviction_low_watermark_ratio.Set("");
    env.use_uring.Set("");

    const auto config = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(config.scanmeta_iterator_keys_limit, 111);
    EXPECT_DOUBLE_EQ(config.disk_eviction_high_watermark_ratio, 0.77);
    EXPECT_DOUBLE_EQ(config.disk_eviction_low_watermark_ratio, 0.55);
    EXPECT_FALSE(config.use_uring);
}

TEST_F(FileStorageConfigTest, PreservesInvalidPreferredAliasBehavior) {
    env.legacy_scanmeta_iterator_keys_limit.Set("111");
    env.legacy_disk_eviction_high_watermark_ratio.Set("0.77");
    env.legacy_disk_eviction_low_watermark_ratio.Set("0.55");
    env.legacy_use_uring.Set("true");
    env.scanmeta_iterator_keys_limit.Set("invalid");
    env.disk_eviction_high_watermark_ratio.Set("invalid");
    env.disk_eviction_low_watermark_ratio.Set("invalid");
    env.use_uring.Set("invalid");

    const auto config = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(config.scanmeta_iterator_keys_limit, 111);
    EXPECT_DOUBLE_EQ(config.disk_eviction_high_watermark_ratio, 0.90);
    EXPECT_DOUBLE_EQ(config.disk_eviction_low_watermark_ratio, 0.80);
    EXPECT_FALSE(config.use_uring);
}

TEST_F(FileStorageConfigTest, InvalidValuesUseDefaultsAndPreserveDiagnostics) {
    env.local_buffer_size.Set("invalid");
    env.pinned_restore_arena_size.Set("invalid");
    env.scanmeta_iterator_keys_limit.Set("invalid");
    env.total_keys_limit.Set("invalid");
    env.total_size_limit.Set("invalid");
    env.heartbeat_interval.Set("invalid");
    env.client_buffer_gc_interval.Set("invalid");
    env.client_buffer_gc_ttl.Set("invalid");
    env.enable_disk_watermark_eviction.Set("invalid");
    env.disk_eviction_high_watermark_ratio.Set("0.75suffix");
    env.disk_eviction_low_watermark_ratio.Set("nan");
    env.use_uring.Set("invalid");

    ::testing::internal::CaptureStderr();
    const auto config = FileStorageConfig::FromEnvironment();
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
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "abc");
    SetEnv("MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES", "sdfsdf");
    SetEnv("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "-1");

    const auto config = FileStorageConfig::FromEnvironment();
    const auto bucket_backend_config = BucketBackendConfig::FromEnvironment();

    EXPECT_EQ(bucket_backend_config.bucket_keys_limit, 500);
    EXPECT_EQ(config.total_size_limit, 2ULL * 1024 * 1024 * 1024 * 1024);
    EXPECT_EQ(config.heartbeat_interval_seconds, 10u);
    EXPECT_DOUBLE_EQ(config.disk_eviction_high_watermark_ratio, 0.90);
    EXPECT_DOUBLE_EQ(config.disk_eviction_low_watermark_ratio, 0.80);
}

TEST_F(FileStorageConfigTest, OutOfRangeValueUsesDefault) {
    SetEnv("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "4294967296");
    const auto too_large = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(too_large.heartbeat_interval_seconds, 10u);

    SetEnv("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "-10");
    const auto negative = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(negative.heartbeat_interval_seconds, 10u);
}

TEST_F(FileStorageConfigTest, EmptyEnvValueUsesDefault) {
    bucket_keys_limit.Set("");

    const auto config = BucketBackendConfig::FromEnvironment();
    EXPECT_EQ(config.bucket_keys_limit, 500);
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
