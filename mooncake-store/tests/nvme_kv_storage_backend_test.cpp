#include "nvme_kv_backend.h"
#include "nvme_kv_executor_util.h"
#include "storage_backend.h"

#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

namespace mooncake::test {

namespace {

class EnvVarGuard {
   public:
    EnvVarGuard(const char *name, const char *value) : name_(name) {
        if (const char *old_value = getenv(name)) {
            old_value_ = old_value;
        }
        setenv(name, value, 1);
    }

    ~EnvVarGuard() {
        if (old_value_.has_value()) {
            setenv(name_.c_str(), old_value_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::optional<std::string> old_value_;
};

class NvmeKvStorageBackendTest : public ::testing::Test {
   protected:
    void SetUp() override {
        data_path_ = (fs::current_path() / "nvme_kv_test_data").string();
        std::error_code ec;
        fs::remove_all(data_path_, ec);
        ASSERT_FALSE(ec) << ec.message();
        fs::create_directories(data_path_, ec);
        ASSERT_FALSE(ec) << ec.message();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(data_path_, ec);
    }

    std::string data_path_;
};

}  // namespace

TEST_F(NvmeKvStorageBackendTest, PhysicalKeyPackingEncodesCommandSet) {
    NvmeKvCommandExecutor::PhysicalKey key{};
    for (size_t i = 0; i < key.size(); ++i) {
        key[i] = static_cast<uint8_t>(0x10 + i);
    }

    const auto fields = PackNvmeKvPhysicalKey(key);
    uint32_t expected_cdw14 = 0;
    std::memcpy(&expected_cdw14, key.data() + 8, sizeof(expected_cdw14));
    expected_cdw14 &= 0x00FFFFFFu;
    expected_cdw14 |= static_cast<uint32_t>(kNvmeKvCommandSetIdentifier) << 24;

    EXPECT_EQ(fields.cdw14, expected_cdw14);
    EXPECT_EQ(static_cast<uint8_t>(fields.cdw14 >> 24),
              kNvmeKvCommandSetIdentifier);
}

TEST_F(NvmeKvStorageBackendTest, StatusMappingHandlesKvSpecificStatusCodes) {
    EXPECT_EQ(MapNvmeKvStatus(0x81, true), ErrorCode::KEYS_ULTRA_LIMIT);
    EXPECT_EQ(MapNvmeKvStatus(0x85, true), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(MapNvmeKvStatus(0x86, false), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(MapNvmeKvStatus(0x87, false), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(MapNvmeKvStatus(0x89, true), ErrorCode::OBJECT_ALREADY_EXISTS);

    EXPECT_EQ(MapNvmeKvTransportError(ENOENT, false),
              ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(MapNvmeKvTransportError(ENOSPC, true),
              ErrorCode::KEYS_ULTRA_LIMIT);
    EXPECT_EQ(MapNvmeKvTransportError(ENOMEM, false),
              ErrorCode::BUFFER_OVERFLOW);
    EXPECT_EQ(MapNvmeKvTransportError(0x4087, false),
              ErrorCode::OBJECT_NOT_FOUND);

    constexpr uint32_t kRawValueSize = 1234;
    std::array<char, 4096> raw_value{};
    EXPECT_EQ(
        ResolveNvmeKvInitialRetrieveBytes(kRawValueSize, raw_value.size()),
        RoundUpToNvmeKvTransferBytes(kRawValueSize));
    EXPECT_EQ(ResolveNvmeKvRetrievedValueSize(raw_value.data(), 0,
                                              raw_value.size(), kRawValueSize),
              kRawValueSize);
    EXPECT_EQ(ResolveNvmeKvRetrievedValueSize(raw_value.data(), 0, 1024,
                                              kRawValueSize),
              0);

    EXPECT_TRUE(ShouldRetryNvmeKvRetrieveWithMaxBuffer(
        ErrorCode::INVALID_PARAMS, 0, kDefaultNvmeKvTransferAlignmentBytes,
        kDefaultNvmeKvRuntimeTransferLimit));
    EXPECT_FALSE(ShouldRetryNvmeKvRetrieveWithMaxBuffer(
        ErrorCode::INVALID_PARAMS, kDefaultNvmeKvTransferAlignmentBytes,
        kDefaultNvmeKvTransferAlignmentBytes,
        kDefaultNvmeKvRuntimeTransferLimit));
    EXPECT_FALSE(ShouldRetryNvmeKvRetrieveWithMaxBuffer(
        ErrorCode::OBJECT_NOT_FOUND, 0, kDefaultNvmeKvTransferAlignmentBytes,
        kDefaultNvmeKvRuntimeTransferLimit));
    EXPECT_FALSE(ShouldRetryNvmeKvRetrieveWithMaxBuffer(
        ErrorCode::INTERNAL_ERROR, 0, kDefaultNvmeKvTransferAlignmentBytes,
        kDefaultNvmeKvRuntimeTransferLimit));
    EXPECT_FALSE(ShouldRetryNvmeKvRetrieveWithMaxBuffer(
        ErrorCode::INVALID_PARAMS, 0, kDefaultNvmeKvRuntimeTransferLimit,
        kDefaultNvmeKvRuntimeTransferLimit));
}

TEST_F(NvmeKvStorageBackendTest, BackendLoadsKnownObjectsFromDevice) {
    EnvVarGuard driver_guard("MOONCAKE_NVME_KV_DRIVER", "stub");

    FileStorageConfig config;
    config.storage_filepath = data_path_ + "/known_objects";
    config.storage_backend_type = StorageBackendType::kNvmeKv;

    constexpr int kObjectCount = 9;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kObjectCount);
    values.reserve(kObjectCount);
    std::unordered_map<std::string, std::vector<Slice>> batch;
    for (int i = 0; i < kObjectCount; ++i) {
        keys.emplace_back("nvme_kv_known_key_" + std::to_string(i));
        const size_t value_size = i == 0 ? 4096 : 128 * 1024 + 4096 + i * 17;
        values.emplace_back(value_size, static_cast<char>('a' + (i % 26)));
        batch.emplace(keys.back(),
                      std::vector<Slice>{
                          Slice{values.back().data(), values.back().size()}});
    }

    {
        NvmeKvStorageBackend backend(config);
        ASSERT_TRUE(backend.Init().has_value());
        auto offload_result = backend.BatchOffload(
            batch,
            [](const std::vector<std::string> &,
               std::vector<StorageObjectMetadata> &) { return ErrorCode::OK; });
        ASSERT_TRUE(offload_result.has_value());
        ASSERT_EQ(offload_result.value(), kObjectCount);
    }

    NvmeKvStorageBackend reader(config);
    ASSERT_TRUE(reader.Init().has_value());
    for (const auto &key : keys) {
        EXPECT_TRUE(reader.IsExist(key).value_or(false));
    }
    EXPECT_FALSE(reader.IsExist("nvme_kv_missing_key").value_or(true));

    auto duplicate_result = reader.BatchOffload(
        batch,
        [](const std::vector<std::string> &,
           std::vector<StorageObjectMetadata> &) { return ErrorCode::OK; });
    ASSERT_TRUE(duplicate_result.has_value());
    EXPECT_EQ(duplicate_result.value(), kObjectCount);

    std::vector<std::string> loaded_values;
    loaded_values.reserve(kObjectCount);
    std::unordered_map<std::string, Slice> load_batch;
    for (int i = 0; i < kObjectCount; ++i) {
        loaded_values.emplace_back(values[i].size(), '\0');
        load_batch.emplace(keys[i], Slice{loaded_values.back().data(),
                                          loaded_values.back().size()});
    }
    ASSERT_TRUE(reader.BatchLoad(load_batch).has_value());
    EXPECT_EQ(loaded_values, values);
}

}  // namespace mooncake::test
