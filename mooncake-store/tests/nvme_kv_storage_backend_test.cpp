#include "nvme_kv_backend.h"
#include "nvme_kv_executor_util.h"
#include "storage_backend.h"

#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
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

void WriteFile(const fs::path &path, const std::string &contents) {
    std::ofstream stream(path);
    ASSERT_TRUE(stream.is_open());
    stream << contents;
}

}  // namespace

TEST_F(NvmeKvStorageBackendTest, ResolvesNofEndpointToKernelDevices) {
    const fs::path sysfs_root = fs::path(data_path_) / "sys" / "class" / "nvme";
    const fs::path dev_root = fs::path(data_path_) / "dev";
    const fs::path controller_path = sysfs_root / "nvme7";
    const fs::path namespace_path = controller_path / "nvme7n3";
    ASSERT_TRUE(fs::create_directories(namespace_path));
    ASSERT_TRUE(fs::create_directories(dev_root));

    WriteFile(controller_path / "address",
              "trtype=rdma,traddr=192.168.65.56,trsvcid=4420,adrfam=ipv4\n");
    WriteFile(controller_path / "subsysnqn", "nqn.2016-06.io.spdk:cnode1\n");
    WriteFile(namespace_path / "nsid", "3\n");
    WriteFile(dev_root / "nvme7n3", "");
    WriteFile(dev_root / "ng7n3", "");

    const std::string sysfs_root_string = sysfs_root.string();
    const std::string dev_root_string = dev_root.string();
    EnvVarGuard sysfs_guard("MOONCAKE_NVME_KV_SYSFS_ROOT",
                            sysfs_root_string.c_str());
    EnvVarGuard dev_guard("MOONCAKE_NVME_KV_DEV_ROOT", dev_root_string.c_str());

    const std::string endpoint =
        "traddr:192.168.65.56 trsvcid:4420 "
        "subnqn:nqn.2016-06.io.spdk:cnode1 trtype:RDMA adrfam:IPv4 ns:3";

    auto block_path = ResolveNvmeKvDevicePath(
        endpoint, NvmeKvDevicePathType::kBlockNamespace, 1);
    ASSERT_TRUE(block_path.has_value());
    EXPECT_EQ((dev_root / "nvme7n3").string(), block_path->path);
    EXPECT_EQ(3, block_path->nsid);

    auto generic_path = ResolveNvmeKvDevicePath(
        endpoint, NvmeKvDevicePathType::kGenericCharacter, 1);
    ASSERT_TRUE(generic_path.has_value());
    EXPECT_EQ((dev_root / "ng7n3").string(), generic_path->path);
    EXPECT_EQ(3, generic_path->nsid);

    const std::string equals_endpoint =
        "traddr=192.168.65.56 trsvcid=4420 "
        "subnqn=nqn.2016-06.io.spdk:cnode1 trtype=rdma adrfam=ipv4 nsid=3";
    auto equals_path = ResolveNvmeKvDevicePath(
        equals_endpoint, NvmeKvDevicePathType::kGenericCharacter, 1);
    ASSERT_TRUE(equals_path.has_value());
    EXPECT_EQ((dev_root / "ng7n3").string(), equals_path->path);
    EXPECT_EQ(3, equals_path->nsid);
}

TEST_F(NvmeKvStorageBackendTest, UsesConfiguredNsidWhenEndpointOmitsNsid) {
    const fs::path sysfs_root = fs::path(data_path_) / "sys" / "class" / "nvme";
    const fs::path dev_root = fs::path(data_path_) / "dev";
    const fs::path controller_path = sysfs_root / "nvme7";
    const fs::path namespace_path = controller_path / "nvme7n3";
    ASSERT_TRUE(fs::create_directories(namespace_path));
    ASSERT_TRUE(fs::create_directories(dev_root));

    WriteFile(controller_path / "address",
              "trtype=rdma,traddr=192.168.65.56,trsvcid=4420,adrfam=ipv4\n");
    WriteFile(controller_path / "subsysnqn", "nqn.2016-06.io.spdk:cnode1\n");
    WriteFile(namespace_path / "nsid", "3\n");
    WriteFile(dev_root / "ng7n3", "");

    const std::string sysfs_root_string = sysfs_root.string();
    const std::string dev_root_string = dev_root.string();
    EnvVarGuard sysfs_guard("MOONCAKE_NVME_KV_SYSFS_ROOT",
                            sysfs_root_string.c_str());
    EnvVarGuard dev_guard("MOONCAKE_NVME_KV_DEV_ROOT", dev_root_string.c_str());

    auto path = ResolveNvmeKvDevicePath(
        "traddr:192.168.65.56 subnqn:nqn.2016-06.io.spdk:cnode1",
        NvmeKvDevicePathType::kGenericCharacter, 3);
    ASSERT_TRUE(path.has_value());
    EXPECT_EQ((dev_root / "ng7n3").string(), path->path);
    EXPECT_EQ(3, path->nsid);
}

TEST_F(NvmeKvStorageBackendTest, RejectsInvalidNofNamespaceId) {
    const fs::path sysfs_root = fs::path(data_path_) / "sys" / "class" / "nvme";
    const fs::path dev_root = fs::path(data_path_) / "dev";
    const fs::path controller_path = sysfs_root / "nvme7";
    const fs::path namespace_path = controller_path / "nvme7n3";
    ASSERT_TRUE(fs::create_directories(namespace_path));
    ASSERT_TRUE(fs::create_directories(dev_root));

    WriteFile(controller_path / "address",
              "trtype=rdma,traddr=192.168.65.56,trsvcid=4420,adrfam=ipv4\n");
    WriteFile(controller_path / "subsysnqn", "nqn.2016-06.io.spdk:cnode1\n");
    WriteFile(namespace_path / "nsid", "3\n");
    WriteFile(dev_root / "ng7n3", "");

    const std::string sysfs_root_string = sysfs_root.string();
    const std::string dev_root_string = dev_root.string();
    EnvVarGuard sysfs_guard("MOONCAKE_NVME_KV_SYSFS_ROOT",
                            sysfs_root_string.c_str());
    EnvVarGuard dev_guard("MOONCAKE_NVME_KV_DEV_ROOT", dev_root_string.c_str());

    auto path = ResolveNvmeKvDevicePath(
        "traddr:192.168.65.56 subnqn:nqn.2016-06.io.spdk:cnode1 ns:bad",
        NvmeKvDevicePathType::kGenericCharacter, 1);
    ASSERT_FALSE(path.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, path.error());
}

TEST_F(NvmeKvStorageBackendTest, RejectsUnmatchedNofEndpoint) {
    const fs::path sysfs_root =
        fs::path(data_path_) / "empty-sys" / "class" / "nvme";
    ASSERT_TRUE(fs::create_directories(sysfs_root));
    const std::string sysfs_root_string = sysfs_root.string();
    EnvVarGuard sysfs_guard("MOONCAKE_NVME_KV_SYSFS_ROOT",
                            sysfs_root_string.c_str());

    auto path = ResolveNvmeKvDevicePath(
        "traddr:192.168.65.57 subnqn:nqn.2016-06.io.spdk:cnode1",
        NvmeKvDevicePathType::kGenericCharacter, 1);
    ASSERT_FALSE(path.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, path.error());
}

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

TEST_F(NvmeKvStorageBackendTest, PipelinedStubRoundTripChunkedObjects) {
    EnvVarGuard driver_guard("MOONCAKE_NVME_KV_DRIVER", "stub");
    EnvVarGuard queue_depth_guard("MOONCAKE_NVME_KV_QUEUE_DEPTH", "8");
    EnvVarGuard runtime_limit_guard("MOONCAKE_NVME_KV_RUNTIME_TRANSFER_LIMIT",
                                    "65536");
    EnvVarGuard io_concurrency_guard("MOONCAKE_NVME_KV_IO_CONCURRENCY", "8");
    EnvVarGuard batch_submit_guard("MOONCAKE_NVME_KV_BATCH_SUBMIT_CONCURRENCY",
                                   "3");
    EnvVarGuard root_submit_guard("MOONCAKE_NVME_KV_ROOT_SUBMIT_CONCURRENCY",
                                  "2");

    FileStorageConfig config;
    config.storage_filepath = data_path_ + "/pipelined";
    config.storage_backend_type = StorageBackendType::kNvmeKv;

    NvmeKvStorageBackend backend(config);
    ASSERT_TRUE(backend.Init().has_value());

    constexpr int kObjectCount = 24;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kObjectCount);
    values.reserve(kObjectCount);
    std::unordered_map<std::string, std::vector<Slice>> batch;
    for (int i = 0; i < kObjectCount; ++i) {
        keys.emplace_back("nvme_kv_pipelined_key_" + std::to_string(i));
        values.emplace_back(180 * 1024 + i * 4096,
                            static_cast<char>('A' + (i % 26)));
        batch.emplace(keys.back(),
                      std::vector<Slice>{
                          Slice{values.back().data(), values.back().size()}});
    }

    auto offload_result = backend.BatchOffload(
        batch,
        [](const std::vector<std::string> &,
           std::vector<StorageObjectMetadata> &) { return ErrorCode::OK; });
    ASSERT_TRUE(offload_result.has_value());
    EXPECT_EQ(offload_result.value(), kObjectCount);

    std::vector<std::string> loaded_values;
    loaded_values.reserve(kObjectCount);
    std::unordered_map<std::string, Slice> load_batch;
    for (int i = 0; i < kObjectCount; ++i) {
        loaded_values.emplace_back(values[i].size(), '\0');
        load_batch.emplace(keys[i], Slice{loaded_values.back().data(),
                                          loaded_values.back().size()});
    }
    ASSERT_TRUE(backend.BatchLoad(load_batch).has_value());
    EXPECT_EQ(loaded_values, values);
}

TEST_F(NvmeKvStorageBackendTest, ManifestCacheUpdatesAfterSameKeyRewrite) {
    EnvVarGuard driver_guard("MOONCAKE_NVME_KV_DRIVER", "stub");
    EnvVarGuard runtime_limit_guard("MOONCAKE_NVME_KV_RUNTIME_TRANSFER_LIMIT",
                                    "65536");

    FileStorageConfig config;
    config.storage_filepath = data_path_ + "/manifest_cache";
    config.storage_backend_type = StorageBackendType::kNvmeKv;

    NvmeKvStorageBackend backend(config);
    ASSERT_TRUE(backend.Init().has_value());

    const std::string key = "nvme_kv_manifest_cache_rewrite";
    std::string first_value(192 * 1024, 'x');
    std::string second_value(first_value.size(), 'y');

    auto store_value = [&](std::string &value) {
        std::unordered_map<std::string, std::vector<Slice>> batch;
        batch.emplace(key,
                      std::vector<Slice>{Slice{value.data(), value.size()}});
        auto offload_result = backend.BatchOffload(
            batch,
            [](const std::vector<std::string> &,
               std::vector<StorageObjectMetadata> &) { return ErrorCode::OK; });
        ASSERT_TRUE(offload_result.has_value());
        EXPECT_EQ(offload_result.value(), 1);
    };

    store_value(first_value);
    std::string loaded(first_value.size(), '\0');
    std::unordered_map<std::string, Slice> load_batch{
        {key, Slice{loaded.data(), loaded.size()}}};
    ASSERT_TRUE(backend.BatchLoad(load_batch).has_value());
    EXPECT_EQ(loaded, first_value);

    store_value(second_value);
    std::fill(loaded.begin(), loaded.end(), '\0');
    ASSERT_TRUE(backend.BatchLoad(load_batch).has_value());
    EXPECT_EQ(loaded, second_value);
}

}  // namespace mooncake::test
