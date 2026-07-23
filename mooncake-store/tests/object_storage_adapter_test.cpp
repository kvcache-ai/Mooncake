#include "storage/distributed/object_storage_adapter.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <unistd.h>

#include "storage/distributed/distributed_storage_backend.h"

namespace mooncake {
namespace {

class FakeObjectStorageAdapter : public ObjectStorageAdapter {
   public:
    std::map<std::string, std::string> objects;
    std::unordered_set<std::string> fail_keys;
    int init_calls = 0;
    int putv_calls = 0;
    int get_calls = 0;
    bool initialized = false;

    tl::expected<void, ErrorCode> Put(const std::string& key,
                                      std::span<const char> data) override {
        objects[key] = std::string(data.begin(), data.end());
        return {};
    }

    tl::expected<void, ErrorCode> PutV(const std::string& key, const iovec* iov,
                                       int iovcnt) override {
        ++putv_calls;
        if (fail_keys.contains(key)) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }

        std::string buffer;
        for (int i = 0; i < iovcnt; ++i) {
            buffer.append(static_cast<const char*>(iov[i].iov_base),
                          iov[i].iov_len);
        }
        objects[key] = std::move(buffer);
        return {};
    }

    tl::expected<size_t, ErrorCode> Get(const std::string& key, void* buf,
                                        size_t len) override {
        ++get_calls;
        auto it = objects.find(key);
        if (it == objects.end()) {
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        }

        size_t bytes_read = std::min(len, it->second.size());
        if (bytes_read > 0) {
            std::memcpy(buf, it->second.data(), bytes_read);
        }
        return bytes_read;
    }

    tl::expected<bool, ErrorCode> Exists(const std::string& key) override {
        return objects.contains(key);
    }

    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override {
        std::vector<KeyInfo> result;
        result.reserve(objects.size());
        for (const auto& [key, value] : objects) {
            result.push_back({key, value.size()});
        }
        return result;
    }

    tl::expected<void, ErrorCode> Init() override {
        ++init_calls;
        initialized = true;
        return {};
    }

    tl::expected<void, ErrorCode> Shutdown() override {
        initialized = false;
        return {};
    }

    const char* GetName() const override { return "fake-object-storage"; }
};

class FakeFileSystemAdapter : public FileSystemAdapter {
   public:
    bool initialized = false;

    tl::expected<size_t, ErrorCode> WriteFile(
        const std::string&, std::span<const char> data) override {
        return data.size();
    }

    tl::expected<size_t, ErrorCode> ReadFile(const std::string&, void*,
                                             size_t) override {
        return 0;
    }

    tl::expected<size_t, ErrorCode> VectorWriteFile(const std::string&,
                                                    const iovec* iov,
                                                    int iovcnt,
                                                    off_t) override {
        size_t total = 0;
        for (int i = 0; i < iovcnt; ++i) total += iov[i].iov_len;
        return total;
    }

    tl::expected<size_t, ErrorCode> VectorReadFile(const std::string&,
                                                   const iovec*, int,
                                                   off_t) override {
        return 0;
    }

    tl::expected<void, ErrorCode> DeleteFile(const std::string&) override {
        return {};
    }

    tl::expected<bool, ErrorCode> FileExists(const std::string&) override {
        return false;
    }

    tl::expected<std::vector<std::string>, ErrorCode> ListFiles(
        const std::string&) override {
        return std::vector<std::string>{};
    }

    tl::expected<void, ErrorCode> Init(const std::string&) override {
        initialized = true;
        return {};
    }

    tl::expected<void, ErrorCode> Shutdown() override {
        initialized = false;
        return {};
    }

    const char* GetName() const override { return "fake-filesystem"; }
};

class ObjectStorageAdapterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        root_dir_ = std::filesystem::temp_directory_path() /
                    ("mooncake_object_storage_adapter_test_" +
                     std::to_string(getpid()));
        std::error_code ec;
        std::filesystem::remove_all(root_dir_, ec);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(root_dir_, ec);
    }

    std::unique_ptr<DistributedStorageBackend> MakeObjectStorageBackend(
        FakeObjectStorageAdapter*& adapter,
        FileStorageConfig file_config = {}) const {
        DistributedStorageConfig distributed_config;
        distributed_config.fsdir = root_dir_.string();
        auto owned_adapter = std::make_unique<FakeObjectStorageAdapter>();
        adapter = owned_adapter.get();
        return std::make_unique<DistributedStorageBackend>(
            file_config, distributed_config, nullptr, std::move(owned_adapter));
    }

    std::filesystem::path root_dir_;
};

TEST_F(ObjectStorageAdapterTest, ObjectStorageModeInitSkipsDirectories) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);

    EXPECT_EQ(backend->GetStorageMode(),
              DistributedStorageMode::kObjectStorage);
    EXPECT_TRUE(backend->UsesObjectStorage());
    ASSERT_TRUE(backend->Init());
    EXPECT_TRUE(adapter->initialized);
    EXPECT_EQ(adapter->init_calls, 1);
    EXPECT_FALSE(std::filesystem::exists(root_dir_));

    ASSERT_TRUE(backend->Init());
    EXPECT_EQ(adapter->init_calls, 1);
}

TEST_F(ObjectStorageAdapterTest, BatchOffloadWritesMultiSliceObjects) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    std::string first_a = "hello";
    std::string first_b = "-world";
    std::string second = "payload";
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"first",
         {{first_a.data(), first_a.size()}, {first_b.data(), first_b.size()}}},
        {"second", {{second.data(), second.size()}}},
    };
    std::vector<std::string> completed_keys;
    std::vector<StorageObjectMetadata> completed_metadata;

    auto result = backend->BatchOffload(
        batch, [&](const std::vector<std::string>& keys,
                   std::vector<StorageObjectMetadata>& metadata) {
            completed_keys = keys;
            completed_metadata = metadata;
            return ErrorCode::OK;
        });

    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 2);
    EXPECT_EQ(adapter->putv_calls, 2);
    EXPECT_EQ(adapter->objects.at("first"), "hello-world");
    EXPECT_EQ(adapter->objects.at("second"), "payload");
    EXPECT_EQ(completed_keys.size(), 2);
    ASSERT_EQ(completed_metadata.size(), 2);

    std::map<std::string, int64_t> sizes;
    for (size_t i = 0; i < completed_keys.size(); ++i) {
        sizes[completed_keys[i]] = completed_metadata[i].data_size;
    }
    EXPECT_EQ(sizes.at("first"), 11);
    EXPECT_EQ(sizes.at("second"), 7);
}

TEST_F(ObjectStorageAdapterTest, BatchOffloadAllowsPartialFailure) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());
    adapter->fail_keys.insert("bad");

    std::string good = "good-data";
    std::string bad = "bad-data";
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"good", {{good.data(), good.size()}}},
        {"bad", {{bad.data(), bad.size()}}},
    };
    std::vector<std::string> completed_keys;

    auto result =
        backend->BatchOffload(batch, [&](const std::vector<std::string>& keys,
                                         std::vector<StorageObjectMetadata>&) {
            completed_keys = keys;
            return ErrorCode::OK;
        });

    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 1);
    ASSERT_EQ(completed_keys.size(), 1);
    EXPECT_EQ(completed_keys.front(), "good");
    EXPECT_TRUE(adapter->objects.contains("good"));
    EXPECT_FALSE(adapter->objects.contains("bad"));
}

TEST_F(ObjectStorageAdapterTest, BatchOffloadIgnoresEvictionHandler) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    std::string value = "value";
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"key", {{value.data(), value.size()}}},
    };
    bool eviction_called = false;

    auto result = backend->BatchOffload(
        batch,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; },
        [&](const std::vector<std::string>&) -> tl::expected<void, ErrorCode> {
            eviction_called = true;
            return {};
        });

    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 1);
    EXPECT_FALSE(eviction_called);
}

TEST_F(ObjectStorageAdapterTest, BatchLoadReadsObjects) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());
    adapter->objects["first"] = "hello";
    adapter->objects["second"] = "world";

    std::array<char, 5> first{};
    std::array<char, 5> second{};
    std::unordered_map<std::string, Slice> slices{
        {"first", {first.data(), first.size()}},
        {"second", {second.data(), second.size()}},
    };

    ASSERT_TRUE(backend->BatchLoad(slices));
    EXPECT_EQ(std::string(first.data(), first.size()), "hello");
    EXPECT_EQ(std::string(second.data(), second.size()), "world");
    EXPECT_EQ(adapter->get_calls, 2);
}

TEST_F(ObjectStorageAdapterTest, BatchLoadReturnsMissingKeyError) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    std::array<char, 4> buffer{};
    std::unordered_map<std::string, Slice> slices{
        {"missing", {buffer.data(), buffer.size()}},
    };

    auto result = backend->BatchLoad(slices);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::FILE_NOT_FOUND);
}

TEST_F(ObjectStorageAdapterTest, IsExistUsesObjectStorageAdapter) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    auto before = backend->IsExist("key");
    ASSERT_TRUE(before);
    EXPECT_FALSE(*before);

    adapter->objects["key"] = "value";
    auto after = backend->IsExist("key");
    ASSERT_TRUE(after);
    EXPECT_TRUE(*after);
}

TEST_F(ObjectStorageAdapterTest, ScanMetaBatchesKeysAndSizes) {
    FakeObjectStorageAdapter* adapter = nullptr;
    FileStorageConfig file_config;
    file_config.scanmeta_iterator_keys_limit = 2;
    auto backend = MakeObjectStorageBackend(adapter, file_config);
    ASSERT_TRUE(backend->Init());
    adapter->objects = {{"a", "1"}, {"bb", "22"}, {"ccc", "333"}};

    std::vector<size_t> batch_sizes;
    std::map<std::string, StorageObjectMetadata> metadata_by_key;
    auto result =
        backend->ScanMeta([&](const std::vector<std::string>& keys,
                              std::vector<StorageObjectMetadata>& metadata) {
            batch_sizes.push_back(keys.size());
            for (size_t i = 0; i < keys.size(); ++i) {
                metadata_by_key.emplace(keys[i], metadata[i]);
            }
            return ErrorCode::OK;
        });

    ASSERT_TRUE(result);
    EXPECT_EQ(batch_sizes, (std::vector<size_t>{2, 1}));
    ASSERT_EQ(metadata_by_key.size(), 3);
    EXPECT_EQ(metadata_by_key.at("a").key_size, 1);
    EXPECT_EQ(metadata_by_key.at("a").data_size, 1);
    EXPECT_EQ(metadata_by_key.at("bb").key_size, 2);
    EXPECT_EQ(metadata_by_key.at("bb").data_size, 2);
    EXPECT_EQ(metadata_by_key.at("ccc").key_size, 3);
    EXPECT_EQ(metadata_by_key.at("ccc").data_size, 3);
}

TEST(ObjectStorageAdapterTensorTest, DefaultImplementationsRoundTrip) {
    FakeObjectStorageAdapter adapter;
    std::string first = "ab";
    std::string second = "cde";
    std::string third = "f";
    std::array<TensorRegion, 3> write_regions{{
        {first.data(), first.size()},
        {second.data(), second.size()},
        {third.data(), third.size()},
    }};

    ASSERT_TRUE(adapter.PutTensor("tensor", write_regions));
    EXPECT_EQ(adapter.objects.at("tensor"), "abcdef");

    std::array<char, 1> output_first{};
    std::array<char, 3> output_second{};
    std::array<char, 2> output_third{};
    std::array<TensorRegion, 3> read_regions{{
        {output_first.data(), output_first.size()},
        {output_second.data(), output_second.size()},
        {output_third.data(), output_third.size()},
    }};

    ASSERT_TRUE(adapter.GetTensor("tensor", read_regions));
    EXPECT_EQ(std::string(output_first.data(), output_first.size()), "a");
    EXPECT_EQ(std::string(output_second.data(), output_second.size()), "bcd");
    EXPECT_EQ(std::string(output_third.data(), output_third.size()), "ef");
}

TEST(ObjectStorageAdapterTensorTest, GetTensorRejectsShortRead) {
    FakeObjectStorageAdapter adapter;
    adapter.objects["tensor"] = "abc";
    std::array<char, 4> output{};
    std::array<TensorRegion, 1> regions{{
        {output.data(), output.size()},
    }};

    auto result = adapter.GetTensor("tensor", regions);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::FILE_READ_FAIL);
}

TEST_F(ObjectStorageAdapterTest, LegacyFileSystemConstructorStillWorks) {
    FileStorageConfig file_config;
    DistributedStorageConfig distributed_config;
    distributed_config.fsdir = root_dir_.string();
    distributed_config.hash_bucket_count = 1;
    auto owned_adapter = std::make_unique<FakeFileSystemAdapter>();
    auto* adapter = owned_adapter.get();
    DistributedStorageBackend backend(file_config, distributed_config,
                                      std::move(owned_adapter));

    EXPECT_EQ(backend.GetStorageMode(), DistributedStorageMode::kFileSystem);
    EXPECT_FALSE(backend.UsesObjectStorage());
    ASSERT_TRUE(backend.Init());
    EXPECT_TRUE(adapter->initialized);
    EXPECT_TRUE(std::filesystem::is_directory(root_dir_ / "00"));
}

TEST_F(ObjectStorageAdapterTest, RejectsAmbiguousAdapterSelection) {
    FileStorageConfig file_config;
    DistributedStorageConfig distributed_config;
    distributed_config.fsdir = root_dir_.string();

    EXPECT_DEATH(
        {
            DistributedStorageBackend backend(
                file_config, distributed_config,
                std::make_unique<FakeFileSystemAdapter>(),
                std::make_unique<FakeObjectStorageAdapter>());
        },
        "exactly one I/O adapter is required");
}

}  // namespace
}  // namespace mooncake
