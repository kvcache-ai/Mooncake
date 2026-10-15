#include "storage/distributed/kvcs/kvcs_object_storage_adapter.h"
#include "storage/distributed/kvcs/kvcs_capi_driver.h"

#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

class MemoryDriver final : public KvcsDriver {
   public:
    tl::expected<void, ErrorCode> Init() override { return {}; }
    uint64_t MaxValueSize() const override { return 8; }

    KvcsPutResults BatchPut(std::span<const KvcsPutRequest> requests) override {
        ++put_calls_;
        KvcsPutResults results;
        for (const auto& request : requests) {
            if (values_.contains(request.logical_key)) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS));
                continue;
            }
            std::string value;
            for (const auto& slice : request.slices)
                value.append(static_cast<const char*>(slice.ptr), slice.size);
            values_[request.logical_key] = std::move(value);
            results.emplace_back();
        }
        return results;
    }

    KvcsDriverQueryResults BatchQuery(
        std::span<const ObjectKey> keys) override {
        KvcsDriverQueryResults results;
        for (const auto& key : keys) {
            auto it = values_.find(key);
            if (it == values_.end()) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
                continue;
            }
            results.emplace_back();
        }
        return results;
    }

    KvcsGetResults BatchGet(std::span<const KvcsGetRequest> requests) override {
        KvcsGetResults results;
        for (const auto& request : requests) {
            auto it = values_.find(request.logical_key);
            if (it == values_.end()) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
                continue;
            }
            size_t offset = 0;
            if (request.size != it->second.size()) {
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::KVCS_INCOMPLETE));
                continue;
            }
            for (const auto& slice : request.slices) {
                std::memcpy(slice.ptr, it->second.data() + offset, slice.size);
                offset += slice.size;
            }
            if (offset == it->second.size())
                results.emplace_back();
            else
                results.emplace_back(
                    tl::make_unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    KvcsDeleteResults BatchDelete(std::span<const ObjectKey> keys) override {
        KvcsDeleteResults results;
        for (const auto& key : keys) {
            values_.erase(key);
            results.emplace_back();
        }
        return results;
    }

    size_t put_calls() const { return put_calls_; }

   private:
    size_t put_calls_ = 0;
    std::map<ObjectKey, std::string> values_;
};

class ScopedEfcEnvironment {
   public:
    ScopedEfcEnvironment() {
        for (const char* name :
             {"KVCS_BACKEND", "KVCS_EXTRA_BACKENDS", "KVCS_MOUNTPOINTS_JSON"}) {
            const char* value = std::getenv(name);
            saved_.emplace_back(
                name, value ? std::optional<std::string>(value) : std::nullopt);
            unsetenv(name);
        }
    }
    ~ScopedEfcEnvironment() {
        for (const auto& [name, value] : saved_) {
            if (value)
                setenv(name.c_str(), value->c_str(), 1);
            else
                unsetenv(name.c_str());
        }
    }

   private:
    std::vector<std::pair<std::string, std::optional<std::string>>> saved_;
};

KvcsObjectStorageAdapter MakeAdapter() {
    FileStorageConfig config;
    return KvcsObjectStorageAdapter(config, std::make_unique<MemoryDriver>());
}

TEST(KvcsObjectStorageAdapterTest, PutQueryGetAndRemove) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());

    std::string value = "12345678";
    ObjectStoragePutRequest put{"key", {{value.data(), value.size()}}};
    ASSERT_TRUE(adapter.BatchPutV({&put, 1})[0]);

    ASSERT_TRUE(adapter.Exists("key").value());
    const std::array<ObjectKey, 1> keys{"key"};
    auto queried = adapter.BatchQueryProviderUntil(
        keys, std::chrono::steady_clock::now() + std::chrono::seconds(1));
    ASSERT_EQ(queried.size(), 1u);
    ASSERT_TRUE(queried[0]);
    std::string output(value.size(), '\0');
    ObjectStorageGetRequest get{
        "key", {{output.data(), output.size()}}, output.size()};
    ASSERT_TRUE(adapter.BatchGetInto({&get, 1})[0]);
    EXPECT_EQ(output, value);

    ASSERT_TRUE(adapter.Delete("key"));
    EXPECT_FALSE(adapter.Exists("key").value());
}

TEST(KvcsObjectStorageAdapterTest, RejectsOversizeBeforeProviderWrite) {
    FileStorageConfig config;
    auto driver = std::make_unique<MemoryDriver>();
    auto* driver_ptr = driver.get();
    KvcsObjectStorageAdapter adapter(config, std::move(driver));
    ASSERT_TRUE(adapter.Init());
    std::string value(9, 'x');
    ObjectStoragePutRequest put{"large", {{value.data(), value.size()}}};
    auto result = adapter.BatchPutV({&put, 1});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(driver_ptr->put_calls(), 0u);
}

TEST(KvcsObjectStorageAdapterTest, QueryAndDeleteKeepPerKeyErrorsIsolated) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string value = "value";
    const ObjectStoragePutRequest put{"valid", {{value.data(), value.size()}}};
    ASSERT_TRUE(adapter.BatchPutV({&put, 1})[0]);

    const std::array<std::string, 2> keys{"valid", std::string(300, 'x')};
    auto queried = adapter.BatchQueryProvider(keys);
    ASSERT_EQ(queried.size(), keys.size());
    EXPECT_TRUE(queried[0]);
    ASSERT_FALSE(queried[1]);
    EXPECT_EQ(queried[1].error(), ErrorCode::INVALID_PARAMS);

    auto removed = adapter.BatchDelete(keys);
    ASSERT_EQ(removed.size(), keys.size());
    EXPECT_TRUE(removed[0]);
    ASSERT_FALSE(removed[1]);
    EXPECT_EQ(removed[1].error(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(adapter.Exists("valid").value());
}

TEST(KvcsObjectStorageAdapterTest, GetRejectsMasterSizeMismatch) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string value = "value";
    const ObjectStoragePutRequest put{"key", {{value.data(), value.size()}}};
    ASSERT_TRUE(adapter.BatchPutV({&put, 1})[0]);
    std::string output(value.size() - 1, '\0');
    const ObjectStorageGetRequest get{
        "key", {{output.data(), output.size()}}, output.size()};
    auto result = adapter.BatchGetInto({&get, 1});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::KVCS_INCOMPLETE);
}

TEST(KvcsObjectStorageAdapterTest, UpsertRequiresExplicitReplace) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string first = "one";
    ObjectStoragePutRequest put{"key", {{first.data(), first.size()}}};
    ASSERT_TRUE(adapter.BatchPutV({&put, 1})[0]);
    auto duplicate = adapter.BatchPutV({&put, 1});
    ASSERT_FALSE(duplicate[0]);
    EXPECT_EQ(duplicate[0].error(), ErrorCode::OBJECT_ALREADY_EXISTS);

    put.replace_existing = true;
    put.slices = {{nullptr, 0}};
    std::string second = "two";
    const ObjectStoragePutRequest insert{"second",
                                         {{second.data(), second.size()}}};
    const std::array<ObjectStoragePutRequest, 2> requests{put, insert};
    auto invalid = adapter.BatchPutV(requests);
    ASSERT_FALSE(invalid[0]);
    EXPECT_EQ(invalid[0].error(), ErrorCode::NOT_SUPPORTED);
    ASSERT_TRUE(invalid[1]);
    EXPECT_TRUE(adapter.Exists("key").value());
    EXPECT_TRUE(adapter.Exists("second").value());
}

TEST(KvcsObjectStorageAdapterTest, RejectsInvalidRequests) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string value = "x";
    ObjectStoragePutRequest empty{"", {{value.data(), value.size()}}};
    auto result = adapter.BatchPutV({&empty, 1});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::INVALID_PARAMS);
}

TEST(KvcsObjectStorageAdapterTest, DefaultEfcTargetRejectsMultipleMountpoints) {
    ScopedEfcEnvironment environment;
    auto target = LoadKvcsEfcTarget("");
    ASSERT_TRUE(target);
    EXPECT_EQ(target->mountpoint_index, 1u);
    setenv("KVCS_MOUNTPOINTS_JSON",
           R"({"mountPoints":[{"mountPointID":"one"},{"mountPointID":"two"}]})",
           1);
    auto multiple = LoadKvcsEfcTarget("");
    ASSERT_FALSE(multiple);
    EXPECT_EQ(multiple.error(), ErrorCode::INVALID_PARAMS);

    unsetenv("KVCS_MOUNTPOINTS_JSON");
    setenv("KVCS_BACKEND", "diskless", 1);
    setenv("KVCS_EXTRA_BACKENDS", "kvcachestore", 1);
    auto shared_only = LoadKvcsEfcTarget("");
    ASSERT_TRUE(shared_only);
    EXPECT_EQ(shared_only->mountpoint_index, 1u);
}

#ifdef MOONCAKE_KVCS_TEST_NO_SDK
TEST(KvcsObjectStorageAdapterTest, MissingSdkIsExplicitlyUnsupported) {
    ScopedEfcEnvironment environment;
    FileStorageConfig config;
    KvcsObjectStorageAdapter adapter(config);
    auto result = adapter.Init();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::NOT_SUPPORTED);
}
#endif

#ifdef MOONCAKE_KVCS_TEST_SDK
TEST(KvcsObjectStorageAdapterTest, OfficialMockSingleValueRoundTrip) {
    if (!std::getenv("MOONCAKE_KVCS_TEST_MOCK")) GTEST_SKIP();
    auto created = CreateKvcsLowLevelDriver(1);
    ASSERT_TRUE(created);
    auto& driver = **created;
    ASSERT_TRUE(driver.Init());
    std::string value = "value";
    const KvcsPutRequest put{.logical_key = "pr1-mock-roundtrip",
                             .slices = {{value.data(), value.size()}},
                             .size = value.size()};
    ASSERT_TRUE(driver.BatchPut({&put, 1})[0]);

    const std::array<ObjectKey, 1> keys{put.logical_key};
    auto query = driver.BatchQuery(keys);
    ASSERT_TRUE(query[0]);
    std::string output(value.size(), '\0');
    const KvcsGetRequest get{.logical_key = put.logical_key,
                             .slices = {{output.data(), output.size()}},
                             .size = output.size()};
    ASSERT_TRUE(driver.BatchGet({&get, 1})[0]);
    EXPECT_EQ(output, value);
    ASSERT_TRUE(driver.BatchDelete(keys)[0]);
    auto after_delete = driver.BatchQuery(keys);
    ASSERT_FALSE(after_delete[0]);
    EXPECT_EQ(after_delete[0].error(), ErrorCode::OBJECT_NOT_FOUND);
}
#endif

}  // namespace
}  // namespace mooncake
