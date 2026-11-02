#include "storage/distributed/kvcs/kvcs_object_storage_adapter.h"
#include "storage/distributed/kvcs/kvcs_capacity_provider.h"
#include "storage/distributed/kvcs/kvcs_capi_driver.h"
#include "storage/distributed/kvcs/kvcs_efc_target.h"

#include <array>
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
    explicit MemoryDriver(std::vector<KvcsMountpointStat> stats = {})
        : stats_(std::move(stats)) {}

    tl::expected<void, ErrorCode> Init() override { return {}; }
    uint64_t MaxValueSize() const override { return 8; }
    tl::expected<std::vector<KvcsMountpointStat>, ErrorCode>
    GetMountpointStats() override {
        return stats_;
    }

    KvcsIoResults BatchPut(std::span<const KvcsIoRequest> requests) override {
        ++put_calls_;
        KvcsIoResults results;
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

    KvcsIoResults BatchGet(std::span<const KvcsIoRequest> requests) override {
        KvcsIoResults results;
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
                    tl::make_unexpected(ErrorCode::FILE_READ_FAIL));
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

    KvcsIoResults BatchDelete(std::span<const ObjectKey> keys) override {
        KvcsIoResults results;
        for (const auto& key : keys) {
            deleted_keys_.push_back(key);
            values_.erase(key);
            results.emplace_back();
        }
        return results;
    }

    size_t put_calls() const { return put_calls_; }
    const std::vector<ObjectKey>& deleted_keys() const { return deleted_keys_; }

   private:
    size_t put_calls_ = 0;
    std::map<ObjectKey, std::string> values_;
    std::vector<KvcsMountpointStat> stats_;
    std::vector<ObjectKey> deleted_keys_;
};

class ScopedEfcEnvironment {
   public:
    ScopedEfcEnvironment() {
        for (const char* name : {"KVCS_MOUNTPOINTS_JSON"}) {
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
    return KvcsObjectStorageAdapter(std::make_unique<MemoryDriver>());
}

TEST(KvcsCapacityProviderTest, SelectsConfiguredMountpoint) {
    std::vector<KvcsMountpointStat> stats{
        {.index = 1, .id = "other", .total_bytes = 200, .used_bytes = 50},
        {.index = 2, .id = "target-mp", .total_bytes = 100, .used_bytes = 25}};
    KvcsCapacityProvider provider(
        2, "target-mp", std::make_unique<MemoryDriver>(std::move(stats)));
    auto capacity = provider.GetCapacity();
    ASSERT_TRUE(capacity.has_value()) << capacity.error();
    EXPECT_TRUE(capacity->enabled);
    EXPECT_EQ(capacity->used_bytes, 25u);
    EXPECT_EQ(capacity->capacity_bytes, 100u);
}

TEST(KvcsCapacityProviderTest, RejectsUnknownOrInconsistentCapacity) {
    auto get_capacity = [](std::vector<KvcsMountpointStat> stats) {
        KvcsCapacityProvider provider(
            2, "target-mp", std::make_unique<MemoryDriver>(std::move(stats)));
        return provider.GetCapacity();
    };
    EXPECT_FALSE(get_capacity({}).has_value());
    EXPECT_FALSE(
        get_capacity(
            {{.index = 2, .id = "other", .total_bytes = 100, .used_bytes = 25}})
            .has_value());
    EXPECT_FALSE(get_capacity({{.index = 2,
                                .id = "target-mp",
                                .total_bytes = 0,
                                .used_bytes = 0}})
                     .has_value());
    EXPECT_FALSE(get_capacity({{.index = 2,
                                .id = "target-mp",
                                .total_bytes = 100,
                                .used_bytes = 101}})
                     .has_value());
}

TEST(KvcsCapacityProviderTest, DeletesLogicalKeyWithSharedEncoding) {
    auto driver = std::make_unique<MemoryDriver>();
    auto* driver_ptr = driver.get();
    KvcsCapacityProvider provider(1, "target-mp", std::move(driver));
    EXPECT_TRUE(provider.DeleteObject("key"));
    auto invalid = provider.DeleteObject(std::string(300, 'x'));
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error(), ErrorCode::INVALID_PARAMS);
    ASSERT_EQ(driver_ptr->deleted_keys().size(), 1u);
    EXPECT_EQ(driver_ptr->deleted_keys()[0], EncodeKvcsKey("key"));
}

TEST(KvcsObjectStorageAdapterTest, PutGetAndRemove) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());

    std::string value = "12345678";
    iovec put_iov{value.data(), value.size()};
    ASSERT_TRUE(adapter.PutBatch({{"key", &put_iov, 1}})[0]);

    std::string output(value.size(), '\0');
    ASSERT_TRUE(adapter.GetBatch({{"key", output.data(), output.size()}})[0]);
    EXPECT_EQ(output, value);

    ASSERT_TRUE(adapter.Delete("key"));
    auto missing = adapter.GetBatch({{"key", output.data(), output.size()}});
    ASSERT_FALSE(missing[0]);
    EXPECT_EQ(missing[0].error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST(KvcsObjectStorageAdapterTest, RejectsOversizeBeforeProviderWrite) {
    auto driver = std::make_unique<MemoryDriver>();
    auto* driver_ptr = driver.get();
    KvcsObjectStorageAdapter adapter(std::move(driver));
    ASSERT_TRUE(adapter.Init());
    std::string value(9, 'x');
    iovec put_iov{value.data(), value.size()};
    auto result = adapter.PutBatch({{"large", &put_iov, 1}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(driver_ptr->put_calls(), 0u);
}

TEST(KvcsObjectStorageAdapterTest, DeleteKeepsPerKeyErrorsIsolated) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string value = "value";
    iovec put_iov{value.data(), value.size()};
    ASSERT_TRUE(adapter.PutBatch({{"valid", &put_iov, 1}})[0]);

    const std::array<std::string, 2> keys{"valid", std::string(300, 'x')};
    auto removed = adapter.DeleteBatch(keys);
    ASSERT_EQ(removed.size(), keys.size());
    EXPECT_TRUE(removed[0]);
    ASSERT_FALSE(removed[1]);
    EXPECT_EQ(removed[1].error(), ErrorCode::INVALID_PARAMS);
    std::string output(value.size(), '\0');
    auto missing = adapter.GetBatch({{"valid", output.data(), output.size()}});
    ASSERT_FALSE(missing[0]);
    EXPECT_EQ(missing[0].error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST(KvcsObjectStorageAdapterTest, GetRejectsMasterSizeMismatch) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string value = "value";
    iovec put_iov{value.data(), value.size()};
    ASSERT_TRUE(adapter.PutBatch({{"key", &put_iov, 1}})[0]);
    std::string output(value.size() - 1, '\0');
    auto result = adapter.GetBatch({{"key", output.data(), output.size()}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::FILE_READ_FAIL);
}

TEST(KvcsObjectStorageAdapterTest, PutOverwritesAndKeepsErrorsIsolated) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string first = "one";
    iovec first_iov{first.data(), first.size()};
    ASSERT_TRUE(adapter.PutBatch({{"key", &first_iov, 1}})[0]);
    std::string replacement = "new";
    iovec replacement_iov{replacement.data(), replacement.size()};
    ASSERT_TRUE(adapter.PutBatch({{"key", &replacement_iov, 1}})[0]);
    std::string output(replacement.size(), '\0');
    ASSERT_TRUE(adapter.GetBatch({{"key", output.data(), output.size()}})[0]);
    EXPECT_EQ(output, replacement);

    std::string second = "two";
    iovec invalid_iov{nullptr, second.size()};
    iovec second_iov{second.data(), second.size()};
    const std::array<ObjectPutRequest, 2> requests{
        ObjectPutRequest{"second", &invalid_iov, 1},
        ObjectPutRequest{"second", &second_iov, 1}};
    auto invalid = adapter.PutBatch(
        std::vector<ObjectPutRequest>(requests.begin(), requests.end()));
    ASSERT_FALSE(invalid[0]);
    EXPECT_EQ(invalid[0].error(), ErrorCode::INVALID_PARAMS);
    ASSERT_TRUE(invalid[1]);
}

TEST(KvcsObjectStorageAdapterTest, RejectsInvalidRequests) {
    auto adapter = MakeAdapter();
    ASSERT_TRUE(adapter.Init());
    std::string value = "x";
    iovec put_iov{value.data(), value.size()};
    auto result = adapter.PutBatch({{"", &put_iov, 1}});
    ASSERT_FALSE(result[0]);
    EXPECT_EQ(result[0].error(), ErrorCode::INVALID_PARAMS);
}

TEST(KvcsObjectStorageAdapterTest, DefaultEfcTargetRejectsMultipleMountpoints) {
    ScopedEfcEnvironment environment;
    auto target = LoadKvcsEfcTarget("");
    ASSERT_TRUE(target);
    EXPECT_EQ(target->mountpoint_index, 1u);
    EXPECT_EQ(target->mountpoint_id, "kvcachestore-default");
    setenv("KVCS_MOUNTPOINTS_JSON",
           R"({"mountPoints":[{"mountPointID":"one","subpath":"/tenant"}]})",
           1);
    target = LoadKvcsEfcTarget("");
    ASSERT_TRUE(target);
    EXPECT_EQ(target->mountpoint_id, "one");
    setenv("KVCS_MOUNTPOINTS_JSON",
           R"({"mountPoints":[{"mountPointID":"one"},{"mountPointID":"two"}]})",
           1);
    auto multiple = LoadKvcsEfcTarget("");
    ASSERT_FALSE(multiple);
    EXPECT_EQ(multiple.error(), ErrorCode::INVALID_PARAMS);
}

#ifdef MOONCAKE_KVCS_TEST_NO_SDK
TEST(KvcsObjectStorageAdapterTest, MissingSdkIsExplicitlyUnsupported) {
    ScopedEfcEnvironment environment;
    KvcsObjectStorageAdapter adapter;
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
    auto stats = driver.GetMountpointStats();
    ASSERT_TRUE(stats);
    ASSERT_FALSE(stats->empty());
    EXPECT_GT(stats->front().total_bytes, 0u);
    std::string value = "value";
    const KvcsIoRequest put{.logical_key = "pr1-mock-roundtrip",
                            .slices = {{value.data(), value.size()}},
                            .size = value.size()};
    ASSERT_TRUE(driver.BatchPut({&put, 1})[0]);

    std::string output(value.size(), '\0');
    const KvcsIoRequest get{.logical_key = put.logical_key,
                            .slices = {{output.data(), output.size()}},
                            .size = output.size()};
    ASSERT_TRUE(driver.BatchGet({&get, 1})[0]);
    EXPECT_EQ(output, value);
    const std::array<ObjectKey, 1> keys{put.logical_key};
    ASSERT_TRUE(driver.BatchDelete(keys)[0]);
    auto after_delete = driver.BatchGet({&get, 1});
    ASSERT_FALSE(after_delete[0]);
    EXPECT_EQ(after_delete[0].error(), ErrorCode::OBJECT_NOT_FOUND);
}
#endif

}  // namespace
}  // namespace mooncake
