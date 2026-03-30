#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include <hiredis/hiredis.h>

#include "ha/common/redis/redis_test_utils.h"
#include "ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store.h"
#include "types.h"

namespace mooncake::test {

DEFINE_string(redis_endpoint, "",
              "Redis endpoint for snapshot catalog integration tests");

namespace {

class FakeObjectStore final : public SnapshotObjectStore {
   public:
    tl::expected<void, std::string> UploadBuffer(
        const std::string& key, const std::vector<uint8_t>& buffer) override {
        (void)key;
        (void)buffer;
        return {};
    }

    tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& buffer) override {
        (void)key;
        (void)buffer;
        return tl::make_unexpected("unused");
    }

    tl::expected<void, std::string> UploadString(
        const std::string& key, const std::string& data) override {
        (void)key;
        (void)data;
        return {};
    }

    tl::expected<void, std::string> DownloadString(const std::string& key,
                                                   std::string& data) override {
        (void)key;
        (void)data;
        return tl::make_unexpected("unused");
    }

    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) override {
        deleted_prefixes.push_back(prefix);
        return {};
    }

    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix,
        std::vector<std::string>& object_keys) override {
        (void)prefix;
        object_keys.clear();
        return {};
    }

    std::string GetConnectionInfo() const override { return "fake://object"; }

    std::vector<std::string> deleted_prefixes;
};

ha::SnapshotDescriptor MakeDescriptor(const std::string& snapshot_id) {
    ha::SnapshotDescriptor descriptor;
    descriptor.snapshot_id = snapshot_id;
    descriptor.manifest_key =
        "mooncake_master_snapshot/" + snapshot_id + "/manifest.txt";
    descriptor.object_prefix = "mooncake_master_snapshot/" + snapshot_id + "/";
    return descriptor;
}

class RedisSnapshotCatalogStoreTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (FLAGS_redis_endpoint.empty()) {
            GTEST_SKIP() << "Redis endpoint is not configured";
        }
        cluster_namespace_ =
            "snapshot-redis-test-" + UuidToString(generate_uuid());
        store_ =
            std::make_unique<ha::backends::redis::RedisSnapshotCatalogStore>(
                &object_store_, FLAGS_redis_endpoint, cluster_namespace_);
    }

    void TearDown() override {
        if (FLAGS_redis_endpoint.empty()) {
            return;
        }
        auto redis =
            mooncake::testing::ConnectRedisForTest(FLAGS_redis_endpoint);
        ASSERT_TRUE(redis.has_value());
        const auto latest_key =
            mooncake::testing::BuildRedisSnapshotLatestKey(cluster_namespace_);
        const auto index_key =
            mooncake::testing::BuildRedisSnapshotIndexKey(cluster_namespace_);
        mooncake::testing::RedisReplyPtr reply(
            static_cast<redisReply*>(redisCommand(
                redis->get(), "DEL %b %b", latest_key.data(), latest_key.size(),
                index_key.data(), index_key.size())));
        ASSERT_NE(reply, nullptr);
        ASSERT_NE(reply->type, REDIS_REPLY_ERROR);
    }

    FakeObjectStore object_store_;
    std::string cluster_namespace_;
    std::unique_ptr<ha::backends::redis::RedisSnapshotCatalogStore> store_;
};

TEST_F(RedisSnapshotCatalogStoreTest, GetLatestReturnsEmptyWhenCatalogMissing) {
    auto latest = store_->GetLatest();
    ASSERT_TRUE(latest.has_value());
    EXPECT_FALSE(latest->has_value());
}

TEST_F(RedisSnapshotCatalogStoreTest, PublishListAndGetLatestRoundTrip) {
    ASSERT_EQ(store_->Publish(MakeDescriptor("20240301_120000_001")),
              ErrorCode::OK);
    ASSERT_EQ(store_->Publish(MakeDescriptor("20240302_120000_001")),
              ErrorCode::OK);

    auto latest = store_->GetLatest();
    ASSERT_TRUE(latest.has_value());
    ASSERT_TRUE(latest->has_value());
    EXPECT_EQ(latest->value().snapshot_id, "20240302_120000_001");
    EXPECT_EQ(latest->value().manifest_key,
              "mooncake_master_snapshot/20240302_120000_001/manifest.txt");

    auto snapshots = store_->List(0);
    ASSERT_TRUE(snapshots.has_value());
    ASSERT_EQ(snapshots->size(), 2u);
    EXPECT_EQ(snapshots->at(0).snapshot_id, "20240302_120000_001");
    EXPECT_EQ(snapshots->at(1).snapshot_id, "20240301_120000_001");
}

TEST_F(RedisSnapshotCatalogStoreTest,
       DeleteUpdatesLatestAndDeletesPayloadPrefix) {
    ASSERT_EQ(store_->Publish(MakeDescriptor("20240301_120000_001")),
              ErrorCode::OK);
    ASSERT_EQ(store_->Publish(MakeDescriptor("20240302_120000_001")),
              ErrorCode::OK);

    ASSERT_EQ(store_->Delete("20240302_120000_001"), ErrorCode::OK);

    auto latest = store_->GetLatest();
    ASSERT_TRUE(latest.has_value());
    ASSERT_TRUE(latest->has_value());
    EXPECT_EQ(latest->value().snapshot_id, "20240301_120000_001");

    auto snapshots = store_->List(0);
    ASSERT_TRUE(snapshots.has_value());
    ASSERT_EQ(snapshots->size(), 1u);
    EXPECT_EQ(snapshots->at(0).snapshot_id, "20240301_120000_001");

    ASSERT_EQ(object_store_.deleted_prefixes.size(), 1u);
    EXPECT_EQ(object_store_.deleted_prefixes.front(),
              "mooncake_master_snapshot/20240302_120000_001/");
}

}  // namespace
}  // namespace mooncake::test

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
