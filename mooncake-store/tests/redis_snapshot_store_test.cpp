#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <hiredis/hiredis.h>

#include "ha/backends/redis/redis_snapshot_store.h"
#include "serialize/serializer_backend.h"
#include "types.h"

namespace mooncake::test {

DEFINE_string(redis_endpoint, "",
              "Redis endpoint for snapshot catalog integration tests");

namespace {

constexpr auto kRedisDefaultPort = 6379;

struct RedisContextDeleter {
    void operator()(redisContext* context) const {
        if (context != nullptr) {
            redisFree(context);
        }
    }
};

using RedisContextPtr = std::unique_ptr<redisContext, RedisContextDeleter>;

struct RedisReplyDeleter {
    void operator()(redisReply* reply) const {
        if (reply != nullptr) {
            freeReplyObject(reply);
        }
    }
};

using RedisReplyPtr = std::unique_ptr<redisReply, RedisReplyDeleter>;

std::pair<std::string, int> ParseRedisEndpointForTest(
    const std::string& endpoint) {
    std::string normalized = endpoint;
    constexpr std::string_view kRedisScheme = "redis://";
    if (std::string_view(normalized).starts_with(kRedisScheme)) {
        normalized.erase(0, kRedisScheme.size());
    }
    if (normalized.empty()) {
        return {"", kRedisDefaultPort};
    }

    const auto colon_pos = normalized.rfind(':');
    if (colon_pos == std::string::npos || normalized.find(':') != colon_pos) {
        return {normalized, kRedisDefaultPort};
    }
    return {normalized.substr(0, colon_pos),
            std::stoi(normalized.substr(colon_pos + 1))};
}

RedisContextPtr ConnectRedisForTest(const std::string& endpoint) {
    auto [host, port] = ParseRedisEndpointForTest(endpoint);
    if (host.empty()) {
        return RedisContextPtr(nullptr);
    }

    timeval timeout{3, 0};
    RedisContextPtr context(
        redisConnectWithTimeout(host.c_str(), port, timeout));
    if (context == nullptr || context->err != 0) {
        return RedisContextPtr(nullptr);
    }

    const char* password = std::getenv("MC_REDIS_PASSWORD");
    if (password != nullptr && std::strlen(password) > 0) {
        RedisReplyPtr auth_reply(static_cast<redisReply*>(redisCommand(
            context.get(), "AUTH %b", password, std::strlen(password))));
        if (auth_reply == nullptr || auth_reply->type == REDIS_REPLY_ERROR) {
            return RedisContextPtr(nullptr);
        }
    }

    const char* db_index = std::getenv("MC_REDIS_DB_INDEX");
    if (db_index != nullptr && std::strlen(db_index) > 0 &&
        std::string_view(db_index) != "0") {
        RedisReplyPtr select_reply(static_cast<redisReply*>(
            redisCommand(context.get(), "SELECT %s", db_index)));
        if (select_reply == nullptr ||
            select_reply->type == REDIS_REPLY_ERROR) {
            return RedisContextPtr(nullptr);
        }
    }

    return context;
}

std::string SanitizeHashTagComponent(std::string component) {
    if (!component.empty() && component.back() == '/') {
        component.pop_back();
    }
    std::replace(component.begin(), component.end(), '{', '_');
    std::replace(component.begin(), component.end(), '}', '_');
    return component;
}

std::string BuildLatestKey(const std::string& cluster_namespace) {
    const auto hash_tag = SanitizeHashTagComponent(cluster_namespace);
    return "mooncake-store/{" + hash_tag + "}/snapshot/latest";
}

std::string BuildIndexKey(const std::string& cluster_namespace) {
    const auto hash_tag = SanitizeHashTagComponent(cluster_namespace);
    return "mooncake-store/{" + hash_tag + "}/snapshot/index";
}

class FakePayloadBackend final : public SerializerBackend {
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

    std::string GetConnectionInfo() const override { return "fake://payload"; }

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

class RedisSnapshotStoreTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (FLAGS_redis_endpoint.empty()) {
            GTEST_SKIP() << "Redis endpoint is not configured";
        }
        cluster_namespace_ =
            "snapshot-redis-test-" + UuidToString(generate_uuid());
        store_ = std::make_unique<ha::backends::redis::RedisSnapshotStore>(
            &payload_backend_, FLAGS_redis_endpoint, cluster_namespace_);
    }

    void TearDown() override {
        if (FLAGS_redis_endpoint.empty()) {
            return;
        }
        auto redis = ConnectRedisForTest(FLAGS_redis_endpoint);
        ASSERT_NE(redis, nullptr);
        const auto latest_key = BuildLatestKey(cluster_namespace_);
        const auto index_key = BuildIndexKey(cluster_namespace_);
        RedisReplyPtr reply(static_cast<redisReply*>(redisCommand(
            redis.get(), "DEL %b %b", latest_key.data(), latest_key.size(),
            index_key.data(), index_key.size())));
        ASSERT_NE(reply, nullptr);
        ASSERT_NE(reply->type, REDIS_REPLY_ERROR);
    }

    FakePayloadBackend payload_backend_;
    std::string cluster_namespace_;
    std::unique_ptr<ha::backends::redis::RedisSnapshotStore> store_;
};

TEST_F(RedisSnapshotStoreTest, GetLatestReturnsEmptyWhenCatalogMissing) {
    auto latest = store_->GetLatest();
    ASSERT_TRUE(latest.has_value());
    EXPECT_FALSE(latest->has_value());
}

TEST_F(RedisSnapshotStoreTest, PublishListAndGetLatestRoundTrip) {
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

TEST_F(RedisSnapshotStoreTest, DeleteUpdatesLatestAndDeletesPayloadPrefix) {
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

    ASSERT_EQ(payload_backend_.deleted_prefixes.size(), 1u);
    EXPECT_EQ(payload_backend_.deleted_prefixes.front(),
              "mooncake_master_snapshot/20240302_120000_001/");
}

}  // namespace
}  // namespace mooncake::test

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
