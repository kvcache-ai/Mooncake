#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include "ha/oplog/backends/redis/redis_oplog_store.h"
#include "ha/oplog/oplog_codec.h"
#include "ha/oplog/oplog_manager.h"
#include "types.h"

namespace mooncake {
namespace testing {

DEFINE_string(redis_endpoint, "",
              "Redis endpoint for OpLog backend tests, e.g. 127.0.0.1:6379");

namespace {

std::string MakeClusterNamespace() {
    return "ha-redis-oplog-test-" + UuidToString(generate_uuid());
}

OpLogEntry MakeEntry(OpLogManager& manager, OpType op_type,
                     const std::string& key, const std::string& payload) {
    return manager.AllocateEntry(op_type, key, payload);
}

}  // namespace

TEST(RedisOpLogStoreTest, AppendPollAndResolveLatest) {
    if (FLAGS_redis_endpoint.empty()) {
        GTEST_SKIP() << "Redis endpoint is not configured";
    }

    const auto cluster_namespace = MakeClusterNamespace();
    ha::backends::redis::RedisOpLogStore store(FLAGS_redis_endpoint,
                                               cluster_namespace);
    OpLogManager manager;

    OpLogEntry first =
        MakeEntry(manager, OpType::PUT_END, "key-1", "payload-1");
    auto append_first = store.Append(ha::oplog::BuildAppendRequest(first));
    ASSERT_TRUE(append_first.has_value());
    EXPECT_EQ(first.sequence_id, append_first.value());

    auto latest = store.GetLatestSequence();
    ASSERT_TRUE(latest.has_value());
    EXPECT_EQ(first.sequence_id, latest.value());

    auto first_poll = store.PollFrom(0, 10, std::chrono::milliseconds(0));
    ASSERT_TRUE(first_poll.has_value());
    ASSERT_EQ(1u, first_poll->records.size());
    EXPECT_EQ(first.sequence_id, first_poll->records.front().seq);

    auto decoded =
        ha::oplog::DeserializeEntryPayload(first_poll->records.front().payload);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(first.sequence_id, decoded->sequence_id);
    EXPECT_EQ(first.object_key, decoded->object_key);
    EXPECT_EQ(first.payload, decoded->payload);

    auto empty_poll =
        store.PollFrom(first.sequence_id, 10, std::chrono::milliseconds(0));
    ASSERT_TRUE(empty_poll.has_value());
    EXPECT_TRUE(empty_poll->records.empty());
    EXPECT_TRUE(empty_poll->timed_out);

    OpLogEntry second =
        MakeEntry(manager, OpType::REMOVE, "key-2", std::string());
    auto append_second = store.Append(ha::oplog::BuildAppendRequest(second));
    ASSERT_TRUE(append_second.has_value());
    EXPECT_EQ(second.sequence_id, append_second.value());

    auto second_poll =
        store.PollFrom(first.sequence_id, 10, std::chrono::milliseconds(0));
    ASSERT_TRUE(second_poll.has_value());
    ASSERT_EQ(1u, second_poll->records.size());
    EXPECT_EQ(second.sequence_id, second_poll->records.front().seq);

    latest = store.GetLatestSequence();
    ASSERT_TRUE(latest.has_value());
    EXPECT_EQ(second.sequence_id, latest.value());
}

TEST(RedisOpLogStoreTest, CleanupBeforeDeletesSafePrefixOnly) {
    if (FLAGS_redis_endpoint.empty()) {
        GTEST_SKIP() << "Redis endpoint is not configured";
    }

    const auto cluster_namespace = MakeClusterNamespace();
    ha::backends::redis::RedisOpLogStore store(FLAGS_redis_endpoint,
                                               cluster_namespace);
    OpLogManager manager;

    for (int index = 0; index < 5; ++index) {
        auto entry =
            MakeEntry(manager, OpType::PUT_END, "key-" + std::to_string(index),
                      "payload-" + std::to_string(index));
        auto append_result = store.Append(ha::oplog::BuildAppendRequest(entry));
        ASSERT_TRUE(append_result.has_value());
    }

    EXPECT_EQ(ErrorCode::OK, store.CleanupBefore(4));

    auto poll_result = store.PollFrom(0, 10, std::chrono::milliseconds(0));
    ASSERT_TRUE(poll_result.has_value());
    ASSERT_EQ(2u, poll_result->records.size());
    EXPECT_EQ(4u, poll_result->records[0].seq);
    EXPECT_EQ(5u, poll_result->records[1].seq);

    auto latest = store.GetLatestSequence();
    ASSERT_TRUE(latest.has_value());
    EXPECT_EQ(5u, latest.value());
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    return RUN_ALL_TESTS();
}
