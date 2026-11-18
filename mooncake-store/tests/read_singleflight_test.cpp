#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <vector>

#include "common/read_singleflight.h"

namespace mooncake {
namespace {

TEST(ReadSingleFlightTest, SharesOnlyAnActiveRead) {
    ReadSingleFlight<std::string, int> reads;
    auto leader = reads.Acquire("key");
    auto follower = reads.Acquire("key");
    EXPECT_TRUE(leader.IsLeader());
    EXPECT_FALSE(follower.IsLeader());
    EXPECT_EQ(follower.Result().wait_for(std::chrono::seconds(0)),
              std::future_status::timeout);
    leader.Publish(42);
    EXPECT_EQ(follower.Result().get(), 42);
    EXPECT_TRUE(reads.Acquire("key").IsLeader());
}

TEST(ReadSingleFlightTest, AbandonedProducerWakesReadersAndAllowsRetry) {
    ReadSingleFlight<std::string, int> reads;
    std::shared_future<int> result;
    {
        auto leader = reads.Acquire("key");
        result = reads.Acquire("key").Result();
    }
    ASSERT_EQ(result.wait_for(std::chrono::seconds(0)),
              std::future_status::ready);
    try {
        result.get();
        FAIL() << "An abandoned producer must not leave a pending result";
    } catch (const std::future_error& error) {
        EXPECT_EQ(error.code(),
                  std::make_error_code(std::future_errc::broken_promise));
    }
    EXPECT_TRUE(reads.Acquire("key").IsLeader());
}

TEST(ReadSingleFlightTest, InvalidatedProducerCannotEraseItsReplacement) {
    ReadSingleFlight<std::string, int> reads;
    auto old = reads.Acquire("key");
    auto old_follower = reads.Acquire("key");
    reads.InvalidateIf([](const auto& key) { return key == "key"; });
    auto replacement = reads.Acquire("key");
    ASSERT_TRUE(replacement.IsLeader());
    old.Publish(1);
    EXPECT_EQ(old_follower.Result().get(), 1);
    auto new_follower = reads.Acquire("key");
    ASSERT_FALSE(new_follower.IsLeader());
    replacement.Publish(2);
    EXPECT_EQ(new_follower.Result().get(), 2);
}

TEST(ReadSingleFlightTest, RetirementFreezesTheJoinedSet) {
    ReadSingleFlight<std::string, int> reads;
    auto leader = reads.Acquire("key");
    EXPECT_FALSE(leader.Retire());
    auto fresh = reads.Acquire("key");
    EXPECT_TRUE(fresh.IsLeader());
    leader.Publish(1);
    auto follower = reads.Acquire("key");
    EXPECT_FALSE(follower.IsLeader());
    EXPECT_TRUE(fresh.Retire());
    fresh.Publish(2);
    EXPECT_EQ(follower.Result().get(), 2);
}

TEST(ReadSingleFlightTest, MovingProducerTicketsPreservesOwnership) {
    ReadSingleFlight<std::string, int> reads;
    std::vector<decltype(reads.Acquire("key"))> leaders;
    for (int i = 0; i < 32; ++i) {
        leaders.push_back(reads.Acquire(std::to_string(i)));
    }
    for (int i = 0; i < 32; ++i) {
        auto follower = reads.Acquire(std::to_string(i));
        ASSERT_FALSE(follower.IsLeader());
        leaders[i].Publish(i);
        EXPECT_EQ(follower.Result().get(), i);
    }
}

}  // namespace
}  // namespace mooncake
