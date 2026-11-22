#include "engram_coordinator.h"

#include <gtest/gtest.h>
#include <future>
#include <set>

namespace mooncake {
namespace {

EngramControlRequest Join(std::string node, int size = 2) {
    EngramControlRequest request;
    request.action = "join";
    request.pool = "model-and-backend";
    request.node = std::move(node);
    request.incarnation = "launch-1";
    request.mode = "auto";
    request.group_size = size;
    request.num_callers = 4;
    return request;
}

TEST(EngramCoordinator, ConcurrentAdmissionAndIdempotentCallers) {
    EngramCoordinator coordinator;
    std::vector<std::future<EngramControlResponse>> calls;
    for (int node = 0; node < 8; ++node) {
        calls.push_back(std::async(std::launch::async, [&, node] {
            return coordinator.Handle(Join(std::to_string(node), 4));
        }));
    }
    std::map<std::string, std::set<int>> groups;
    for (auto& call : calls) {
        auto response = call.get();
        ASSERT_TRUE(response.error.empty());
        groups[response.group].insert(response.shard);
    }
    ASSERT_EQ(groups.size(), 2);
    for (const auto& [name, shards] : groups) {
        EXPECT_EQ(shards, (std::set<int>{0, 1, 2, 3}));
    }
    auto first = coordinator.Handle(Join("0", 4));
    auto second = coordinator.Handle(Join("0", 4));
    EXPECT_EQ(first.group, second.group);
    EXPECT_EQ(first.shard, second.shard);
    EXPECT_TRUE(second.full);
}

TEST(EngramCoordinator, ManualSlotConflictsAndGroupSizeValidation) {
    EngramCoordinator coordinator;
    auto a = Join("a");
    a.mode = "manual";
    a.group = "operators-group";
    a.shard = 0;
    ASSERT_TRUE(coordinator.Handle(a).error.empty());
    auto b = a;
    b.node = "b";
    EXPECT_FALSE(coordinator.Handle(b).error.empty());
    b.shard = 1;
    EXPECT_TRUE(coordinator.Handle(b).full);
    b.group_size = 3;
    EXPECT_FALSE(coordinator.Handle(b).error.empty());
}

TEST(EngramCoordinator, RestartPreservesShardAndFencesWholeGroup) {
    EngramCoordinator coordinator;
    auto now = std::chrono::steady_clock::now();
    auto a = Join("a");
    auto b = Join("b");
    auto initial_a = coordinator.Handle(a, now);
    auto initial_b = coordinator.Handle(b, now);
    auto old = a;
    old.action = "put";
    old.boot = initial_a.boot;
    old.generation = initial_a.generation;
    old.key = "layer/caller/member";
    old.value = "old address";
    ASSERT_TRUE(coordinator.Handle(old, now).error.empty());
    a.incarnation = "launch-2";
    EXPECT_TRUE(coordinator.Handle(a, now).retry);
    now += std::chrono::seconds(11);
    auto restarted = coordinator.Handle(a, now);
    EXPECT_EQ(restarted.group, initial_a.group);
    EXPECT_EQ(restarted.shard, initial_a.shard);
    EXPECT_GT(restarted.generation, initial_a.generation);
    EXPECT_FALSE(restarted.full);
    EXPECT_FALSE(coordinator.Handle(old, now).error.empty());
    b.incarnation = "launch-2";
    auto rebuilt = coordinator.Handle(b, now);
    EXPECT_EQ(rebuilt.shard, initial_b.shard);
    EXPECT_EQ(rebuilt.generation, restarted.generation);
    EXPECT_TRUE(rebuilt.full);
    auto read = a;
    read.action = "get";
    read.boot = restarted.boot;
    read.generation = restarted.generation;
    read.key = old.key;
    EXPECT_FALSE(coordinator.Handle(read, now).found);
}

TEST(EngramCoordinator, ExpiredOwnerCannotCompleteAGroupForANewNode) {
    for (const std::string mode : {"auto", "manual"}) {
        EngramCoordinator coordinator;
        auto now = std::chrono::steady_clock::now();
        auto a = Join("a");
        auto b = Join("b");
        a.mode = b.mode = mode;
        a.group = b.group = "partial-group";
        a.shard = 0;
        b.shard = 1;
        auto initial = coordinator.Handle(a, now);
        now += std::chrono::seconds(11);
        auto waiting = coordinator.Handle(b, now);
        EXPECT_FALSE(waiting.full) << mode;
        a.incarnation = "launch-2";
        auto replacement = coordinator.Handle(a, now);
        EXPECT_FALSE(replacement.full) << mode;
        EXPECT_GT(replacement.generation, initial.generation);
        // The new node is still polling join and adopts the new generation.
        auto ready = coordinator.Handle(b, now);
        EXPECT_TRUE(ready.full) << mode;
        EXPECT_EQ(ready.generation, replacement.generation);
        EXPECT_TRUE(coordinator.Handle(a, now).full);
    }
}

TEST(EngramCoordinator, MetadataIsImmutableAndFencedByMasterBoot) {
    EngramCoordinator coordinator;
    auto a = Join("a");
    auto joined = coordinator.Handle(a);
    ASSERT_TRUE(coordinator.Handle(Join("b")).full);
    auto request = a;
    request.action = "put";
    request.boot = joined.boot;
    request.generation = joined.generation;
    request.key = "layer/caller/peer";
    request.value = "registered-window";
    ASSERT_TRUE(coordinator.Handle(request).error.empty());
    EXPECT_TRUE(coordinator.Handle(request).error.empty());
    request.value = "different-window";
    EXPECT_FALSE(coordinator.Handle(request).error.empty());
    request.action = "get";
    auto read = coordinator.Handle(request);
    ASSERT_TRUE(read.found);
    EXPECT_EQ(read.value, "registered-window");
    request.boot = "previous-master";
    EXPECT_FALSE(coordinator.Handle(request).error.empty());
}

}  // namespace
}  // namespace mooncake
