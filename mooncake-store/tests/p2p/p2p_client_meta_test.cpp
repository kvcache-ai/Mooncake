#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <chrono>
#include <thread>
#include <vector>

#define private public
#include "p2p/master/p2p_client_meta.h"
#undef private

#include "p2p/master/p2p_master_metric_manager.h"

namespace mooncake {
namespace {

P2PSegment Segment(UUID id = {1, 1}, std::string name = "segment",
                   size_t size = 4096, int priority = 1,
                   std::vector<std::string> tags = {}, size_t usage = 0) {
    return P2PSegment{.id = id,
                      .name = std::move(name),
                      .size = size,
                      .priority = priority,
                      .tags = std::move(tags),
                      .memory_type = MemoryType::DRAM,
                      .usage = usage};
}

std::shared_ptr<P2PClientMeta> Client(
    UUID id = {10, 10}, int64_t disconnect_timeout_sec = 2,
    int64_t crash_timeout_sec = 5) {
    P2PClientMeta::SetTimeouts(disconnect_timeout_sec, crash_timeout_sec);
    return std::make_shared<P2PClientMeta>(id, "127.0.0.1", 50051);
}

TEST(P2PClientMetaTest, OwnsSegmentSnapshots) {
    auto client = Client();
    auto segment = Segment();
    ASSERT_TRUE(client->MountSegment(segment).has_value());

    segment.name = "caller-change";
    auto stored = client->QuerySegment({1, 1});
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->name, "segment");
    auto segments = client->GetSegments();
    ASSERT_TRUE(segments.has_value());
    ASSERT_EQ(segments->size(), 1);

    ASSERT_TRUE(client->UnmountSegment({1, 1}).has_value());
    segments = client->GetSegments();
    ASSERT_TRUE(segments.has_value());
    EXPECT_TRUE(segments->empty());
}

TEST(P2PClientMetaTest, UsesConfiguredTimeouts) {
    auto client = Client({1, 1}, 1, 2);
    client->health_state_.last_heartbeat =
        std::chrono::steady_clock::now() - std::chrono::seconds(3);
    EXPECT_EQ(client->CheckHealth().second, P2PClientStatus::CRASHED);
}

TEST(P2PClientMetaTest, HeartbeatRecoversDisconnectedClient) {
    auto client = Client({1, 1}, 1, 5);
    client->health_state_.last_heartbeat =
        std::chrono::steady_clock::now() - std::chrono::seconds(2);
    EXPECT_EQ(client->CheckHealth().second, P2PClientStatus::DISCONNECTION);

    auto transition = client->Heartbeat();
    EXPECT_EQ(transition.first, P2PClientStatus::DISCONNECTION);
    EXPECT_EQ(transition.second, P2PClientStatus::HEALTH);
}

TEST(P2PClientMetaTest, CrashedClientDoesNotRecover) {
    auto client = Client({1, 1}, 1, 2);
    client->health_state_.last_heartbeat =
        std::chrono::steady_clock::now() - std::chrono::seconds(3);
    ASSERT_EQ(client->CheckHealth().second, P2PClientStatus::CRASHED);
    EXPECT_EQ(client->Heartbeat().second, P2PClientStatus::CRASHED);
}

TEST(P2PClientMetaTest, RecycleInvokesSegmentRemovalCallback) {
    auto& metrics = P2PMasterMetricManager::instance();
    const auto mem_capacity = metrics.get_total_mem_capacity();
    const auto mem_usage = metrics.get_allocated_mem_size();
    const auto file_capacity = metrics.get_total_file_capacity();
    const auto file_usage = metrics.get_allocated_file_size();
    auto client = Client({7, 7});
    auto dram = Segment({1, 1}, "recycle-dram", 4096, 1, {}, 100);
    auto nvme = Segment({2, 2}, "recycle-nvme", 8192, 1, {}, 200);
    nvme.memory_type = MemoryType::NVME;
    ASSERT_TRUE(client->MountSegment(dram).has_value());
    ASSERT_TRUE(client->MountSegment(nvme).has_value());

    std::vector<UUID> removed_segments;
    client->SetSegmentRemovalCallback([&](const UUID& segment_id) {
        // Callback targets may acquire the client lock after retirement.
        EXPECT_FALSE(client->is_health());
        auto available = client->CheckSegmentAvailable(segment_id);
        ASSERT_FALSE(available.has_value());
        EXPECT_EQ(available.error(), ErrorCode::CLIENT_UNHEALTHY);
        removed_segments.push_back(segment_id);
    });
    client->RecycleMeta();
    EXPECT_EQ(removed_segments.size(), 2);
    EXPECT_EQ(metrics.get_total_mem_capacity(), mem_capacity);
    EXPECT_EQ(metrics.get_allocated_mem_size(), mem_usage);
    EXPECT_EQ(metrics.get_total_file_capacity(), file_capacity);
    EXPECT_EQ(metrics.get_allocated_file_size(), file_usage);
    auto segments = client->GetSegments();
    ASSERT_FALSE(segments.has_value());
    EXPECT_EQ(segments.error(), ErrorCode::CLIENT_UNHEALTHY);
    EXPECT_EQ(client->GetAvailableCapacity(), 0);
    auto stored_segments = client->segment_manager_.GetSegments();
    ASSERT_TRUE(stored_segments.has_value());
    EXPECT_TRUE(stored_segments->empty());
    client->RecycleMeta();
    EXPECT_EQ(removed_segments.size(), 2);
    EXPECT_EQ(metrics.get_total_mem_capacity(), mem_capacity);
    EXPECT_EQ(metrics.get_allocated_mem_size(), mem_usage);
    EXPECT_EQ(metrics.get_total_file_capacity(), file_capacity);
    EXPECT_EQ(metrics.get_allocated_file_size(), file_usage);
}

TEST(P2PClientMetaTest, ConcurrentRecycleOnlyRemovesSegmentsOnce) {
    auto client = Client({7, 7});
    for (uint64_t i = 0; i < 32; ++i) {
        ASSERT_TRUE(client->MountSegment(
                              Segment({i + 1, i + 1}, std::to_string(i)))
                        .has_value());
    }
    std::atomic<size_t> removed{0};
    client->SetSegmentRemovalCallback(
        [&](const UUID&) { removed.fetch_add(1, std::memory_order_relaxed); });
    std::vector<std::thread> workers;
    for (size_t i = 0; i < 8; ++i) {
        workers.emplace_back([&]() { client->RecycleMeta(); });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    EXPECT_EQ(removed.load(), 32);
    auto segments = client->GetSegments();
    ASSERT_FALSE(segments.has_value());
    EXPECT_EQ(segments.error(), ErrorCode::CLIENT_UNHEALTHY);
    EXPECT_EQ(client->GetAvailableCapacity(), 0);
    auto stored_segments = client->segment_manager_.GetSegments();
    ASSERT_TRUE(stored_segments.has_value());
    EXPECT_TRUE(stored_segments->empty());
}

TEST(P2PClientMetaTest, RecycledClientRejectsCapturedPointerOperations) {
    auto client = Client();
    ASSERT_TRUE(client->MountSegment(Segment()).has_value());
    auto captured = client;
    client->RecycleMeta();

    auto mounted = captured->MountSegment(Segment({2, 2}, "late"));
    ASSERT_FALSE(mounted.has_value());
    EXPECT_EQ(mounted.error(), ErrorCode::CLIENT_UNHEALTHY);
    auto queried = captured->QuerySegment({1, 1});
    ASSERT_FALSE(queried.has_value());
    EXPECT_EQ(queried.error(), ErrorCode::CLIENT_UNHEALTHY);
    auto available = captured->CheckSegmentAvailable({1, 1});
    ASSERT_FALSE(available.has_value());
    EXPECT_EQ(available.error(), ErrorCode::CLIENT_UNHEALTHY);
    auto unmounted = captured->UnmountSegment({1, 1});
    ASSERT_FALSE(unmounted.has_value());
    EXPECT_EQ(unmounted.error(), ErrorCode::CLIENT_UNHEALTHY);
    EXPECT_FALSE(captured->is_health());
    EXPECT_EQ(captured->GetAvailableCapacity(), 0);
    auto stored_segments = captured->segment_manager_.GetSegments();
    ASSERT_TRUE(stored_segments.has_value());
    EXPECT_TRUE(stored_segments->empty());
}

TEST(P2PClientMetaTest, ConcurrentMountAndRecycleDoNotLeakSegments) {
    for (uint64_t round = 0; round < 32; ++round) {
        auto client = Client({round + 1, 99});
        const auto segment = Segment({round + 1, 1}, "racing-mount");
        std::atomic<size_t> removed{0};
        client->SetSegmentRemovalCallback([&](const UUID& segment_id) {
            EXPECT_EQ(segment_id, segment.id);
            removed.fetch_add(1, std::memory_order_relaxed);
        });

        std::barrier start(3);
        ErrorCode mount_error = ErrorCode::OK;
        std::thread mounting([&]() {
            start.arrive_and_wait();
            auto result = client->MountSegment(segment);
            if (!result.has_value()) {
                mount_error = result.error();
            }
        });
        std::thread recycling([&]() {
            start.arrive_and_wait();
            client->RecycleMeta();
        });
        start.arrive_and_wait();
        mounting.join();
        recycling.join();

        if (mount_error == ErrorCode::OK) {
            EXPECT_EQ(removed.load(), 1);
        } else {
            EXPECT_EQ(mount_error, ErrorCode::CLIENT_UNHEALTHY);
            EXPECT_EQ(removed.load(), 0);
        }
        EXPECT_FALSE(client->is_health());
        EXPECT_EQ(client->segment_manager_.GetCapacityUsage(),
                  std::make_pair(size_t{0}, size_t{0}));
        auto stored_segments = client->segment_manager_.GetSegments();
        ASSERT_TRUE(stored_segments.has_value());
        EXPECT_TRUE(stored_segments->empty());
        client->RecycleMeta();
        EXPECT_EQ(removed.load(), mount_error == ErrorCode::OK ? 1 : 0);
    }
}

TEST(P2PClientMetaTest, SegmentAvailabilityChecksExistenceAndCurrentHealth) {
    auto client = Client({1, 1}, 1, 5);
    ASSERT_TRUE(client->MountSegment(Segment()).has_value());
    EXPECT_TRUE(client->CheckSegmentAvailable({1, 1}).has_value());
    auto missing = client->CheckSegmentAvailable({2, 2});
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::SEGMENT_NOT_FOUND);

    client->health_state_.last_heartbeat =
        std::chrono::steady_clock::now() - std::chrono::seconds(2);
    ASSERT_EQ(client->CheckHealth().second, P2PClientStatus::DISCONNECTION);
    auto disconnected = client->CheckSegmentAvailable({1, 1});
    ASSERT_FALSE(disconnected.has_value());
    EXPECT_EQ(disconnected.error(), ErrorCode::CLIENT_UNHEALTHY);

    ASSERT_EQ(client->Heartbeat().second, P2PClientStatus::HEALTH);
    EXPECT_TRUE(client->CheckSegmentAvailable({1, 1}).has_value());
    ASSERT_TRUE(client->UnmountSegment({1, 1}).has_value());
    auto removed = client->CheckSegmentAvailable({1, 1});
    ASSERT_FALSE(removed.has_value());
    EXPECT_EQ(removed.error(), ErrorCode::SEGMENT_NOT_FOUND);

    client->health_state_.last_heartbeat =
        std::chrono::steady_clock::now() - std::chrono::seconds(6);
    ASSERT_EQ(client->CheckHealth().second, P2PClientStatus::CRASHED);
    auto crashed = client->CheckSegmentAvailable({1, 1});
    ASSERT_FALSE(crashed.has_value());
    EXPECT_EQ(crashed.error(), ErrorCode::CLIENT_UNHEALTHY);
}

TEST(P2PClientMetaTest, DerivesAvailableCapacityFromSegmentManager) {
    auto client = Client();
    ASSERT_TRUE(client->MountSegment(Segment({1, 1}, "a", 4096, 1, {}, 100))
                    .has_value());
    ASSERT_TRUE(client->MountSegment(Segment({2, 2}, "b", 8192, 1, {}, 200))
                    .has_value());
    EXPECT_EQ(client->GetAvailableCapacity(), 11988);

    TierUsageInfo update;
    update.segment_id = {1, 1};
    update.usage = 1000;
    auto result = client->UpdateSegmentUsages({update});
    ASSERT_EQ(result.sub_results.size(), 1);
    EXPECT_EQ(result.sub_results.front().error, ErrorCode::OK);
    EXPECT_EQ(client->GetAvailableCapacity(), 11088);
}

TEST(P2PClientMetaTest, UpdateUsageReportsPerSegmentErrors) {
    auto client = Client();
    ASSERT_TRUE(client->MountSegment(Segment()).has_value());
    TierUsageInfo good{.segment_id = {1, 1}, .usage = 100};
    TierUsageInfo missing{.segment_id = {9, 9}, .usage = 100};
    auto result = client->UpdateSegmentUsages({good, missing});
    ASSERT_EQ(result.sub_results.size(), 2);
    EXPECT_EQ(result.sub_results[0].error, ErrorCode::OK);
    EXPECT_EQ(result.sub_results[1].error, ErrorCode::SEGMENT_NOT_FOUND);
}

TEST(P2PClientMetaTest, ScoresOnlyEligibleSegments) {
    auto client = Client();
    ASSERT_TRUE(client->MountSegment(
                          Segment({1, 1}, "slow", 1000, 1, {"cold"}, 100))
                    .has_value());
    ASSERT_TRUE(client->MountSegment(
                          Segment({2, 2}, "fast", 2000, 10, {"hot"}, 500))
                    .has_value());

    P2PWriteRouteConfig config;
    config.top_tier_only = true;
    config.priority_limit = 5;
    auto candidate = client->GetWriteRouteCandidate(config);
    ASSERT_TRUE(candidate.has_value());
    EXPECT_EQ(candidate->available_capacity, 1500);
    EXPECT_DOUBLE_EQ(candidate->score, 0.75);

    config.tag_filters = {"hot"};
    EXPECT_FALSE(client->GetWriteRouteCandidate(config).has_value());
}

TEST(P2PClientMetaTest, WriteCandidateDefersHealthChecksUntilKeySelection) {
    auto client = Client({1, 1}, 1, 5);
    ASSERT_TRUE(client->MountSegment(Segment()).has_value());
    client->health_state_.last_heartbeat =
        std::chrono::steady_clock::now() - std::chrono::seconds(2);
    ASSERT_EQ(client->CheckHealth().second, P2PClientStatus::DISCONNECTION);

    // Config filtering keeps capacity available for recovery during a batch.
    // The service checks current health separately for each key.
    P2PWriteRouteConfig config;
    auto candidate = client->GetWriteRouteCandidate(config);
    ASSERT_TRUE(candidate.has_value());
    EXPECT_EQ(candidate->available_capacity, 4096);
    EXPECT_FALSE(client->is_health());
    ASSERT_EQ(client->Heartbeat().second, P2PClientStatus::HEALTH);
    EXPECT_TRUE(client->is_health());
    EXPECT_TRUE(client->GetWriteRouteCandidate(config).has_value());

    client->health_state_.last_heartbeat =
        std::chrono::steady_clock::now() - std::chrono::seconds(6);
    ASSERT_EQ(client->CheckHealth().second, P2PClientStatus::CRASHED);
    EXPECT_FALSE(client->is_health());
    EXPECT_TRUE(client->GetWriteRouteCandidate(config).has_value());
    client->RecycleMeta();
    EXPECT_EQ(client->GetAvailableCapacity(), 0);
    EXPECT_FALSE(client->GetWriteRouteCandidate(config).has_value());
}

TEST(P2PClientMetaTest, WriteCandidateUsesEligibleCapacity) {
    auto client = Client();
    ASSERT_TRUE(client->MountSegment(Segment({1, 1}, "slow", 1000, 1, {}, 100))
                    .has_value());
    ASSERT_TRUE(client->MountSegment(Segment({2, 2}, "fast", 2000, 10, {}, 500))
                    .has_value());
    ASSERT_TRUE(client->MountSegment(Segment({3, 3}, "peer", 1000, 10, {}, 500))
                    .has_value());
    ASSERT_TRUE(client->MountSegment(
                          Segment({4, 4}, "filtered", 4000, 20, {"skip"}, 1000))
                    .has_value());

    P2PWriteRouteConfig config;
    config.tag_filters = {"skip"};
    const auto top_config = config;
    const auto candidate = client->GetWriteRouteCandidate(config);
    EXPECT_EQ(client->GetAvailableCapacity(), 5900);
    ASSERT_TRUE(candidate.has_value());
    EXPECT_EQ(candidate->available_capacity, 2000);
    EXPECT_DOUBLE_EQ(candidate->score, 2.0 / 3.0);

    config.top_tier_only = false;
    auto all_tiers = client->GetWriteRouteCandidate(config);
    ASSERT_TRUE(all_tiers.has_value());
    EXPECT_EQ(all_tiers->available_capacity, 2900);
    EXPECT_DOUBLE_EQ(all_tiers->score, 2900.0 / 4000.0);
    config.priority_limit = 11;
    EXPECT_FALSE(client->GetWriteRouteCandidate(config).has_value());
    EXPECT_EQ(client->GetAvailableCapacity(), 5900);

    auto updated = client->UpdateSegmentUsages(
        {TierUsageInfo{.segment_id = {2, 2}, .usage = 1500}});
    ASSERT_EQ(updated.sub_results.size(), 1);
    ASSERT_EQ(updated.sub_results[0].error, ErrorCode::OK);
    const auto next = client->GetWriteRouteCandidate(top_config);
    EXPECT_EQ(client->GetAvailableCapacity(), 4900);
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(next->available_capacity, 1000);
    EXPECT_EQ(candidate->available_capacity, 2000);
}

TEST(P2PClientMetaTest, FullTierRemainsEligibleForZeroSizeKeys) {
    auto client = Client();
    ASSERT_TRUE(client->MountSegment(Segment({1, 1}, "full", 1000, 1, {}, 1000))
                    .has_value());
    auto candidate = client->GetWriteRouteCandidate(P2PWriteRouteConfig{});
    ASSERT_TRUE(candidate.has_value());
    EXPECT_EQ(candidate->available_capacity, 0);
    EXPECT_DOUBLE_EQ(candidate->score, 0);
}

TEST(P2PClientMetaTest, CandidateCapacityAndScoreUseTheSameSegmentVersion) {
    auto client = Client();
    ASSERT_TRUE(client->MountSegment(Segment({1, 1}, "segment", 1000))
                    .has_value());
    std::atomic<bool> start{false};
    std::thread writer([&]() {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (size_t i = 0; i < 1000; ++i) {
            auto result = client->UpdateSegmentUsages(
                {TierUsageInfo{.segment_id = {1, 1},
                               .usage = i % 2 == 0 ? 100u : 900u}});
            ASSERT_EQ(result.sub_results.size(), 1);
            EXPECT_EQ(result.sub_results[0].error, ErrorCode::OK);
        }
    });
    start.store(true, std::memory_order_release);
    for (size_t i = 0; i < 1000; ++i) {
        auto candidate = client->GetWriteRouteCandidate(P2PWriteRouteConfig{});
        EXPECT_TRUE(candidate.has_value());
        if (candidate) {
            EXPECT_DOUBLE_EQ(candidate->score,
                             candidate->available_capacity / 1000.0);
        }
    }
    writer.join();
}

}  // namespace
}  // namespace mooncake
