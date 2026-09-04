// V2's operator-facing metrics (acceptance item 7).
//
// The point of these is a graduated rollout: someone switching
// data_manager_version to v2 has to keep seeing the same tier series V1
// produced, and has to be able to see the four things that only V2 can go
// wrong at. A counter nobody can observe is the same as no counter.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "p2p/client/data_manager_types.h"
#include "p2p/client/p2p_client_metric.h"
#include "p2p/client/v2/data_manager_v2.h"
#include "transfer_engine.h"
#include "types.h"

namespace mooncake::v2 {
namespace {

constexpr const char* kTwoTiers = R"({
    "tiers": [
        {"type": "DRAM", "capacity": 8388608, "priority": 100,
         "allocator_type": "OFFSET"},
        {"type": "STORAGE", "capacity": 67108864, "priority": 10}
    ]
})";

Json::Value ParseJson(const std::string& text) {
    Json::Value value;
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;
    EXPECT_TRUE(
        reader->parse(text.data(), text.data() + text.size(), &value, &errs))
        << errs;
    return value;
}

std::string Payload(size_t size, char seed) { return std::string(size, seed); }

}  // namespace

class V2MetricsTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("V2MetricsTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        storage_dir_ = std::filesystem::temp_directory_path() /
                       ("mooncake_v2_metrics_" +
                        std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(storage_dir_);
        std::filesystem::create_directories(storage_dir_);
        setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", storage_dir_.c_str(), 1);
        transfer_engine_ = std::make_shared<TransferEngine>(false);
        tier_metric_ = std::make_shared<TierMetric>();
    }

    void TearDown() override {
        if (manager_) {
            manager_->Stop();
            manager_->Destroy();
            manager_.reset();
        }
        std::filesystem::remove_all(storage_dir_);
    }

    /** Build a manager, optionally overriding the v2 config block. */
    void Build(const std::string& tiers_json = kTwoTiers,
               std::chrono::milliseconds stop_timeout =
                   std::chrono::milliseconds(200)) {
        auto config = ParseDataManagerV2Config(
            ParseJson(tiers_json), LocalTransferConfig{}, KeyLeaseConfig{});
        ASSERT_TRUE(config.has_value()) << toString(config.error());
        config->register_tiers_with_transfer_engine = false;
        config->local_transfer.local_memcpy_async_worker_num = 0;
        config->local_transfer.te_async_poll_worker_num = 0;
        config->stop_drain_timeout = stop_timeout;

        manager_ = std::make_unique<DataManagerV2>(
            *config, transfer_engine_, MetadataCallbacks{}, tier_metric_);
        ASSERT_TRUE(manager_->Init().has_value());
    }

    tl::expected<void, ErrorCode> Put(const std::string& key,
                                      const std::string& payload) {
        std::vector<Slice> slices = {
            {const_cast<char*>(payload.data()), payload.size()}};
        auto handle = manager_->Put(key, slices);
        if (!handle) return tl::make_unexpected(handle.error());
        return handle.value()->Wait();
    }

    std::filesystem::path storage_dir_;
    std::shared_ptr<TransferEngine> transfer_engine_;
    std::shared_ptr<TierMetric> tier_metric_;
    std::unique_ptr<DataManagerV2> manager_;
};

// The label is what an operator's dashboard is keyed on, so it has to be the
// same string V1 emitted for the same tier.
TEST_F(V2MetricsTest, EveryConfiguredTierGetsV1sMetricSeries) {
    Build();
    const auto views = manager_->GetTierViews();
    ASSERT_EQ(views.size(), 2U);

    std::string serialized;
    tier_metric_->serialize(serialized);
    for (const auto& view : views) {
        EXPECT_TRUE(tier_metric_->HasTier(view.id))
            << "tier " << view.GetName() << " has no metric series";
        EXPECT_NE(serialized.find(MakeTierSegmentName(view.id)),
                  std::string::npos)
            << "tier label missing from the serialized metrics";
    }
    EXPECT_NE(serialized.find("mooncake_p2p_tier_capacity_bytes"),
              std::string::npos);
    EXPECT_NE(serialized.find("mooncake_p2p_tier_used_bytes"),
              std::string::npos);
}

// The usage gauge has to track the tier, not a value frozen at registration.
TEST_F(V2MetricsTest, UsageGaugeFollowsRealUsage) {
    Build();
    const auto views = manager_->GetTierViews();
    UUID fast{0, 0};
    for (const auto& view : views) {
        if (view.type == MemoryType::DRAM) fast = view.id;
    }
    const std::array<std::string, 1> label = {MakeTierSegmentName(fast)};

    std::string before;
    tier_metric_->serialize(before);
    const int64_t used_before = tier_metric_->used_bytes.value(label);
    const int64_t key_count_before = tier_metric_->key_count.value(label);

    ASSERT_TRUE(Put("metrics/key", Payload(256 * 1024, 'm')).has_value());

    std::string after;
    tier_metric_->serialize(after);
    EXPECT_GT(tier_metric_->used_bytes.value(label), used_before)
        << "the usage gauge did not follow a write";
    EXPECT_EQ(tier_metric_->key_count.value(label), key_count_before + 1)
        << "a committed replica was not counted";

    ASSERT_TRUE(manager_->Delete("metrics/key").has_value());
    EXPECT_EQ(tier_metric_->key_count.value(label), key_count_before)
        << "a removed replica was not uncounted";
}

// A caller that never waits on its handle must not stall shutdown, and the
// operator must be able to see that it happened.
TEST_F(V2MetricsTest, StopDrainTimeoutIsCounted) {
    Build(kTwoTiers, std::chrono::milliseconds(100));
    EXPECT_EQ(manager_->Metrics().stop_drain_timeout_hit, 0U);

    const std::string payload = Payload(4096, 'p');
    std::vector<Slice> slices = {
        {const_cast<char*>(payload.data()), payload.size()}};
    auto handle = manager_->Put("metrics/unwaited", slices);
    ASSERT_TRUE(handle.has_value());

    manager_->Stop();
    EXPECT_EQ(manager_->Metrics().stop_drain_timeout_hit, 1U)
        << "Stop cancelled without recording that it had to";
}

// Section 7.3 accepts that rectify can fire on a key that is in fact live.
// Accepting a risk is only reasonable while it stays visible.
TEST_F(V2MetricsTest, ARectifiedKeyThatComesBackIsCountedAsAFalsePositive) {
    Build();
    bool rectified = false;
    manager_->SetRectifyCallback(
        [&](std::string_view, std::optional<UUID>) { rectified = true; });

    EXPECT_EQ(manager_->Metrics().rectify_false_positive_suspected, 0U);

    // A miss, so rectify asks Master to drop the replica...
    manager_->RectifyReadRoute("metrics/racy");
    ASSERT_TRUE(rectified);
    EXPECT_EQ(manager_->Metrics().rectify_false_positive_suspected, 0U)
        << "a plain miss is not yet a false positive";

    // ...and then the key turns out to exist locally after all.
    ASSERT_TRUE(Put("metrics/racy", Payload(4096, 'r')).has_value());
    EXPECT_EQ(manager_->Metrics().rectify_false_positive_suspected, 1U)
        << "a rectified key that came straight back was not flagged";

    // Only blamed once: a second commit of the same key is a normal write.
    ASSERT_TRUE(manager_->Delete("metrics/racy").has_value());
    ASSERT_TRUE(Put("metrics/racy", Payload(4096, 'r')).has_value());
    EXPECT_EQ(manager_->Metrics().rectify_false_positive_suspected, 1U);
}

// A saturated event queue must not stall the writer; it applies the fact
// inline instead, and says how often it had to.
TEST_F(V2MetricsTest, InlineAppliedLifecycleEventsAreCounted) {
    // One shard, one slot: the second lifecycle event of any burst cannot be
    // queued, which is the situation the counter exists for.
    static constexpr const char* kTinyQueue = R"({
        "tiers": [
            {"type": "DRAM", "capacity": 8388608, "priority": 100,
             "allocator_type": "OFFSET"}
        ],
        "v2": {"events": {"shard_count": 1, "event_queue_capacity": 1,
                          "movement_queue_capacity": 1,
                          "movement_worker_count": 1}}
    })";
    Build(kTinyQueue);

    // Written concurrently on purpose: one writer at a time gives the single
    // worker time to drain the slot between commits, and the queue is never
    // actually full. A burst is what a saturated queue looks like in practice.
    constexpr int kThreads = 8;
    constexpr int kPerThread = 40;
    std::vector<std::thread> writers;
    std::atomic<int> committed{0};
    writers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        writers.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                const std::string key = "metrics/burst/" + std::to_string(t) +
                                        "/" + std::to_string(i);
                if (Put(key, Payload(4096, 'b')).has_value()) {
                    committed.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& writer : writers) writer.join();
    ASSERT_GT(committed.load(), 0);

    // Not an exact number: how many events the single worker drains between
    // commits is a scheduling detail. That the writers never blocked, and that
    // the facts were applied rather than lost, is not.
    EXPECT_GT(manager_->Metrics().lifecycle_event_inline_applied, 0U)
        << "a one-slot queue absorbed " << committed.load()
        << " concurrent commits without ever falling back";
}

// Configuration that used to be unreachable: if these did not parse, the
// tiny-queue test above could not have been written.
TEST_F(V2MetricsTest, EventAndPolicyConfigurationIsReachableFromJson) {
    auto config =
        ParseDataManagerV2Config(ParseJson(R"({
        "tiers": [{"type": "DRAM", "capacity": 1048576, "priority": 10}],
        "v2": {
            "events": {"shard_count": 2, "event_queue_capacity": 32,
                       "movement_queue_capacity": 8,
                       "movement_worker_count": 3},
            "placement_policy": {"offload_high_watermark": 0.75,
                                 "offload_low_watermark": 0.5,
                                 "onboard_min_frequency": 4},
            "max_registration_retry": 3,
            "stop_drain_timeout_ms": 250,
            "lease_shard_count": 8,
            "hot_key_snapshot_limit": 16
        }
    })"),
                                 LocalTransferConfig{}, KeyLeaseConfig{});
    ASSERT_TRUE(config.has_value()) << toString(config.error());

    EXPECT_EQ(config->events.shard_count, 2U);
    EXPECT_EQ(config->events.event_queue_capacity, 32U);
    // The command queue moved to the migration engine and became one queue
    // per route; the old JSON name still sets the total, so an existing tier
    // file keeps meaning what it meant.
    EXPECT_EQ(config->migration.max_queued_requests, 8U);
    EXPECT_EQ(config->movement_worker_count, 3U);
    // The placement_policy JSON block keeps its name and its keys, but the
    // monolithic policy behind it is gone: each key now lands where that
    // decision actually lives. offload_low_watermark had no reader once
    // offload became one decision per event, so it is accepted and ignored.
    EXPECT_DOUBLE_EQ(config->movement.offload_high_watermark, 0.75);
    EXPECT_DOUBLE_EQ(config->movement.onboard_min_read_heat, 4.0);
    EXPECT_EQ(config->max_registration_retry, 3U);
    EXPECT_EQ(config->stop_drain_timeout, std::chrono::milliseconds(250));
    EXPECT_EQ(config->lease_shard_count, 8U);
    EXPECT_EQ(config->frequency_tracker.max_snapshot_keys, 16U);
}

TEST_F(V2MetricsTest, InvalidEventAndPolicyConfigurationIsRejected) {
    auto zero_queue =
        ParseDataManagerV2Config(ParseJson(R"({
        "tiers": [{"type": "DRAM", "capacity": 1048576}],
        "v2": {"events": {"event_queue_capacity": 0}}
    })"),
                                 LocalTransferConfig{}, KeyLeaseConfig{});
    EXPECT_EQ(zero_queue.error(), ErrorCode::INVALID_PARAMS);

    // offload_low_watermark no longer exists: offload is one decision per
    // commit rather than a drain down to a floor, so there is nothing for a
    // low watermark to mean. The watermark that does remain has to leave room
    // before the tier is full, because offload is the only thing keeping a
    // single-tier object alive.
    auto no_headroom =
        ParseDataManagerV2Config(ParseJson(R"({
        "tiers": [{"type": "DRAM", "capacity": 1048576}],
        "v2": {"placement_policy": {"offload_high_watermark": 1.0}}
    })"),
                                 LocalTransferConfig{}, KeyLeaseConfig{});
    ASSERT_FALSE(no_headroom.has_value());
    EXPECT_EQ(no_headroom.error(), ErrorCode::INVALID_PARAMS);

    auto zero_timeout =
        ParseDataManagerV2Config(ParseJson(R"({
        "tiers": [{"type": "DRAM", "capacity": 1048576}],
        "v2": {"stop_drain_timeout_ms": 0}
    })"),
                                 LocalTransferConfig{}, KeyLeaseConfig{});
    EXPECT_EQ(zero_timeout.error(), ErrorCode::INVALID_PARAMS);
}

// A capped snapshot is a partial answer to "everything you track", and
// HARecoveryManager acts on exactly what it gets back.
TEST_F(V2MetricsTest, ATruncatedHotKeySnapshotIsNotSilent) {
    static constexpr const char* kTinyLimit = R"({
        "tiers": [
            {"type": "DRAM", "capacity": 8388608, "priority": 100,
             "allocator_type": "OFFSET"}
        ],
        "v2": {"hot_key_snapshot_limit": 4}
    })";
    Build(kTinyLimit);

    for (int i = 0; i < 12; ++i) {
        ASSERT_TRUE(Put("metrics/hot/" + std::to_string(i), Payload(1024, 'h'))
                        .has_value());
    }

    auto stats = manager_->GetHotKeyStats(/*hot_key_num=*/0);
    EXPECT_EQ(stats.hot_keys.size(), 4U)
        << "the configured cap was not applied";
    EXPECT_GT(manager_->HotKeyTruncationCount(), 0U)
        << "the answer was cut short without saying so";
}

}  // namespace mooncake::v2
