#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <latch>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "p2p/client/data_manager_factory.h"
#include "p2p/client/p2p_client_metric.h"
#include "p2p/client/data_manager/v2/data_manager_v2.h"

namespace mooncake::v2 {
namespace {
using namespace std::chrono_literals;

class ManualClock final : public Clock {
   public:
    time_point Now() const override {
        return time_point(std::chrono::milliseconds(ms_.load()));
    }
    void Advance(std::chrono::milliseconds delta) { ms_ += delta.count(); }

   private:
    std::atomic<int64_t> ms_{3600000};
};

class ReviewRegressionTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag once;
        std::call_once(
            once, [] { google::InitGoogleLogging("ReviewRegressionTest"); });
    }
    void SetUp() override {
        directory_ = std::filesystem::temp_directory_path() /
                     ("kvbm-review-" +
                      std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(directory_);
    }
    void TearDown() override {
        manager_.reset();
        std::filesystem::remove_all(directory_);
    }
    Json::Value Tiers() const {
        Json::Value tiers;
        auto& fast = tiers["tiers"][0];
        fast["type"] = "DRAM";
        fast["capacity"] = 8u << 20;
        fast["priority"] = 100;
        fast["allocator_type"] = "OFFSET";
        auto& slow = tiers["tiers"][1];
        slow["type"] = "STORAGE";
        slow["capacity"] = 32u << 20;
        slow["priority"] = 10;
        slow["file_path"] = (directory_ / "slow.data").string();
        return tiers;
    }
    void Build(size_t workers = 0, MetadataCallbacks callbacks = {},
               LocalTransferMode mode = LocalTransferMode::MEMCPY) {
        LocalTransferConfig local;
        local.mode = mode;
        local.local_memcpy_async_worker_num = workers;
        local.te_async_poll_worker_num =
            mode == LocalTransferMode::TE ? workers : 0;
        auto config =
            ParseDataManagerV2Config(Tiers(), local, KeyLeaseConfig{});
        ASSERT_TRUE(config);
        config->register_tiers_with_transfer_engine = false;
        config->movement.offload_high_watermark = 0.5;
        config->stop_drain_timeout = 20ms;
        config->migration.max_batch_items = 1;
        clock_ = std::make_shared<ManualClock>();
        metric_ = std::make_shared<TierMetric>();
        retention_ = std::make_shared<KeyRetentionMetric>("review_retention");
        manager_ = std::make_unique<DataManagerV2>(
            *config, std::make_shared<TransferEngine>(false),
            std::move(callbacks), metric_, retention_, clock_);
        ASSERT_TRUE(manager_->Init());
    }
    tl::expected<void, ErrorCode> Put(const std::string& key,
                                      const std::string& value) {
        std::vector<Slice> slices{
            {const_cast<char*>(value.data()), value.size()}};
        auto task = manager_->Put(key, slices);
        if (!task) return tl::make_unexpected(task.error());
        return (*task)->Wait();
    }
    void CheckCounts() {
        for (const auto& tier : manager_->GetTierViews()) {
            size_t actual = 0;
            manager_->ForEachKeyBatch(
                [&](std::vector<ReplicaLocation>&& batch) {
                    for (const auto& replica : batch)
                        if (replica.tier_id == tier.id) ++actual;
                    return true;
                });
            EXPECT_EQ(metric_->key_count.value({tier.GetName()}), actual);
        }
    }
    std::filesystem::path directory_;
    std::shared_ptr<ManualClock> clock_;
    std::shared_ptr<TierMetric> metric_;
    std::shared_ptr<KeyRetentionMetric> retention_;
    std::unique_ptr<DataManagerV2> manager_;
};

TEST_F(ReviewRegressionTest, ResidencyAndBurstOffloadUseProductionHooks) {
    Build();
    for (int i = 0; i < 7; ++i)
        ASSERT_TRUE(Put(std::to_string(i), std::string(1u << 20, 'x')));
    manager_->DrainForTest();
    auto tiers = manager_->GetTierViews();
    EXPECT_EQ(metric_->key_count.value({tiers.front().GetName()}), 7);
    CheckCounts();
    clock_->Advance(1001ms);
    // No new write: the background retry must carry the burst below watermark.
    for (int i = 0;
         i < 200 && manager_->GetTierViews().front().usage >= 4u << 20; ++i) {
        std::this_thread::sleep_for(10ms);
    }
    manager_->DrainForTest();
    EXPECT_LT(manager_->GetTierViews().front().usage, 4u << 20);
    CheckCounts();
    EXPECT_GT(metric_->offloaded_keys.value({tiers.front().GetName()}), 0);
    clock_->Advance(1001ms);
    std::string out(1u << 20, '\0');
    for (int i = 0; i < 12; ++i) {
        auto read =
            manager_->Get("0", std::vector<Slice>{{out.data(), out.size()}});
        ASSERT_TRUE(read);
        ASSERT_TRUE(read->task_handle->Wait());
    }
    manager_->DrainForTest();
    CheckCounts();
    EXPECT_GT(metric_->onboarded_keys.value({tiers.back().GetName()}), 0);
    EXPECT_TRUE(manager_->Delete("0"));
    CheckCounts();
    EXPECT_TRUE(manager_->RemoveAll());
    CheckCounts();
    EXPECT_EQ(retention_->Snapshot().live_count, 0);
    for (int i = 0; i < 7; ++i)
        ASSERT_TRUE(Put(std::to_string(i), std::string(1u << 20, 'y')));
    clock_->Advance(1001ms);
    manager_->DrainForTest();
    EXPECT_LT(manager_->GetTierViews().front().usage, 4u << 20);
    CheckCounts();
    EXPECT_TRUE(manager_->RemoveAll());
    EXPECT_EQ(retention_->Snapshot().live_count, 0);
}

TEST_F(ReviewRegressionTest,
       ConcurrentCommitAndClearKeepReplicaCountsBalanced) {
    Build();
    std::latch start{2};
    auto writer = std::async(std::launch::async, [&] {
        start.arrive_and_wait();
        for (int i = 0; i < 128; ++i) {
            EXPECT_TRUE(
                Put("churn-" + std::to_string(i), std::string(4096, 'c')));
            std::this_thread::yield();
        }
    });
    auto clearer = std::async(std::launch::async, [&] {
        start.arrive_and_wait();
        for (int i = 0; i < 128; ++i) {
            EXPECT_TRUE(manager_->RemoveAll());
            std::this_thread::yield();
        }
    });
    writer.get();
    clearer.get();
    EXPECT_TRUE(manager_->RemoveAll());
    CheckCounts();
    EXPECT_EQ(retention_->Snapshot().live_count, 0);
}

TEST_F(ReviewRegressionTest, AsyncCopyRunsAwayFromTheCaller) {
    std::promise<std::thread::id> committed;
    MetadataCallbacks callbacks;
    callbacks.add_replica = [&](std::string_view, const UUID&,
                                size_t) -> tl::expected<void, ErrorCode> {
        committed.set_value(std::this_thread::get_id());
        return {};
    };
    Build(1, std::move(callbacks));
    auto caller = std::this_thread::get_id();
    ASSERT_TRUE(Put("async", std::string(4096, 'a')));
    EXPECT_NE(committed.get_future().get(), caller);
    std::string out(4096, '\0');
    auto get =
        manager_->Get("async", std::vector<Slice>{{out.data(), out.size()}});
    ASSERT_TRUE(get);
    ASSERT_TRUE(async_simple::coro::syncAwait(get->task_handle->WaitAsync()));
    EXPECT_EQ(out, std::string(4096, 'a'));
}

TEST_F(ReviewRegressionTest, TeWorkerConfigurationAlsoDispatchesLocalIo) {
    std::promise<std::thread::id> committed;
    MetadataCallbacks callbacks;
    callbacks.add_replica = [&](std::string_view, const UUID&,
                                size_t) -> tl::expected<void, ErrorCode> {
        committed.set_value(std::this_thread::get_id());
        return {};
    };
    // This local-only fixture has no TE endpoint, so the copier falls back to
    // CPU memory; dispatch must still honour TE's configured worker count.
    Build(1, std::move(callbacks), LocalTransferMode::TE);
    const auto caller = std::this_thread::get_id();
    ASSERT_TRUE(Put("te-workers", std::string(4096, 't')));
    EXPECT_NE(committed.get_future().get(), caller);
}

TEST_F(ReviewRegressionTest, DeferredHandleRejectsExecutionAfterDestruction) {
    Build();
    std::string value(4096, 'd');
    std::vector<Slice> slices{{value.data(), value.size()}};
    auto pending = manager_->Put("deferred", slices);
    ASSERT_TRUE(pending);
    manager_.reset();
    auto result = (*pending)->Wait();
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::SHUTTING_DOWN);
}

TEST_F(ReviewRegressionTest, UsageUnregistrationWaitsForCurrentSampling) {
    auto metric = std::make_shared<TierMetric>();
    std::promise<void> entered, release;
    auto released = release.get_future().share();
    std::atomic<bool> block{false};
    const UUID tier{1, 1};
    metric->RegisterTier(tier, "blocking", MemoryType::DRAM, 1, 4096, [&] {
        if (block.exchange(false)) {
            entered.set_value();
            released.wait();
        }
        return size_t{4096};
    });
    block = true;
    auto sampler = std::async(std::launch::async, [&] {
        std::string out;
        metric->serialize(out);
    });
    entered.get_future().wait();
    auto unregister = std::async(std::launch::async,
                                 [&] { metric->UnregisterTierUsage(tier); });
    EXPECT_EQ(unregister.wait_for(50ms), std::future_status::timeout);
    release.set_value();
    sampler.get();
    unregister.get();
    std::string out;
    metric->serialize(out);
    EXPECT_EQ(metric->used_bytes.value({"blocking"}), 0);
}

TEST_F(ReviewRegressionTest, StopWaitsForWorkThatPassedAdmission) {
    std::promise<void> entered, release;
    auto released = release.get_future().share();
    MetadataCallbacks callbacks;
    callbacks.add_replica = [&](std::string_view, const UUID&,
                                size_t) -> tl::expected<void, ErrorCode> {
        entered.set_value();
        released.wait();
        return {};
    };
    Build(0, std::move(callbacks));
    const std::string value(4096, 's');
    auto work =
        std::async(std::launch::async, [&] { return Put("stopping", value); });
    entered.get_future().wait();
    auto stop = std::async(std::launch::async, [&] { manager_->Stop(); });
    EXPECT_EQ(stop.wait_for(100ms), std::future_status::timeout);
    release.set_value();
    EXPECT_TRUE(work.get());
    EXPECT_EQ(stop.wait_for(2s), std::future_status::ready);
    stop.get();
}

TEST_F(ReviewRegressionTest,
       RetainedMetricsCanSampleDuringAndAfterDestruction) {
    for (auto version : {DataManagerVersion::kV1, DataManagerVersion::kV2}) {
        DataManagerConfig config;
        config.version = version;
        config.tier_config = Tiers();
        config.tier_config["tiers"].resize(1);
        config.local_transfer.mode = LocalTransferMode::MEMCPY;
        config.local_transfer.local_memcpy_async_worker_num = 0;
        config.local_transfer.te_async_poll_worker_num = 0;
        config.register_tiers_with_transfer_engine = false;
        auto metric = std::make_shared<TierMetric>();
        DataManagerMetrics metrics;
        metrics.tier_metric = metric;
        auto dm = CreateDataManager(
            config, std::make_shared<TransferEngine>(false), {}, metrics);
        ASSERT_TRUE(dm);
        const auto name = (*dm)->GetTierViews().front().GetName();
        std::atomic<bool> done{false};
        std::thread sampler([&] {
            while (!done.load()) {
                std::string out;
                metric->serialize(out);
            }
        });
        (*dm)->Stop();
        (*dm)->Destroy();
        dm->reset();
        done = true;
        sampler.join();
        std::string out;
        metric->serialize(out);
        EXPECT_EQ(metric->used_bytes.value({name}), 0);
    }
}

}  // namespace
}  // namespace mooncake::v2
