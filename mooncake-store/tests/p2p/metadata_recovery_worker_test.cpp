/**
 * @file metadata_recovery_worker_test.cpp
 * @brief Metadata replay, cancellation and result reporting. Service policy
 *        and notifier lifecycle are covered by ha_integration_test.
 */
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#define private public
#define protected public
#include "p2p/ha/metadata_recovery_worker.h"
#include "p2p/client/v1/data_manager_v1.h"
#undef protected
#undef private

#include "p2p/client/async_metadata_notifier.h"
#include "p2p/master/p2p_master_client.h"
#include "p2p/client/tiered_cache/tiered_backend.h"
#include "test_p2p_server_helpers.h"
#include "types.h"
#include "../utils/common.h"

namespace mooncake {
namespace test {

static bool ParseJsonString(const std::string& json_str, Json::Value& value) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;
    return reader->parse(json_str.data(), json_str.data() + json_str.size(),
                         &value, &errs);
}

// ============================================================================
// Test fixture
// ============================================================================

class MetadataRecoveryWorkerTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("MetadataRecoveryWorkerTest");
        FLAGS_logtostderr = 1;

        ASSERT_TRUE(master_.Start()) << "Failed to start in-proc P2P master";
        master_addr_ = master_.master_address();
    }

    static void TearDownTestSuite() {
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    void SetUp() override {
        client_id_ = generate_uuid();
        segment_ = MakeSegment();

        // Register client + segment with master
        P2PRegisterClientRequest reg;
        reg.client_id = client_id_;
        reg.ip_address = "127.0.0.1";
        reg.rpc_port = 50099;
        reg.segments.push_back(segment_);
        auto& svc = master_.GetWrapped().GetMasterService();
        auto res = svc.RegisterClient(reg);
        ASSERT_TRUE(res.has_value())
            << "RegisterClient failed: " << res.error();
        ASSERT_TRUE(
            svc.Heartbeat({.client_id = client_id_,
                           .service_state = P2PClientServiceState::ONLINE})
                .has_value());

        // Connect P2PMasterClient to in-proc master
        master_client_ = std::make_unique<P2PMasterClient>(client_id_);
        auto ec = master_client_->Connect(master_addr_);
        ASSERT_EQ(ec, ErrorCode::OK) << "Connect failed";

        // Initialise with an empty TieredBackend (no tiers, no keys) and a
        // default TransferEngine (not init()'d — only RDMA ops need it, and
        // the recovery pipeline only iterates metadata which is empty here).
        data_manager_ =
            std::make_unique<DataManagerV1>(std::make_unique<TieredBackend>(),
                                            std::make_shared<TransferEngine>());
        notifier_.reset();
    }

    void TearDown() override {
        notifier_.reset();
        master_client_.reset();
    }

    static P2PSegment MakeSegment(size_t size = 16 * 1024 * 1024) {
        P2PSegment seg;
        seg.id = generate_uuid();
        seg.name = "test_segment";
        seg.size = size;
        seg.priority = 0;
        seg.memory_type = MemoryType::DRAM;
        return seg;
    }

    std::unique_ptr<MetadataRecoveryWorker> CreateWorker(
        MetadataRecoveryWorker::RecoveryMode mode =
            MetadataRecoveryWorker::RecoveryMode::FullMetadataSync) {
        return std::make_unique<MetadataRecoveryWorker>(
            *data_manager_, notifier_.get(),
            [this](std::string_view key, const UUID& tier_id, size_t size) {
                return master_client_->PublishRoute(
                    {key, size, client_id_, tier_id});
            },
            mode);
    }

    std::unique_ptr<MetadataRecoveryWorker> CreateWorkerWithNotifier(
        MetadataRecoveryWorker::RecoveryMode mode =
            MetadataRecoveryWorker::RecoveryMode::FullMetadataSync) {
        notifier_ = std::make_unique<AsyncMetadataNotifier>(
            *master_client_, client_id_, 1, 2000, 4000);
        EXPECT_EQ(notifier_->Start(), ErrorCode::OK);
        return CreateWorker(mode);
    }

    std::optional<UUID> InitLocalDataManagerAndPut(const std::string& key,
                                                   const std::string& value) {
        std::string json_config_str = R"({
            "tiers": [
                {
                    "type": "DRAM",
                    "capacity": 67108864,
                    "priority": 10,
                    "tags": ["fast", "local"],
                    "allocator_type": "OFFSET"
                }
            ]
        })";
        Json::Value config;
        if (!ParseJsonString(json_config_str, config)) {
            ADD_FAILURE() << "Failed to parse test tier config";
            return std::nullopt;
        }

        auto tiered_backend = std::make_unique<TieredBackend>();
        auto init_result = InitTieredBackendForTest(*tiered_backend, config);
        if (!init_result.has_value()) {
            ADD_FAILURE() << "InitTieredBackendForTest failed: "
                          << init_result.error();
            return std::nullopt;
        }

        auto transfer_engine = std::make_shared<TransferEngine>(false);
        LocalTransferConfig transfer_config;
        transfer_config.mode = LocalTransferMode::MEMCPY;

        data_manager_.reset();
        data_manager_ = std::make_unique<DataManagerV1>(
            std::move(tiered_backend), transfer_engine,
            /*metadata_shard_count=*/1024, transfer_config);

        std::vector<char> buffer(value.begin(), value.end());
        std::vector<Slice> slices = {
            Slice{buffer.data(), static_cast<uint64_t>(buffer.size())}};
        auto put_result = data_manager_->Put(key, slices);
        if (!put_result.has_value()) {
            ADD_FAILURE() << "DataManager Put failed: " << put_result.error();
            return std::nullopt;
        }
        auto wait_result = put_result.value()->Wait();
        if (!wait_result.has_value()) {
            ADD_FAILURE() << "DataManager Put wait failed: "
                          << wait_result.error();
            return std::nullopt;
        }

        auto tier_ids = data_manager_->GetReplicaTierIds(key);
        if (tier_ids.size() != 1) {
            ADD_FAILURE() << "Expected 1 local replica tier, got "
                          << tier_ids.size();
            return std::nullopt;
        }
        return tier_ids[0];
    }

    void MountLocalTierOnMaster(const UUID& tier_id) {
        P2PSegment local_segment = MakeSegment();
        local_segment.id = tier_id;
        local_segment.name =
            "local_recovery_tier_" + std::to_string(tier_id.first);
        auto& svc = master_.GetWrapped().GetMasterService();
        auto result = svc.MountSegment(local_segment, client_id_);
        ASSERT_TRUE(result.has_value())
            << "MountSegment failed: " << result.error();
    }

    void WaitUntilCompleted(MetadataRecoveryWorker& worker) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (worker.GetStatus() == MetadataRecoveryWorker::Status::RUNNING &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(worker.GetStatus(),
                  MetadataRecoveryWorker::Status::COMPLETED);
    }

    bool WaitUntilReplicaVisible(
        const std::string& key,
        std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
        auto& svc = master_.GetWrapped().GetMasterService();
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        do {
            if (svc.GetReadRoute(key).has_value()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } while (std::chrono::steady_clock::now() < deadline);
        return false;
    }

    static testing::InProcP2PMaster master_;
    static std::string master_addr_;

    UUID client_id_{};
    P2PSegment segment_;
    std::unique_ptr<P2PMasterClient> master_client_;
    std::unique_ptr<DataManager> data_manager_;
    std::unique_ptr<AsyncMetadataNotifier> notifier_;
};

testing::InProcP2PMaster MetadataRecoveryWorkerTest::master_;
std::string MetadataRecoveryWorkerTest::master_addr_;

TEST_F(MetadataRecoveryWorkerTest, EmptyRecoveryAndRepeatedStop) {
    auto worker = CreateWorkerWithNotifier();
    EXPECT_EQ(worker->GetStatus(), MetadataRecoveryWorker::Status::IDLE);
    EXPECT_EQ(worker->Start(), ErrorCode::OK);
    WaitUntilCompleted(*worker);
    worker->Stop();
    worker->Stop();
    // Stopping a recovery task must not stop the Service-owned sender.
    EXPECT_TRUE(notifier_->running_.load());
    EXPECT_EQ(worker->Start(), ErrorCode::OK);
    WaitUntilCompleted(*worker);
}

TEST_F(MetadataRecoveryWorkerTest, ConcurrentStartAndStopOwnOneTask) {
    auto worker = CreateWorkerWithNotifier();
    std::thread starts([&] {
        for (int i = 0; i < 10; ++i) {
            EXPECT_EQ(worker->Start(), ErrorCode::OK);
        }
    });
    std::thread stops([&] {
        for (int i = 0; i < 10; ++i) {
            worker->Stop();
        }
    });
    starts.join();
    stops.join();
    worker->Stop();
    EXPECT_NE(worker->GetStatus(), MetadataRecoveryWorker::Status::RUNNING);
    EXPECT_TRUE(notifier_->running_.load());
}

TEST_F(MetadataRecoveryWorkerTest, ReplaysLocalReplicaWithOrWithoutNotifier) {
    for (bool use_notifier : {false, true}) {
        SCOPED_TRACE(use_notifier);
        const std::string key = "recovery-" + std::to_string(use_notifier);
        auto tier_id = InitLocalDataManagerAndPut(key, "recovery-value");
        ASSERT_TRUE(tier_id.has_value());
        MountLocalTierOnMaster(*tier_id);
        auto worker =
            use_notifier ? CreateWorkerWithNotifier() : CreateWorker();
        EXPECT_EQ(worker->Start(), ErrorCode::OK);
        WaitUntilCompleted(*worker);
        ASSERT_TRUE(WaitUntilReplicaVisible(key));
        auto route = master_.GetWrapped().GetMasterService().GetReadRoute(key);
        ASSERT_TRUE(route.has_value());
        ASSERT_EQ(route->size(), 1);
        EXPECT_EQ(route->front().client_id, client_id_);
        EXPECT_EQ(route->front().segment_id, *tier_id);
        worker->Stop();
    }
}

TEST_F(MetadataRecoveryWorkerTest, RegisterOnlySkipsReplicaScan) {
    auto tier_id = InitLocalDataManagerAndPut("register-only", "local-value");
    ASSERT_TRUE(tier_id.has_value());
    MountLocalTierOnMaster(*tier_id);
    auto worker = CreateWorkerWithNotifier(
        MetadataRecoveryWorker::RecoveryMode::RegisterOnly);
    EXPECT_EQ(worker->Start(), ErrorCode::OK);
    WaitUntilCompleted(*worker);
    EXPECT_FALSE(master_.GetWrapped()
                     .GetMasterService()
                     .GetReadRoute("register-only")
                     .has_value());
    EXPECT_FALSE(worker->recovery_thread_.joinable());
    EXPECT_EQ(worker->Start(), ErrorCode::OK);
    WaitUntilCompleted(*worker);
    EXPECT_FALSE(master_.GetWrapped()
                     .GetMasterService()
                     .GetReadRoute("register-only")
                     .has_value());
}

TEST_F(MetadataRecoveryWorkerTest, SynchronousBusinessFailureIsReported) {
    auto tier_id = InitLocalDataManagerAndPut("failed-recovery", "local-value");
    ASSERT_TRUE(tier_id.has_value());
    MetadataRecoveryWorker worker(
        *data_manager_, nullptr,
        [](std::string_view, const UUID&,
           size_t) -> tl::expected<void, ErrorCode> {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        });
    EXPECT_EQ(worker.Start(), ErrorCode::OK);
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (worker.GetStatus() == MetadataRecoveryWorker::Status::RUNNING &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(worker.GetStatus(), MetadataRecoveryWorker::Status::FAILED);
}

TEST_F(MetadataRecoveryWorkerTest,
       ExistingReplicaDoesNotAbortRemainingRecovery) {
    auto tier_id = InitLocalDataManagerAndPut("existing-recovery", "value");
    ASSERT_TRUE(tier_id.has_value());
    MountLocalTierOnMaster(*tier_id);
    std::string value = "value";
    std::vector<Slice> slices{{value.data(), value.size()}};
    auto put = data_manager_->Put("missing-recovery", slices);
    ASSERT_TRUE(put.has_value());
    ASSERT_TRUE((*put)->Wait().has_value());
    auto worker = CreateWorker();
    std::atomic<int> publications{0};
    worker->publish_replica_ = [&](std::string_view key, const UUID& tier,
                                   size_t size) {
        // Make the first scanned replica already exist regardless of scan
        // order.
        if (publications.fetch_add(1) == 0) {
            EXPECT_TRUE(
                master_client_->PublishRoute({key, size, client_id_, tier})
                    .has_value());
        }
        return master_client_->PublishRoute({key, size, client_id_, tier});
    };
    EXPECT_EQ(worker->Start(), ErrorCode::OK);
    WaitUntilCompleted(*worker);
    worker->Stop();
    EXPECT_EQ(publications.load(), 2);
    EXPECT_TRUE(WaitUntilReplicaVisible("existing-recovery"));
    EXPECT_TRUE(WaitUntilReplicaVisible("missing-recovery"));
}

TEST_F(MetadataRecoveryWorkerTest, SynchronousRpcFailureRetriesUntilPublished) {
    auto tier_id = InitLocalDataManagerAndPut("retry-recovery", "value");
    ASSERT_TRUE(tier_id.has_value());
    MountLocalTierOnMaster(*tier_id);
    std::atomic<int> attempts{0};
    auto worker = CreateWorker();
    worker->publish_replica_ =
        [&](std::string_view key, const UUID& tier,
            size_t size) -> tl::expected<void, ErrorCode> {
        if (attempts.fetch_add(1) < 2) {
            return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        return master_client_->PublishRoute({key, size, client_id_, tier});
    };
    EXPECT_EQ(worker->Start(), ErrorCode::OK);
    WaitUntilCompleted(*worker);
    worker->Stop();
    EXPECT_EQ(attempts.load(), 3);
    EXPECT_TRUE(WaitUntilReplicaVisible("retry-recovery"));
}

TEST_F(MetadataRecoveryWorkerTest, StopCancelsSynchronousRetryBackoff) {
    auto tier_id = InitLocalDataManagerAndPut("cancel-retry", "value");
    ASSERT_TRUE(tier_id.has_value());
    std::promise<void> retrying;
    std::atomic<int> attempts{0};
    auto worker = CreateWorker();
    worker->publish_replica_ = [&](std::string_view, const UUID&,
                                   size_t) -> tl::expected<void, ErrorCode> {
        if (attempts.fetch_add(1) == 4) {
            retrying.set_value();
        }
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    };
    EXPECT_EQ(worker->Start(), ErrorCode::OK);
    auto backoff = retrying.get_future();
    ASSERT_EQ(backoff.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    auto stopped = std::async(std::launch::async, [&] { worker->Stop(); });
    EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds(500)),
              std::future_status::ready);
    stopped.get();
    EXPECT_EQ(worker->GetStatus(), MetadataRecoveryWorker::Status::CANCELLED);
}

TEST_F(MetadataRecoveryWorkerTest, CancellationWaitsForInflightPublication) {
    auto tier_id = InitLocalDataManagerAndPut("cancel-recovery", "local-value");
    ASSERT_TRUE(tier_id.has_value());
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    MetadataRecoveryWorker worker(*data_manager_, nullptr,
                                  [&](std::string_view, const UUID&,
                                      size_t) -> tl::expected<void, ErrorCode> {
                                      entered.set_value();
                                      released.wait();
                                      return {};
                                  });
    EXPECT_EQ(worker.Start(), ErrorCode::OK);
    auto publication = entered.get_future();
    const auto started = publication.wait_for(std::chrono::seconds(5));
    if (started != std::future_status::ready) {
        release.set_value();
        FAIL() << "Recovery did not reach publication";
    }
    auto stopped = std::async(std::launch::async, [&] { worker.Stop(); });
    EXPECT_EQ(stopped.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    release.set_value();
    stopped.get();
    EXPECT_EQ(worker.GetStatus(), MetadataRecoveryWorker::Status::CANCELLED);
}

}  // namespace test
}  // namespace mooncake
