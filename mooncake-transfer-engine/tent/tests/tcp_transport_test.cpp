// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/transport/tcp/tcp_transport.h"

namespace mooncake {
namespace tent {

class TcpTransportTestPeer {
   public:
    static const TcpParams& params(const TcpTransport& transport) {
        return transport.params_;
    }

    static std::future<void> enqueueInternal(TcpTransport& transport,
                                             std::function<void()> task) {
        return transport.thread_pool_->enqueue(std::move(task));
    }
};

namespace {

using namespace std::chrono_literals;

std::shared_ptr<ControlService> makeP2PMetadata() {
    return std::make_shared<ControlService>("p2p", "", nullptr);
}

std::shared_ptr<Config> makeSingleWorkerConfig() {
    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_concurrent_tasks", 1);
    conf->set("transports/tcp/max_retry_count", 0);
    return conf;
}

Request makeUnresolvableRequest() {
    Request request;
    request.opcode = Request::WRITE;
    request.source = nullptr;
    request.target_id = 999999;
    request.target_offset = 0;
    request.length = 1;
    return request;
}

TEST(TcpTransportConfigTest, InstallWithoutConfigKeepsStructDefaults) {
    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-config-default";
    ASSERT_TRUE(transport.install(name, metadata, nullptr).ok());

    const TcpParams defaults;
    const auto& params = TcpTransportTestPeer::params(transport);
    EXPECT_EQ(params.max_retry_count, defaults.max_retry_count);
    EXPECT_EQ(params.retry_base_delay_ms, defaults.retry_base_delay_ms);
    EXPECT_EQ(params.retry_max_delay_ms, defaults.retry_max_delay_ms);
    EXPECT_EQ(params.max_concurrent_tasks, defaults.max_concurrent_tasks);

    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportConfigTest, InstallReadsConfigKeys) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_retry_count", 5);
    conf->set("transports/tcp/retry_base_delay_ms", 200ULL);
    conf->set("transports/tcp/retry_max_delay_ms", 4000ULL);
    conf->set("transports/tcp/max_concurrent_tasks", 32);

    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-config-override";
    ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());

    const auto& params = TcpTransportTestPeer::params(transport);
    EXPECT_EQ(params.max_retry_count, 5u);
    EXPECT_EQ(params.retry_base_delay_ms, 200ULL);
    EXPECT_EQ(params.retry_max_delay_ms, 4000ULL);
    EXPECT_EQ(params.max_concurrent_tasks, 32u);

    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpSubBatchTest, PointerStabilityAfterReserve) {
    // allocateSubBatch reserves task_list to max_size so submitTransferTasks
    // can take stable TcpTask* after emplace.
    TcpSubBatch batch;
    batch.max_size = 8;
    batch.task_list.reserve(batch.max_size);

    batch.task_list.emplace_back();
    TcpTask* first = &batch.task_list[0];
    first->target_addr = 42;

    for (int i = 1; i < 8; ++i) {
        batch.task_list.emplace_back();
    }

    EXPECT_EQ(first->target_addr, 42u);
    EXPECT_EQ(&batch.task_list[0], first);
}

TEST(TcpRetryBackoffTest, ExponentialGrowthWithCap) {
    const uint64_t base = 100;
    const uint64_t cap = 2000;
    uint64_t delay = base;

    std::vector<uint64_t> delays;
    for (int attempt = 0; attempt < 6; ++attempt) {
        delays.push_back(delay);
        delay = nextTcpRetryDelay(delay, cap);
    }

    EXPECT_EQ(delays[0], 100u);
    EXPECT_EQ(delays[1], 200u);
    EXPECT_EQ(delays[2], 400u);
    EXPECT_EQ(delays[3], 800u);
    EXPECT_EQ(delays[4], 1600u);
    EXPECT_EQ(delays[5], 2000u);
}

TEST(TcpTransportQuiesceTest, DrainsQueuedTcpTaskBeforeReturning) {
    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-quiesce-queued";
    ASSERT_TRUE(
        transport.install(name, metadata, nullptr, makeSingleWorkerConfig())
            .ok());

    std::promise<void> blocker_started;
    auto blocker_started_future = blocker_started.get_future();
    std::promise<void> release_blocker;
    auto release_blocker_future = release_blocker.get_future().share();
    auto blocker = TcpTransportTestPeer::enqueueInternal(transport, [&] {
        blocker_started.set_value();
        release_blocker_future.wait();
    });
    ASSERT_EQ(blocker_started_future.wait_for(3s), std::future_status::ready);

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    batch->progress_batch_id = 42;

    std::atomic<int> callbacks{0};
    std::promise<void> progress_notified;
    auto progress_notified_future = progress_notified.get_future();
    batch->notify_progress = [&](BatchID batch_id) {
        EXPECT_EQ(batch_id, 42u);
        callbacks.fetch_add(1, std::memory_order_relaxed);
        progress_notified.set_value();
    };

    std::vector<Request> requests{makeUnresolvableRequest()};
    ASSERT_TRUE(transport.submitTransferTasks(batch, requests).ok());

    auto quiesce_result =
        std::async(std::launch::async, [&] { return transport.quiesce(); });
    EXPECT_EQ(quiesce_result.wait_for(100ms), std::future_status::timeout);

    release_blocker.set_value();
    ASSERT_EQ(blocker.wait_for(3s), std::future_status::ready);
    blocker.get();
    ASSERT_EQ(quiesce_result.wait_for(3s), std::future_status::ready);
    EXPECT_TRUE(quiesce_result.get().ok());
    ASSERT_EQ(progress_notified_future.wait_for(3s), std::future_status::ready);
    EXPECT_EQ(callbacks.load(std::memory_order_relaxed), 1);

    TransferStatus status;
    ASSERT_TRUE(transport.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);

    EXPECT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportQuiesceTest, WaitsForRunningTcpTaskCallback) {
    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-quiesce-running";
    ASSERT_TRUE(
        transport.install(name, metadata, nullptr, makeSingleWorkerConfig())
            .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    batch->progress_batch_id = 7;

    std::promise<void> callback_entered;
    auto callback_entered_future = callback_entered.get_future();
    std::promise<void> release_callback;
    auto release_callback_future = release_callback.get_future().share();
    std::atomic<int> callbacks{0};
    batch->notify_progress = [&](BatchID batch_id) {
        EXPECT_EQ(batch_id, 7u);
        callbacks.fetch_add(1, std::memory_order_relaxed);
        callback_entered.set_value();
        release_callback_future.wait();
    };

    std::vector<Request> requests{makeUnresolvableRequest()};
    ASSERT_TRUE(transport.submitTransferTasks(batch, requests).ok());
    ASSERT_EQ(callback_entered_future.wait_for(3s), std::future_status::ready);

    TransferStatus status;
    ASSERT_TRUE(transport.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);

    auto quiesce_result =
        std::async(std::launch::async, [&] { return transport.quiesce(); });
    EXPECT_EQ(quiesce_result.wait_for(100ms), std::future_status::timeout);

    release_callback.set_value();
    ASSERT_EQ(quiesce_result.wait_for(3s), std::future_status::ready);
    EXPECT_TRUE(quiesce_result.get().ok());
    EXPECT_EQ(callbacks.load(std::memory_order_relaxed), 1);

    EXPECT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportQuiesceTest, IdleAndRepeatedCleanupAreIdempotent) {
    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-quiesce-idle";
    ASSERT_TRUE(transport.install(name, metadata, nullptr).ok());

    EXPECT_TRUE(transport.quiesce().ok());
    EXPECT_TRUE(transport.quiesce().ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    std::vector<Request> requests{makeUnresolvableRequest()};
    auto submit_status = transport.submitTransferTasks(batch, requests);
    EXPECT_TRUE(submit_status.IsInternalError());
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_TRUE(transport.freeSubBatch(batch).ok());

    auto metadata2 = makeP2PMetadata();
    ASSERT_TRUE(transport.install(name, metadata2, nullptr).ok());
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    EXPECT_TRUE(transport.submitTransferTasks(batch, requests).ok());
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
