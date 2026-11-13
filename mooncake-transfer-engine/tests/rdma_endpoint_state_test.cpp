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
#include <dlfcn.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common.h"
#include "config.h"
#include "error.h"
#include "transfer_metadata.h"
#include "transfer_metadata_plugin.h"
#include "topology.h"
#include "transport/rdma_transport/endpoint_store.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"

#if defined(__has_feature)
#define MC_HAS_FEATURE(x) __has_feature(x)
#else
#define MC_HAS_FEATURE(x) 0
#endif
#if defined(__SANITIZE_ADDRESS__) || MC_HAS_FEATURE(address_sanitizer)
#include <sanitizer/lsan_interface.h>
#define MC_LSAN_IGNORE_OBJECT(p) __lsan_ignore_object(p)
#else
#define MC_LSAN_IGNORE_OBJECT(p) ((void)(p))
#endif

using namespace mooncake;

namespace {
ibv_context *auto_gid_test_context = nullptr;
}

namespace mooncake {

class RdmaTransportTestPeer {
   public:
    static void bindMetadata(RdmaTransport &transport,
                             std::shared_ptr<TransferMetadata> metadata,
                             const std::string &local_server_name) {
        transport.metadata_ = std::move(metadata);
        transport.local_server_name_ = local_server_name;
        transport.rdma_server_name_ = local_server_name;
    }

    static void bindTopology(RdmaTransport &transport,
                             std::shared_ptr<Topology> topology) {
        transport.local_topology_ = std::move(topology);
    }

    static void addContext(RdmaTransport &transport,
                           std::shared_ptr<RdmaContext> context) {
        transport.context_list_.push_back(std::move(context));
    }
};

class RdmaContextTestPeer {
   public:
    static void enableAutoRecovery(RdmaContext &context, ibv_context *verbs) {
        auto_gid_test_context = verbs;
        context.context_ = verbs;
        context.port_ = 1;
        context.gid_ = {};
        context.gid_.raw[15] = 1;
        context.gid_index_ = 0;
        context.auto_gid_selection_enabled_ = true;
        context.peer_auto_gid_states_.configure(2, true);
    }
    static void clearAutoRecovery(RdmaContext &context) {
        context.context_ = nullptr;
        auto_gid_test_context = nullptr;
    }

    static void bindCompletionQueue(RdmaContext &context, ibv_cq *cq) {
        context.cq_list_.emplace_back();
        context.cq_list_.back().native = cq;
        cq->cq_context = &context.cq_list_.back().outstanding;
        context.gid_ = {};
        context.gid_index_ = 0;
        context.lid_ = 0;
    }

    static void bindFakeEndpointStore(RdmaContext &context, ibv_cq *cq) {
        bindCompletionQueue(context, cq);
        context.endpoint_store_ = std::make_shared<SIEVEEndpointStore>(8);
        context.transfer_worker_count_ = 1;
        context.endpoint_lifecycle_locks_.clear();
        context.endpoint_lifecycle_locks_.push_back(
            std::make_unique<std::mutex>());
        context.active_.store(true, std::memory_order_release);
    }

    static std::unique_lock<std::mutex> tryLockEndpointLifecycle(
        RdmaContext &context, const std::string &peer_nic_path) {
        const int owner_thread = context.postingThreadForPeer(peer_nic_path);
        if (owner_thread < 0 || static_cast<size_t>(owner_thread) >=
                                    context.endpoint_lifecycle_locks_.size()) {
            return std::unique_lock<std::mutex>();
        }
        return std::unique_lock<std::mutex>(
            *context.endpoint_lifecycle_locks_[owner_thread], std::try_to_lock);
    }

    static void insertEndpointForTest(RdmaContext &context,
                                      const std::string &peer_nic_path,
                                      std::shared_ptr<RdmaEndPoint> endpoint) {
        auto store = std::dynamic_pointer_cast<SIEVEEndpointStore>(
            context.endpoint_store_);
        ASSERT_NE(store, nullptr);
        store->testOnlyInsertEndpoint(peer_nic_path, std::move(endpoint));
    }

    static void clearCompletionQueues(RdmaContext &context) {
        context.cq_list_.clear();
    }

    static void clearNativeCompletionQueue(RdmaContext &context, int cq_index) {
        ASSERT_GE(cq_index, 0);
        ASSERT_LT(static_cast<size_t>(cq_index), context.cq_list_.size());
        context.cq_list_[cq_index].native = nullptr;
    }
};

class WorkerPoolTestPeer {
   public:
    static void stopWorkers(WorkerPool &pool) {
        if (!pool.workers_running_.load(std::memory_order_acquire)) return;
        pool.workers_running_.store(false, std::memory_order_release);
        pool.cond_var_.notify_all();
        for (auto &entry : pool.worker_thread_) {
            if (entry.joinable()) entry.join();
        }
    }

    static void queueSlice(WorkerPool &pool, int thread_id,
                           const std::string &peer_nic_path,
                           Transport::Slice *slice) {
        pool.worker_slice_queue_[thread_id][peer_nic_path].push_back(slice);
    }

    static void setPendingSetup(WorkerPool &pool, const std::string &path,
                                std::shared_ptr<RdmaEndPoint> endpoint,
                                std::future<int> result) {
        pool.pending_setups_[0][path] = {
            std::move(endpoint), std::move(result),
            std::chrono::steady_clock::now() + std::chrono::seconds(5)};
    }

    static void performPostSend(WorkerPool &pool, int thread_id) {
        pool.performPostSend(thread_id);
    }

    static void waitForSetups(WorkerPool &pool) {
        for (auto &owner : pool.pending_setups_)
            for (auto &entry : owner)
                if (entry.second.result.valid()) entry.second.result.wait();
    }

    static void expireSetups(WorkerPool &pool) {
        for (auto &owner : pool.pending_setups_)
            for (auto &entry : owner) entry.second.deadline = {};
    }

    static int railErrors(WorkerPool &pool, const std::string &path) {
        return pool.rail_states_[path].error_count;
    }

    static void retry(WorkerPool &pool, Transport::Slice &slice) {
        std::vector<Transport::Slice *> slices{&slice};
        pool.redispatch(slices, 0);
    }

    static void pollRetries(WorkerPool &pool) {
        pool.performPendingRedispatch(0);
    }

    static void waitForRetries(WorkerPool &pool) {
        for (auto &entry : pool.pending_redispatch_[0])
            if (entry.second.result.valid()) entry.second.result.wait();
    }

    static size_t pendingRetries(WorkerPool &pool) {
        return pool.pending_redispatch_[0].size();
    }

    static void expireRetries(WorkerPool &pool) {
        for (auto &entry : pool.pending_redispatch_[0])
            entry.second.deadline = {};
    }

    static void clearQueuedSlices(WorkerPool &pool) {
        for (auto &queue : pool.collective_slice_queue_) queue.clear();
        for (auto &queue : pool.worker_slice_queue_) queue.clear();
    }

    static uint64_t lastPollTs(const WorkerPool &pool) {
        return pool.last_poll_ts_ns_.load(std::memory_order_acquire);
    }

    static bool waitForPollTs(WorkerPool &pool,
                              std::chrono::milliseconds timeout) {
        if (lastPollTs(pool) != 0) return true;

        std::mutex mutex;
        std::condition_variable cv;
        bool observed = false;
        std::thread waiter([&] {
            uint64_t poll_ts =
                pool.last_poll_ts_ns_.load(std::memory_order_acquire);
            while (poll_ts == 0) {
                pool.last_poll_ts_ns_.wait(poll_ts, std::memory_order_acquire);
                poll_ts = pool.last_poll_ts_ns_.load(std::memory_order_acquire);
            }
            {
                std::lock_guard<std::mutex> lock(mutex);
                observed = true;
            }
            cv.notify_one();
        });

        std::unique_lock<std::mutex> lock(mutex);
        const bool ok = cv.wait_for(lock, timeout, [&] { return observed; });
        if (!ok) {
            pool.last_poll_ts_ns_.store(std::numeric_limits<uint64_t>::max(),
                                        std::memory_order_release);
            pool.last_poll_ts_ns_.notify_all();
        }
        lock.unlock();
        waiter.join();
        return ok;
    }
};

class RdmaEndPointTestPeer {
   public:
    static void setStatus(RdmaEndPoint &endpoint, RdmaEndPoint::Status status) {
        endpoint.status_.store(status, std::memory_order_relaxed);
    }

    static void setReadyWaitStartTs(RdmaEndPoint &endpoint, uint64_t start_ts) {
        endpoint.ready_wait_start_ts_.store(start_ts,
                                            std::memory_order_relaxed);
    }

    static void setPeerQpNums(RdmaEndPoint &endpoint,
                              std::vector<uint32_t> peer_qp_nums) {
        endpoint.peer_qp_num_list_ = std::move(peer_qp_nums);
    }

    static int reconstruct(RdmaEndPoint &endpoint) {
        RWSpinlock::WriteGuard guard(endpoint.lock_);
        return endpoint.reconstruct();
    }
};

}  // namespace mooncake

namespace {

class InProcessRdmaTransport : public RdmaTransport {
   public:
    int sendHandshake(const std::string &peer_server_name,
                      const HandShakeDesc &local_desc,
                      HandShakeDesc &peer_desc) override {
        (void)peer_server_name;
        if (!local_desc.ready_ack) {
            auto lifecycle_lock = RdmaContextTestPeer::tryLockEndpointLifecycle(
                *local_context_, local_desc.peer_nic_path);
            // Posting passes may briefly reacquire the gate while the RPC runs.
            // Still fail if a worker holds it throughout the handshake.
            const auto deadline = std::chrono::steady_clock::now() +
                                  std::chrono::milliseconds(500);
            while (!lifecycle_lock.owns_lock() &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::yield();
                lifecycle_lock = RdmaContextTestPeer::tryLockEndpointLifecycle(
                    *local_context_, local_desc.peer_nic_path);
            }
            if (!lifecycle_lock.owns_lock()) {
                lifecycle_gate_was_free_.store(false,
                                               std::memory_order_release);
                return ERR_ENDPOINT;
            }
            lifecycle_lock.unlock();

            std::unique_lock<std::mutex> lock(barrier_->mutex);
            ++barrier_->arrivals;
            barrier_->cv.notify_all();
            if (!barrier_->cv.wait_for(lock, std::chrono::seconds(5), [&] {
                    return barrier_->arrivals >= barrier_->required_arrivals ||
                           barrier_->released;
                })) {
                return ERR_ENDPOINT;
            }
            if (fail_after_barrier_) return failure_code_;
        }

        return peer_->onSetupRdmaConnections(local_desc, peer_desc);
    }

    bool lifecycleGateWasFree() const {
        return lifecycle_gate_was_free_.load(std::memory_order_acquire);
    }

    struct Barrier {
        std::mutex mutex;
        std::condition_variable cv;
        int arrivals = 0;
        int required_arrivals = 2;
        bool released = false;
    };

    RdmaContext *local_context_ = nullptr;
    InProcessRdmaTransport *peer_ = nullptr;
    std::shared_ptr<Barrier> barrier_;
    bool fail_after_barrier_ = false;
    int failure_code_ = ERR_ENDPOINT;

   private:
    std::atomic<bool> lifecycle_gate_was_free_{true};
};

struct FakeRdmaPeer {
    std::shared_ptr<TransferMetadata> metadata;
    std::unique_ptr<InProcessRdmaTransport> transport;
    std::shared_ptr<Topology> topology;
    std::shared_ptr<RdmaContext> context;
    ibv_cq cq = {};
    std::unique_ptr<WorkerPool> worker_pool;
    std::string server_name;
    std::string device_name;
};

void initFakePeer(FakeRdmaPeer &peer, const std::string &server_name,
                  const std::string &device_name,
                  std::shared_ptr<InProcessRdmaTransport::Barrier> barrier) {
    peer.server_name = server_name;
    peer.device_name = device_name;
    peer.metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    peer.transport = std::make_unique<InProcessRdmaTransport>();
    RdmaTransportTestPeer::bindMetadata(*peer.transport, peer.metadata,
                                        server_name);

    peer.topology = std::make_shared<Topology>();
    ASSERT_EQ(
        peer.topology->parse("{\"cpu:0\": [[\"" + device_name + "\"], []]}"),
        0);
    RdmaTransportTestPeer::bindTopology(*peer.transport, peer.topology);

    peer.context =
        std::make_shared<RdmaContext>(*peer.transport, peer.device_name);
    RdmaContextTestPeer::bindFakeEndpointStore(*peer.context, &peer.cq);
    RdmaTransportTestPeer::addContext(*peer.transport, peer.context);

    peer.transport->local_context_ = peer.context.get();
    peer.transport->barrier_ = std::move(barrier);

    peer.worker_pool = std::make_unique<WorkerPool>(*peer.context);
    WorkerPoolTestPeer::stopWorkers(*peer.worker_pool);
}

void queueHandshakeSlice(FakeRdmaPeer &peer, const std::string &peer_nic_path,
                         Transport::Slice &slice,
                         Transport::TransferTask &task) {
    task = Transport::TransferTask();
    slice.task = &task;
    slice.status = Transport::Slice::PENDING;
    slice.peer_nic_path = peer_nic_path;
    slice.rdma.retry_cnt = 0;
    slice.rdma.max_retry_cnt = 8;
    WorkerPoolTestPeer::queueSlice(*peer.worker_pool, 0, peer_nic_path, &slice);
}

void installZeroQpEndpoint(FakeRdmaPeer &peer,
                           const std::string &peer_nic_path) {
    auto endpoint = std::make_shared<RdmaEndPoint>(*peer.context);
    ASSERT_EQ(endpoint->construct(&peer.cq, 0), 0);
    endpoint->setPeerNicPath(peer_nic_path);
    RdmaContextTestPeer::insertEndpointForTest(*peer.context, peer_nic_path,
                                               std::move(endpoint));
}

class RdmaEndPointStateTest : public ::testing::Test {
   protected:
    void SetUp() override {
        transport_ = new RdmaTransport();
        // Intentional leak: ~RdmaTransport dereferences metadata_, which is
        // null until install(). We only need it as RdmaContext's owner.
        MC_LSAN_IGNORE_OBJECT(transport_);
        context_ = std::make_unique<RdmaContext>(*transport_, "unused");
        endpoint_ = std::make_unique<RdmaEndPoint>(*context_);
    }

    void TearDown() override {
        endpoint_->destroyQP();
        RdmaContextTestPeer::clearAutoRecovery(*context_);
        RdmaContextTestPeer::clearCompletionQueues(*context_);
    }

    RdmaTransport *transport_ = nullptr;
    std::unique_ptr<RdmaContext> context_;
    std::unique_ptr<RdmaEndPoint> endpoint_;
};

TEST_F(RdmaEndPointStateTest, WaitingReadyAckIsConnectedButNotReadyToSend) {
    RdmaEndPointTestPeer::setStatus(*endpoint_,
                                    RdmaEndPoint::CONNECTED_WAIT_READY_ACK);

    EXPECT_TRUE(endpoint_->connected());
    EXPECT_FALSE(endpoint_->readyToSend());
}

TEST_F(RdmaEndPointStateTest, ConnectedIsReadyToSend) {
    RdmaEndPointTestPeer::setStatus(*endpoint_, RdmaEndPoint::CONNECTED);

    EXPECT_TRUE(endpoint_->connected());
    EXPECT_TRUE(endpoint_->readyToSend());
}

#ifdef MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL
TEST_F(RdmaEndPointStateTest, LimitedSubmitLeavesDeferredSuffixQueued) {
    Transport::Slice first{};
    Transport::Slice deferred{};
    std::vector<Transport::Slice *> queued{&first, &deferred};
    std::vector<Transport::Slice *> failed;

    endpoint_->submitPostSendLimited(queued, failed, 1);

    ASSERT_EQ(failed.size(), 1u);
    EXPECT_EQ(failed.front(), &first);
    ASSERT_EQ(queued.size(), 1u);
    EXPECT_EQ(queued.front(), &deferred);
}
#endif

TEST_F(RdmaEndPointStateTest, ReadyAckTimeoutOnlyAppliesToWaitingState) {
    RdmaEndPointTestPeer::setReadyWaitStartTs(*endpoint_, 1);

    RdmaEndPointTestPeer::setStatus(*endpoint_,
                                    RdmaEndPoint::CONNECTED_WAIT_READY_ACK);
    EXPECT_TRUE(endpoint_->readyAckTimedOut());

    RdmaEndPointTestPeer::setStatus(*endpoint_, RdmaEndPoint::CONNECTED);
    EXPECT_FALSE(endpoint_->readyAckTimedOut());
}

TEST_F(RdmaEndPointStateTest, ReadyAckWithSamePeerQpMarksEndpointReady) {
    endpoint_->setPeerNicPath("peer@nic");
    RdmaEndPointTestPeer::setPeerQpNums(*endpoint_, {11, 22});
    RdmaEndPointTestPeer::setReadyWaitStartTs(*endpoint_, 1);
    RdmaEndPointTestPeer::setStatus(*endpoint_,
                                    RdmaEndPoint::CONNECTED_WAIT_READY_ACK);

    RdmaEndPoint::HandShakeDesc peer_desc;
    peer_desc.ready_ack = true;
    peer_desc.ready_ack_supported = true;
    peer_desc.qp_num = {11, 22};
    RdmaEndPoint::HandShakeDesc local_desc;

    EXPECT_EQ(0, endpoint_->setupConnectionsByPassive(peer_desc, local_desc));
    EXPECT_TRUE(local_desc.reply_msg.empty());
    EXPECT_TRUE(endpoint_->connected());
    EXPECT_TRUE(endpoint_->readyToSend());
    EXPECT_FALSE(endpoint_->readyAckTimedOut());
}

TEST_F(RdmaEndPointStateTest, StaleReadyAckWithDifferentPeerQpDoesNotReset) {
    endpoint_->setPeerNicPath("peer@nic");
    RdmaEndPointTestPeer::setPeerQpNums(*endpoint_, {11, 22});
    RdmaEndPointTestPeer::setReadyWaitStartTs(*endpoint_, 1);
    RdmaEndPointTestPeer::setStatus(*endpoint_,
                                    RdmaEndPoint::CONNECTED_WAIT_READY_ACK);

    RdmaEndPoint::HandShakeDesc peer_desc;
    peer_desc.ready_ack = true;
    peer_desc.ready_ack_supported = true;
    peer_desc.qp_num = {33, 44};
    RdmaEndPoint::HandShakeDesc local_desc;

    EXPECT_EQ(ERR_REJECT_HANDSHAKE,
              endpoint_->setupConnectionsByPassive(peer_desc, local_desc));
    EXPECT_FALSE(local_desc.reply_msg.empty());
    EXPECT_TRUE(endpoint_->connected());
    EXPECT_FALSE(endpoint_->readyToSend());
    EXPECT_TRUE(endpoint_->readyAckTimedOut());
}

TEST_F(RdmaEndPointStateTest,
       StaleActiveHandshakeReplyDoesNotConnectReconstructedEndpoint) {
    TransferMetadata server_metadata(P2PHANDSHAKE);
    int sockfd = -1;
    const uint16_t port = findAvailableTcpPort(sockfd);
    ASSERT_NE(port, 0);
    const std::string host = globalConfig().use_ipv6 ? "::1" : "127.0.0.1";
    const std::string peer_server_name =
        maybeWrapIpV6(host) + ":" + std::to_string(port);

    std::mutex callback_mutex;
    std::condition_variable callback_cv;
    bool callback_started = false;
    bool release_reply = false;
    // Hold the Q1 reply until the test has reconstructed the endpoint as Q2.
    ASSERT_EQ(server_metadata.startHandshakeDaemon(
                  [&](const RdmaEndPoint::HandShakeDesc &peer_desc,
                      RdmaEndPoint::HandShakeDesc &local_desc) {
                      std::unique_lock<std::mutex> lock(callback_mutex);
                      callback_started = true;
                      callback_cv.notify_all();
                      callback_cv.wait(lock, [&] { return release_reply; });

                      local_desc.local_nic_path = peer_desc.peer_nic_path;
                      local_desc.peer_nic_path = peer_desc.local_nic_path;
                      local_desc.local_gid =
                          "00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00";
                      local_desc.local_lid = 0;
                      local_desc.qp_num.clear();
                      local_desc.ready_ack_supported = false;
                      return 0;
                  },
                  port, sockfd),
              0);

    auto client_metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    RdmaTransportTestPeer::bindMetadata(*transport_, client_metadata,
                                        "rdma-endpoint-state-client");

    // Zero QPs keep the regression hardware-free while still exercising the
    // real active-handshake and reconstruct state transitions.
    ibv_cq fake_cq = {};
    RdmaContextTestPeer::bindCompletionQueue(*context_, &fake_cq);
    ASSERT_EQ(endpoint_->construct(&fake_cq, 0), 0);
    endpoint_->setPeerNicPath(peer_server_name + "@peer-nic");

    auto active_result = std::async(std::launch::async, [&] {
        return endpoint_->setupConnectionsByActive();
    });

    bool observed_callback = false;
    {
        std::unique_lock<std::mutex> lock(callback_mutex);
        observed_callback = callback_cv.wait_for(
            lock, std::chrono::seconds(5), [&] { return callback_started; });
    }
    if (!observed_callback) {
        {
            std::lock_guard<std::mutex> lock(callback_mutex);
            release_reply = true;
        }
        callback_cv.notify_all();
        if (active_result.wait_for(std::chrono::seconds(5)) ==
            std::future_status::ready) {
            active_result.get();
        }
        FAIL() << "Active handshake did not reach the blocking callback";
    }

    const int reconstruct_result =
        RdmaEndPointTestPeer::reconstruct(*endpoint_);
    const bool connected_after_reconstruct = endpoint_->connected();

    // The successful Q1 reply is now stale and must not configure Q2.
    {
        std::lock_guard<std::mutex> lock(callback_mutex);
        release_reply = true;
    }
    callback_cv.notify_all();

    const int active_rc = active_result.get();
    ASSERT_EQ(reconstruct_result, 0);
    ASSERT_FALSE(connected_after_reconstruct);
    EXPECT_EQ(active_rc, ERR_ENDPOINT);
    EXPECT_FALSE(endpoint_->connected());
}

TEST_F(RdmaEndPointStateTest,
       StaleActiveAdvisoryDoesNotMutateNewPassiveConnection) {
    TransferMetadata server_metadata(P2PHANDSHAKE);
    int sockfd = -1;
    const uint16_t port = findAvailableTcpPort(sockfd);
    ASSERT_NE(port, 0);
    const std::string host = globalConfig().use_ipv6 ? "::1" : "127.0.0.1";
    const std::string peer_server_name =
        maybeWrapIpV6(host) + ":" + std::to_string(port);

    std::mutex callback_mutex;
    std::condition_variable callback_cv;
    bool callback_started = false;
    bool release_reply = false;
    // Hold the Q1 reply until the test has reconstructed the endpoint as Q2.
    ASSERT_EQ(server_metadata.startHandshakeDaemon(
                  [&](const RdmaEndPoint::HandShakeDesc &peer_desc,
                      RdmaEndPoint::HandShakeDesc &local_desc) {
                      std::unique_lock<std::mutex> lock(callback_mutex);
                      callback_started = true;
                      callback_cv.notify_all();
                      callback_cv.wait(lock, [&] { return release_reply; });

                      local_desc.local_nic_path = peer_desc.peer_nic_path;
                      local_desc.peer_nic_path = peer_desc.local_nic_path;
                      local_desc.local_gid =
                          "00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00";
                      local_desc.local_lid = 0;
                      local_desc.qp_num.clear();
                      local_desc.ready_ack_supported = false;
                      local_desc.auto_gid_reprobe_from = peer_desc.local_gid;
                      return 0;
                  },
                  port, sockfd),
              0);

    auto client_metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    RdmaTransportTestPeer::bindMetadata(*transport_, client_metadata,
                                        "rdma-endpoint-state-client");

    // Zero QPs keep the regression hardware-free while still exercising the
    // real active-handshake and reconstruct state transitions.
    ibv_cq fake_cq = {};
    RdmaContextTestPeer::bindCompletionQueue(*context_, &fake_cq);
    ASSERT_EQ(endpoint_->construct(&fake_cq, 0), 0);
    endpoint_->setPeerNicPath(peer_server_name + "@peer-nic");
    ibv_context fake_verbs = {};
    RdmaContextTestPeer::enableAutoRecovery(*context_, &fake_verbs);

    auto active_result = std::async(std::launch::async, [&] {
        return endpoint_->setupConnectionsByActive();
    });

    bool observed_callback = false;
    {
        std::unique_lock<std::mutex> lock(callback_mutex);
        observed_callback = callback_cv.wait_for(
            lock, std::chrono::seconds(5), [&] { return callback_started; });
    }
    if (!observed_callback) {
        {
            std::lock_guard<std::mutex> lock(callback_mutex);
            release_reply = true;
        }
        callback_cv.notify_all();
        if (active_result.wait_for(std::chrono::seconds(5)) ==
            std::future_status::ready) {
            active_result.get();
        }
        FAIL() << "Active handshake did not reach the blocking callback";
    }

    const int reconstruct_result =
        RdmaEndPointTestPeer::reconstruct(*endpoint_);
    const bool connected_after_reconstruct = endpoint_->connected();
    RdmaEndPoint::HandShakeDesc passive_request, passive_response;
    passive_request.local_nic_path = endpoint_->peerNicPath();
    passive_request.peer_nic_path = context_->nicPath();
    passive_request.local_gid = context_->gid();
    passive_request.ready_ack_supported = false;
    const int passive_rc =
        endpoint_->setupConnectionsByPassive(passive_request, passive_response);
    const bool passive_connected = endpoint_->connected();
    const auto live_before = endpoint_->autoGidCompletionToken();
    const auto selection_before =
        context_->preparePeerGidSelection(endpoint_->peerNicPath());

    // The successful Q1 reply is now stale and must not configure Q2.
    {
        std::lock_guard<std::mutex> lock(callback_mutex);
        release_reply = true;
    }
    callback_cv.notify_all();

    const int active_rc = active_result.get();
    ASSERT_EQ(reconstruct_result, 0);
    ASSERT_FALSE(connected_after_reconstruct);
    ASSERT_EQ(passive_rc, 0);
    ASSERT_TRUE(passive_connected);
    ASSERT_TRUE(live_before.valid);
    EXPECT_EQ(active_rc, 0);
    EXPECT_TRUE(endpoint_->connected());
    const auto selection_after =
        context_->preparePeerGidSelection(endpoint_->peerNicPath());
    EXPECT_EQ(selection_before.selection_epoch,
              selection_after.selection_epoch);
    EXPECT_EQ(selection_before.selection.gid, selection_after.selection.gid);
    const auto live_after = endpoint_->autoGidCompletionToken();
    EXPECT_EQ(live_after.selection_epoch, selection_after.selection_epoch);
    EXPECT_EQ(context_->recordAutoGidCompletion(
                  endpoint_->peerNicPath(),
                  {live_after.endpoint_id, live_after.selection_epoch}, false),
              AutoGidDataPathAction::kRetrySameGid);
}

TEST(RdmaEndpointLifecycleGateTest,
     BidirectionalActiveHandshakeDoesNotHoldLifecycleGateDuringRpc) {
    auto barrier = std::make_shared<InProcessRdmaTransport::Barrier>();
    FakeRdmaPeer peer_a;
    FakeRdmaPeer peer_b;

    ASSERT_NO_FATAL_FAILURE(
        initFakePeer(peer_a, "rdma-gate-a:10000", "mlx5_gate_a", barrier));
    ASSERT_NO_FATAL_FAILURE(
        initFakePeer(peer_b, "rdma-gate-b:10000", "mlx5_gate_b", barrier));
    peer_a.transport->peer_ = peer_b.transport.get();
    peer_b.transport->peer_ = peer_a.transport.get();

    const std::string a_to_b =
        MakeNicPath(peer_b.server_name, peer_b.device_name);
    const std::string b_to_a =
        MakeNicPath(peer_a.server_name, peer_a.device_name);

    ASSERT_NO_FATAL_FAILURE(installZeroQpEndpoint(peer_a, a_to_b));
    ASSERT_NO_FATAL_FAILURE(installZeroQpEndpoint(peer_b, b_to_a));

    Transport::Slice slice_a{};
    Transport::Slice slice_b{};
    Transport::TransferTask task_a;
    Transport::TransferTask task_b;
    queueHandshakeSlice(peer_a, a_to_b, slice_a, task_a);
    queueHandshakeSlice(peer_b, b_to_a, slice_b, task_b);

    auto active_a = std::async(std::launch::async, [&] {
        WorkerPoolTestPeer::performPostSend(*peer_a.worker_pool, 0);
    });
    auto active_b = std::async(std::launch::async, [&] {
        WorkerPoolTestPeer::performPostSend(*peer_b.worker_pool, 0);
    });

    ASSERT_EQ(active_a.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    ASSERT_EQ(active_b.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    active_a.get();
    active_b.get();

    WorkerPoolTestPeer::waitForSetups(*peer_a.worker_pool);
    WorkerPoolTestPeer::waitForSetups(*peer_b.worker_pool);

    EXPECT_TRUE(peer_a.transport->lifecycleGateWasFree());
    EXPECT_TRUE(peer_b.transport->lifecycleGateWasFree());

    auto endpoint_a = peer_a.context->findEndpoint(a_to_b);
    auto endpoint_b = peer_b.context->findEndpoint(b_to_a);
    ASSERT_NE(endpoint_a, nullptr);
    ASSERT_NE(endpoint_b, nullptr);
    EXPECT_TRUE(endpoint_a->readyToSend());
    EXPECT_TRUE(endpoint_b->readyToSend());

    WorkerPoolTestPeer::clearQueuedSlices(*peer_a.worker_pool);
    WorkerPoolTestPeer::clearQueuedSlices(*peer_b.worker_pool);
}

TEST(RdmaEndpointLifecycleGateTest, PendingHandshakeAllowsPostingAndCqPolling) {
    auto barrier = std::make_shared<InProcessRdmaTransport::Barrier>();
    barrier->required_arrivals =
        3;  // Keep both peer RPCs blocked until released.
    FakeRdmaPeer peer;

    ASSERT_NO_FATAL_FAILURE(
        initFakePeer(peer, "rdma-drain-a:10000", "mlx5_drain_a", barrier));
    peer.transport->fail_after_barrier_ = true;

    const std::string peer_nic_path =
        MakeNicPath("rdma-drain-b:10000", "mlx5_drain_b");
    ASSERT_NO_FATAL_FAILURE(installZeroQpEndpoint(peer, peer_nic_path));
    const std::string other_peer_path =
        MakeNicPath("rdma-drain-c:10000", "mlx5_drain_c");
    ASSERT_NO_FATAL_FAILURE(installZeroQpEndpoint(peer, other_peer_path));

    // The fake CQ is not a real verbs object. Clearing the native pointer keeps
    // the regression hardware-free while still proving the owner worker enters
    // its CQ poll path during the blocked active handshake.
    RdmaContextTestPeer::clearNativeCompletionQueue(*peer.context, 0);
    peer.context->cqOutstandingCount(0)->store(1, std::memory_order_relaxed);

    Transport::Slice slice{};
    Transport::TransferTask task;
    queueHandshakeSlice(peer, peer_nic_path, slice, task);
    slice.rdma.max_retry_cnt = 1;
    Transport::Slice other_slice{};
    Transport::TransferTask other_task;
    queueHandshakeSlice(peer, other_peer_path, other_slice, other_task);
    other_slice.rdma.max_retry_cnt = 1;

    auto active = std::async(std::launch::async, [&] {
        WorkerPoolTestPeer::performPostSend(*peer.worker_pool, 0);
        WorkerPoolTestPeer::performPostSend(*peer.worker_pool, 0);
    });

    {
        std::unique_lock<std::mutex> lock(barrier->mutex);
        EXPECT_TRUE(barrier->cv.wait_for(lock, std::chrono::seconds(1), [&] {
            return barrier->arrivals >= 2;
        }));
    }

    EXPECT_EQ(active.wait_for(std::chrono::milliseconds(100)),
              std::future_status::ready);
    EXPECT_TRUE(WorkerPoolTestPeer::waitForPollTs(*peer.worker_pool,
                                                  std::chrono::seconds(5)));

    {
        std::lock_guard<std::mutex> lock(barrier->mutex);
        EXPECT_EQ(barrier->arrivals,
                  2);  // One RPC per peer across both passes.
        barrier->released = true;
    }
    barrier->cv.notify_all();

    ASSERT_EQ(active.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    active.get();

    WorkerPoolTestPeer::waitForSetups(*peer.worker_pool);
    WorkerPoolTestPeer::performPostSend(*peer.worker_pool, 0);
    EXPECT_EQ(slice.status, Transport::Slice::FAILED);
    EXPECT_EQ(other_slice.status, Transport::Slice::FAILED);
    WorkerPoolTestPeer::clearQueuedSlices(*peer.worker_pool);
}

class PendingHandshakeTest : public ::testing::Test {
   protected:
    void SetUp() override {
        old_conn_pause_ttl_ = globalConfig().conn_pause_ttl_ms;
        globalConfig().conn_pause_ttl_ms = 0;
        const std::string name =
            ::testing::UnitTest::GetInstance()->current_test_info()->name();
        for (int i = 0; i < 2; ++i) {
            const auto nic = std::to_string(i);
            initFakePeer(peers[i], name + nic + ":10000", "local" + nic,
                         barrier);
            peers[i].transport->fail_after_barrier_ = true;
            paths[i] = MakeNicPath(name + ":10000", "remote" + nic);
            installZeroQpEndpoint(peers[i], paths[i]);
        }
    }
    void TearDown() override {
        release();
        for (auto &peer : peers) {
            if (!peer.worker_pool) continue;
            WorkerPoolTestPeer::waitForSetups(*peer.worker_pool);
            WorkerPoolTestPeer::clearQueuedSlices(*peer.worker_pool);
        }
        globalConfig().conn_pause_ttl_ms = old_conn_pause_ttl_;
    }
    void release() {
        {
            std::lock_guard<std::mutex> lock(barrier->mutex);
            barrier->released = true;
        }
        barrier->cv.notify_all();
    }
    void queue(int i) {
        queueHandshakeSlice(peers[i], paths[i], slices[i], tasks[i]);
    }
    void post(int i) {
        WorkerPoolTestPeer::performPostSend(*peers[i].worker_pool, 0);
    }
    void wait(int i) {
        WorkerPoolTestPeer::waitForSetups(*peers[i].worker_pool);
    }
    void expire(int i) {
        WorkerPoolTestPeer::expireSetups(*peers[i].worker_pool);
    }

    std::shared_ptr<InProcessRdmaTransport::Barrier> barrier =
        std::make_shared<InProcessRdmaTransport::Barrier>();
    FakeRdmaPeer peers[2];
    std::string paths[2];
    Transport::Slice slices[2]{};
    Transport::TransferTask tasks[2];
    int old_conn_pause_ttl_ = 0;
};

TEST_F(PendingHandshakeTest, ShutdownWaitsForPendingHandshake) {
    queue(0);
    post(0);
    auto shutdown =
        std::async(std::launch::async, [&] { peers[0].worker_pool.reset(); });
    EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    release();
    EXPECT_EQ(shutdown.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    shutdown.get();
}

TEST_F(PendingHandshakeTest, RpcFailureIsIsolatedAcrossLocalRnics) {
    globalConfig().conn_pause_ttl_ms = 10000;
    peers[0].transport->failure_code_ = peers[1].transport->failure_code_ =
        ERR_SOCKET;
    queue(0);
    queue(1);
    post(0);
    post(1);
    // Only admission waits expire; an admitted RPC must consume its result.
    expire(0);
    expire(1);
    post(0);
    post(1);
    EXPECT_EQ(slices[0].status, Transport::Slice::PENDING);
    EXPECT_EQ(slices[1].status, Transport::Slice::FAILED);
    release();
    wait(0);
    post(0);
    EXPECT_EQ(slices[0].status, Transport::Slice::FAILED);
    // New requests on either RNIC fail in cooldown without metadata retries.
    for (int i = 0; i < 2; ++i) {
        queue(i);
        post(i);
        EXPECT_EQ(slices[i].status, Transport::Slice::FAILED);
        EXPECT_EQ(slices[i].rdma.retry_cnt, 0);
    }
    EXPECT_EQ(barrier->arrivals, 1);
}

TEST_F(PendingHandshakeTest, NewSubmissionDiscardsCompletedIdleSetup) {
    release();
    // An expired queue's RPC finished while idle; the next request needs a new
    // RPC.
    std::promise<int> old_result;
    old_result.set_value(ERR_SOCKET);
    WorkerPoolTestPeer::setPendingSetup(
        *peers[0].worker_pool, paths[0],
        peers[0].context->findEndpoint(paths[0]), old_result.get_future());
    expire(0);
    queue(0);
    post(0);
    wait(0);
    EXPECT_EQ(barrier->arrivals, 1);
}

TEST_F(PendingHandshakeTest, CompletedSetupSurvivesOtherRnicCooldown) {
    globalConfig().conn_pause_ttl_ms = 10000;
    release();
    auto endpoint = peers[0].context->findEndpoint(paths[0]);
    std::promise<int> successful_setup;
    WorkerPoolTestPeer::setPendingSetup(*peers[0].worker_pool, paths[0],
                                        endpoint,
                                        successful_setup.get_future());
    queue(0);
    post(0);
    RdmaEndPointTestPeer::setStatus(*endpoint, RdmaEndPoint::CONNECTED);
    successful_setup.set_value(0);
    // Another RNIC fails after A connects, before A consumes its result.
    peers[1].transport->failure_code_ = ERR_SOCKET;
    queue(1);
    post(1);
    wait(1);
    post(1);
    EXPECT_EQ(slices[1].status, Transport::Slice::FAILED);
    post(0);
    EXPECT_EQ(slices[0].status, Transport::Slice::PENDING);
    EXPECT_EQ(slices[0].rdma.endpoint,
              endpoint.get());  // Reached posting on zero QPs.
}

TEST_F(PendingHandshakeTest, ExpiredSetupDoesNotUseReleasedAdmission) {
    queue(0);
    queue(1);
    post(0);
    post(1);
    expire(1);
    // Release admission after B's deadline, without arming an RPC cooldown.
    release();
    wait(0);
    post(1);
    wait(1);
    EXPECT_EQ(slices[1].status, Transport::Slice::FAILED);
    EXPECT_EQ(barrier->arrivals, 1);
}

TEST_F(PendingHandshakeTest, PassiveConnectionBypassesPendingAdmission) {
    queue(0);
    queue(1);
    post(0);
    post(1);
    // An incoming handshake connects B while A still owns server admission.
    auto endpoint = peers[1].context->findEndpoint(paths[1]);
    RdmaEndPointTestPeer::setStatus(*endpoint, RdmaEndPoint::CONNECTED);
    expire(1);
    post(1);
    EXPECT_EQ(slices[1].status, Transport::Slice::PENDING);
    EXPECT_EQ(slices[1].rdma.endpoint, endpoint.get());
    release();
    wait(0);
    EXPECT_EQ(barrier->arrivals, 1);
}

TEST_F(PendingHandshakeTest, InFlightDeadlinePreservesPassiveAndNewSlices) {
    auto endpoint = peers[0].context->findEndpoint(paths[0]);
    std::promise<int> result;
    WorkerPoolTestPeer::setPendingSetup(*peers[0].worker_pool, paths[0],
                                        endpoint, result.get_future());
    queue(0);
    post(0);
    expire(0);
    RdmaEndPointTestPeer::setStatus(*endpoint, RdmaEndPoint::CONNECTED);
    Transport::Slice fresh{};
    Transport::TransferTask fresh_task;
    queueHandshakeSlice(peers[0], paths[0], fresh, fresh_task);
    post(0);
    EXPECT_EQ(slices[0].status, Transport::Slice::PENDING);
    EXPECT_EQ(fresh.status, Transport::Slice::PENDING);
    result.set_value(0);
    post(0);
    EXPECT_EQ(slices[0].rdma.endpoint, endpoint.get());
    EXPECT_EQ(fresh.rdma.endpoint, endpoint.get());
}

TEST_F(PendingHandshakeTest, NewSlicesDoNotInheritExpiredAdmission) {
    queue(0);
    queue(1);
    post(0);
    post(1);
    expire(1);
    Transport::Slice fresh{};
    Transport::TransferTask fresh_task;
    queueHandshakeSlice(peers[1], paths[1], fresh, fresh_task);
    post(1);
    EXPECT_EQ(slices[1].status, Transport::Slice::FAILED);
    EXPECT_EQ(fresh.status, Transport::Slice::PENDING);
    EXPECT_EQ(fresh.rdma.retry_cnt, 0);
}

TEST_F(PendingHandshakeTest, IdleNonRpcErrorsRetireTheirEndpoint) {
    for (int error : {ERR_ENDPOINT, ERR_REJECT_HANDSHAKE}) {
        auto endpoint = peers[0].context->findEndpoint(paths[0]);
        std::promise<int> result;
        result.set_value(error);
        WorkerPoolTestPeer::setPendingSetup(*peers[0].worker_pool, paths[0],
                                            endpoint, result.get_future());
        const int errors =
            WorkerPoolTestPeer::railErrors(*peers[0].worker_pool, paths[0]);
        post(0);
        EXPECT_EQ(peers[0].context->findEndpoint(paths[0]), nullptr);
        EXPECT_EQ(
            WorkerPoolTestPeer::railErrors(*peers[0].worker_pool, paths[0]),
            errors + 1);
        installZeroQpEndpoint(peers[0], paths[0]);
    }
}

TEST_F(PendingHandshakeTest, IdleFailureDoesNotRetireReplacement) {
    auto old = peers[0].context->findEndpoint(paths[0]);
    std::promise<int> result;
    result.set_value(ERR_ENDPOINT);
    WorkerPoolTestPeer::setPendingSetup(*peers[0].worker_pool, paths[0], old,
                                        result.get_future());
    peers[0].context->deleteEndpointByPtr(old.get());
    installZeroQpEndpoint(peers[0], paths[0]);
    auto replacement = peers[0].context->findEndpoint(paths[0]);
    post(0);
    EXPECT_EQ(peers[0].context->findEndpoint(paths[0]), replacement);
    EXPECT_EQ(WorkerPoolTestPeer::railErrors(*peers[0].worker_pool, paths[0]),
              0);
}

TEST_F(PendingHandshakeTest, ZeroTtlAllowsNextHandshakeAfterRpcFailure) {
    release();
    peers[0].transport->failure_code_ = ERR_SOCKET;
    for (int attempt = 0; attempt < 2; ++attempt) {
        queue(0);
        post(0);
        wait(0);
        post(0);
        EXPECT_EQ(slices[0].status, Transport::Slice::FAILED);
        EXPECT_EQ(barrier->arrivals, attempt + 1);
    }
}

namespace {
class BlockingMetadataStorage : public MetadataStoragePlugin {
   public:
    bool get(const std::string &key, Json::Value &value) override {
        return getWithStatus(key, value) == GetResult::kFound;
    }
    GetResult getWithStatus(const std::string &key,
                            Json::Value &value) override {
        std::unique_lock<std::mutex> lock(mutex);
        ++reads;
        cv.notify_all();
        if (!cv.wait_for(lock, std::chrono::seconds(5),
                         [&] { return !held; }) ||
            fail)
            return GetResult::kUnavailable;
        auto it = values.find(key);
        if (it == values.end()) return GetResult::kNotFound;
        value = it->second;
        return GetResult::kFound;
    }
    bool set(const std::string &key, const Json::Value &value) override {
        std::lock_guard<std::mutex> lock(mutex);
        values[key] = value;
        return true;
    }
    bool remove(const std::string &key) override {
        std::lock_guard<std::mutex> lock(mutex);
        values.erase(key);
        return true;
    }
    void hold(bool fail_fetch = false) {
        std::lock_guard<std::mutex> lock(mutex);
        held = true;
        fail = fail_fetch;
        reads = 0;
    }
    void release() {
        std::lock_guard<std::mutex> lock(mutex);
        held = false;
        cv.notify_all();
    }
    bool waitForRead() {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(1),
                           [&] { return reads > 0; });
    }
    std::atomic<int> reads{0};

   private:
    std::mutex mutex;
    std::condition_variable cv;
    std::unordered_map<std::string, Json::Value> values;
    bool held = false;
    bool fail = false;
};

class MetadataWithStorage : public TransferMetadata {
   public:
    explicit MetadataWithStorage(std::shared_ptr<MetadataStoragePlugin> storage)
        : TransferMetadata(std::move(storage)) {}
};
}  // namespace

class MetadataRetryTest : public PendingHandshakeTest {
   protected:
    void SetUp() override {
        old_metacache = globalConfig().metacache;
        old_interval = globalConfig().te_metadata_refresh_interval_seconds;
        globalConfig().metacache = true;
        globalConfig().te_metadata_refresh_interval_seconds = 0;
        PendingHandshakeTest::SetUp();
        storage = std::make_shared<BlockingMetadataStorage>();
        auto metadata = std::make_shared<MetadataWithStorage>(storage);
        peers[0].metadata = metadata;
        RdmaTransportTestPeer::bindMetadata(*peers[0].transport, metadata,
                                            peers[0].server_name);
        desc.name = getServerNameFromNicPath(paths[0]);
        desc.protocol = "rdma";
        desc.devices.push_back({"remote0", 0, "::1", ""});
        ASSERT_EQ(desc.topology.parse("{\"cpu:0\":[[\"remote0\"],[]]}"), 0);
        TransferMetadata::BufferDesc buffer{};
        buffer.name = "cpu:0";
        buffer.addr = 0x1000;
        buffer.length = 4096;
        buffer.rkey = {111};
        buffer.lkey = {1};
        desc.buffers.push_back(buffer);
        ASSERT_EQ(metadata->updateSegmentDesc(desc.name, desc), 0);
        id = metadata->getSegmentID(desc.name);
        ASSERT_NE(id, static_cast<SegmentID>(-1));
        queue(0);
        WorkerPoolTestPeer::clearQueuedSlices(*peers[0].worker_pool);
        slices[0].target_id = id;
        slices[0].rdma.dest_addr = 0x1000;
        slices[0].length = 64;
        slices[0].rdma.dest_rkey = 111;
        slices[0].rdma.retry_cnt = 1;
        auto endpoint = peers[0].context->findEndpoint(paths[0]);
        RdmaEndPointTestPeer::setStatus(*endpoint, RdmaEndPoint::CONNECTED);
    }
    void TearDown() override {
        if (storage) storage->release();
        if (peers[0].worker_pool)
            WorkerPoolTestPeer::waitForRetries(*peers[0].worker_pool);
        PendingHandshakeTest::TearDown();
        globalConfig().metacache = old_metacache;
        globalConfig().te_metadata_refresh_interval_seconds = old_interval;
    }
    void startRefresh(bool fail = false) {
        storage->hold(fail);
        WorkerPoolTestPeer::retry(*peers[0].worker_pool, slices[0]);
        WorkerPoolTestPeer::pollRetries(*peers[0].worker_pool);
        ASSERT_TRUE(storage->waitForRead());
    }
    void finishRefresh() {
        storage->release();
        WorkerPoolTestPeer::waitForRetries(*peers[0].worker_pool);
        post(0);
    }
    void checkRecovery() {
        desc.buffers[0].rkey[0] = 222;
        ASSERT_EQ(peers[0].metadata->updateSegmentDesc(desc.name, desc), 0);
        startRefresh();
        WorkerPoolTestPeer::expireRetries(*peers[0].worker_pool);
        // A different peer on the same posting worker still reaches posting.
        const auto healthy_path =
            MakeNicPath(desc.name + "-healthy", "remote0");
        installZeroQpEndpoint(peers[0], healthy_path);
        auto healthy_ep = peers[0].context->findEndpoint(healthy_path);
        RdmaEndPointTestPeer::setStatus(*healthy_ep, RdmaEndPoint::CONNECTED);
        Transport::Slice healthy{};
        Transport::TransferTask healthy_task;
        queueHandshakeSlice(peers[0], healthy_path, healthy, healthy_task);
        post(0);
        EXPECT_EQ(healthy.rdma.endpoint, healthy_ep.get());
        EXPECT_EQ(slices[0].rdma.dest_rkey, 111u);
        EXPECT_EQ(slices[0].rdma.retry_cnt, 1);
        EXPECT_EQ(storage->reads, 1);
        finishRefresh();
        EXPECT_EQ(slices[0].rdma.dest_rkey, 222u);
        EXPECT_EQ(slices[0].rdma.endpoint,
                  peers[0].context->findEndpoint(paths[0]).get());
        EXPECT_EQ(slices[0].status, Transport::Slice::PENDING);
        EXPECT_EQ(slices[0].rdma.retry_cnt, 1);
        EXPECT_EQ(WorkerPoolTestPeer::pendingRetries(*peers[0].worker_pool),
                  0u);
        WorkerPoolTestPeer::clearQueuedSlices(*peers[0].worker_pool);
    }
    std::shared_ptr<BlockingMetadataStorage> storage;
    TransferMetadata::SegmentDesc desc{};
    SegmentID id = 0;
    bool old_metacache = true;
    uint64_t old_interval = 0;
};

TEST_F(MetadataRetryTest, CurrentRetryUsesRefreshedRkeyWithoutAnotherSubmit) {
    checkRecovery();
}

TEST_F(MetadataRetryTest, DisabledMetacacheStillRefreshesOutsideWorker) {
    globalConfig().metacache = false;
    checkRecovery();
}

TEST_F(MetadataRetryTest, FetchFailureFallsBackWithoutPoisoningSubmissions) {
    auto cached = peers[0].metadata->getCachedSegmentDescByID(id);
    startRefresh(true);
    finishRefresh();
    EXPECT_EQ(slices[0].rdma.dest_rkey, 111u);
    EXPECT_EQ(slices[0].status, Transport::Slice::PENDING);
    EXPECT_EQ(peers[0].metadata->getSegmentDescByID(id), cached);
    EXPECT_EQ(peers[0].metadata->getSegmentDescByID(id), cached);
    EXPECT_EQ(storage->reads, 1);

    // The same transfer can refresh again after a transient backend failure.
    WorkerPoolTestPeer::clearQueuedSlices(*peers[0].worker_pool);
    desc.buffers[0].rkey[0] = 222;
    ASSERT_EQ(peers[0].metadata->updateSegmentDesc(desc.name, desc), 0);
    startRefresh();
    finishRefresh();
    EXPECT_EQ(slices[0].rdma.dest_rkey, 222u);
    EXPECT_EQ(slices[0].status, Transport::Slice::PENDING);
    EXPECT_EQ(slices[0].rdma.retry_cnt, 1);
}

TEST_F(MetadataRetryTest, NewRetryDoesNotInheritExpiredAdmission) {
    auto &pool = *peers[0].worker_pool;
    storage->hold();
    queue(1);
    post(1);  // The other RNIC owns this server's RPC slot.
    WorkerPoolTestPeer::retry(pool, slices[0]);
    WorkerPoolTestPeer::pollRetries(pool);
    WorkerPoolTestPeer::expireRetries(pool);

    Transport::Slice fresh{};
    Transport::TransferTask task;
    queueHandshakeSlice(peers[0], paths[0], fresh, task);
    WorkerPoolTestPeer::clearQueuedSlices(pool);
    fresh.target_id = id;
    fresh.rdma.dest_addr = 0x1000;
    fresh.length = 64;
    WorkerPoolTestPeer::retry(pool, fresh);
    WorkerPoolTestPeer::pollRetries(pool);
    EXPECT_EQ(slices[0].status, Transport::Slice::FAILED);
    EXPECT_EQ(fresh.status, Transport::Slice::PENDING);
    EXPECT_EQ(storage->reads, 0);

    release();
    wait(1);
    WorkerPoolTestPeer::pollRetries(pool);
    ASSERT_TRUE(storage->waitForRead());
    finishRefresh();
    EXPECT_EQ(fresh.rdma.dest_rkey, 111u);
    EXPECT_EQ(fresh.status, Transport::Slice::PENDING);
}

TEST_F(MetadataRetryTest, ConcurrentRemovalDoesNotReuseDeletedDescriptor) {
    startRefresh();
    ASSERT_EQ(peers[0].metadata->removeLocalSegment(desc.name), 0);
    finishRefresh();
    EXPECT_EQ(slices[0].status, Transport::Slice::FAILED);
    EXPECT_EQ(peers[0].metadata->getCachedSegmentDescByID(id), nullptr);
}

TEST_F(MetadataRetryTest, RefreshAndHandshakeShareServerAdmissionAcrossRnics) {
    startRefresh();
    queue(1);
    post(1);
    EXPECT_EQ(barrier->arrivals, 0);
    EXPECT_EQ(slices[1].status, Transport::Slice::PENDING);
    finishRefresh();
    release();
    post(1);
    wait(1);
    EXPECT_EQ(barrier->arrivals, 1);
}

TEST_F(MetadataRetryTest, ShutdownWaitsForMetadataRpc) {
    startRefresh();
    auto shutdown =
        std::async(std::launch::async, [&] { peers[0].worker_pool.reset(); });
    EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    storage->release();
    EXPECT_EQ(shutdown.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    shutdown.get();
}

TEST_F(RdmaEndPointStateTest, ActiveAdvisoryAdvancesOnceAndBindsSelectedGid) {
    TransferMetadata server_metadata(P2PHANDSHAKE);
    int sockfd = -1;
    const uint16_t port = findAvailableTcpPort(sockfd);
    ASSERT_NE(port, 0);
    const std::string host = globalConfig().use_ipv6 ? "::1" : "127.0.0.1";
    const std::string peer_server_name =
        maybeWrapIpV6(host) + ":" + std::to_string(port);
    std::vector<std::string> observed_gids;
    ASSERT_EQ(server_metadata.startHandshakeDaemon(
                  [&](const RdmaEndPoint::HandShakeDesc &peer_desc,
                      RdmaEndPoint::HandShakeDesc &local_desc) {
                      observed_gids.push_back(peer_desc.local_gid);
                      local_desc.local_nic_path = peer_desc.peer_nic_path;
                      local_desc.peer_nic_path = peer_desc.local_nic_path;
                      local_desc.local_gid = peer_desc.local_gid;
                      local_desc.local_lid = 0;
                      local_desc.qp_num.clear();
                      local_desc.ready_ack_supported = false;
                      // Repeating the original advice must not advance twice.
                      local_desc.auto_gid_reprobe_from = observed_gids.front();
                      return 0;
                  },
                  port, sockfd),
              0);
    auto client_metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    RdmaTransportTestPeer::bindMetadata(*transport_, client_metadata,
                                        "rdma-endpoint-state-client");
    ibv_cq fake_cq = {};
    RdmaContextTestPeer::bindCompletionQueue(*context_, &fake_cq);
    ASSERT_EQ(endpoint_->construct(&fake_cq, 0), 0);
    endpoint_->setPeerNicPath(peer_server_name + "@peer-nic");
    ibv_context fake_verbs = {};
    RdmaContextTestPeer::enableAutoRecovery(*context_, &fake_verbs);
    const auto initial =
        context_->preparePeerGidSelection(endpoint_->peerNicPath());
    ASSERT_EQ(endpoint_->setupConnectionsByActive(), 0);
    ASSERT_TRUE(endpoint_->connected());
    ASSERT_EQ(observed_gids.size(), 2u);
    EXPECT_EQ(observed_gids.front(), initial.selection.gid);
    EXPECT_NE(observed_gids.back(), initial.selection.gid);
    const auto selected =
        context_->preparePeerGidSelection(endpoint_->peerNicPath());
    EXPECT_EQ(observed_gids.back(), selected.selection.gid);
    EXPECT_NE(initial.selection_epoch, selected.selection_epoch);
    const auto token = endpoint_->autoGidCompletionToken();
    EXPECT_TRUE(token.valid);
    EXPECT_EQ(token.selection_epoch, selected.selection_epoch);
    EXPECT_EQ(context_->gid(), initial.selection.gid);
}

}  // namespace

#undef ibv_query_port
extern "C" {
int ibv_query_port(ibv_context *context, uint8_t port,
                   _compat_ibv_port_attr *compat) {
    if (context != auto_gid_test_context || !auto_gid_test_context) {
        using QueryPort =
            int (*)(ibv_context *, uint8_t, _compat_ibv_port_attr *);
        static auto real_query =
            reinterpret_cast<QueryPort>(dlsym(RTLD_NEXT, "ibv_query_port"));
        return real_query ? real_query(context, port, compat) : ENOSYS;
    }
    auto *attr = reinterpret_cast<ibv_port_attr *>(compat);
    *attr = {};
    attr->gid_tbl_len = 2;
    return 0;
}
int _ibv_query_gid_ex(ibv_context *context, uint32_t port, uint32_t index,
                      ibv_gid_entry *entry, uint32_t flags, size_t entry_size) {
    if (context != auto_gid_test_context || !auto_gid_test_context) {
        using QueryGid = int (*)(ibv_context *, uint32_t, uint32_t,
                                 ibv_gid_entry *, uint32_t, size_t);
        static auto real_query =
            reinterpret_cast<QueryGid>(dlsym(RTLD_NEXT, "_ibv_query_gid_ex"));
        return real_query
                   ? real_query(context, port, index, entry, flags, entry_size)
                   : ENOSYS;
    }
    if (index >= 2) return EINVAL;
    *entry = {};
    entry->gid.raw[15] = index + 1;
    entry->gid_type = IBV_GID_TYPE_ROCE_V2;
    return 0;
}
}
