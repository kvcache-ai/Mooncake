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
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common.h"
#include "config.h"
#include "error.h"
#include "transfer_metadata.h"
#include "transfer_metadata_plugin.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"
#include "transport/rdma_transport/rdma_transport.h"

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
};

class RdmaContextTestPeer {
   public:
    static void bindCompletionQueue(RdmaContext &context, ibv_cq *cq) {
        context.cq_list_.emplace_back();
        context.cq_list_.back().native = cq;
        cq->cq_context = &context.cq_list_.back().outstanding;
        context.gid_ = {};
        context.gid_index_ = 0;
        context.lid_ = 0;
    }

    static void clearCompletionQueues(RdmaContext &context) {
        context.cq_list_.clear();
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

}  // namespace
