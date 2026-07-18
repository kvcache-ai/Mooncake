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
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "error.h"
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

}  // namespace
