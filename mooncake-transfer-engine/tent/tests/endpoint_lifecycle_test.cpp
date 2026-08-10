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

#include <memory>

#include "tent/common/utils/string_builder.h"
#include "tent/transport/rdma/endpoint.h"
#include "tent/transport/rdma/slice.h"

namespace mooncake {
namespace tent {

class EndpointTestAccess {
   public:
    // Puts a context-less endpoint into the state a completed bootstrap
    // leaves behind, so accept() can be driven without an RDMA device.
    static void markConnected(RdmaEndPoint& endpoint,
                              const std::string& peer_server_name,
                              const std::string& peer_nic_name,
                              const std::vector<uint32_t>& peer_qp_num_list) {
        endpoint.peer_server_name_ = peer_server_name;
        endpoint.peer_nic_name_ = peer_nic_name;
        endpoint.peer_qp_num_list_ = peer_qp_num_list;
        endpoint.status_.store(RdmaEndPoint::EP_READY,
                               std::memory_order_relaxed);
    }
};

namespace {

TEST(EndpointLifecycleTest, DefaultConstructedEndpointOwnsNoResources) {
    RdmaEndPoint endpoint;

    EXPECT_EQ(endpoint.status(), RdmaEndPoint::EP_UNINIT);
    EXPECT_TRUE(endpoint.qpNum().empty());
    EXPECT_EQ(endpoint.getInflightSlices(), 0);
    EXPECT_EQ(endpoint.notifyQpNum(), 0);
}

TEST(EndpointLifecycleTest, DefaultConstructedEndpointCanBeDestroyed) {
    RdmaEndPoint endpoint;

    EXPECT_EQ(endpoint.deconstruct(), 0);
    EXPECT_EQ(endpoint.status(), RdmaEndPoint::EP_DESTROYED);
    EXPECT_TRUE(endpoint.qpNum().empty());
}

TEST(EndpointLifecycleTest, DeconstructIsIdempotent) {
    RdmaEndPoint endpoint;

    ASSERT_EQ(endpoint.deconstruct(), 0);
    EXPECT_EQ(endpoint.deconstruct(), 0);
    EXPECT_EQ(endpoint.status(), RdmaEndPoint::EP_DESTROYED);
}

TEST(EndpointLifecycleTest, DestroyedEndpointCannotBeConstructedAgain) {
    RdmaEndPoint endpoint;

    ASSERT_EQ(endpoint.deconstruct(), 0);
    EXPECT_NE(endpoint.construct(nullptr, nullptr, "reused"), 0);
    EXPECT_EQ(endpoint.status(), RdmaEndPoint::EP_DESTROYED);
}

TEST(EndpointLifecycleTest, TwoPhaseDestroyHandlesUninitializedEndpoint) {
    RdmaEndPoint endpoint;

    endpoint.beginDestroy();
    EXPECT_EQ(endpoint.status(), RdmaEndPoint::EP_DESTROYING);
    EXPECT_TRUE(endpoint.finishDestroy());
    EXPECT_EQ(endpoint.status(), RdmaEndPoint::EP_DESTROYED);
}

TEST(EndpointLifecycleTest, FinishDestroyRejectsNonRetiringEndpoint) {
    RdmaEndPoint endpoint;

    EXPECT_FALSE(endpoint.finishDestroy());
    EXPECT_EQ(endpoint.status(), RdmaEndPoint::EP_UNINIT);
}

TEST(EndpointLifecycleTest, FinishDestroyIsIdempotent) {
    RdmaEndPoint endpoint;

    endpoint.beginDestroy();
    ASSERT_TRUE(endpoint.finishDestroy());
    EXPECT_TRUE(endpoint.finishDestroy());
    EXPECT_EQ(endpoint.status(), RdmaEndPoint::EP_DESTROYED);
}

TEST(EndpointLifecycleTest, NotificationFailsWhenEndpointIsNotConnected) {
    RdmaEndPoint endpoint;

    EXPECT_FALSE(endpoint.sendNotification("name", "message"));
}

TEST(EndpointLifecycleTest, SharedFromThisUsesRealEndpointOwnership) {
    auto endpoint = std::make_shared<RdmaEndPoint>();
    std::weak_ptr<RdmaEndPoint> weak = endpoint->shared_from_this();

    auto locked = weak.lock();
    ASSERT_NE(locked, nullptr);
    EXPECT_EQ(locked.get(), endpoint.get());

    locked.reset();
    endpoint.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(EndpointLifecycleTest, SliceAccessWhileEndpointAlive) {
    auto endpoint = std::make_shared<RdmaEndPoint>();
    RdmaSlice slice;
    slice.ep_weak_ptr = endpoint;

    auto locked = slice.ep_weak_ptr.lock();
    ASSERT_NE(locked, nullptr);
    EXPECT_EQ(locked.get(), endpoint.get());
}

TEST(EndpointLifecycleTest, SliceAccessAfterEndpointEvicted) {
    RdmaSlice slice;
    {
        auto endpoint = std::make_shared<RdmaEndPoint>();
        slice.ep_weak_ptr = endpoint;
        ASSERT_FALSE(slice.ep_weak_ptr.expired());
    }

    EXPECT_TRUE(slice.ep_weak_ptr.expired());
    EXPECT_EQ(slice.ep_weak_ptr.lock(), nullptr);
}

TEST(EndpointLifecycleTest, MultipleSlicesShareEndpointLifetime) {
    auto endpoint = std::make_shared<RdmaEndPoint>();
    RdmaSlice slices[3];
    for (auto& slice : slices) slice.ep_weak_ptr = endpoint;

    for (auto& slice : slices) {
        auto locked = slice.ep_weak_ptr.lock();
        ASSERT_NE(locked, nullptr);
        EXPECT_EQ(locked.get(), endpoint.get());
    }

    endpoint.reset();
    for (auto& slice : slices) {
        EXPECT_TRUE(slice.ep_weak_ptr.expired());
        EXPECT_EQ(slice.ep_weak_ptr.lock(), nullptr);
    }
}

TEST(EndpointLifecycleTest, SliceWeakPtrResetClearsReference) {
    auto endpoint = std::make_shared<RdmaEndPoint>();
    RdmaSlice slice;
    slice.ep_weak_ptr = endpoint;

    slice.ep_weak_ptr.reset();

    EXPECT_TRUE(slice.ep_weak_ptr.expired());
    EXPECT_EQ(slice.ep_weak_ptr.lock(), nullptr);
    EXPECT_NE(endpoint, nullptr);
}

TEST(EndpointLifecycleTest, BootstrapWithNewPeerQpsRetiresEstablishedEndpoint) {
    // The peer dropped its endpoint (store eviction or a transfer failure)
    // and bootstraps again with a fresh QP set. The established endpoint now
    // points at QPs that no longer exist, so it must retire instead of
    // staying EP_READY and rejecting every later bootstrap from that peer.
    RdmaEndPoint endpoint;
    EndpointTestAccess::markConnected(endpoint, "10.0.0.1:12345", "mlx5_0",
                                      {100, 101});

    BootstrapDesc peer_desc, local_desc;
    peer_desc.local_nic_path = MakeNicPath("10.0.0.1:12345", "mlx5_0");
    peer_desc.qp_num = {200, 201};

    EXPECT_FALSE(endpoint.accept(peer_desc, local_desc).ok());
    EXPECT_EQ(endpoint.status(), RdmaEndPoint::EP_DESTROYING);
}

TEST(EndpointLifecycleTest, ExternalOwnerCanReleaseAfterExplicitDeconstruct) {
    auto endpoint = std::make_shared<RdmaEndPoint>();
    std::weak_ptr<RdmaEndPoint> weak = endpoint;

    ASSERT_EQ(endpoint->deconstruct(), 0);
    EXPECT_EQ(endpoint->status(), RdmaEndPoint::EP_DESTROYED);

    endpoint.reset();
    EXPECT_TRUE(weak.expired());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
