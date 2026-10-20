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

// Behavioral tests for the EFA endpoint handshake state machine. The real
// EfaEndPoint source is compiled against the test-only RPC/address-vector
// fakes in efa_endpoint_handshake_fakes/ (a shadowed efa_context.h), so
// the locking and generation logic runs without an EFA device. All waits
// are bounded: a locking regression must fail, not hang the runner.
#include <chrono>
#include <future>
#include <vector>

#include <gtest/gtest.h>

#include "transport/efa_transport/efa_endpoint.h"

namespace mooncake {

namespace {

using Desc = TransferMetadata::HandShakeDesc;

Desc Reply(const Desc& request) {
    Desc result;
    result.local_nic_path = request.peer_nic_path;
    result.peer_nic_path = request.local_nic_path;
    result.efa_addr = result.local_nic_path;
    return result;
}

TEST(EfaEndPointHandshakeTest, ReciprocalActiveSetupDoesNotDeadlock) {
    constexpr auto kWait = std::chrono::seconds(30);
    EfaContext a("a:1@efa"), b("b:2@efa");
    EfaEndPoint ea(a), eb(b);
    ea.setPeerNicPath(b.name);
    eb.setPeerNicPath(a.name);
    std::promise<void> a_started, b_started;
    auto a_ready = a_started.get_future();
    auto b_ready = b_started.get_future();
    a.transport.rpc = [&](const Desc& request, Desc& response) {
        a_started.set_value();
        if (b_ready.wait_for(kWait) != std::future_status::ready) {
            return ERR_SOCKET;
        }
        return eb.setupConnectionsByPassive(request, response);
    };
    b.transport.rpc = [&](const Desc& request, Desc& response) {
        b_started.set_value();
        if (a_ready.wait_for(kWait) != std::future_status::ready) {
            return ERR_SOCKET;
        }
        return ea.setupConnectionsByPassive(request, response);
    };
    // Each side's passive handler runs while the other side's active setup
    // is blocked in its handshake RPC. This only completes if the active
    // path does not hold the endpoint state lock across the RPC.
    auto first = std::async(std::launch::async,
                            [&] { return ea.setupConnectionsByActive(); });
    auto second = std::async(std::launch::async,
                             [&] { return eb.setupConnectionsByActive(); });
    ASSERT_EQ(std::future_status::ready, first.wait_for(kWait))
        << "reciprocal active setup deadlocked";
    ASSERT_EQ(std::future_status::ready, second.wait_for(kWait))
        << "reciprocal active setup deadlocked";
    ASSERT_EQ(first.get(), 0);
    ASSERT_EQ(second.get(), 0);
    EXPECT_TRUE(ea.connected() && eb.connected());
    // Depending on which side connects first, a late passive handshake may
    // re-insert the peer address. Either way each side ends with exactly one
    // live address.
    EXPECT_EQ(1, a.inserts - a.removals);
    EXPECT_EQ(1, b.inserts - b.removals);
    EXPECT_LE(a.removals, 1);
    EXPECT_LE(b.removals, 1);
}

void CancelledHandshake(bool change_path, bool detach) {
    constexpr auto kWait = std::chrono::seconds(30);
    EfaContext context("a:1@efa");
    EfaEndPoint endpoint(context);
    endpoint.setPeerNicPath("b:2@efa");
    std::promise<void> started, release;
    auto ready = started.get_future();
    auto released = release.get_future();
    context.transport.rpc = [&](const Desc& request, Desc& response) {
        started.set_value();
        if (released.wait_for(kWait) != std::future_status::ready) {
            return ERR_SOCKET;
        }
        response = Reply(request);
        return 0;
    };
    auto active = std::async(std::launch::async, [&] {
        return endpoint.setupConnectionsByActive();
    });
    ASSERT_EQ(std::future_status::ready, ready.wait_for(kWait));
    if (change_path) {
        endpoint.setPeerNicPath("c:3@efa");
    } else if (detach) {
        endpoint.markDetachedForTeardown();
    } else {
        endpoint.disconnect();
    }
    release.set_value();
    // The reply crosses a disconnect/nic-path change/teardown and is stale.
    ASSERT_EQ(std::future_status::ready, active.wait_for(kWait));
    EXPECT_NE(active.get(), 0);
    EXPECT_FALSE(endpoint.connected());
    EXPECT_EQ(0, context.inserts);
}

TEST(EfaEndPointHandshakeTest, StaleReplyAfterDisconnectIsRejected) {
    CancelledHandshake(/*change_path=*/false, /*detach=*/false);
}

TEST(EfaEndPointHandshakeTest, StaleReplyAfterNicPathChangeIsRejected) {
    CancelledHandshake(/*change_path=*/true, /*detach=*/false);
}

TEST(EfaEndPointHandshakeTest, StaleReplyAfterTeardownIsRejected) {
    CancelledHandshake(/*change_path=*/false, /*detach=*/true);
}

TEST(EfaEndPointHandshakeTest, ConcurrentActiveCallersShareOneHandshake) {
    EfaContext context("a:1@efa");
    EfaEndPoint endpoint(context);
    endpoint.setPeerNicPath("b:2@efa");
    int calls = 0;
    context.transport.rpc = [&](const Desc& request, Desc& response) {
        ++calls;
        response = Reply(request);
        return 0;
    };
    std::vector<std::future<int>> callers;
    for (int i = 0; i != 8; ++i) {
        callers.push_back(std::async(std::launch::async, [&] {
            return endpoint.setupConnectionsByActive();
        }));
    }
    for (auto& call : callers) {
        ASSERT_EQ(call.get(), 0);
    }
    EXPECT_EQ(1, calls);
    EXPECT_EQ(1, context.inserts);
    endpoint.disconnect();
    context.transport.rpc = [](const Desc&, Desc&) { return ERR_SOCKET; };
    EXPECT_EQ(ERR_SOCKET, endpoint.setupConnectionsByActive());
    EXPECT_FALSE(endpoint.connected());
    context.transport.rpc = [&](const Desc& request, Desc& response) {
        response = Reply(request);
        return 0;
    };
    EXPECT_EQ(0, endpoint.setupConnectionsByActive());
}

TEST(EfaEndPointHandshakeTest,
     ActiveSetupReusesPassiveConnectionAfterRpcFailure) {
    EfaContext context("a:1@efa");
    EfaEndPoint endpoint(context);
    endpoint.setPeerNicPath("b:2@efa");
    context.transport.rpc = [&](const Desc& request, Desc&) {
        Desc incoming = Reply(request), response;
        EXPECT_EQ(0, endpoint.setupConnectionsByPassive(incoming, response));
        // A repeated passive handshake re-inserts the peer address (see the
        // comment in setupConnectionsByPassive). The in-flight active setup
        // must still succeed on the resulting connection.
        EXPECT_EQ(0, endpoint.setupConnectionsByPassive(incoming, response));
        return ERR_SOCKET;
    };
    EXPECT_EQ(0, endpoint.setupConnectionsByActive());
    EXPECT_TRUE(endpoint.connected());
    EXPECT_EQ(2, context.inserts);
    EXPECT_EQ(1, context.removals);
    endpoint.disconnect();
    context.transport.rpc = [](const Desc&, Desc&) { return 0; };
    // A reciprocal reply with an empty/invalid address is rejected.
    EXPECT_EQ(ERR_REJECT_HANDSHAKE, endpoint.setupConnectionsByActive());
    EXPECT_FALSE(endpoint.connected());
    EXPECT_EQ(2, context.inserts);
}

}  // namespace
}  // namespace mooncake
