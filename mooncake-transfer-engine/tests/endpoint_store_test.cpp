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

// Regression coverage for #1845. Asserts that
// SIEVEEndpointStore::reclaimEndpoint drains quiescent entries from
// waiting_list_ without requiring a subsequent insertEndpoint call. This is the
// invariant the periodic-reclaim tick in monitorWorker depends on.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "transport/rdma_transport/endpoint_store.h"
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
class RdmaEndpointStoreNotificationTestPeer {
   public:
    using CleanupOps = RdmaEndPoint::NotificationCleanupOps;

    static void rollbackNotificationConstruction(RdmaEndPoint& endpoint,
                                                 const CleanupOps& ops) {
        endpoint.rollbackNotificationConstruction(ops);
    }

    static bool hasResources(const RdmaEndPoint& endpoint) {
        std::lock_guard<std::mutex> guard(endpoint.notify_.mutex);
        return endpoint.notify_.qp || endpoint.notify_.send_mr ||
               endpoint.notify_.recv_mr || endpoint.notify_.send_buffer ||
               endpoint.notify_.recv_buffer;
    }

    static void seedResources(RdmaEndPoint& endpoint) {
        std::lock_guard<std::mutex> guard(endpoint.notify_.mutex);
        endpoint.notify_.qp = reinterpret_cast<ibv_qp*>(0x1);
        endpoint.notify_.send_mr = reinterpret_cast<ibv_mr*>(0x2);
        endpoint.notify_.recv_mr = reinterpret_cast<ibv_mr*>(0x3);
        endpoint.notify_.send_buffer = std::make_unique<char[]>(1);
        endpoint.notify_.recv_buffer = std::make_unique<char[]>(1);
        endpoint.notify_.enabled = true;
    }

    static bool enabled(const RdmaEndPoint& endpoint) {
        std::lock_guard<std::mutex> guard(endpoint.notify_.mutex);
        return endpoint.notify_.enabled;
    }

    static void clearResources(RdmaEndPoint& endpoint) {
        std::lock_guard<std::mutex> guard(endpoint.notify_.mutex);
        endpoint.notify_.qp = nullptr;
        endpoint.notify_.send_mr = nullptr;
        endpoint.notify_.recv_mr = nullptr;
        endpoint.notify_.send_buffer.reset();
        endpoint.notify_.recv_buffer.reset();
        endpoint.notify_.enabled = false;
    }
};
}  // namespace mooncake

namespace {

// Build an RdmaEndPoint that owns zero QPs and has active_=false. construct()
// is deliberately not called — the store's reclaim logic only inspects
// hasOutstandingSlice(), which for an endpoint with empty qp_list_ returns
// whatever active_ is.
std::shared_ptr<RdmaEndPoint> makeQuiescentEndpoint(RdmaContext& ctx) {
    auto ep = std::make_shared<RdmaEndPoint>(ctx);
    ep->set_active(false);
    return ep;
}

std::shared_ptr<RdmaEndPoint> makeActiveEndpoint(RdmaContext& ctx) {
    // Default ctor leaves active_=true.
    return std::make_shared<RdmaEndPoint>(ctx);
}

class EndpointStoreTest : public ::testing::Test {
   protected:
    // Leaked on purpose: RdmaTransport's destructor dereferences metadata_,
    // which is null when the engine was never init()ed. We only need a live
    // reference for RdmaContext's constructor; the engine object is otherwise
    // unused by the reclaim logic under test.
    RdmaTransport* transport_ = nullptr;
    std::unique_ptr<RdmaContext> ctx_;

    void SetUp() override {
        transport_ = new RdmaTransport();
        // Intentional leak: ~RdmaTransport dereferences metadata_, which is
        // null until install(). Marking it ignored keeps LSAN under ASAN
        // builds from flagging this one allocation while still catching
        // real leaks elsewhere.
        MC_LSAN_IGNORE_OBJECT(transport_);
        ctx_ = std::make_unique<RdmaContext>(*transport_, "unused");
    }
};

// The core invariant behind #1845's fix: reclaimEndpoint must drain quiescent
// entries on its own, without needing a subsequent insertEndpoint to trigger
// it. Before the fix, reclaim ran only on insertion, so if insertions stopped
// (e.g., all peers died), waiting_list_ grew unboundedly. The periodic tick
// from monitorWorker calls this method every second; this test asserts its
// contract in isolation.
TEST_F(EndpointStoreTest, ReclaimDrainsQuiescentEntries) {
    SIEVEEndpointStore store(/*max_size=*/4);

    constexpr size_t kN = 10;
    for (size_t i = 0; i < kN; ++i) {
        store.testOnlyInsertWaiting(makeQuiescentEndpoint(*ctx_));
    }
    EXPECT_EQ(store.waitingListSize(), kN);

    store.reclaimEndpoint();
    EXPECT_EQ(store.waitingListSize(), 0u)
        << "reclaimEndpoint must drain quiescent entries with no insertion "
           "prerequisite";
}

// Negative control: reclaim must leave entries in place if they still report
// outstanding slices. Ensures we didn't break the hasOutstandingSlice gate.
TEST_F(EndpointStoreTest, ReclaimLeavesActiveEntries) {
    SIEVEEndpointStore store(4);

    store.testOnlyInsertWaiting(makeActiveEndpoint(*ctx_));
    store.testOnlyInsertWaiting(makeActiveEndpoint(*ctx_));
    store.testOnlyInsertWaiting(makeQuiescentEndpoint(*ctx_));
    EXPECT_EQ(store.waitingListSize(), 3u);

    store.reclaimEndpoint();
    EXPECT_EQ(store.waitingListSize(), 2u)
        << "reclaim should drop only the quiescent endpoint, keep the two "
           "active ones";
}

TEST_F(EndpointStoreTest, ReclaimIsIdempotentWhenEmpty) {
    SIEVEEndpointStore store(4);

    store.reclaimEndpoint();
    EXPECT_EQ(store.waitingListSize(), 0u);

    store.testOnlyInsertWaiting(makeQuiescentEndpoint(*ctx_));
    store.reclaimEndpoint();
    EXPECT_EQ(store.waitingListSize(), 0u);

    store.reclaimEndpoint();  // second call is a no-op
    EXPECT_EQ(store.waitingListSize(), 0u);
}

// Demonstrates the #1845 failure mode: once insertions stop but evictions
// keep landing in the waiting list, nothing drains them without an explicit
// reclaim call. Before this fix, reclaimEndpoint ran only from insertEndpoint,
// so "many evictions, no new peers to connect to" meant waiting_list_ grew
// without bound. This test simulates that workload without any RDMA or
// scheduler; the assertion is a strict "zero reclaim calls leaves the leak
// at its peak."
TEST_F(EndpointStoreTest, LeakManifestsWithoutReclaimCall) {
    SIEVEEndpointStore store(/*max_size=*/4);

    constexpr size_t kEvictions = 1118;  // match reporter's eviction count
    for (size_t i = 0; i < kEvictions; ++i) {
        store.testOnlyInsertWaiting(makeQuiescentEndpoint(*ctx_));
    }

    // Without a reclaim call the leak is at its peak.
    EXPECT_EQ(store.waitingListSize(), kEvictions)
        << "baseline confirmation: waiting_list_ accumulates as expected";

    // The fix is a 1 Hz invocation of this single method from monitorWorker.
    // One call is enough to drain the entire backlog (because the entries are
    // quiescent by the time the peer-death path finishes). This is the
    // invariant the PR relies on.
    store.reclaimEndpoint();
    EXPECT_EQ(store.waitingListSize(), 0u)
        << "a single reclaim call drains the full backlog once insertions "
           "stop; this is what the periodic tick in monitorWorker provides";
}

// Guards against a future regression that re-breaks the reclaim contract —
// e.g., someone changing reclaimEndpoint to no-op when endpoint_map_ is
// empty, on the incorrect assumption that reclaim only runs from
// insertEndpoint. Walking 1000 quiescent entries should still drain them.
TEST_F(EndpointStoreTest, ReclaimDoesNotRequireActiveMap) {
    SIEVEEndpointStore store(4);
    EXPECT_EQ(store.getSize(), 0u);  // endpoint_map_ empty

    for (size_t i = 0; i < 1000; ++i) {
        store.testOnlyInsertWaiting(makeQuiescentEndpoint(*ctx_));
    }
    EXPECT_EQ(store.getSize(), 0u);  // still empty
    EXPECT_EQ(store.waitingListSize(), 1000u);

    store.reclaimEndpoint();
    EXPECT_EQ(store.waitingListSize(), 0u);
}

TEST_F(EndpointStoreTest,
       StaleRawPointerLookupAndDeleteStressDoesNotDereference) {
    SIEVEEndpointStore store(4);
    auto sentinel = makeQuiescentEndpoint(*ctx_);
    std::vector<RdmaEndPoint*> stale_ptrs;
    stale_ptrs.reserve(1000);

    for (size_t i = 0; i < 1000; ++i) {
        auto endpoint = makeQuiescentEndpoint(*ctx_);
        const std::string peer_nic_path = "peer@" + std::to_string(i);
        store.testOnlyInsertEndpoint(peer_nic_path, endpoint);
        stale_ptrs.push_back(endpoint.get());
        EXPECT_EQ(endpoint, store.getEndpointByPtr(endpoint.get()));
        EXPECT_EQ(0, store.deleteEndpointByPtr(endpoint.get()));
    }

    EXPECT_EQ(store.getSize(), 0u);
    EXPECT_EQ(store.waitingListSize(), 1000u);
    store.testOnlyInsertEndpoint("sentinel@peer", sentinel);
    store.reclaimEndpoint();
    EXPECT_EQ(store.waitingListSize(), 0u);
    EXPECT_EQ(store.getSize(), 1u);

    // The endpoints were live in the store, deleted by raw pointer, then
    // reclaimed. The raw pointers are now stale while the sentinel keeps the
    // active map non-empty. Store APIs must compare pointer identity only;
    // dereferencing would be caught by ASan builds.
    for (auto* stale_ptr : stale_ptrs) {
        EXPECT_EQ(nullptr, store.getEndpointByPtr(stale_ptr));
        EXPECT_EQ(-1, store.deleteEndpointByPtr(stale_ptr));
    }
    EXPECT_EQ(sentinel, store.getEndpointByPtr(sentinel.get()));
}

struct NotificationRollbackProbe {
    int destroy_qp_calls = 0;
    int dereg_mr_calls = 0;
    int destroy_qp_result = 0;
    int dereg_mr_result = 0;

    static NotificationRollbackProbe* current;

    static int destroyQp(ibv_qp*) {
        ++current->destroy_qp_calls;
        return current->destroy_qp_result;
    }

    static int deregMr(ibv_mr*) {
        ++current->dereg_mr_calls;
        return current->dereg_mr_result;
    }
};

NotificationRollbackProbe* NotificationRollbackProbe::current = nullptr;

TEST_F(EndpointStoreTest, NotificationConstructionRollsBackResources) {
    RdmaEndPoint endpoint(*ctx_);
    NotificationRollbackProbe probe;
    NotificationRollbackProbe::current = &probe;

    RdmaEndpointStoreNotificationTestPeer::seedResources(endpoint);

    RdmaEndpointStoreNotificationTestPeer::rollbackNotificationConstruction(
        endpoint, {NotificationRollbackProbe::destroyQp,
                   NotificationRollbackProbe::deregMr});

    EXPECT_EQ(probe.destroy_qp_calls, 1);
    EXPECT_EQ(probe.dereg_mr_calls, 2);
    EXPECT_FALSE(RdmaEndpointStoreNotificationTestPeer::hasResources(endpoint));
    EXPECT_FALSE(RdmaEndpointStoreNotificationTestPeer::enabled(endpoint));

    NotificationRollbackProbe::current = nullptr;
}

TEST_F(EndpointStoreTest, NotificationConstructionRetainsFailedCleanup) {
    RdmaEndPoint endpoint(*ctx_);
    NotificationRollbackProbe probe;
    probe.destroy_qp_result = EBUSY;
    probe.dereg_mr_result = EBUSY;
    NotificationRollbackProbe::current = &probe;

    RdmaEndpointStoreNotificationTestPeer::seedResources(endpoint);

    RdmaEndpointStoreNotificationTestPeer::rollbackNotificationConstruction(
        endpoint, {NotificationRollbackProbe::destroyQp,
                   NotificationRollbackProbe::deregMr});

    EXPECT_TRUE(RdmaEndpointStoreNotificationTestPeer::hasResources(endpoint));
    EXPECT_TRUE(RdmaEndpointStoreNotificationTestPeer::enabled(endpoint));

    RdmaEndpointStoreNotificationTestPeer::clearResources(endpoint);
    NotificationRollbackProbe::current = nullptr;
}

}  // namespace
