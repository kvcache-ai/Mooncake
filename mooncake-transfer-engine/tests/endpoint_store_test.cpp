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

}  // namespace
