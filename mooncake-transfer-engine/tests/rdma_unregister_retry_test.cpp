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

// Regression tests for partial-failure semantics of
// RdmaTransport::unregisterLocalMemory: the metadata entry is removed BEFORE
// the per-context MR deregistration, so a failed ibv_dereg_mr leaves a live MR
// behind a missing metadata entry. A retry must re-attempt the MR cleanup and
// must only report ERR_ADDRESS_NOT_REGISTERED once nothing remains.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <memory>

#include <sys/mman.h>

#include "error.h"
#include "rdma_test_peers.h"
#include "transfer_engine_impl.h"
#include "transfer_metadata.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"

using namespace mooncake;

// Interpose the libibverbs boundary for this test binary (same mechanism as
// rdma_context_reprobe_test.cpp): rdma-core implements ibv_dereg_mr as an
// exported library symbol, so this definition takes precedence and lets the
// test script deregistration failures without RDMA hardware. Atomics: the
// batch unregister path runs one worker thread per buffer.
std::atomic<int> g_fake_dereg_failures_remaining{0};
std::atomic<int> g_fake_dereg_calls{0};

extern "C" int ibv_dereg_mr(ibv_mr*) {
    ++g_fake_dereg_calls;
    // Claim a failure token with a CAS loop: batch unregistrations call this
    // from one worker thread per buffer, and a check-then-decrement race
    // would hand one token to two callers.
    int remaining = g_fake_dereg_failures_remaining.load();
    while (remaining > 0 &&
           !g_fake_dereg_failures_remaining.compare_exchange_weak(
               remaining, remaining - 1)) {
    }
    return remaining > 0 ? EIO : 0;
}

// The friend declarations name mooncake::TransferEngineImplTestPeer, so this
// peer must be defined in namespace mooncake BEFORE its first use below
// (mirrors the peer in transport_uint_test.cpp).
namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static void replaceTransports(TransferEngineImpl& engine,
                                  std::shared_ptr<Transport> transport) {
        engine.multi_transports_->transport_map_.clear();
        engine.multi_transports_->transport_map_.emplace("rdma",
                                                         std::move(transport));
    }

    static void seedLocalMemoryRegion(TransferEngineImpl& engine, void* addr,
                                      uint64_t length) {
        std::unique_lock<std::shared_mutex> lock(engine.mutex_);
        engine.insertMemoryRegionLocked(
            TransferEngineImpl::MemoryRegion{addr, length, "cpu", true});
    }
};
}  // namespace mooncake

namespace {

class RdmaUnregisterRetryTest : public ::testing::Test {
   protected:
    void SetUp() override {
        buffer_ = mmap(nullptr, kLength, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(buffer_, MAP_FAILED);

        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        auto local_desc = std::make_shared<TransferMetadata::SegmentDesc>();
        local_desc->name = "local-rdma-segment";
        local_desc->protocol = "rdma";
        ASSERT_EQ(
            metadata_->addLocalSegment(LOCAL_SEGMENT_ID, "local-rdma-segment",
                                       std::move(local_desc)),
            0);

        transport_ = std::make_unique<RdmaTransport>();
        RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                            "local-rdma-segment");
        context_ =
            std::make_shared<RdmaContext>(*transport_, "nonexistent-device");
        RdmaTransportTestPeer::addContext(*transport_, context_);

        g_fake_dereg_failures_remaining = 0;
        g_fake_dereg_calls = 0;

        // A synthetic MR record: only addr/length are read by
        // RdmaContext::unregisterMemoryRegion (plus mr itself, handed to the
        // interposed ibv_dereg_mr above).
        mr_ = {};
        mr_.addr = buffer_;
        mr_.length = kLength;
        RdmaContextTestPeer::seedMemoryRegion(*context_, buffer_, kLength,
                                              &mr_);
    }

    void TearDown() override {
        // Destroy the transport (and its contexts) before unmapping so no
        // deferred cleanup can touch the buffer. The MR map must be empty by
        // now: a leaked entry would dangle a pointer to the stack mr_.
        EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_, buffer_));
        transport_.reset();
        context_.reset();
        if (buffer_ != MAP_FAILED) munmap(buffer_, kLength);
    }

    void addMetadataEntry() {
        TransferMetadata::BufferDesc desc{};
        desc.name = "local-rdma-segment";
        desc.addr = reinterpret_cast<uint64_t>(buffer_);
        desc.length = kLength;
        ASSERT_EQ(metadata_->addLocalMemoryBuffer(desc, false), 0);
    }

    static constexpr size_t kLength = 64 * 1024;
    void* buffer_ = MAP_FAILED;
    std::shared_ptr<TransferMetadata> metadata_;
    std::unique_ptr<RdmaTransport> transport_;
    std::shared_ptr<RdmaContext> context_;
    ibv_mr mr_{};
};

// A failed ibv_dereg_mr after the metadata entry was removed must surface the
// real error (not NOT_REGISTERED), and the retry must reach the MR cleanup
// behind the missing metadata entry instead of short-circuiting.
TEST_F(RdmaUnregisterRetryTest, RetryReachesLiveMrBehindMissingMetadata) {
    addMetadataEntry();

    // First attempt: metadata is removed, then the MR deregistration fails.
    g_fake_dereg_failures_remaining = 1;
    EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false), ERR_CONTEXT);
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_, buffer_))
        << "failed deregistration must keep the MR so a retry can find it";
    EXPECT_EQ(g_fake_dereg_calls.load(), 1);

    // Retry: the metadata entry is already gone, but the pending record lets
    // the cleanup reach the live MR. Confirmed cleanup reports success so the
    // engine erases its region record and the range can be registered again.
    EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false), 0);
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_, buffer_));
    EXPECT_EQ(g_fake_dereg_calls.load(), 2);

    // A clean address keeps reporting NOT_REGISTERED without touching verbs.
    EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_EQ(g_fake_dereg_calls.load(), 2);
}

// While an MR survives, the retry must keep failing: success may only be
// reported once the per-context deregistration has actually succeeded.
TEST_F(RdmaUnregisterRetryTest, RetryKeepsFailingWhileMrSurvives) {
    addMetadataEntry();

    g_fake_dereg_failures_remaining = 2;
    EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false), ERR_CONTEXT);
    // The second call must not be classified as "already unregistered" just
    // because the metadata entry is gone.
    EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false), ERR_CONTEXT);
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_, buffer_));

    EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false), 0);
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_, buffer_));
    EXPECT_EQ(g_fake_dereg_calls.load(), 3);
}

// An address that was never a registration base (e.g. an interior pointer)
// has no metadata entry either, but the fall-through must not touch MRs: a
// containing-range lookup would deregister the live base registration while
// its metadata keeps advertising the range.
TEST_F(RdmaUnregisterRetryTest,
       InteriorAddressUnregisterLeavesRegistrationIntact) {
    addMetadataEntry();

    EXPECT_EQ(transport_->unregisterLocalMemory(
                  static_cast<char*>(buffer_) + 4096, false),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_EQ(g_fake_dereg_calls.load(), 0)
        << "an interior address must not reach the MR deregistration";
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_, buffer_));

    // The original registration is fully intact: unregistering the base still
    // deregisters exactly once and succeeds.
    EXPECT_EQ(transport_->unregisterLocalMemory(buffer_, false), 0);
    EXPECT_EQ(g_fake_dereg_calls.load(), 1);
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_, buffer_));
}

// End to end through the TransferEngine API: a failed deregistration keeps
// the engine's region record (re-registration reports OVERLAPPED), and the
// confirmed-cleanup retry must reconcile that record so the same range can
// be registered again.
TEST_F(RdmaUnregisterRetryTest, RetryThenReregisterThroughEngineApi) {
    addMetadataEntry();
    TransferEngineImpl engine;
    // init() creates multi_transports_; replaceTransports below derefs it.
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:17945"), 0);
    TransferEngineImplTestPeer::replaceTransports(engine,
                                                  std::move(transport_));
    TransferEngineImplTestPeer::seedLocalMemoryRegion(engine, buffer_, kLength);

    // First attempt fails at ibv_dereg_mr: the engine keeps its record, so
    // re-registering the same range is rejected as overlapped.
    g_fake_dereg_failures_remaining = 1;
    EXPECT_EQ(engine.unregisterLocalMemory(buffer_, false), ERR_CONTEXT);
    EXPECT_EQ(engine.registerLocalMemory(buffer_, kLength, "cpu", true, false),
              ERR_ADDRESS_OVERLAPPED);

    // Retry: the pending cleanup finishes, the transport reports success, and
    // the engine erases its record — registration is no longer blocked. (The
    // register itself may still fail without RDMA hardware; only the absence
    // of OVERLAPPED is asserted.)
    EXPECT_EQ(engine.unregisterLocalMemory(buffer_, false), 0);
    EXPECT_NE(engine.registerLocalMemory(buffer_, kLength, "cpu", true, false),
              ERR_ADDRESS_OVERLAPPED);
}

// Batch retry after a partial failure: the member whose deregistration
// failed finishes through its pending record, while the already-cleaned
// sibling reports NOT_REGISTERED. The batch must treat that as "nothing
// remains" (aligned with the single-address confirmed-cleanup path), or the
// engine would keep both region records stuck forever.
TEST_F(RdmaUnregisterRetryTest, BatchRetryIgnoresAlreadyCleanedSibling) {
    // Second registered buffer — the sibling that succeeds on the first
    // attempt.
    void* sibling = mmap(nullptr, kLength, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(sibling, MAP_FAILED);
    ibv_mr sibling_mr{};
    sibling_mr.addr = sibling;
    sibling_mr.length = kLength;
    RdmaContextTestPeer::seedMemoryRegion(*context_, sibling, kLength,
                                          &sibling_mr);
    TransferMetadata::BufferDesc sibling_desc{};
    sibling_desc.name = "local-rdma-segment";
    sibling_desc.addr = reinterpret_cast<uint64_t>(sibling);
    sibling_desc.length = kLength;
    ASSERT_EQ(metadata_->addLocalMemoryBuffer(sibling_desc, false), 0);
    addMetadataEntry();

    TransferEngineImpl engine;
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:17946"), 0);
    TransferEngineImplTestPeer::replaceTransports(engine,
                                                  std::move(transport_));
    TransferEngineImplTestPeer::seedLocalMemoryRegion(engine, buffer_, kLength);
    TransferEngineImplTestPeer::seedLocalMemoryRegion(engine, sibling, kLength);

    // First batch attempt: exactly one MR deregistration fails (which member
    // fails is nondeterministic — the batch runs one worker per buffer), the
    // sibling is fully cleaned. The engine keeps both records.
    g_fake_dereg_failures_remaining = 1;
    EXPECT_EQ(engine.unregisterLocalMemoryBatch({buffer_, sibling}),
              ERR_CONTEXT);
    EXPECT_EQ(engine.registerLocalMemory(buffer_, kLength, "cpu", true, false),
              ERR_ADDRESS_OVERLAPPED);
    EXPECT_EQ(engine.registerLocalMemory(sibling, kLength, "cpu", true, false),
              ERR_ADDRESS_OVERLAPPED);

    // Retry: the failed member finishes through its pending record; the
    // sibling's NOT_REGISTERED must not fail the batch, so the engine erases
    // both records and both ranges can be registered again.
    EXPECT_EQ(engine.unregisterLocalMemoryBatch({buffer_, sibling}), 0);
    EXPECT_EQ(g_fake_dereg_calls.load(), 3)
        << "the retry must reach only the pending member's MR";
    EXPECT_NE(engine.registerLocalMemory(buffer_, kLength, "cpu", true, false),
              ERR_ADDRESS_OVERLAPPED);
    EXPECT_NE(engine.registerLocalMemory(sibling, kLength, "cpu", true, false),
              ERR_ADDRESS_OVERLAPPED);

    munmap(sibling, kLength);
}

// When every transport reports NOT_REGISTERED the address was never
// registered: the engine keeps the NOT_REGISTERED contract (and never
// touches verbs), instead of normalizing it to success.
TEST_F(RdmaUnregisterRetryTest, NeverRegisteredKeepsNotRegisteredContract) {
    // buffer_ has a seeded MR but no metadata entry: as far as the engine
    // and the metadata are concerned it was never registered.
    TransferEngineImpl engine;
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:17950"), 0);
    TransferEngineImplTestPeer::replaceTransports(engine,
                                                  std::move(transport_));

    EXPECT_EQ(engine.unregisterLocalMemory(buffer_, false),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_EQ(engine.unregisterLocalMemoryBatch({buffer_}),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_EQ(g_fake_dereg_calls.load(), 0)
        << "a never-registered address must not reach the MR deregistration";
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_, buffer_));

    // Release the seeded MR so TearDown's invariant holds.
    addMetadataEntry();
    EXPECT_EQ(engine.unregisterLocalMemory(buffer_, false), 0);
}

}  // namespace
