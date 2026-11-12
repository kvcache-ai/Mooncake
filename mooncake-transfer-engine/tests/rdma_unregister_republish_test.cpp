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

// Regression tests for the two pending-cleanup states of
// RdmaTransport::unregisterLocalMemoryInternal.
//
// A failed unregister leaves either pending_dereg_ (the MRs may still be live,
// so a retry must re-enter the per-context MR cleanup) or pending_republish_
// (the MRs are confirmed gone and only the final updateLocalSegmentDesc() is
// owed). While the publish is owed, a disjoint or overlapping view of the same
// range may register. A retry of the old address must then never look up an MR:
// RdmaContext::unregisterMemoryRegion() resolves an address by containing
// range, so it would deregister the newer view's live MR (and madvise
// MADV_DOFORK over it).
//
// No RDMA device, metadata server or reserved hugepages are required: this
// binary interposes the libibverbs registration boundary (same mechanism as
// rdma_unregister_retry_test.cpp) and injects an in-memory metadata storage
// plugin whose set() can be armed to fail.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <infiniband/verbs.h>
#include <sys/mman.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "config.h"
#include "error.h"
#include "rdma_test_peers.h"
#include "transfer_metadata.h"
#include "transfer_metadata_plugin.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"

using namespace mooncake;

// Interpose the libibverbs boundary for this test binary. rdma-core exports
// ibv_reg_mr/ibv_dereg_mr, so these definitions take precedence. Atomics: the
// batch paths run one worker thread per buffer.
std::atomic<int> g_fake_reg_calls{0};
std::atomic<int> g_fake_dereg_calls{0};
std::atomic<int> g_fake_dereg_failures_remaining{0};
std::atomic<int> g_fake_einval_failures_remaining{0};
std::atomic<void *> g_fake_einval_addr{nullptr};
std::atomic<uint32_t> g_fake_next_key{1};

// Fake MRs are tracked so a test that leaves a registration live does not leak
// them (and TearDown can release whatever is left after the transport is gone).
std::mutex g_fake_mr_mutex;
std::set<ibv_mr *> g_fake_live_mrs;

extern "C" struct ibv_mr *ibv_reg_mr(struct ibv_pd *, void *addr, size_t length,
                                     int) {
    ++g_fake_reg_calls;
    // Optional one-shot EINVAL at a chosen address, for the translation test.
    if (addr == g_fake_einval_addr.load()) {
        int remaining = g_fake_einval_failures_remaining.load();
        while (remaining > 0 &&
               !g_fake_einval_failures_remaining.compare_exchange_weak(
                   remaining, remaining - 1)) {
        }
        if (remaining > 0) {
            errno = EINVAL;
            return nullptr;
        }
    }
    auto *mr = new ibv_mr{};
    mr->addr = addr;
    mr->length = length;
    mr->lkey = g_fake_next_key.fetch_add(1);
    mr->rkey = g_fake_next_key.fetch_add(1);
    {
        std::lock_guard<std::mutex> lock(g_fake_mr_mutex);
        g_fake_live_mrs.insert(mr);
    }
    return mr;
}

extern "C" int ibv_dereg_mr(struct ibv_mr *mr) {
    ++g_fake_dereg_calls;
    // Claim a failure token with a CAS loop: batch paths call this from one
    // worker thread per buffer, and a check-then-decrement race would hand one
    // token to two callers.
    int remaining = g_fake_dereg_failures_remaining.load();
    while (remaining > 0 &&
           !g_fake_dereg_failures_remaining.compare_exchange_weak(
               remaining, remaining - 1)) {
    }
    if (remaining > 0) return EIO;  // the MR stays live, as with rdma-core
    {
        std::lock_guard<std::mutex> lock(g_fake_mr_mutex);
        g_fake_live_mrs.erase(mr);
    }
    delete mr;
    return 0;
}

namespace mooncake {

namespace {

constexpr const char *kSegmentName = "local-rdma-segment";
constexpr size_t kRegionLen = 256 * 1024;
constexpr size_t kLen = 64 * 1024;
constexpr size_t kPage = 4096;

// In-memory MetadataStoragePlugin with an armable publish failure. The P2P
// handshake mode cannot fail updateLocalSegmentDesc(), so the publish-failure
// state machine needs this seam. Unlike the fake in transfer_metadata_test.cpp,
// set() reports a configurable number of failures before storing.
class FakeMetadataStoragePlugin : public MetadataStoragePlugin {
   public:
    bool get(const std::string &key, Json::Value &value) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = store_.find(key);
        if (it == store_.end()) return false;
        value = it->second;
        return true;
    }

    bool set(const std::string &key, const Json::Value &value) override {
        ++set_calls_;
        int remaining = fail_sets_remaining_.load();
        while (remaining > 0 && !fail_sets_remaining_.compare_exchange_weak(
                                    remaining, remaining - 1)) {
        }
        if (remaining > 0) return false;
        std::lock_guard<std::mutex> lock(mutex_);
        store_[key] = value;
        return true;
    }

    bool remove(const std::string &key) override {
        std::lock_guard<std::mutex> lock(mutex_);
        store_.erase(key);
        return true;
    }

    void failNextSet(int n) { fail_sets_remaining_ = n; }

    // Number of publish attempts (successful or not).
    int setCalls() const { return set_calls_.load(); }

   private:
    std::mutex mutex_;
    std::map<std::string, Json::Value> store_;
    std::atomic<int> fail_sets_remaining_{0};
    std::atomic<int> set_calls_{0};
};

// Exposes the protected storage-plugin injection seam.
class TestableTransferMetadata : public TransferMetadata {
   public:
    explicit TestableTransferMetadata(
        std::shared_ptr<MetadataStoragePlugin> storage_plugin)
        : TransferMetadata(std::move(storage_plugin)) {}
};

}  // namespace

class RdmaUnregisterRepublishTest : public ::testing::Test {
   protected:
    void SetUp() override {
        region_ = mmap(nullptr, kRegionLen, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(region_, MAP_FAILED);

        plugin_ = std::make_shared<FakeMetadataStoragePlugin>();
        metadata_ = std::make_shared<TestableTransferMetadata>(plugin_);
        auto local_desc = std::make_shared<TransferMetadata::SegmentDesc>();
        local_desc->name = kSegmentName;
        local_desc->protocol = "rdma";
        ASSERT_EQ(metadata_->addLocalSegment(LOCAL_SEGMENT_ID, kSegmentName,
                                             std::move(local_desc)),
                  0);

        transport_ = std::make_unique<RdmaTransport>();
        RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                            kSegmentName);
        context_ =
            std::make_shared<RdmaContext>(*transport_, "nonexistent-device");
        // A non-null PD makes registerMemoryRegion() reach the interposed
        // ibv_reg_mr instead of taking the placeholder-context early return.
        RdmaContextTestPeer::bindProtectionDomain(*context_, &pd_);
        RdmaTransportTestPeer::addContext(*transport_, context_);

        saved_max_mr_size_ = globalConfig().max_mr_size;
        g_fake_reg_calls = 0;
        g_fake_dereg_calls = 0;
        g_fake_dereg_failures_remaining = 0;
        g_fake_einval_failures_remaining = 0;
        g_fake_einval_addr = nullptr;
    }

    void TearDown() override {
        globalConfig().max_mr_size = saved_max_mr_size_;
        transport_.reset();
        context_.reset();
        // Release anything a failed assertion left live. The transport and its
        // context are gone, so nothing can still reach these MRs.
        {
            std::lock_guard<std::mutex> lock(g_fake_mr_mutex);
            for (ibv_mr *mr : g_fake_live_mrs) delete mr;
            g_fake_live_mrs.clear();
        }
        if (region_ != MAP_FAILED) munmap(region_, kRegionLen);
    }

    char *region() { return static_cast<char *>(region_); }
    // A's base sits above the region base; B (a later registration) starts at
    // the region base and covers A, so a containing-range MR lookup for A
    // resolves to B's live MR.
    char *addrA() { return region() + 4 * kPage; }
    char *addrB() { return region(); }

    int registerRange(void *addr, size_t length, bool publish = false) {
        return transport_->registerLocalMemory(addr, length, "cpu:0", true,
                                               publish);
    }

    bool hasLocalBuffer(void *addr) {
        auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (!desc) return false;
        for (const auto &buf : desc->buffers)
            if (buf.addr == reinterpret_cast<uint64_t>(addr)) return true;
        return false;
    }

    bool hasPendingDereg(void *addr) {
        return RdmaTransportTestPeer::hasPendingDereg(
            *transport_, reinterpret_cast<uint64_t>(addr));
    }

    bool hasPendingRepublish(void *addr) {
        return RdmaTransportTestPeer::hasPendingRepublish(
            *transport_, reinterpret_cast<uint64_t>(addr));
    }

    void *region_ = MAP_FAILED;
    ibv_pd pd_{};
    std::shared_ptr<FakeMetadataStoragePlugin> plugin_;
    std::shared_ptr<TransferMetadata> metadata_;
    std::unique_ptr<RdmaTransport> transport_;
    std::shared_ptr<RdmaContext> context_;
    uint64_t saved_max_mr_size_ = 0;
};

// Single buffer: the first failure happens while the MRs are still live, the
// retry reaches the MR cleanup and then fails only at the publish. The retry in
// that second state must be metadata-only: B (registered in between) keeps its
// MR and its metadata entry.
TEST_F(RdmaUnregisterRepublishTest,
       SingleBufferPublishFailureSplitsIntoMetadataOnlyRetry) {
    ASSERT_EQ(registerRange(addrA(), kLen), 0);
    ASSERT_EQ(g_fake_reg_calls.load(), 1);
    ASSERT_TRUE(hasLocalBuffer(addrA()));

    // 1) Removing the local metadata entry succeeds, the publish fails: the
    //    MR is still live, so this is pending_dereg_.
    plugin_->failNextSet(1);
    EXPECT_EQ(transport_->unregisterLocalMemory(addrA(), true), ERR_METADATA);
    EXPECT_TRUE(hasPendingDereg(addrA()));
    EXPECT_FALSE(hasPendingRepublish(addrA()));
    EXPECT_EQ(g_fake_dereg_calls.load(), 0);
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_, addrA()));

    // 2) Retry: the MR is deregistered, but the publish still fails. Now only
    //    the publish is owed; the address must never reach an MR lookup again.
    plugin_->failNextSet(1);
    EXPECT_EQ(transport_->unregisterLocalMemory(addrA(), true), ERR_METADATA);
    EXPECT_FALSE(hasPendingDereg(addrA()));
    EXPECT_TRUE(hasPendingRepublish(addrA()));
    EXPECT_EQ(g_fake_dereg_calls.load(), 1);
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_, addrA()));

    // 3) A newer registration covers the old address.
    ASSERT_EQ(registerRange(addrB(), kRegionLen), 0);
    ASSERT_TRUE(hasLocalBuffer(addrB()));
    const int dereg_after_b = g_fake_dereg_calls.load();

    // 4) Finishing A's cleanup publishes only. With the old single pending
    //    state this re-entered the MR cleanup, and
    //    findMemoryRegionContaining(A) resolved to B's live MR.
    EXPECT_EQ(transport_->unregisterLocalMemory(addrA(), true), 0);
    EXPECT_EQ(g_fake_dereg_calls.load(), dereg_after_b)
        << "a metadata-only retry must not reach the verbs";
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_, addrB()));
    EXPECT_TRUE(hasLocalBuffer(addrB()))
        << "the newer registration's metadata entry must survive";
    EXPECT_FALSE(hasPendingDereg(addrA()));
    EXPECT_FALSE(hasPendingRepublish(addrA()));

    EXPECT_EQ(transport_->unregisterLocalMemory(addrB(), false), 0);
}

// Chunked buffer: every chunk MR is gone before the single publish is
// attempted, so the failure state is metadata-only from the start. The retry
// must not walk the chunk or MR bookkeeping again.
TEST_F(RdmaUnregisterRepublishTest, ChunkedPublishFailureIsMetadataOnly) {
    const size_t chunk = 64 * 1024;
    char *a = region() + 4 * kPage;
    const size_t a_len = 2 * chunk;
    globalConfig().max_mr_size = chunk;

    ASSERT_EQ(registerRange(a, a_len), 0);
    globalConfig().max_mr_size = saved_max_mr_size_;
    ASSERT_EQ(g_fake_reg_calls.load(), 2) << "expected two chunk MRs";
    ASSERT_TRUE(hasLocalBuffer(a));
    ASSERT_TRUE(hasLocalBuffer(a + chunk));

    plugin_->failNextSet(1);
    EXPECT_EQ(transport_->unregisterLocalMemory(a, true), ERR_METADATA);
    EXPECT_TRUE(hasPendingRepublish(a));
    EXPECT_FALSE(hasPendingDereg(a));
    EXPECT_EQ(g_fake_dereg_calls.load(), 2);
    EXPECT_FALSE(hasLocalBuffer(a));
    EXPECT_FALSE(hasLocalBuffer(a + chunk));

    ASSERT_EQ(registerRange(addrB(), kRegionLen), 0);
    const int dereg_after_b = g_fake_dereg_calls.load();

    EXPECT_EQ(transport_->unregisterLocalMemory(a, true), 0);
    EXPECT_EQ(g_fake_dereg_calls.load(), dereg_after_b);
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_, addrB()));
    EXPECT_TRUE(hasLocalBuffer(addrB()));

    EXPECT_EQ(transport_->unregisterLocalMemory(addrB(), false), 0);
}

// The reviewer's exact sequence on the batch path: a member that finished all
// per-buffer cleanup defers its publish to the batch; when that publish fails
// the member must be recorded as pending_republish_, never pending_dereg_.
TEST_F(RdmaUnregisterRepublishTest,
       BatchPublishFailureLeavesRepublishNotDereg) {
    ASSERT_EQ(registerRange(addrA(), kLen), 0);
    ASSERT_EQ(g_fake_dereg_calls.load(), 0);

    plugin_->failNextSet(1);
    EXPECT_EQ(transport_->unregisterLocalMemoryBatch({addrA()}), ERR_METADATA);
    EXPECT_TRUE(hasPendingRepublish(addrA()))
        << "a cleaned batch member owes only the collective publish";
    EXPECT_FALSE(hasPendingDereg(addrA()));
    EXPECT_EQ(g_fake_dereg_calls.load(), 1);

    ASSERT_EQ(registerRange(addrB(), kRegionLen), 0);
    const int dereg_after_b = g_fake_dereg_calls.load();

    EXPECT_EQ(transport_->unregisterLocalMemoryBatch({addrA()}), 0);
    EXPECT_EQ(g_fake_dereg_calls.load(), dereg_after_b)
        << "the retry must not deregister the newer view's MR";
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_, addrB()));
    EXPECT_TRUE(hasLocalBuffer(addrB()));
    EXPECT_FALSE(hasPendingRepublish(addrA()));

    EXPECT_EQ(transport_->unregisterLocalMemory(addrB(), false), 0);
}

// Same batch sequence with a chunked member: the chunk addresses it stored in
// pending_republish_ survive the chunk_map_ erasure, and the retry only removes
// metadata (already gone here) before publishing.
TEST_F(RdmaUnregisterRepublishTest,
       BatchChunkedPublishFailureStaysMetadataOnly) {
    const size_t chunk = 64 * 1024;
    char *a = region() + 4 * kPage;
    globalConfig().max_mr_size = chunk;

    ASSERT_EQ(registerRange(a, 2 * chunk), 0);
    globalConfig().max_mr_size = saved_max_mr_size_;
    ASSERT_EQ(g_fake_reg_calls.load(), 2);

    plugin_->failNextSet(1);
    EXPECT_EQ(transport_->unregisterLocalMemoryBatch({a}), ERR_METADATA);
    EXPECT_TRUE(hasPendingRepublish(a));
    EXPECT_FALSE(hasPendingDereg(a));
    EXPECT_EQ(g_fake_dereg_calls.load(), 2);

    ASSERT_EQ(registerRange(addrB(), kRegionLen), 0);
    const int dereg_after_b = g_fake_dereg_calls.load();

    EXPECT_EQ(transport_->unregisterLocalMemoryBatch({a}), 0);
    EXPECT_EQ(g_fake_dereg_calls.load(), dereg_after_b);
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_, addrB()));
    EXPECT_TRUE(hasLocalBuffer(addrB()));

    EXPECT_EQ(transport_->unregisterLocalMemory(addrB(), false), 0);
}

// Re-registering the address supersedes the half-finished cleanup: the stale
// record is dropped, and a later unregister deregisters the new MR.
TEST_F(RdmaUnregisterRepublishTest, ReregistrationClearsStalePendingState) {
    ASSERT_EQ(registerRange(addrA(), kLen), 0);

    plugin_->failNextSet(1);
    EXPECT_EQ(transport_->unregisterLocalMemory(addrA(), true), ERR_METADATA);
    EXPECT_TRUE(hasPendingDereg(addrA()));

    plugin_->failNextSet(1);
    EXPECT_EQ(transport_->unregisterLocalMemory(addrA(), true), ERR_METADATA);
    EXPECT_TRUE(hasPendingRepublish(addrA()));
    ASSERT_EQ(g_fake_dereg_calls.load(), 1);

    // A successful registration at the same address supersedes the record.
    ASSERT_EQ(registerRange(addrA(), kLen), 0);
    EXPECT_FALSE(hasPendingDereg(addrA()));
    EXPECT_FALSE(hasPendingRepublish(addrA()));
    EXPECT_TRUE(hasLocalBuffer(addrA()));
    const int dereg_before = g_fake_dereg_calls.load();

    EXPECT_EQ(transport_->unregisterLocalMemory(addrA(), false), 0);
    EXPECT_EQ(g_fake_dereg_calls.load(), dereg_before + 1)
        << "the new MR must be deregistered exactly once";
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_, addrA()));
}

// A caller that explicitly owes no publish finishes the metadata-only state
// without touching verbs.
TEST_F(RdmaUnregisterRepublishTest,
       UnpublishableMetadataOnlyRetryTouchesNoVerbs) {
    ASSERT_EQ(registerRange(addrA(), kLen), 0);

    plugin_->failNextSet(1);
    EXPECT_EQ(transport_->unregisterLocalMemory(addrA(), true), ERR_METADATA);
    plugin_->failNextSet(1);
    EXPECT_EQ(transport_->unregisterLocalMemory(addrA(), true), ERR_METADATA);
    ASSERT_TRUE(hasPendingRepublish(addrA()));
    const int dereg_before = g_fake_dereg_calls.load();

    EXPECT_EQ(transport_->unregisterLocalMemory(addrA(), false), 0);
    EXPECT_EQ(g_fake_dereg_calls.load(), dereg_before);
    EXPECT_FALSE(hasPendingDereg(addrA()));
    EXPECT_FALSE(hasPendingRepublish(addrA()));
}

// A non-HugeTLB registration rejected with EINVAL is reported as ERR_CONTEXT
// and is not retried: widening only applies to hugepage-backed host memory.
// The internal EINVAL sentinel must never escape.
TEST_F(RdmaUnregisterRepublishTest, NonHugetlbEinvlIsTranslatedAndNotRetried) {
    void *a = addrA();
    g_fake_einval_addr = a;
    g_fake_einval_failures_remaining = 1;

    const int rc = registerRange(a, kLen);
    EXPECT_EQ(rc, ERR_CONTEXT);
    EXPECT_NE(rc, ERR_CONTEXT_EINVAL);
    EXPECT_EQ(g_fake_reg_calls.load(), 1)
        << "a non-HugeTLB EINVAL must not be retried over any other range";
    EXPECT_FALSE(hasLocalBuffer(a));
    EXPECT_FALSE(hasPendingDereg(a));
    EXPECT_FALSE(hasPendingRepublish(a));

    // Nothing was registered, so an unregister stays a no-op.
    EXPECT_EQ(transport_->unregisterLocalMemory(a, false),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_EQ(g_fake_dereg_calls.load(), 0);
}

// A chunked unregister whose per-context deregistration fails must still
// publish the chunk removals it already made locally; otherwise the metadata
// server keeps advertising MRs that are gone on some contexts. The retained
// chunk_map_ entry keeps the cleanup retryable, so the address must not be
// recorded as metadata-only while an MR may still be live.
TEST_F(RdmaUnregisterRepublishTest, ChunkedDeregFailureStillPublishesRemoval) {
    const size_t chunk = 64 * 1024;
    char *a = region() + 4 * kPage;
    globalConfig().max_mr_size = chunk;

    ASSERT_EQ(registerRange(a, 2 * chunk), 0);
    globalConfig().max_mr_size = saved_max_mr_size_;
    ASSERT_EQ(g_fake_reg_calls.load(), 2);

    const int sets_before = plugin_->setCalls();
    g_fake_dereg_failures_remaining = 1;
    EXPECT_EQ(transport_->unregisterLocalMemory(a, /*update_metadata=*/true),
              ERR_CONTEXT);
    EXPECT_EQ(plugin_->setCalls(), sets_before + 1)
        << "the removals must be published even though a deregistration "
           "failed";
    EXPECT_FALSE(hasLocalBuffer(a));
    EXPECT_FALSE(hasLocalBuffer(a + chunk));
    EXPECT_FALSE(hasPendingRepublish(a))
        << "an MR may still be live: the retry has to re-enter the MR cleanup";
    EXPECT_EQ(g_fake_dereg_calls.load(), 2);

    // The retained chunk_map_ entry finishes the cleanup on retry.
    EXPECT_EQ(transport_->unregisterLocalMemory(a, /*update_metadata=*/true),
              0);
    EXPECT_EQ(g_fake_dereg_calls.load(), 3);
}

}  // namespace mooncake
