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

// HugeTLB narrow-first registration, exercised without RDMA hardware.
//
// RdmaTransport::registerLocalMemoryInternal tries the caller's range first and
// widens to the enclosing huge pages only when ibv_reg_mr() rejects it with
// EINVAL (the kernel refuses to split a hugetlb VMA for the MADV_DONTFORK that
// ibv_fork_init installs). This binary emulates that rejection by interposing
// libibverbs registration directly: any range that is not whole-hugepage
// aligned fails with EINVAL, exactly as a fork-protected hugetlb VMA behaves.
//
// A real HugeTLB mapping is still required, because the transport detects the
// huge page size from /proc/self/smaps. The test self-skips when two 2 MiB
// hugepages cannot be mapped.

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
#include <optional>
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

// Kept at namespace scope, like the interposed verbs functions below: an
// anonymous namespace would give those functions internal linkage and stop
// them from interposing the library's symbols.
constexpr size_t kHugepageSize = 2ull << 20;

// MAP_HUGE_2MB, written out so the value does not depend on <linux/mman.h>
// being pulled in ahead of <sys/mman.h>.
#ifndef MAP_HUGE_SHIFT
#define MAP_HUGE_SHIFT 26
#endif
constexpr int kHuge2MBFlag = 21 << MAP_HUGE_SHIFT;

// Emulated hugetlb behavior: 0 disables it (not used here).
std::atomic<size_t> g_fake_page_size{0};
// Misaligned registrations that are allowed to succeed before EINVAL starts
// being reported. One token models "the first context accepted, the second
// rejected" for a multi-context partial failure.
std::atomic<int> g_fake_misaligned_successes{0};
std::atomic<int> g_fake_reg_calls{0};
std::atomic<int> g_fake_dereg_calls{0};
std::atomic<int> g_fake_dereg_failures_remaining{0};
// Targeted registration failure: the next N registrations of exactly this
// address fail with a non-EINVAL error. Used to fail a chunked registration
// part-way through, so its committed chunks have to be rolled back.
std::atomic<void *> g_fake_reg_fail_addr{nullptr};
std::atomic<int> g_fake_reg_failures_remaining{0};
std::atomic<uint32_t> g_fake_next_key{1};

std::mutex g_fake_mr_mutex;
std::set<ibv_mr *> g_fake_live_mrs;

extern "C" struct ibv_mr *ibv_reg_mr(struct ibv_pd *, void *addr, size_t length,
                                     int) {
    ++g_fake_reg_calls;
    if (addr == g_fake_reg_fail_addr.load()) {
        int remaining = g_fake_reg_failures_remaining.load();
        while (remaining > 0 &&
               !g_fake_reg_failures_remaining.compare_exchange_weak(
                   remaining, remaining - 1)) {
        }
        if (remaining > 0) {
            errno = EIO;
            return nullptr;
        }
    }
    const size_t page = g_fake_page_size.load();
    const bool misaligned =
        page != 0 && (((uintptr_t)addr % page) != 0 || (length % page) != 0);
    if (misaligned) {
        int remaining = g_fake_misaligned_successes.load();
        while (remaining > 0 &&
               !g_fake_misaligned_successes.compare_exchange_weak(
                   remaining, remaining - 1)) {
        }
        if (remaining <= 0) {
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
    int remaining = g_fake_dereg_failures_remaining.load();
    while (remaining > 0 &&
           !g_fake_dereg_failures_remaining.compare_exchange_weak(
               remaining, remaining - 1)) {
    }
    if (remaining > 0) return EIO;
    {
        std::lock_guard<std::mutex> lock(g_fake_mr_mutex);
        g_fake_live_mrs.erase(mr);
    }
    delete mr;
    return 0;
}

namespace {

constexpr const char *kSegmentName = "local-rdma-segment";

// In-memory MetadataStoragePlugin with an armable publish failure.
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

   private:
    std::mutex mutex_;
    std::map<std::string, Json::Value> store_;
    std::atomic<int> fail_sets_remaining_{0};
};

class TestableTransferMetadata : public TransferMetadata {
   public:
    explicit TestableTransferMetadata(
        std::shared_ptr<MetadataStoragePlugin> storage_plugin)
        : TransferMetadata(std::move(storage_plugin)) {}
};

// A hugepage-aligned mapping of two 2 MiB pages, or nullptr when unavailable.
void *mapHugepages(size_t length) {
    void *ptr =
        mmap(nullptr, length, PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | kHuge2MBFlag, -1, 0);
    return ptr == MAP_FAILED ? nullptr : ptr;
}

}  // namespace

class RdmaHugetlbSubviewFakeMrTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Two pages: the widened-and-chunked case needs a 4 MiB
        // hugepage-aligned range. Self-skip when the host has no reserved 2 MiB
        // pages.
        huge_ = mapHugepages(2 * kHugepageSize);
        if (huge_ == nullptr) {
            GTEST_SKIP() << "cannot map two 2 MiB huge pages (no hugetlb pages "
                            "reserved?)";
        }
        // From here on every misaligned registration behaves like a
        // fork-protected hugetlb VMA.
        g_fake_page_size = kHugepageSize;
        g_fake_misaligned_successes = 0;
        g_fake_reg_calls = 0;
        g_fake_dereg_calls = 0;
        g_fake_dereg_failures_remaining = 0;
        g_fake_reg_fail_addr = nullptr;
        g_fake_reg_failures_remaining = 0;

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
        context_a_ = makeContext("device-a", &pd_a_);
        context_b_ = makeContext("device-b", &pd_b_);

        saved_max_mr_size_ = globalConfig().max_mr_size;
        saved_parallel_reg_mr_ = globalConfig().parallel_reg_mr;
        // Register and unregister the same way regardless of the environment:
        // with two contexts MC_ENABLE_PARALLEL_REG_MR would otherwise change
        // how many contexts are attempted (and therefore the per-context call
        // counts) and which context a fake failure token lands on.
        globalConfig().parallel_reg_mr = 0;
    }

    void TearDown() override {
        globalConfig().max_mr_size = saved_max_mr_size_;
        globalConfig().parallel_reg_mr = saved_parallel_reg_mr_;
        transport_.reset();
        context_a_.reset();
        context_b_.reset();
        {
            std::lock_guard<std::mutex> lock(g_fake_mr_mutex);
            for (ibv_mr *mr : g_fake_live_mrs) delete mr;
            g_fake_live_mrs.clear();
        }
        g_fake_page_size = 0;
        if (huge_ != nullptr) munmap(huge_, 2 * kHugepageSize);
    }

    std::shared_ptr<RdmaContext> makeContext(const std::string &name,
                                             ibv_pd *pd) {
        auto context = std::make_shared<RdmaContext>(*transport_, name);
        // A non-null PD makes registerMemoryRegion() reach the interposed
        // ibv_reg_mr instead of taking the placeholder-context early return.
        RdmaContextTestPeer::bindProtectionDomain(*context, pd);
        RdmaTransportTestPeer::addContext(*transport_, context);
        return context;
    }

    char *base() { return static_cast<char *>(huge_); }

    int registerRange(void *addr, size_t length, bool update_metadata = true) {
        return transport_->registerLocalMemory(addr, length, "cpu:0", true,
                                               update_metadata);
    }

    bool hasLocalBuffer(void *addr) {
        auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (!desc) return false;
        for (const auto &buf : desc->buffers)
            if (buf.addr == reinterpret_cast<uint64_t>(addr)) return true;
        return false;
    }

    std::optional<uint64_t> widenAliasBase(void *addr) {
        return RdmaTransportTestPeer::widenAliasBase(
            *transport_, reinterpret_cast<uint64_t>(addr));
    }

    bool isClaimed(void *addr) {
        return RdmaTransportTestPeer::isHugepageClaimed(
            *transport_, reinterpret_cast<uint64_t>(addr));
    }

    bool hasPendingDereg(void *addr) {
        return RdmaTransportTestPeer::hasPendingDereg(
            *transport_, reinterpret_cast<uint64_t>(addr));
    }

    void *huge_ = nullptr;
    ibv_pd pd_a_{};
    ibv_pd pd_b_{};
    std::shared_ptr<FakeMetadataStoragePlugin> plugin_;
    std::shared_ptr<TransferMetadata> metadata_;
    std::unique_ptr<RdmaTransport> transport_;
    std::shared_ptr<RdmaContext> context_a_;
    std::shared_ptr<RdmaContext> context_b_;
    uint64_t saved_max_mr_size_ = 0;
    int saved_parallel_reg_mr_ = 0;
};

// A misaligned sub-view is first rejected with EINVAL and then registered over
// the enclosing huge page. The MR, the alias and the claim all live at the page
// base; the published BufferDesc stays at the caller's range.
TEST_F(RdmaHugetlbSubviewFakeMrTest, MisalignedSubviewWidensOnEinvl) {
    char *sub = base() + 4096;
    const size_t len = 4096;

    ASSERT_EQ(registerRange(sub, len), 0);
    // One MR per context: the narrow attempt is rejected on the first context,
    // then the widened retry registers on both.
    EXPECT_EQ(g_fake_reg_calls.load(), 3)
        << "one rejected narrow attempt, then one widened MR per context";
    EXPECT_EQ(widenAliasBase(sub),
              static_cast<uint64_t>(reinterpret_cast<uintptr_t>(base())));
    EXPECT_TRUE(isClaimed(base()));
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_a_, base()));
    EXPECT_TRUE(hasLocalBuffer(sub));
    EXPECT_FALSE(hasLocalBuffer(base()))
        << "a widened registration advertises the caller's range only";

    EXPECT_EQ(transport_->unregisterLocalMemory(sub), 0);
    EXPECT_EQ(widenAliasBase(sub), std::nullopt);
    EXPECT_FALSE(isClaimed(base()));
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_a_, base()));
    EXPECT_FALSE(hasLocalBuffer(sub));
    EXPECT_EQ(g_fake_dereg_calls.load(), 2)
        << "the widened MR is released on every context";
}

// Two disjoint sub-views of one huge page: while the first is widened and live,
// the second would widen onto the same MR base, so it is refused. Once the
// first is released the second registers normally.
TEST_F(RdmaHugetlbSubviewFakeMrTest,
       DisjointSubviewRefusedWhileFirstHugepageLive) {
    char *first = base() + 4096;
    char *second = base() + 8192;

    ASSERT_EQ(registerRange(first, 4096), 0);
    ASSERT_EQ(g_fake_reg_calls.load(), 3)
        << "one rejected narrow attempt, then one widened MR per context";
    EXPECT_EQ(registerRange(second, 4096), ERR_ADDRESS_OVERLAPPED);
    EXPECT_EQ(g_fake_reg_calls.load(), 3)
        << "the overlap is detected before any registration attempt";
    EXPECT_FALSE(hasLocalBuffer(second));

    ASSERT_EQ(transport_->unregisterLocalMemory(first), 0);
    ASSERT_EQ(registerRange(second, 4096), 0);
    EXPECT_TRUE(hasLocalBuffer(second));
    EXPECT_EQ(transport_->unregisterLocalMemory(second), 0);
}

// A registration that already covers whole huge pages needs no widening: it
// creates no alias and holds a claim over its own range.
TEST_F(RdmaHugetlbSubviewFakeMrTest, AlignedRegistrationCreatesNoAlias) {
    ASSERT_EQ(registerRange(base(), kHugepageSize), 0);
    EXPECT_EQ(g_fake_reg_calls.load(), 2)
        << "one MR per context, no rejected narrow attempt and no retry";
    EXPECT_EQ(widenAliasBase(base()), std::nullopt);
    EXPECT_TRUE(isClaimed(base()));
    EXPECT_TRUE(hasLocalBuffer(base()));

    EXPECT_EQ(transport_->unregisterLocalMemory(base()), 0);
    EXPECT_FALSE(isClaimed(base()));
}

// A multi-context narrow attempt can fail after another context already
// registered its MR. That partial MR is deregistered before the widened retry,
// so the retry does not leak it or collide on its base.
TEST_F(RdmaHugetlbSubviewFakeMrTest,
       PartialNarrowFailureRolledBackBeforeWidenRetry) {
    char *sub = base() + 4096;
    // The first context's misaligned registration succeeds; the second's is
    // rejected, which is what makes the narrow attempt fail part-way.
    g_fake_misaligned_successes = 1;

    ASSERT_EQ(registerRange(sub, 4096), 0);
    EXPECT_EQ(g_fake_reg_calls.load(), 4)
        << "two narrow attempts, then two widened ones";
    EXPECT_EQ(g_fake_dereg_calls.load(), 1)
        << "the surviving narrow MR must be released before the retry";
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_a_, base()));
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_b_, base()));
    EXPECT_EQ(widenAliasBase(sub),
              static_cast<uint64_t>(reinterpret_cast<uintptr_t>(base())));

    EXPECT_EQ(transport_->unregisterLocalMemory(sub), 0);
    EXPECT_EQ(g_fake_dereg_calls.load(), 3);
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_a_, base()));
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_b_, base()));
}

// If the partial narrow MR cannot be rolled back, the widened retry is refused
// (its base is still owned) and the surviving MR stays reachable through the
// pending-dereg record, so a later unregister can still release it.
TEST_F(RdmaHugetlbSubviewFakeMrTest,
       RetainedNarrowMrBlocksRetryAndStaysCleanable) {
    char *sub = base() + 4096;
    g_fake_misaligned_successes = 1;
    g_fake_dereg_failures_remaining = 1;

    EXPECT_EQ(registerRange(sub, 4096), ERR_CONTEXT);
    EXPECT_EQ(g_fake_reg_calls.load(), 2)
        << "no widened retry while a narrowed MR is retained";
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_a_, sub));
    EXPECT_TRUE(isClaimed(sub));
    EXPECT_EQ(widenAliasBase(sub), std::nullopt);
    EXPECT_FALSE(hasLocalBuffer(sub));
    EXPECT_TRUE(hasPendingDereg(sub));

    // The orphan bookkeeping makes the survivor reachable from the caller's
    // address even though it has no metadata entry.
    EXPECT_EQ(transport_->unregisterLocalMemory(sub, false), 0);
    EXPECT_EQ(g_fake_dereg_calls.load(), 2);
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_a_, sub));
    EXPECT_FALSE(isClaimed(sub));
    EXPECT_FALSE(hasPendingDereg(sub));
}

// A sub-view whose widened form exceeds the chunk limit is refused with the
// error from the caller's own range (ERR_CONTEXT), not a layout error: before
// widening existed the same request simply failed with EINVAL.
TEST_F(RdmaHugetlbSubviewFakeMrTest, WidenedChunkedReportsFirstError) {
    globalConfig().max_mr_size = kHugepageSize;
    char *sub = base() + 4096;
    const size_t len = 3 * 1024 * 1024;  // widens to the full 4 MiB

    EXPECT_EQ(registerRange(sub, len), ERR_CONTEXT);
    EXPECT_EQ(g_fake_reg_calls.load(), 1)
        << "the misaligned first chunk fails before any widened attempt";
    EXPECT_FALSE(isClaimed(base()));
    EXPECT_EQ(widenAliasBase(sub), std::nullopt);
    EXPECT_FALSE(hasLocalBuffer(sub));
}

// The reviewer's sequence on a hugepage-backed sub-view: a failed removal
// publish leaves pending_dereg_, the retry completes the MR cleanup and then
// leaves pending_republish_, a new view registers at the same page base, and
// finishing the old cleanup must not deregister the new view's MR.
TEST_F(RdmaHugetlbSubviewFakeMrTest, MetadataOnlyRetrySparesNewerView) {
    char *a = base() + 4096;
    char *b = base() + 8192;
    ASSERT_EQ(registerRange(a, 4096, /*update_metadata=*/false), 0);
    ASSERT_EQ(widenAliasBase(a),
              static_cast<uint64_t>(reinterpret_cast<uintptr_t>(base())));

    plugin_->failNextSet(1);
    EXPECT_EQ(transport_->unregisterLocalMemory(a, true), ERR_METADATA);
    EXPECT_TRUE(hasPendingDereg(a));
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_a_, base()));

    plugin_->failNextSet(1);
    EXPECT_EQ(transport_->unregisterLocalMemory(a, true), ERR_METADATA);
    EXPECT_FALSE(hasPendingDereg(a));
    EXPECT_TRUE(RdmaTransportTestPeer::hasPendingRepublish(
        *transport_, reinterpret_cast<uint64_t>(a)));
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_a_, base()));
    EXPECT_FALSE(isClaimed(base()));
    EXPECT_EQ(widenAliasBase(a), std::nullopt);

    // A new view of the same huge page registers at the same MR base.
    ASSERT_EQ(registerRange(b, 4096), 0);
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_a_, base()));
    ASSERT_TRUE(isClaimed(base()));
    ASSERT_TRUE(hasLocalBuffer(b));
    const int dereg_after_b = g_fake_dereg_calls.load();

    EXPECT_EQ(transport_->unregisterLocalMemory(a, true), 0);
    EXPECT_EQ(g_fake_dereg_calls.load(), dereg_after_b)
        << "finishing A's publish must not touch B's live MR";
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_a_, base()));
    EXPECT_TRUE(isClaimed(base()));
    EXPECT_TRUE(hasLocalBuffer(b));

    EXPECT_EQ(transport_->unregisterLocalMemory(b), 0);
    EXPECT_FALSE(isClaimed(base()));
}

// A failed chunked registration that cannot roll back one chunk has to
// supersede a metadata-only record an earlier failed unregister left at the
// same address. Otherwise the next unregister takes the republish branch,
// reports success without touching the surviving MR, and strands its claim.
TEST_F(RdmaHugetlbSubviewFakeMrTest,
       ChunkedOrphanSupersedesStaleRepublishRecord) {
    // An aligned registration of the whole mapping, chunked by max_mr_size.
    globalConfig().max_mr_size = kHugepageSize;
    ASSERT_EQ(registerRange(base(), 2 * kHugepageSize), 0);

    // Leave a metadata-only record: every MR is gone, but the publish fails.
    plugin_->failNextSet(1);
    EXPECT_EQ(transport_->unregisterLocalMemory(base(), true), ERR_METADATA);
    EXPECT_TRUE(RdmaTransportTestPeer::hasPendingRepublish(
        *transport_, reinterpret_cast<uint64_t>(base())));

    // Re-registering the same range fails on its second chunk, and the first
    // chunk's MR cannot be rolled back on the second context.
    g_fake_reg_fail_addr = base() + kHugepageSize;
    g_fake_reg_failures_remaining = 1;
    g_fake_dereg_failures_remaining = 1;
    EXPECT_EQ(registerRange(base(), 2 * kHugepageSize), ERR_CONTEXT);
    EXPECT_TRUE(hasPendingDereg(base()));
    EXPECT_FALSE(RdmaTransportTestPeer::hasPendingRepublish(
        *transport_, reinterpret_cast<uint64_t>(base())))
        << "a surviving MR must never leave the address in the metadata-only "
           "state";
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_a_, base()));
    EXPECT_TRUE(RdmaContextTestPeer::hasMemoryRegion(*context_b_, base()));

    // The orphaned chunk MRs stay reachable: unregistering the caller's
    // address finishes the chunked cleanup instead of returning success.
    EXPECT_EQ(transport_->unregisterLocalMemory(base(), true), 0);
    EXPECT_FALSE(isClaimed(base()));
    EXPECT_FALSE(hasPendingDereg(base()));
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_a_, base()));
    EXPECT_FALSE(RdmaContextTestPeer::hasMemoryRegion(*context_b_, base()));
}
