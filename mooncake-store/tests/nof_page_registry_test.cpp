#include "nof/page_registry.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cerrno>
#include <cstdint>
#include <map>
#include <set>

namespace mooncake {
namespace {

constexpr uintptr_t kPage = 2ULL << 20;

// Fake SPDK translation table: tracks registration depth per 2MB page and
// can be told to fail pages with -EBUSY (already registered by the DPDK
// memseg walk), -EIO on register before marking anything (genuine failure),
// -EIO on register AFTER marking the page (partial state, mirroring
// spdk_mem_register's mark-then-notify ordering), or -EIO on unregister.
struct FakeTranslationTable {
    int RegisterPage(uintptr_t page) {
        ++register_calls;
        if (busy_pages.count(page)) return -EBUSY;
        if (fail_pages.count(page)) return -EIO;
        ++registered[page];
        // mark_fail_pages stay registered in the fake table despite the
        // failure — the partial state the registry must clean up.
        if (mark_fail_pages.count(page)) return -EIO;
        return 0;
    }

    int UnregisterPage(uintptr_t page) {
        ++unregister_calls;
        if (fail_unregister_pages.count(page)) return -EIO;
        auto it = registered.find(page);
        if (it == registered.end() || it->second == 0) return -EINVAL;
        --it->second;
        return 0;
    }

    int Depth(uintptr_t page) const {
        auto it = registered.find(page);
        return it == registered.end() ? 0 : it->second;
    }

    int register_calls = 0;
    int unregister_calls = 0;
    std::map<uintptr_t, int> registered;
    std::set<uintptr_t> busy_pages;
    std::set<uintptr_t> fail_pages;
    std::set<uintptr_t> mark_fail_pages;
    std::set<uintptr_t> fail_unregister_pages;
};

FakeTranslationTable* g_fake = nullptr;

int FakeRegister(void* addr, size_t len) {
    EXPECT_EQ(len, kPage);
    return g_fake->RegisterPage(reinterpret_cast<uintptr_t>(addr));
}

int FakeUnregister(void* addr, size_t len) {
    EXPECT_EQ(len, kPage);
    return g_fake->UnregisterPage(reinterpret_cast<uintptr_t>(addr));
}

// Fake 2MB-aligned addresses; never dereferenced, used only as map keys.
void* PageAddr(uint64_t index, uint64_t offset = 0) {
    return reinterpret_cast<void*>((16 + index) * kPage + offset);
}

class NofPageRegistryTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("NofPageRegistryTest");
        g_fake = &fake_;
    }

    void TearDown() override {
        g_fake = nullptr;
        google::ShutdownGoogleLogging();
    }

    FakeTranslationTable fake_;
    NofPageRegistry registry_{&FakeRegister, &FakeUnregister};
    void* owner_a_ = reinterpret_cast<void*>(0xA000);
    void* owner_b_ = reinterpret_cast<void*>(0xB000);
};

TEST_F(NofPageRegistryTest, SameOwnerSameRangeIsIdempotent) {
    void* ptr = PageAddr(0);
    ASSERT_EQ(registry_.Register(owner_a_, ptr, 4096), ErrorCode::OK);
    ASSERT_EQ(registry_.Register(owner_a_, ptr, 4096), ErrorCode::OK);
    EXPECT_EQ(fake_.register_calls, 1);
    EXPECT_EQ(fake_.Depth(reinterpret_cast<uintptr_t>(ptr)), 1);
}

TEST_F(NofPageRegistryTest, AdjacentBuffersShareOnePage) {
    void* p0 = PageAddr(0);
    void* p1 = PageAddr(0, 4096);
    ASSERT_EQ(registry_.Register(owner_a_, p0, 4096), ErrorCode::OK);
    ASSERT_EQ(registry_.Register(owner_a_, p1, 4096), ErrorCode::OK);
    EXPECT_EQ(fake_.register_calls, 1);

    ASSERT_EQ(registry_.Unregister(owner_a_, p0), ErrorCode::OK);
    EXPECT_EQ(fake_.unregister_calls, 0);  // p1 still uses the page
    ASSERT_EQ(registry_.Unregister(owner_a_, p1), ErrorCode::OK);
    EXPECT_EQ(fake_.unregister_calls, 1);
    EXPECT_EQ(fake_.Depth(reinterpret_cast<uintptr_t>(p0)), 0);
}

TEST_F(NofPageRegistryTest, SecondOwnerKeepsPagesRegistered) {
    void* ptr = PageAddr(0);
    ASSERT_EQ(registry_.Register(owner_a_, ptr, 4096), ErrorCode::OK);
    ASSERT_EQ(registry_.Register(owner_b_, ptr, 4096), ErrorCode::OK);
    EXPECT_EQ(fake_.register_calls, 1);  // page already registered

    ASSERT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::OK);
    EXPECT_EQ(fake_.unregister_calls, 0);
    EXPECT_EQ(fake_.Depth(reinterpret_cast<uintptr_t>(ptr)), 1);

    ASSERT_EQ(registry_.Unregister(owner_b_, ptr), ErrorCode::OK);
    EXPECT_EQ(fake_.unregister_calls, 1);
    EXPECT_EQ(fake_.Depth(reinterpret_cast<uintptr_t>(ptr)), 0);
}

TEST_F(NofPageRegistryTest, LargerReregisterExtendsCoverage) {
    void* ptr = PageAddr(0);
    const uintptr_t page0 = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t page1 = page0 + kPage;

    ASSERT_EQ(registry_.Register(owner_a_, ptr, kPage / 2), ErrorCode::OK);
    EXPECT_EQ(fake_.register_calls, 1);

    // Same ptr, larger size: only the newly covered page is registered.
    ASSERT_EQ(registry_.Register(owner_a_, ptr, 3 * kPage / 2), ErrorCode::OK);
    EXPECT_EQ(fake_.register_calls, 2);
    EXPECT_EQ(fake_.Depth(page0), 1);
    EXPECT_EQ(fake_.Depth(page1), 1);

    ASSERT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::OK);
    EXPECT_EQ(fake_.unregister_calls, 2);
    EXPECT_EQ(fake_.Depth(page0), 0);
    EXPECT_EQ(fake_.Depth(page1), 0);
}

TEST_F(NofPageRegistryTest, UnregisterUnknownPtrIsNoOp) {
    EXPECT_EQ(registry_.Unregister(owner_a_, PageAddr(0)), ErrorCode::OK);
    EXPECT_EQ(registry_.UnregisterAll(owner_a_), ErrorCode::OK);
    EXPECT_EQ(fake_.unregister_calls, 0);
}

TEST_F(NofPageRegistryTest, OtherOwnersRegistrationsAreUntouched) {
    void* ptr = PageAddr(0);
    ASSERT_EQ(registry_.Register(owner_a_, ptr, 4096), ErrorCode::OK);
    ASSERT_EQ(registry_.Unregister(owner_b_, ptr), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(reinterpret_cast<uintptr_t>(ptr)), 1);
    EXPECT_EQ(fake_.unregister_calls, 0);
}

TEST_F(NofPageRegistryTest, ExternalPageNeverUnregistered) {
    const uintptr_t page = reinterpret_cast<uintptr_t>(PageAddr(0));
    fake_.busy_pages.insert(page);  // DPDK memseg walk got there first

    ASSERT_EQ(registry_.Register(owner_a_, PageAddr(0), 4096), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(page), 0);  // not registered by us
    ASSERT_EQ(registry_.Unregister(owner_a_, PageAddr(0)), ErrorCode::OK);
    EXPECT_EQ(fake_.unregister_calls, 0);
}

// The failing page's cleanup succeeds: the whole failure is confirmed clean,
// this call's pages are rolled back, and nothing is retained.
TEST_F(NofPageRegistryTest, FailingPageCleanedUpWhenConfirmed) {
    void* ptr = PageAddr(0);
    const uintptr_t page0 = reinterpret_cast<uintptr_t>(ptr);
    fake_.mark_fail_pages.insert(page0);  // marks, then fails

    ASSERT_EQ(registry_.Register(owner_a_, ptr, 4096),
              ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(fake_.unregister_calls, 1);  // cleanup ran on the failing page
    EXPECT_EQ(fake_.Depth(page0), 0);

    // No bookkeeping retained: a later Unregister is a no-op, and a retry
    // registers fresh once the backend recovers.
    const int unregister_calls_after = fake_.unregister_calls;
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::OK);
    EXPECT_EQ(fake_.unregister_calls, unregister_calls_after);
    fake_.mark_fail_pages.erase(page0);
    ASSERT_EQ(registry_.Register(owner_a_, ptr, 4096), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(page0), 1);
}

// The failing page's cleanup cannot be confirmed: it is quarantined
// terminally, while this call's successfully registered pages stay charged
// to this owner (per-page refcounts partition owners).
TEST_F(NofPageRegistryTest, FailureQuarantinesFailingPageKeepsChargedPages) {
    void* ptr = PageAddr(0);
    const uintptr_t page0 = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t page1 = page0 + kPage;
    fake_.fail_pages.insert(page1);  // fails before marking anything

    ASSERT_EQ(registry_.Register(owner_a_, ptr, 3 * kPage / 2),
              ErrorCode::NOF_REGISTRATION_STUCK);
    // page0 stays charged (no rollback); page1 got one cleanup attempt and
    // is now quarantined.
    EXPECT_EQ(fake_.Depth(page0), 1);
    EXPECT_EQ(fake_.unregister_calls, 1);

    // Another owner can still register page0 normally.
    ASSERT_EQ(registry_.Register(owner_b_, ptr, 4096), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(page0), 1);

    // A's teardown drops exactly its own page0 reference and fails on the
    // quarantined page1; B's page is never touched.
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(fake_.Depth(page0), 1);
    ASSERT_EQ(registry_.Unregister(owner_b_, ptr), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(page0), 0);

    // Quarantine is terminal: A's retry keeps failing.
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::INTERNAL_ERROR);
}

// Hugepage-slice scenario (page_base + 4096): an unaligned stuck record must
// never release another owner's reference on the containing page.
TEST_F(NofPageRegistryTest, UnalignedStuckUnregisterNeverTouchesOtherOwner) {
    void* base = PageAddr(0);
    void* slice = PageAddr(0, 4096);
    const uintptr_t page0 = reinterpret_cast<uintptr_t>(base);
    const uintptr_t page1 = page0 + kPage;
    ASSERT_EQ(registry_.Register(owner_b_, base, 4096), ErrorCode::OK);

    fake_.mark_fail_pages.insert(page1);
    fake_.fail_unregister_pages.insert(page1);
    ASSERT_EQ(registry_.Register(owner_a_, slice, 3 * kPage / 2),
              ErrorCode::NOF_REGISTRATION_STUCK);
    EXPECT_EQ(fake_.Depth(page0), 1);
    EXPECT_EQ(fake_.Depth(page1), 1);

    // A's teardown drops exactly its own page0 reference (B's stays) and
    // fails only on the quarantined page1.
    EXPECT_EQ(registry_.Unregister(owner_a_, slice), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(fake_.Depth(page0), 1);
    ASSERT_EQ(registry_.Unregister(owner_b_, base), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(page0), 0);
}

// A stuck record covers only the charged prefix: a retry registers the
// untouched tail instead of returning OK without restoring coverage.
TEST_F(NofPageRegistryTest, StuckRetryExtendsOverUntouchedTail) {
    void* ptr = PageAddr(0);
    const uintptr_t page0 = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t page1 = page0 + kPage;
    const uintptr_t page2 = page1 + kPage;
    fake_.mark_fail_pages.insert(page1);
    fake_.fail_unregister_pages.insert(page1);

    ASSERT_EQ(registry_.Register(owner_a_, ptr, 3 * kPage),
              ErrorCode::NOF_REGISTRATION_STUCK);
    EXPECT_EQ(fake_.Depth(page0), 1);
    EXPECT_EQ(fake_.Depth(page2), 0);  // tail never attempted
    EXPECT_EQ(fake_.register_calls, 2);

    // Retry: the covered prefix is skipped; only the tail is registered.
    fake_.fail_unregister_pages.erase(page1);
    fake_.mark_fail_pages.erase(page1);
    ASSERT_EQ(registry_.Register(owner_a_, ptr, 3 * kPage), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(page2), 1);
    EXPECT_EQ(fake_.register_calls, 3);  // exactly one new backend call

    // Teardown: page0/page2 release; the quarantined page1 fails.
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(fake_.Depth(page0), 0);
    EXPECT_EQ(fake_.Depth(page2), 0);
}

// A quarantined page is treated like an external one by later registrations
// (OK), but is terminally unreleasable for every owner.
TEST_F(NofPageRegistryTest, QuarantinedPageStaysPinnedForAllOwners) {
    void* ptr = PageAddr(0);
    const uintptr_t page0 = reinterpret_cast<uintptr_t>(ptr);
    fake_.mark_fail_pages.insert(page0);
    fake_.fail_unregister_pages.insert(page0);
    ASSERT_EQ(registry_.Register(owner_a_, ptr, 4096),
              ErrorCode::NOF_REGISTRATION_STUCK);

    ASSERT_EQ(registry_.Register(owner_b_, ptr, 4096), ErrorCode::OK);
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(registry_.Unregister(owner_b_, ptr), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(fake_.Depth(page0), 1);  // pinned
}

// A backend unregister failure quarantines the page terminally; the error
// propagates and the record is retained so the caller's mapping quarantines.
TEST_F(NofPageRegistryTest, UnregisterFailureQuarantinesTerminally) {
    void* ptr = PageAddr(0);
    const uintptr_t page0 = reinterpret_cast<uintptr_t>(ptr);
    ASSERT_EQ(registry_.Register(owner_a_, ptr, 4096), ErrorCode::OK);

    fake_.fail_unregister_pages.insert(page0);
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(fake_.Depth(page0), 1);

    // Terminal: retries never succeed, the record stays for quarantine.
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(registry_.UnregisterAll(owner_a_), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(fake_.Depth(page0), 1);
}

TEST_F(NofPageRegistryTest, UnregisterAllPropagatesPartialFailure) {
    void* p0 = PageAddr(0);
    void* p1 = PageAddr(1);
    ASSERT_EQ(registry_.Register(owner_a_, p0, 4096), ErrorCode::OK);
    ASSERT_EQ(registry_.Register(owner_a_, p1, 4096), ErrorCode::OK);

    fake_.fail_unregister_pages.insert(reinterpret_cast<uintptr_t>(p1));
    EXPECT_EQ(registry_.UnregisterAll(owner_a_), ErrorCode::INTERNAL_ERROR);
    // The healthy range was released; the failing page is quarantined.
    EXPECT_EQ(fake_.Depth(reinterpret_cast<uintptr_t>(p0)), 0);
    EXPECT_EQ(fake_.Depth(reinterpret_cast<uintptr_t>(p1)), 1);

    // Quarantine is terminal even after the backend recovers.
    fake_.fail_unregister_pages.clear();
    EXPECT_EQ(registry_.UnregisterAll(owner_a_), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(fake_.Depth(reinterpret_cast<uintptr_t>(p1)), 1);
}

TEST_F(NofPageRegistryTest, UnregisterAllReleasesEverything) {
    void* p0 = PageAddr(0);
    void* p1 = PageAddr(1);
    ASSERT_EQ(registry_.Register(owner_a_, p0, 4096), ErrorCode::OK);
    ASSERT_EQ(registry_.Register(owner_a_, p1, 4096), ErrorCode::OK);

    ASSERT_EQ(registry_.UnregisterAll(owner_a_), ErrorCode::OK);
    EXPECT_EQ(fake_.unregister_calls, 2);

    ASSERT_EQ(registry_.Register(owner_b_, p0, 4096), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(reinterpret_cast<uintptr_t>(p0)), 1);
}

}  // namespace
}  // namespace mooncake
