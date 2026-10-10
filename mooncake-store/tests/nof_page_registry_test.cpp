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

    // The first owner's unregister must not unmap pages the second owner
    // still relies on.
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

    // Unregister releases the extended range.
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
    // Owner B never registered: unregistering must not decrement the page.
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

TEST_F(NofPageRegistryTest, FailureRollsBackOnlyThisCall) {
    void* ptr = PageAddr(0);
    const uintptr_t page0 = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t page1 = page0 + kPage;
    fake_.fail_pages.insert(page1);

    // Two-page registration fails on the second page: the first page bumped
    // by this call is rolled back (unregister_calls=1), and the failing page
    // gets a cleanup attempt (unregister_calls=2). spdk_mem_unregister
    // answers -EINVAL for a never-marked page, so cleanup cannot be
    // confirmed and the range is retained as NOF_REGISTRATION_STUCK.
    ASSERT_EQ(registry_.Register(owner_a_, ptr, 3 * kPage / 2),
              ErrorCode::NOF_REGISTRATION_STUCK);
    EXPECT_EQ(fake_.Depth(page0), 0);
    EXPECT_EQ(fake_.unregister_calls, 2);

    // ...and a later registration by another owner sees a clean table.
    ASSERT_EQ(registry_.Register(owner_b_, ptr, 4096), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(page0), 1);
}

// The failing page can leave partial backend state (spdk_mem_register marks
// before its notify callbacks and does not roll back). When the cleanup
// unregister succeeds, the failure is a plain error and owns nothing.
TEST_F(NofPageRegistryTest, FailingPageCleanedUpWhenConfirmed) {
    void* ptr = PageAddr(0);
    const uintptr_t page0 = reinterpret_cast<uintptr_t>(ptr);
    fake_.mark_fail_pages.insert(page0);  // marks, then fails

    ASSERT_EQ(registry_.Register(owner_a_, ptr, 4096),
              ErrorCode::INTERNAL_ERROR);
    // Cleanup ran on the failing page and removed its partial state.
    EXPECT_EQ(fake_.unregister_calls, 1);
    EXPECT_EQ(fake_.Depth(page0), 0);

    // No bookkeeping retained: a later Unregister is a no-op, and a retry
    // registers fresh (no stale count, no external misclassification).
    const int unregister_calls_after = fake_.unregister_calls;
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::OK);
    EXPECT_EQ(fake_.unregister_calls, unregister_calls_after);
    fake_.mark_fail_pages.erase(page0);  // backend recovers
    ASSERT_EQ(registry_.Register(owner_a_, ptr, 4096), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(page0), 1);
}

// When the cleanup unregister also fails, the page state is unknown: the
// page must stay charged and the owner range retained so teardown retries.
TEST_F(NofPageRegistryTest, FailingPageRetainedWhenCleanupUnconfirmed) {
    void* ptr = PageAddr(0);
    const uintptr_t page0 = reinterpret_cast<uintptr_t>(ptr);
    fake_.mark_fail_pages.insert(page0);
    fake_.fail_unregister_pages.insert(page0);

    ASSERT_EQ(registry_.Register(owner_a_, ptr, 4096),
              ErrorCode::NOF_REGISTRATION_STUCK);
    // The partial mark is still there, and the registry kept the page.
    EXPECT_EQ(fake_.Depth(page0), 1);

    // A same-range retry must NOT re-call register_fn_: the leftover
    // translation would answer -EBUSY and be misclassified as external.
    const int register_calls_after = fake_.register_calls;
    EXPECT_EQ(registry_.Register(owner_a_, ptr, 4096), ErrorCode::OK);
    EXPECT_EQ(fake_.register_calls, register_calls_after);

    // Unregister retries the cleanup; while the backend keeps failing it
    // propagates the error and retains the record...
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(fake_.Depth(page0), 1);

    // ...and once the backend recovers, the retry releases it.
    fake_.fail_unregister_pages.erase(page0);
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(page0), 0);
}

// Backend unregister failures must propagate and retain state for retry
// (the shm quarantine paths key off the error return).
TEST_F(NofPageRegistryTest, UnregisterFailurePropagatesAndRetainsState) {
    void* ptr = PageAddr(0);
    const uintptr_t page0 = reinterpret_cast<uintptr_t>(ptr);
    ASSERT_EQ(registry_.Register(owner_a_, ptr, 4096), ErrorCode::OK);

    fake_.fail_unregister_pages.insert(page0);
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::INTERNAL_ERROR);
    // State retained: the page is still registered in the backend, still
    // charged to the owner, and no re-registration happened.
    EXPECT_EQ(fake_.Depth(page0), 1);
    EXPECT_EQ(fake_.register_calls, 1);

    fake_.fail_unregister_pages.erase(page0);
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(page0), 0);
    // Owner record was erased: a third Unregister is a no-op.
    const int unregister_calls_after = fake_.unregister_calls;
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::OK);
    EXPECT_EQ(fake_.unregister_calls, unregister_calls_after);
}

TEST_F(NofPageRegistryTest, UnregisterAllPropagatesPartialFailure) {
    void* p0 = PageAddr(0);
    void* p1 = PageAddr(1);
    ASSERT_EQ(registry_.Register(owner_a_, p0, 4096), ErrorCode::OK);
    ASSERT_EQ(registry_.Register(owner_a_, p1, 4096), ErrorCode::OK);

    fake_.fail_unregister_pages.insert(reinterpret_cast<uintptr_t>(p1));
    EXPECT_EQ(registry_.UnregisterAll(owner_a_), ErrorCode::INTERNAL_ERROR);
    // The healthy range was released; the failing one is retained for retry.
    EXPECT_EQ(fake_.Depth(reinterpret_cast<uintptr_t>(p0)), 0);
    EXPECT_EQ(fake_.Depth(reinterpret_cast<uintptr_t>(p1)), 1);

    fake_.fail_unregister_pages.clear();
    EXPECT_EQ(registry_.UnregisterAll(owner_a_), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(reinterpret_cast<uintptr_t>(p1)), 0);
}

// The rollback path has the same unconfirmed-cleanup problem as the failing
// page: if an earlier page's rollback unregister fails, its record must stay
// charged — erasing it would let a later Register misread the leftover
// backend state as -EBUSY/external.
TEST_F(NofPageRegistryTest, RollbackFailureIsRetainedAndRetried) {
    void* ptr = PageAddr(0);
    const uintptr_t page0 = reinterpret_cast<uintptr_t>(ptr);
    const uintptr_t page1 = page0 + kPage;
    fake_.mark_fail_pages.insert(page1);        // page1: marks, then fails
    fake_.fail_unregister_pages.insert(page0);  // page0's rollback fails

    ASSERT_EQ(registry_.Register(owner_a_, ptr, 3 * kPage / 2),
              ErrorCode::NOF_REGISTRATION_STUCK);
    // page1's partial mark was cleaned up (its unregister succeeded);
    // page0's rollback unregister failed, so it stays registered AND
    // recorded.
    EXPECT_EQ(fake_.Depth(page1), 0);
    EXPECT_EQ(fake_.Depth(page0), 1);

    // A same-range retry does not re-register the retained page.
    const int register_calls_after = fake_.register_calls;
    EXPECT_EQ(registry_.Register(owner_a_, ptr, 3 * kPage / 2), ErrorCode::OK);
    EXPECT_EQ(fake_.register_calls, register_calls_after);

    // Teardown retry releases the retained page once the backend recovers.
    fake_.fail_unregister_pages.erase(page0);
    EXPECT_EQ(registry_.Unregister(owner_a_, ptr), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(page0), 0);
}

TEST_F(NofPageRegistryTest, UnregisterAllReleasesEverything) {
    void* p0 = PageAddr(0);
    void* p1 = PageAddr(1);
    ASSERT_EQ(registry_.Register(owner_a_, p0, 4096), ErrorCode::OK);
    ASSERT_EQ(registry_.Register(owner_a_, p1, 4096), ErrorCode::OK);

    // Teardown: the owner goes away without per-buffer unregisters.
    ASSERT_EQ(registry_.UnregisterAll(owner_a_), ErrorCode::OK);
    EXPECT_EQ(fake_.unregister_calls, 2);

    // Reopen: a new owner registering the same address gets a fresh,
    // independent registration.
    ASSERT_EQ(registry_.Register(owner_b_, p0, 4096), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(reinterpret_cast<uintptr_t>(p0)), 1);
    EXPECT_EQ(fake_.register_calls, 3);
}

}  // namespace
}  // namespace mooncake
