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
// memseg walk) or -EIO (genuine failure).
struct FakeTranslationTable {
    int RegisterPage(uintptr_t page) {
        ++register_calls;
        if (busy_pages.count(page)) return -EBUSY;
        if (fail_pages.count(page)) return -EIO;
        ++registered[page];
        return 0;
    }

    int UnregisterPage(uintptr_t page) {
        ++unregister_calls;
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
    // by this call is rolled back...
    ASSERT_EQ(registry_.Register(owner_a_, ptr, 3 * kPage / 2),
              ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(fake_.Depth(page0), 0);
    EXPECT_EQ(fake_.unregister_calls, 1);

    // ...and a later registration sees a clean table.
    ASSERT_EQ(registry_.Register(owner_b_, ptr, 4096), ErrorCode::OK);
    EXPECT_EQ(fake_.Depth(page0), 1);
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
