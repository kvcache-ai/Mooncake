// Isolated allocator overrides exercise rollback without changing the allocator
// used by other V2 tests.
#include "p2p/client/data_manager/v2/block_index.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <string>
#include <string_view>

#include "p2p/client/data_manager/v2/block_pool.h"
#include "p2p/client/data_manager/v2/block_registry.h"
#include "common_types.h"

namespace {

// Allocations left before the injected failure; negative while disarmed. Per
// thread, so a background allocation elsewhere cannot consume the countdown.
thread_local int64_t g_allocations_until_failure = -1;

bool ShouldFailAllocation() {
    // Fires once and leaves itself disarmed: the rollback path under test
    // logs, and logging allocates, so a second failure would only turn the
    // recovery being measured into a std::terminate.
    return g_allocations_until_failure >= 0 &&
           g_allocations_until_failure-- == 0;
}

void* AllocateOrFail(std::size_t size) {
    if (ShouldFailAllocation()) throw std::bad_alloc();
    void* memory = std::malloc(size == 0 ? 1 : size);
    if (memory == nullptr) throw std::bad_alloc();
    return memory;
}

void* AllocateAlignedOrFail(std::size_t size, std::size_t alignment) {
    if (ShouldFailAllocation()) throw std::bad_alloc();
    // std::aligned_alloc wants a size that is a multiple of the alignment.
    const std::size_t rounded =
        ((size + alignment - 1) / alignment) * alignment;
    void* memory =
        std::aligned_alloc(alignment, rounded == 0 ? alignment : rounded);
    if (memory == nullptr) throw std::bad_alloc();
    return memory;
}

/** Fail the `nth` (0-based) allocation this thread makes from now on. */
void FailAllocationAfter(int64_t nth) { g_allocations_until_failure = nth; }

void DisarmAllocationFailure() { g_allocations_until_failure = -1; }

}  // namespace

void* operator new(std::size_t size) { return AllocateOrFail(size); }
void* operator new[](std::size_t size) { return AllocateOrFail(size); }

void* operator new(std::size_t size, std::align_val_t alignment) {
    return AllocateAlignedOrFail(size, static_cast<std::size_t>(alignment));
}
void* operator new[](std::size_t size, std::align_val_t alignment) {
    return AllocateAlignedOrFail(size, static_cast<std::size_t>(alignment));
}

void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
    try {
        return AllocateOrFail(size);
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
    try {
        return AllocateOrFail(size);
    } catch (...) {
        return nullptr;
    }
}
void* operator new(std::size_t size, std::align_val_t alignment,
                   const std::nothrow_t&) noexcept {
    try {
        return AllocateAlignedOrFail(size, static_cast<std::size_t>(alignment));
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](std::size_t size, std::align_val_t alignment,
                     const std::nothrow_t&) noexcept {
    try {
        return AllocateAlignedOrFail(size, static_cast<std::size_t>(alignment));
    } catch (...) {
        return nullptr;
    }
}

void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete[](void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void* memory, std::size_t) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::size_t, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::size_t, std::align_val_t) noexcept {
    std::free(memory);
}
void operator delete(void* memory, const std::nothrow_t&) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, const std::nothrow_t&) noexcept {
    std::free(memory);
}
void operator delete(void* memory, std::align_val_t,
                     const std::nothrow_t&) noexcept {
    std::free(memory);
}
void operator delete[](void* memory, std::align_val_t,
                       const std::nothrow_t&) noexcept {
    std::free(memory);
}

namespace mooncake::v2 {
namespace {

constexpr UUID kTilerId{0xFA57, 0x0001};
constexpr size_t kCapacity = 1u << 20;
constexpr size_t kBlockBytes = 100;

// Delegate real DRAM storage to its pool while observing the moment physical
// release happens. The callback reenters the index: holding either its ordering
// lock or a shard lock during release would deadlock this test.
class ReentrantFreeState final : public BlockPoolState {
   public:
    ReentrantFreeState(BlockAllocation allocation,
                       std::function<void()> before_free)
        : allocation_(std::move(allocation)),
          before_free_(std::move(before_free)) {}

    tl::expected<void, ErrorCode> Free(PhysicalBlockId id) override {
        EXPECT_EQ(id, allocation_.Id());
        before_free_();
        allocation_.Reset();
        return {};
    }
    BlockDataHandle* Get(PhysicalBlockId) const override {
        return &allocation_.Data();
    }
    size_t Capacity() const override { return kCapacity; }
    size_t Usage() const override {
        return allocation_ ? allocation_.Size() : 0;
    }
    BlockPoolCapabilities Capabilities() const override { return {}; }

   private:
    BlockAllocation allocation_;
    std::function<void()> before_free_;
};

class BlockIndexFailureTest : public ::testing::TestWithParam<const char*> {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("BlockIndexFailureTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        DramArenaConfig arena;
        arena.capacity_bytes = kCapacity;
        arena.alignment = 1;
        DramBlockPoolConfig config;
        config.arenas.push_back(arena);
        pool_ = std::make_shared<DramBlockPool>(config, nullptr);
        ASSERT_TRUE(pool_->Init());
        registry_ = BlockRegistry(BlockRegistryConfig{});
        ResetIndex();
    }

    void TearDown() override {
        DisarmAllocationFailure();
        // Keep the index alive while callbacks inspect it during final release.
        auto entries = index_->Drain();
        entries.clear();
        index_.reset();
    }

    void ResetIndex() {
        if (index_) {
            auto entries = index_->Drain();
            entries.clear();
        }
        BlockIndexConfig config;
        config.type = GetParam();
        config.shard_count = 4;
        auto created = CreateBlockIndex(config, kTilerId, kCapacity);
        CHECK(created);
        index_ = std::move(*created);
    }

    BlockRegistrationHandle Register(std::string_view key) {
        auto registered = registry_.Register(key);
        CHECK(registered);
        return *registered;
    }

    CompletedBlock MakeCompleted(const BlockRegistrationHandle& registration) {
        auto allocation = pool_->Allocate(kBlockBytes, 1);
        CHECK(allocation);
        const auto physical_id = allocation->Id();
        auto* data = &allocation->Data();
        auto observer = std::make_shared<ReentrantFreeState>(
            std::move(*allocation), [this, registration] {
                (void)index_->Lookup(registration);
                (void)index_->Stats();
                (void)index_->SelectVictims(kCapacity);
                ++physical_frees_;
            });
        auto mutable_block =
            MutableBlock::MakeForTiler(BlockAllocation::MakeForPool(
                physical_id, kBlockBytes, data, std::move(observer)));
        const std::string payload(kBlockBytes, 'x');
        CHECK(mutable_block.Write(
            0, std::as_bytes(std::span<const char>(payload))));
        auto completed =
            std::move(mutable_block).Complete(std::string(registration.Key()));
        CHECK(completed);
        return std::move(*completed);
    }

    std::shared_ptr<DramBlockPool> pool_;
    BlockRegistry registry_;
    std::unique_ptr<BlockIndex> index_;
    size_t physical_frees_ = 0;
};

TEST_P(BlockIndexFailureTest, AllocationFailureRollsBackVisibilityAndOrdering) {
    size_t failures = 0;
    size_t successes = 0;
    // Sweep every possible allocation site rather than assuming a particular
    // hash-map implementation or entry-allocation order.
    for (int64_t nth = 0; nth < 16; ++nth) {
        ResetIndex();
        auto stable = Register("stable");
        ASSERT_TRUE(index_->Insert(MakeCompleted(stable), stable));
        auto doomed = Register("doomed");
        auto completed = MakeCompleted(doomed);
        const size_t frees_before = physical_frees_;
        auto inserted = [&] {
            struct DisarmOnReturn {
                ~DisarmOnReturn() { DisarmAllocationFailure(); }
            } disarm;
            FailAllocationAfter(nth);
            return index_->Insert(std::move(completed), doomed);
        }();
        EXPECT_FALSE(static_cast<bool>(completed)) << "nth=" << nth;
        if (!inserted) {
            ++failures;
            EXPECT_EQ(inserted.error(), ErrorCode::INTERNAL_ERROR);
            EXPECT_FALSE(index_->Lookup(doomed));
            EXPECT_EQ(index_->Stats().entry_count, 1u);
            EXPECT_EQ(pool_->Usage(), kBlockBytes);
            EXPECT_EQ(physical_frees_, frees_before + 1);
        } else {
            ++successes;
            EXPECT_TRUE(index_->Lookup(doomed));
            EXPECT_EQ(index_->Stats().entry_count, 2u);
        }
        const auto victims = index_->SelectVictims(kCapacity);
        EXPECT_EQ(victims.size(), index_->Stats().entry_count) << "nth=" << nth;
        EXPECT_EQ(index_->Stats().indexed_bytes, victims.size() * kBlockBytes);
        for (const auto& victim : victims) {
            auto registration = victim.registration.Lock();
            ASSERT_TRUE(registration);
            auto visible = index_->Lookup(*registration);
            ASSERT_TRUE(visible);
            EXPECT_EQ(visible->Id(), victim.block_id);
        }
        if (!inserted) {
            // A failed insert leaves no node that could reject or hide a retry.
            auto retry = index_->Insert(MakeCompleted(doomed), doomed);
            ASSERT_TRUE(retry) << "nth=" << nth;
            EXPECT_EQ(index_->SelectVictims(kCapacity).size(), 2u);
        }
    }
    EXPECT_GT(failures, 0u)
        << "the injector must reach actual insert allocations";
    EXPECT_GT(successes, 0u) << "the sweep must also cover the successful path";
}

TEST_P(BlockIndexFailureTest,
       FactoryAllocationFailureReturnsAnErrorAndRecovers) {
    BlockIndexConfig config;
    config.type = GetParam();
    auto failed = [&] {
        struct DisarmOnReturn {
            ~DisarmOnReturn() { DisarmAllocationFailure(); }
        } disarm;
        FailAllocationAfter(0);
        return CreateBlockIndex(config, kTilerId, kCapacity);
    }();
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error(), ErrorCode::INTERNAL_ERROR);

    auto recovered = CreateBlockIndex(config, kTilerId, kCapacity);
    ASSERT_TRUE(recovered);
    EXPECT_EQ((*recovered)->TilerId(), kTilerId);
    EXPECT_EQ((*recovered)->ShardCount(), config.shard_count);
    EXPECT_EQ((*recovered)->Stats().entry_count, 0u);
    EXPECT_TRUE((*recovered)->SelectVictims(kCapacity).empty());
}

TEST_P(BlockIndexFailureTest,
       RejectedAndDetachedAllocationsReleaseOutsideLocks) {
    auto registration = Register("physical-release");
    {
        auto inserted =
            index_->Insert(MakeCompleted(registration), registration);
        ASSERT_TRUE(inserted);
    }
    const size_t frees_before = physical_frees_;
    auto duplicate = MakeCompleted(registration);
    auto rejected = index_->Insert(std::move(duplicate), registration);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    EXPECT_FALSE(static_cast<bool>(duplicate));
    EXPECT_EQ(physical_frees_, frees_before + 1);
    EXPECT_EQ(pool_->Usage(), kBlockBytes);

    auto snapshot = index_->Lookup(registration);
    ASSERT_TRUE(snapshot);
    auto detached = index_->Erase(registration);
    ASSERT_NE(detached, nullptr);
    detached.reset();
    EXPECT_EQ(physical_frees_, frees_before + 1);
    snapshot.reset();
    EXPECT_EQ(physical_frees_, frees_before + 2);
    EXPECT_EQ(pool_->Usage(), 0u);

    ASSERT_TRUE(index_->Insert(MakeCompleted(registration), registration));
    auto drained = index_->Drain();
    EXPECT_EQ(physical_frees_, frees_before + 2);
    drained.clear();
    EXPECT_EQ(physical_frees_, frees_before + 3);
    EXPECT_EQ(pool_->Usage(), 0u);
}

INSTANTIATE_TEST_SUITE_P(Orderings, BlockIndexFailureTest,
                         ::testing::Values("lru", "multi_lru", "tinylfu_lru"));

}  // namespace
}  // namespace mooncake::v2
