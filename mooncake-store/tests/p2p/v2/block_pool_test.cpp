// Tests for the V2 BlockPool layer: DramBlockPool, the CreateBlockPool
// factory, and the BlockAllocation / MutableBlock / CompletedBlock lifecycle
// wrappers that sit directly on top of a pool.
//
// Everything here is written against the public headers only. Every pool is
// built with a null TransferEngine, so no arena is TE-registered and a
// TransferAddress must never appear; that is itself one of the invariants
// under test (section 5.4: "optional rather than address 0 means
// unsupported").
//
// The load-bearing case is
// ExhaustedPoolReturnsNoAvailableHandleAndKeepsLiveBlocks. Sections 3.5, 5.6
// and invariant 7.4.12 forbid a pool from reclaiming anything on its own,
// which is exactly what V1's StorageTier did when it evicted whole buckets and
// told the metadata layer afterwards. V2 has no such upward channel, so "out
// of space" must stay out of space until a lease is dropped.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "p2p/client/v2/block.h"
#include "p2p/client/v2/block_pool.h"
#include "types.h"

namespace mooncake::v2 {
namespace {

constexpr size_t kArenaCapacity = 1u << 20;  // 1 MiB
constexpr size_t kBlockSize = 4096;
constexpr size_t kDefaultAlignment = 64;

DramArenaConfig Arena(size_t capacity_bytes,
                      size_t alignment = kDefaultAlignment) {
    DramArenaConfig arena;
    arena.capacity_bytes = capacity_bytes;
    arena.alignment = alignment;
    return arena;
}

// Every pool in this file is deliberately built without a TransferEngine.
std::unique_ptr<DramBlockPool> MakePool(std::vector<DramArenaConfig> arenas) {
    DramBlockPoolConfig config;
    config.arenas = std::move(arenas);
    auto pool = std::make_unique<DramBlockPool>(
        config, std::shared_ptr<TransferEngine>{});
    if (!pool->Init()) return nullptr;
    return pool;
}

ErrorCode InitErrorOf(std::vector<DramArenaConfig> arenas) {
    DramBlockPoolConfig config;
    config.arenas = std::move(arenas);
    DramBlockPool pool(config, std::shared_ptr<TransferEngine>{});
    auto initialized = pool.Init();
    return initialized.has_value() ? ErrorCode::OK : initialized.error();
}

// uint8_t payloads keep gtest's failure output readable; the handle API takes
// byte spans.
std::vector<uint8_t> Pattern(size_t length, uint8_t seed) {
    std::vector<uint8_t> data(length);
    for (size_t i = 0; i < length; ++i) {
        data[i] = static_cast<uint8_t>((seed + i) & 0xff);
    }
    return data;
}

std::span<const std::byte> AsBytes(const std::vector<uint8_t>& data) {
    return std::as_bytes(std::span<const uint8_t>(data));
}

std::span<std::byte> AsWritableBytes(std::vector<uint8_t>& data) {
    return std::as_writable_bytes(std::span<uint8_t>(data));
}

class BlockPoolTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("BlockPoolTest");
            FLAGS_logtostderr = 1;
        });
    }

    // Every storage case gets its own directory. V2 and V1 must not share a
    // data file (their space management is incompatible), and neither should
    // two tests.
    void SetUp() override {
        storage_dir_ = std::filesystem::temp_directory_path() /
                       ("mooncake_v2_pool_" +
                        std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(storage_dir_);
        std::filesystem::create_directories(storage_dir_);
    }

    void TearDown() override { std::filesystem::remove_all(storage_dir_); }

    std::string StoragePath(const std::string& name) const {
        return (storage_dir_ / name).string();
    }

    SSDDeviceConfig SsdDevice(const std::string& name, size_t capacity) const {
        SSDDeviceConfig device;
        device.file_path = StoragePath(name);
        device.capacity_bytes = capacity;
        return device;
    }

    std::unique_ptr<SSDBlockPool> MakeSsdPool(
        const std::vector<SSDDeviceConfig>& devices) const {
        SSDBlockPoolConfig config;
        config.devices = devices;
        auto pool = std::make_unique<SSDBlockPool>(config);
        auto initialized = pool->Init();
        if (!initialized) {
            ADD_FAILURE() << "SSDBlockPool::Init failed: "
                          << toString(initialized.error());
            return nullptr;
        }
        return pool;
    }

    std::filesystem::path storage_dir_;
};

// ---------------------------------------------------------------------------
// Allocate / read / write
// ---------------------------------------------------------------------------

TEST_F(BlockPoolTest, AllocatedBlockRoundTripsTheBytesItWasGiven) {
    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);

    auto allocated = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value()) << toString(allocated.error());
    EXPECT_TRUE(static_cast<bool>(*allocated));
    EXPECT_EQ(allocated->Size(), kBlockSize);
    EXPECT_EQ(allocated->Id().pool_id, pool->Id());

    BlockDataHandle& handle = allocated->Data();
    EXPECT_EQ(handle.Size(), kBlockSize);

    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x5a);
    ASSERT_TRUE(handle.Write(0, AsBytes(payload)).has_value());

    std::vector<uint8_t> readback(kBlockSize, 0);
    ASSERT_TRUE(handle.Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, payload);

    // A partial read must see the same bytes at the same offsets, i.e. the
    // handle addresses its own extent and not the arena base.
    std::vector<uint8_t> tail(16, 0);
    ASSERT_TRUE(handle.Read(kBlockSize - tail.size(), AsWritableBytes(tail))
                    .has_value());
    EXPECT_EQ(tail, std::vector<uint8_t>(payload.end() - 16, payload.end()));

    // A partial write must not disturb its neighbours.
    const std::vector<uint8_t> patch = Pattern(8, 0xf0);
    ASSERT_TRUE(handle.Write(64, AsBytes(patch)).has_value());
    std::vector<uint8_t> window(10, 0);
    ASSERT_TRUE(handle.Read(63, AsWritableBytes(window)).has_value());
    EXPECT_EQ(window[0], payload[63]);
    EXPECT_EQ(window[9], payload[72]);
    for (size_t i = 0; i < patch.size(); ++i) {
        EXPECT_EQ(window[i + 1], patch[i]) << "at patch byte " << i;
    }

    // DRAM has nothing to flush, but the barrier must still succeed so that
    // Complete() has a uniform contract across media.
    EXPECT_TRUE(handle.Commit().has_value());
}

TEST_F(BlockPoolTest, ReadAndWritePastTheEndOfTheBlockAreRejected) {
    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);
    auto allocated = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value());
    BlockDataHandle& handle = allocated->Data();

    std::vector<uint8_t> buffer(8, 0);

    // Offset inside the block but the range runs off the end: the check has to
    // be on offset+length, not on offset alone.
    auto straddling = handle.Read(kBlockSize - 4, AsWritableBytes(buffer));
    ASSERT_FALSE(straddling.has_value());
    EXPECT_EQ(straddling.error(), ErrorCode::INVALID_PARAMS);

    auto straddling_write = handle.Write(kBlockSize - 4, AsBytes(buffer));
    ASSERT_FALSE(straddling_write.has_value());
    EXPECT_EQ(straddling_write.error(), ErrorCode::INVALID_PARAMS);

    auto past_end = handle.Read(kBlockSize, AsWritableBytes(buffer));
    ASSERT_FALSE(past_end.has_value());
    EXPECT_EQ(past_end.error(), ErrorCode::INVALID_PARAMS);

    std::vector<uint8_t> whole_block_plus_one(kBlockSize + 1, 0);
    auto too_long = handle.Write(0, AsBytes(whole_block_plus_one));
    ASSERT_FALSE(too_long.has_value());
    EXPECT_EQ(too_long.error(), ErrorCode::INVALID_PARAMS);

    // An offset beyond the block is rejected even with nothing to copy, so a
    // caller cannot probe the arena with empty requests.
    std::vector<uint8_t> nothing;
    auto empty_past_end = handle.Read(kBlockSize + 1, AsWritableBytes(nothing));
    ASSERT_FALSE(empty_past_end.has_value());
    EXPECT_EQ(empty_past_end.error(), ErrorCode::INVALID_PARAMS);

    // The one-past-the-end boundary with an empty span is the legal edge.
    EXPECT_TRUE(handle.Read(kBlockSize, AsWritableBytes(nothing)).has_value());
    EXPECT_TRUE(handle.Write(kBlockSize, AsBytes(nothing)).has_value());
}

TEST_F(BlockPoolTest, OffsetPlusLengthThatOverflowsIsRejected) {
    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);
    auto allocated = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value());
    BlockDataHandle& handle = allocated->Data();

    // offset + length wraps around SIZE_MAX; a naive bound check would let
    // this through and memcpy far outside the arena.
    constexpr size_t kNearMax = std::numeric_limits<size_t>::max() - 8;
    std::vector<uint8_t> buffer(16, 0);

    auto overflowing_read = handle.Read(kNearMax, AsWritableBytes(buffer));
    ASSERT_FALSE(overflowing_read.has_value());
    EXPECT_EQ(overflowing_read.error(), ErrorCode::INVALID_PARAMS);

    auto overflowing_write = handle.Write(kNearMax, AsBytes(buffer));
    ASSERT_FALSE(overflowing_write.has_value());
    EXPECT_EQ(overflowing_write.error(), ErrorCode::INVALID_PARAMS);

    auto at_max = handle.Read(std::numeric_limits<size_t>::max(),
                              AsWritableBytes(buffer));
    ASSERT_FALSE(at_max.has_value());
    EXPECT_EQ(at_max.error(), ErrorCode::INVALID_PARAMS);
}

// ---------------------------------------------------------------------------
// Alignment
// ---------------------------------------------------------------------------

TEST_F(BlockPoolTest, StricterAlignmentThanTheArenaIsHonoured) {
    constexpr size_t kAlignment = 4096;
    auto pool = MakePool({Arena(kArenaCapacity, kDefaultAlignment)});
    ASSERT_NE(pool, nullptr);

    auto allocated = pool->Allocate(kBlockSize, kAlignment);
    ASSERT_TRUE(allocated.has_value()) << toString(allocated.error());

    auto address = allocated->Data().GetTransferAddress();
    ASSERT_TRUE(address.has_value());
    EXPECT_EQ(address->addr % kAlignment, 0u)
        << "the requested alignment was not honoured";
    EXPECT_EQ(address->size, kBlockSize);
    EXPECT_EQ(allocated->Data().Size(), kBlockSize);
    // The arena is charged for the padding the alignment required.
    EXPECT_GE(pool->Usage(), kBlockSize + kAlignment - 1);

    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x11);
    ASSERT_TRUE(allocated->Data().Write(0, AsBytes(payload)).has_value());
    std::vector<uint8_t> readback(kBlockSize, 0);
    ASSERT_TRUE(
        allocated->Data().Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, payload);
}

TEST_F(BlockPoolTest, ZeroAlignmentMeansThePoolMinimum) {
    constexpr size_t kArenaAlignment = 4096;
    auto pool = MakePool({Arena(kArenaCapacity, kArenaAlignment)});
    ASSERT_NE(pool, nullptr);
    EXPECT_EQ(pool->Capabilities().minimum_alignment, kArenaAlignment);

    {
        auto allocated = pool->Allocate(kBlockSize, 0);
        ASSERT_TRUE(allocated.has_value());
        EXPECT_EQ(allocated->Size(), kBlockSize);
        // Charged as if the caller had asked for the arena's alignment.
        EXPECT_GE(pool->Usage(), kBlockSize + kArenaAlignment - 1);
    }
    EXPECT_EQ(pool->Usage(), 0u);
}

TEST_F(BlockPoolTest, NonPowerOfTwoAlignmentAndZeroSizeAreRejected) {
    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);

    for (size_t alignment : {size_t{3}, size_t{48}, size_t{1000}}) {
        auto rejected = pool->Allocate(kBlockSize, alignment);
        ASSERT_FALSE(rejected.has_value()) << "alignment " << alignment;
        EXPECT_EQ(rejected.error(), ErrorCode::INVALID_PARAMS);
    }

    auto empty = pool->Allocate(0, kDefaultAlignment);
    ASSERT_FALSE(empty.has_value());
    EXPECT_EQ(empty.error(), ErrorCode::INVALID_PARAMS);

    auto empty_default_alignment = pool->Allocate(0, 0);
    ASSERT_FALSE(empty_default_alignment.has_value());
    EXPECT_EQ(empty_default_alignment.error(), ErrorCode::INVALID_PARAMS);

    // A rejected request must not have consumed anything.
    EXPECT_EQ(pool->Usage(), 0u);
}

// te_addressable is a property of the medium, not of this process's wiring:
// DRAM blocks expose an address a TransferEngine can use, and whether the
// arenas were actually registered is a deployment fact (warned about at pool
// init). Modelling it the other way would make a missing TransferEngine
// silently reroute writes to a different tier.
TEST_F(BlockPoolTest, DramReportsAnAddressEvenWithoutATransferEngine) {
    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);
    EXPECT_TRUE(pool->Capabilities().te_addressable);
    EXPECT_TRUE(pool->Capabilities().direct_cpu_access);
    EXPECT_FALSE(pool->Capabilities().persistent);

    auto allocated = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value());
    auto address = allocated->Data().GetTransferAddress();
    ASSERT_TRUE(address.has_value());
    // The point of returning an optional rather than address 0: a medium with
    // no address says so, and a DRAM block never reports a bogus one. V1's bug
    // was publishing 0 and only failing on the far side.
    EXPECT_NE(address->addr, 0u);
    EXPECT_EQ(address->size, kBlockSize);

    auto block = MutableBlock::MakeForTiler(std::move(*allocated));
    ASSERT_TRUE(block.GetTransferAddress().has_value());
    EXPECT_EQ(block.GetTransferAddress()->addr, address->addr);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

TEST_F(BlockPoolTest, InvalidArenaConfigurationIsRejectedAtInit) {
    EXPECT_EQ(InitErrorOf({}), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(InitErrorOf({Arena(0)}), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(InitErrorOf({Arena(kArenaCapacity, 0)}),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(InitErrorOf({Arena(kArenaCapacity, 96)}),
              ErrorCode::INVALID_PARAMS);
    // A bad arena anywhere in the list fails the whole pool: a partially
    // built pool would silently under-provision.
    EXPECT_EQ(InitErrorOf({Arena(kArenaCapacity), Arena(0)}),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(InitErrorOf({Arena(kArenaCapacity), Arena(kArenaCapacity)}),
              ErrorCode::OK);
}

TEST_F(BlockPoolTest, CapacityIsTheSumOfTheConfiguredArenas) {
    auto single = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(single, nullptr);
    EXPECT_EQ(single->Capacity(), kArenaCapacity);

    auto multi = MakePool({Arena(kArenaCapacity), Arena(2 * kArenaCapacity),
                           Arena(kArenaCapacity / 2)});
    ASSERT_NE(multi, nullptr);
    EXPECT_EQ(multi->Capacity(),
              kArenaCapacity + 2 * kArenaCapacity + kArenaCapacity / 2);
    // Capacity is a static property of the configuration, not of usage.
    auto allocated = multi->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value());
    EXPECT_EQ(multi->Capacity(),
              kArenaCapacity + 2 * kArenaCapacity + kArenaCapacity / 2);
}

// ---------------------------------------------------------------------------
// Usage accounting and the no-self-reclaim invariant
// ---------------------------------------------------------------------------

TEST_F(BlockPoolTest, UsageRisesOnAllocateAndFallsWhenTheLeaseDies) {
    auto pool = MakePool({Arena(kArenaCapacity, kDefaultAlignment)});
    ASSERT_NE(pool, nullptr);
    EXPECT_EQ(pool->Usage(), 0u);

    size_t after_first = 0;
    {
        auto first = pool->Allocate(kBlockSize, 0);
        ASSERT_TRUE(first.has_value());
        after_first = pool->Usage();
        // The charge covers the block plus at most alignment-1 of padding,
        // which is the headroom used to slide the base to an aligned address.
        EXPECT_GE(after_first, kBlockSize);
        EXPECT_LE(after_first, kBlockSize + kDefaultAlignment - 1);
        EXPECT_LE(after_first, pool->Capacity());

        {
            auto second = pool->Allocate(kBlockSize, 0);
            ASSERT_TRUE(second.has_value());
            EXPECT_GE(pool->Usage(), 2 * kBlockSize);
        }
        // Dropping the second lease returns exactly its own space.
        EXPECT_EQ(pool->Usage(), after_first);
    }
    EXPECT_EQ(pool->Usage(), 0u);
}

TEST_F(BlockPoolTest, ExhaustedPoolReturnsNoAvailableHandleAndKeepsLiveBlocks) {
    constexpr size_t kBigBlock = 32u << 10;
    constexpr size_t kMarkerBytes = 32;
    constexpr int kSafetyBound = 4096;
    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);

    std::vector<BlockAllocation> live;
    ErrorCode exhaustion = ErrorCode::OK;
    for (int i = 0; i < kSafetyBound; ++i) {
        auto allocated = pool->Allocate(kBigBlock, 0);
        if (!allocated.has_value()) {
            exhaustion = allocated.error();
            break;
        }
        const std::vector<uint8_t> marker =
            Pattern(kMarkerBytes, static_cast<uint8_t>(i));
        ASSERT_TRUE(allocated->Data().Write(0, AsBytes(marker)).has_value());
        live.push_back(std::move(*allocated));
    }
    ASSERT_EQ(exhaustion, ErrorCode::NO_AVAILABLE_HANDLE);
    ASSERT_FALSE(live.empty());

    const size_t used_at_exhaustion = pool->Usage();
    const size_t live_count = live.size();

    // Asking again must not tempt the pool into making room. V1's StorageTier
    // evicted a whole bucket at this point and notified the metadata layer
    // afterwards; V2 has no such channel, so the only legal answer is the
    // same refusal (sections 3.5 / 5.6, invariant 7.4.12).
    auto retried = pool->Allocate(kBigBlock, 0);
    ASSERT_FALSE(retried.has_value());
    EXPECT_EQ(retried.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(pool->Usage(), used_at_exhaustion);
    EXPECT_EQ(live.size(), live_count);

    // Nothing was recycled behind the holders' backs: every block still has
    // its own bytes, so no extent was handed to somebody else.
    for (size_t i = 0; i < live.size(); ++i) {
        ASSERT_TRUE(static_cast<bool>(live[i]));
        std::vector<uint8_t> readback(kMarkerBytes, 0);
        ASSERT_TRUE(
            live[i].Data().Read(0, AsWritableBytes(readback)).has_value());
        EXPECT_EQ(readback, Pattern(kMarkerBytes, static_cast<uint8_t>(i)))
            << "block " << i << " was recycled under its holder";
        EXPECT_EQ(pool->Get(live[i].Id()), &live[i].Data());
    }

    // Releasing leases is the one and only way space comes back.
    live.clear();
    EXPECT_EQ(pool->Usage(), 0u);
    EXPECT_TRUE(pool->Allocate(kBigBlock, 0).has_value());
}

// ---------------------------------------------------------------------------
// Multiple arenas (pool-internal placement, invisible above)
// ---------------------------------------------------------------------------

TEST_F(BlockPoolTest, AllocationsSpreadAcrossEveryArena) {
    auto pool = MakePool({Arena(kArenaCapacity), Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);
    EXPECT_EQ(pool->Capacity(), 2 * kArenaCapacity);

    std::vector<BlockAllocation> live;
    std::set<uint32_t> targets;
    for (int i = 0; i < 4; ++i) {
        auto allocated = pool->Allocate(kBlockSize, 0);
        ASSERT_TRUE(allocated.has_value());
        // target_index is opaque above the pool; it is read here only to
        // prove that placement really did use both arenas.
        targets.insert(allocated->Id().target_index);
        live.push_back(std::move(*allocated));
    }
    EXPECT_EQ(targets.size(), 2u) << "placement never left the first arena";
    EXPECT_GE(pool->Usage(), 4 * kBlockSize);

    live.clear();
    EXPECT_EQ(pool->Usage(), 0u);
}

TEST_F(BlockPoolTest, AllocationFallsThroughWhenOneArenaCannotServeIt) {
    constexpr size_t kSmallArena = 64u << 10;
    constexpr size_t kLargeArena = 1u << 20;
    constexpr size_t kOversizedForSmall = 128u << 10;
    auto pool = MakePool({Arena(kSmallArena), Arena(kLargeArena)});
    ASSERT_NE(pool, nullptr);
    EXPECT_EQ(pool->Capacity(), kSmallArena + kLargeArena);

    // No request of this size can ever fit the first arena, so success proves
    // the pool failed over internally instead of surfacing the miss.
    std::vector<BlockAllocation> live;
    for (int i = 0; i < 4; ++i) {
        auto allocated = pool->Allocate(kOversizedForSmall, 0);
        ASSERT_TRUE(allocated.has_value()) << "iteration " << i;
        EXPECT_EQ(allocated->Id().target_index, 1u);
        live.push_back(std::move(*allocated));
    }
    EXPECT_GE(pool->Usage(), 4 * kOversizedForSmall);

    // A block that no arena can hold is still NO_AVAILABLE_HANDLE, not a
    // partial or split allocation.
    auto impossible = pool->Allocate(kSmallArena + kLargeArena, 0);
    ASSERT_FALSE(impossible.has_value());
    EXPECT_EQ(impossible.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    live.clear();
    EXPECT_EQ(pool->Usage(), 0u);
}

// ---------------------------------------------------------------------------
// Free() guards
// ---------------------------------------------------------------------------

TEST_F(BlockPoolTest, FreeingTheSamePhysicalIdTwiceIsRejected) {
    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);

    auto allocated = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value());
    const PhysicalBlockId id = allocated->Id();
    ASSERT_GT(pool->Usage(), 0u);

    ASSERT_TRUE(pool->Free(id).has_value());
    EXPECT_EQ(pool->Usage(), 0u);
    EXPECT_EQ(pool->Get(id), nullptr);

    auto second = pool->Free(id);
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(second.error(), ErrorCode::INVALID_PARAMS);
    // The rejected free must not have subtracted a second time, which would
    // let usage underflow and permanently over-report free space.
    EXPECT_EQ(pool->Usage(), 0u);

    // The stale lease hits the same guard on destruction and the pool stays
    // usable afterwards.
    allocated->Reset();
    EXPECT_EQ(pool->Usage(), 0u);

    auto reused = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(reused.has_value());
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x77);
    ASSERT_TRUE(reused->Data().Write(0, AsBytes(payload)).has_value());
    std::vector<uint8_t> readback(kBlockSize, 0);
    ASSERT_TRUE(reused->Data().Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, payload);
}

TEST_F(BlockPoolTest, FreeWithAForeignOrUnknownIdIsRejected) {
    auto pool = MakePool({Arena(kArenaCapacity)});
    auto other = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);
    ASSERT_NE(other, nullptr);
    ASSERT_NE(pool->Id(), other->Id());

    auto foreign = other->Allocate(kBlockSize, 0);
    ASSERT_TRUE(foreign.has_value());
    const size_t other_usage = other->Usage();

    auto stolen = pool->Free(foreign->Id());
    ASSERT_FALSE(stolen.has_value());
    EXPECT_EQ(stolen.error(), ErrorCode::INVALID_PARAMS);
    // The other pool's block is untouched: pool_id is what keeps two pools
    // from freeing each other's extents.
    EXPECT_EQ(other->Usage(), other_usage);
    EXPECT_NE(other->Get(foreign->Id()), nullptr);
    EXPECT_EQ(pool->Get(foreign->Id()), nullptr);

    const PhysicalBlockId unknown_local{.pool_id = pool->Id(),
                                        .target_index = 0,
                                        .local_id = 987654321,
                                        .generation = 1};
    auto unknown = pool->Free(unknown_local);
    ASSERT_FALSE(unknown.has_value());
    EXPECT_EQ(unknown.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(pool->Get(unknown_local), nullptr);

    const PhysicalBlockId bad_target{.pool_id = pool->Id(),
                                     .target_index = 7,
                                     .local_id = 1,
                                     .generation = 1};
    auto out_of_range = pool->Free(bad_target);
    ASSERT_FALSE(out_of_range.has_value());
    EXPECT_EQ(out_of_range.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(pool->Get(bad_target), nullptr);
}

TEST_F(BlockPoolTest, GetReturnsTheBorrowedHandleOnlyWhileTheLeaseIsAlive) {
    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);

    PhysicalBlockId id{};
    {
        auto allocated = pool->Allocate(kBlockSize, 0);
        ASSERT_TRUE(allocated.has_value());
        id = allocated->Id();
        BlockDataHandle* borrowed = pool->Get(id);
        ASSERT_NE(borrowed, nullptr);
        // The lease and the pool must name the same handle, otherwise the two
        // views of a block could diverge.
        EXPECT_EQ(borrowed, &allocated->Data());
        EXPECT_EQ(borrowed->Size(), kBlockSize);
    }
    EXPECT_EQ(pool->Get(id), nullptr);
}

// ---------------------------------------------------------------------------
// BlockAllocation
// ---------------------------------------------------------------------------

TEST_F(BlockPoolTest, BlockAllocationIsMoveOnlyAndMovesDisarmTheSource) {
    static_assert(!std::is_copy_constructible_v<BlockAllocation>,
                  "a copied lease would free the same block twice");
    static_assert(!std::is_copy_assignable_v<BlockAllocation>);
    static_assert(std::is_move_constructible_v<BlockAllocation>);
    static_assert(std::is_move_assignable_v<BlockAllocation>);

    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);

    auto first = pool->Allocate(kBlockSize, 0);
    auto second = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    const PhysicalBlockId first_id = first->Id();
    const size_t both_alive = pool->Usage();

    BlockAllocation moved(std::move(*first));
    EXPECT_TRUE(static_cast<bool>(moved));
    EXPECT_FALSE(static_cast<bool>(*first));
    EXPECT_EQ(first->Size(), 0u);
    EXPECT_EQ(moved.Id(), first_id);
    EXPECT_EQ(moved.Size(), kBlockSize);
    // A move transfers the lease; it does not release anything.
    EXPECT_EQ(pool->Usage(), both_alive);
    EXPECT_NE(pool->Get(first_id), nullptr);

    // Move-assignment must release whatever the target already held.
    moved = std::move(*second);
    EXPECT_TRUE(static_cast<bool>(moved));
    EXPECT_EQ(pool->Get(first_id), nullptr);
    EXPECT_LT(pool->Usage(), both_alive);
    EXPECT_GT(pool->Usage(), 0u);

    // The disarmed source destructors must not free the moved-to block.
    first->Reset();
    second->Reset();
    EXPECT_TRUE(static_cast<bool>(moved));
    EXPECT_NE(pool->Get(moved.Id()), nullptr);
}

TEST_F(BlockPoolTest, BlockAllocationResetIsIdempotentAndFreesExactlyOnce) {
    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);

    auto allocated = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value());
    const PhysicalBlockId id = allocated->Id();
    ASSERT_GT(pool->Usage(), 0u);

    allocated->Reset();
    EXPECT_FALSE(static_cast<bool>(*allocated));
    EXPECT_EQ(pool->Usage(), 0u);

    // Repeated Reset() (and the destructor that follows) must be silent
    // no-ops, not repeated Free() calls against a recycled id.
    allocated->Reset();
    allocated->Reset();
    EXPECT_EQ(pool->Usage(), 0u);

    auto reallocated = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(reallocated.has_value());
    const size_t used = pool->Usage();
    allocated->Reset();
    EXPECT_EQ(pool->Usage(), used) << "a stale lease released a live block";
    EXPECT_NE(pool->Get(reallocated->Id()), nullptr);

    // A default-constructed lease owns nothing at all.
    BlockAllocation empty;
    EXPECT_FALSE(static_cast<bool>(empty));
    EXPECT_EQ(empty.Size(), 0u);
    empty.Reset();
    EXPECT_EQ(pool->Usage(), used);
    EXPECT_EQ(pool->Get(id), nullptr);
}

// ---------------------------------------------------------------------------
// MutableBlock / CompletedBlock
// ---------------------------------------------------------------------------

TEST_F(BlockPoolTest, UnconsumedMutableBlockReturnsItsSpaceOnDestruction) {
    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);

    PhysicalBlockId id{};
    {
        auto allocated = pool->Allocate(kBlockSize, 0);
        ASSERT_TRUE(allocated.has_value());
        auto block = MutableBlock::MakeForTiler(std::move(*allocated));
        EXPECT_TRUE(static_cast<bool>(block));
        EXPECT_EQ(block.Size(), kBlockSize);
        id = block.PhysicalId();
        EXPECT_GT(pool->Usage(), 0u);

        const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x2a);
        EXPECT_TRUE(block.Write(0, AsBytes(payload)).has_value());
        // The range guard still applies through the wrapper.
        auto past_end = block.Write(kBlockSize, AsBytes(payload));
        ASSERT_FALSE(past_end.has_value());
        EXPECT_EQ(past_end.error(), ErrorCode::INVALID_PARAMS);
    }
    // Abandoning a half-written block is a complete rollback: no leak, no
    // half-visible state.
    EXPECT_EQ(pool->Usage(), 0u);
    EXPECT_EQ(pool->Get(id), nullptr);
}

TEST_F(BlockPoolTest, ExplicitAbortReleasesTheMutableBlockImmediately) {
    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);

    auto allocated = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value());
    auto block = MutableBlock::MakeForTiler(std::move(*allocated));
    const PhysicalBlockId id = block.PhysicalId();

    block.Abort();
    EXPECT_FALSE(static_cast<bool>(block));
    EXPECT_EQ(pool->Usage(), 0u);
    EXPECT_EQ(pool->Get(id), nullptr);

    // An aborted block can no longer accept writes or be completed.
    const std::vector<uint8_t> payload = Pattern(8, 0x01);
    auto write_after_abort = block.Write(0, AsBytes(payload));
    ASSERT_FALSE(write_after_abort.has_value());
    EXPECT_EQ(write_after_abort.error(), ErrorCode::INTERNAL_ERROR);

    auto completed = std::move(block).Complete("key");
    ASSERT_FALSE(completed.has_value());
    EXPECT_EQ(completed.error(), ErrorCode::INTERNAL_ERROR);

    block.Abort();  // idempotent
    EXPECT_EQ(pool->Usage(), 0u);
}

TEST_F(BlockPoolTest, CompleteConsumesTheMutableBlockAndKeepsTheData) {
    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);

    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x3c);
    PhysicalBlockId id{};
    {
        auto allocated = pool->Allocate(kBlockSize, 0);
        ASSERT_TRUE(allocated.has_value());
        auto block = MutableBlock::MakeForTiler(std::move(*allocated));
        id = block.PhysicalId();
        ASSERT_TRUE(block.Write(0, AsBytes(payload)).has_value());
        const size_t used_before = pool->Usage();

        auto completed = std::move(block).Complete("tiler/key");
        ASSERT_TRUE(completed.has_value()) << toString(completed.error());
        EXPECT_TRUE(static_cast<bool>(*completed));
        EXPECT_EQ(completed->Key(), "tiler/key");
        EXPECT_EQ(completed->Size(), kBlockSize);
        EXPECT_EQ(completed->PhysicalId(), id);

        // The source is consumed, so nothing can write to a completed block.
        EXPECT_FALSE(static_cast<bool>(block));
        auto late_write = block.Write(0, AsBytes(payload));
        ASSERT_FALSE(late_write.has_value());
        EXPECT_EQ(late_write.error(), ErrorCode::INTERNAL_ERROR);
        EXPECT_FALSE(block.GetTransferAddress().has_value());

        // Handing ownership over is not a release, and the bytes survive it.
        EXPECT_EQ(pool->Usage(), used_before);
        BlockDataHandle* handle = pool->Get(id);
        ASSERT_NE(handle, nullptr);
        std::vector<uint8_t> readback(kBlockSize, 0);
        ASSERT_TRUE(handle->Read(0, AsWritableBytes(readback)).has_value());
        EXPECT_EQ(readback, payload);
    }
    // An unregistered CompletedBlock is still a rollback when it dies.
    EXPECT_EQ(pool->Usage(), 0u);
    EXPECT_EQ(pool->Get(id), nullptr);
}

TEST_F(BlockPoolTest, AbortedCompletedBlockReturnsItsSpaceImmediately) {
    auto pool = MakePool({Arena(kArenaCapacity)});
    ASSERT_NE(pool, nullptr);

    auto allocated = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value());
    auto block = MutableBlock::MakeForTiler(std::move(*allocated));
    auto completed = std::move(block).Complete("aborted/key");
    ASSERT_TRUE(completed.has_value());
    const PhysicalBlockId id = completed->PhysicalId();
    ASSERT_GT(pool->Usage(), 0u);

    completed->Abort();
    EXPECT_FALSE(static_cast<bool>(*completed));
    EXPECT_EQ(pool->Usage(), 0u);
    EXPECT_EQ(pool->Get(id), nullptr);

    // The destructor must not free the same extent a second time.
    completed->Abort();
    EXPECT_EQ(pool->Usage(), 0u);
}

// ---------------------------------------------------------------------------
// CreateBlockPool
// ---------------------------------------------------------------------------

// Free must credit back exactly what Allocate charged. Recomputing the
// padding from the arena's alignment instead of the requested one leaks a few
// bytes of accounting per allocate/free cycle, and a long-running tier
// eventually reports itself full while the allocator sees it as empty.
TEST_F(BlockPoolTest, UsageAccountingIsExactWhenAlignmentsDiffer) {
    DramArenaConfig arena;
    arena.capacity_bytes = 4ULL * 1024 * 1024;
    arena.alignment = 64;
    auto pool = MakePool({arena});
    ASSERT_NE(pool, nullptr);
    ASSERT_EQ(pool->Usage(), 0u);

    constexpr size_t kBlockSize = 4096;
    for (int i = 0; i < 32; ++i) {
        // Stricter than the arena, so the charged and credited paddings differ
        // if they are derived from different alignments.
        auto block = pool->Allocate(kBlockSize, 4096);
        ASSERT_TRUE(block.has_value()) << "iteration " << i;
        EXPECT_GT(pool->Usage(), 0u);
    }
    EXPECT_EQ(pool->Usage(), 0u)
        << "usage drifted after 32 allocate/free cycles";

    // Looser than the arena, which drifts the other way.
    for (int i = 0; i < 32; ++i) {
        auto block = pool->Allocate(kBlockSize, 1);
        ASSERT_TRUE(block.has_value()) << "iteration " << i;
    }
    EXPECT_EQ(pool->Usage(), 0u);
}

TEST_F(BlockPoolTest, CreateBlockPoolBuildsAnInitializedDramPool) {
    DramBlockPoolConfig dram;
    dram.arenas.push_back(Arena(kArenaCapacity));
    dram.arenas.push_back(Arena(2 * kArenaCapacity, 4096));
    const BlockPoolConfig config = dram;

    auto pool = CreateBlockPool(config, std::shared_ptr<TransferEngine>{});
    ASSERT_TRUE(pool.has_value()) << toString(pool.error());
    ASSERT_NE(pool.value(), nullptr);
    // The factory returns an already-initialized pool: callers above it never
    // see an Init() step, only the BlockPool interface.
    EXPECT_EQ(pool.value()->Capacity(), 3 * kArenaCapacity);
    EXPECT_EQ(pool.value()->Usage(), 0u);

    const BlockPoolCapabilities caps = pool.value()->Capabilities();
    EXPECT_TRUE(caps.direct_cpu_access);
    EXPECT_TRUE(caps.te_addressable);
    EXPECT_FALSE(caps.persistent);
    EXPECT_EQ(caps.minimum_alignment, 4096u);

    auto allocated = pool.value()->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value());
    EXPECT_EQ(allocated->Id().pool_id, pool.value()->Id());
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x09);
    ASSERT_TRUE(allocated->Data().Write(0, AsBytes(payload)).has_value());
    std::vector<uint8_t> readback(kBlockSize, 0);
    ASSERT_TRUE(
        allocated->Data().Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, payload);
}

TEST_F(BlockPoolTest, CreateBlockPoolPropagatesAnInvalidDramConfiguration) {
    DramBlockPoolConfig dram;
    dram.arenas.push_back(Arena(0));
    const BlockPoolConfig config = dram;

    auto pool = CreateBlockPool(config, std::shared_ptr<TransferEngine>{});
    ASSERT_FALSE(pool.has_value());
    EXPECT_EQ(pool.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(BlockPoolTest, CreateBlockPoolBuildsAnInitializedSsdPool) {
    SSDBlockPoolConfig ssd;
    SSDDeviceConfig device;
    device.file_path = StoragePath("factory.data");
    device.capacity_bytes = 1024 * 1024;
    ssd.devices.push_back(device);
    const BlockPoolConfig config = ssd;

    auto pool = CreateBlockPool(config, std::shared_ptr<TransferEngine>{});
    ASSERT_TRUE(pool.has_value()) << toString(pool.error());
    ASSERT_NE(pool.value(), nullptr);
    EXPECT_EQ(pool.value()->Capacity(), 1024u * 1024u);

    const BlockPoolCapabilities caps = pool.value()->Capabilities();
    EXPECT_FALSE(caps.direct_cpu_access);
    EXPECT_FALSE(caps.te_addressable);
    EXPECT_TRUE(caps.persistent);
}

// ---------------------------------------------------------------------------
// SSDBlockPool
// ---------------------------------------------------------------------------

TEST_F(BlockPoolTest, SsdBlockRoundTripsThroughTheFile) {
    auto pool = MakeSsdPool({SsdDevice("roundtrip.data", 4 * 1024 * 1024)});
    ASSERT_NE(pool, nullptr);

    auto allocated = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value()) << toString(allocated.error());

    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x5a);
    ASSERT_TRUE(allocated->Data().Write(0, AsBytes(payload)).has_value());
    ASSERT_TRUE(allocated->Data().Commit().has_value());

    std::vector<uint8_t> readback(kBlockSize, 0);
    ASSERT_TRUE(
        allocated->Data().Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, payload);

    // Partial read at an offset, so the pread loop is exercised with a
    // non-zero start rather than only whole-block IO.
    std::vector<uint8_t> tail(64, 0);
    ASSERT_TRUE(allocated->Data()
                    .Read(kBlockSize - 64, AsWritableBytes(tail))
                    .has_value());
    EXPECT_EQ(tail, std::vector<uint8_t>(payload.end() - 64, payload.end()));
}

// The defining property of a slow tier: it holds data without ever exposing a
// pointer, so nothing above it can publish an address for storage that is not
// memory.
TEST_F(BlockPoolTest, SsdBlocksNeverExposeAnAddress) {
    auto pool = MakeSsdPool({SsdDevice("noaddr.data", 1024 * 1024)});
    ASSERT_NE(pool, nullptr);

    auto allocated = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value());
    EXPECT_FALSE(allocated->Data().GetTransferAddress().has_value());

    auto block = MutableBlock::MakeForTiler(std::move(*allocated));
    EXPECT_FALSE(block.GetTransferAddress().has_value());
}

TEST_F(BlockPoolTest, SsdReadAndWriteOutsideTheBlockAreRejected) {
    auto pool = MakeSsdPool({SsdDevice("range.data", 1024 * 1024)});
    ASSERT_NE(pool, nullptr);
    auto allocated = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value());

    std::vector<uint8_t> buffer(kBlockSize, 0);
    auto past_end = allocated->Data().Read(1, AsWritableBytes(buffer));
    ASSERT_FALSE(past_end.has_value());
    EXPECT_EQ(past_end.error(), ErrorCode::INVALID_PARAMS);

    auto beyond = allocated->Data().Write(kBlockSize + 1, AsBytes(buffer));
    ASSERT_FALSE(beyond.has_value());
    EXPECT_EQ(beyond.error(), ErrorCode::INVALID_PARAMS);

    // offset + length wraps; the bound check must come after the overflow one.
    std::vector<uint8_t> small(8, 0);
    auto overflowing = allocated->Data().Read(
        std::numeric_limits<size_t>::max() - 3, AsWritableBytes(small));
    ASSERT_FALSE(overflowing.has_value());
    EXPECT_EQ(overflowing.error(), ErrorCode::INVALID_PARAMS);
}

// One extent must not overlap another: writing through one block cannot be
// visible through a different one.
TEST_F(BlockPoolTest, SsdExtentsDoNotOverlap) {
    auto pool = MakeSsdPool({SsdDevice("extents.data", 4 * 1024 * 1024)});
    ASSERT_NE(pool, nullptr);

    std::vector<BlockAllocation> blocks;
    for (int i = 0; i < 8; ++i) {
        auto allocated = pool->Allocate(kBlockSize, 0);
        ASSERT_TRUE(allocated.has_value()) << "block " << i;
        const std::vector<uint8_t> payload =
            Pattern(kBlockSize, static_cast<uint8_t>(0x10 + i));
        ASSERT_TRUE(allocated->Data().Write(0, AsBytes(payload)).has_value());
        blocks.push_back(std::move(*allocated));
    }
    for (int i = 0; i < 8; ++i) {
        std::vector<uint8_t> readback(kBlockSize, 0);
        ASSERT_TRUE(
            blocks[i].Data().Read(0, AsWritableBytes(readback)).has_value());
        EXPECT_EQ(readback, Pattern(kBlockSize, static_cast<uint8_t>(0x10 + i)))
            << "block " << i << " was overwritten by a neighbour";
    }
}

TEST_F(BlockPoolTest, SsdExtentsAreReusedAfterRelease) {
    auto pool = MakeSsdPool({SsdDevice("reuse.data", 256 * 1024)});
    ASSERT_NE(pool, nullptr);

    // Fill the device, drop everything, then fill it again: a pure extent
    // allocator must hand the same space back out.
    size_t first_round = 0;
    {
        std::vector<BlockAllocation> blocks;
        for (;;) {
            auto allocated = pool->Allocate(kBlockSize, 0);
            if (!allocated.has_value()) {
                EXPECT_EQ(allocated.error(), ErrorCode::NO_AVAILABLE_HANDLE);
                break;
            }
            blocks.push_back(std::move(*allocated));
            ++first_round;
        }
        ASSERT_GT(first_round, 0u);
    }
    EXPECT_EQ(pool->Usage(), 0u);

    size_t second_round = 0;
    std::vector<BlockAllocation> blocks;
    for (;;) {
        auto allocated = pool->Allocate(kBlockSize, 0);
        if (!allocated.has_value()) break;
        blocks.push_back(std::move(*allocated));
        ++second_round;
    }
    EXPECT_EQ(second_round, first_round);
}

// The V2-defining constraint on the slow tier. V1's StorageTier evicted whole
// buckets from under the metadata layer when it ran out; V2 reports the
// exhaustion and touches nothing.
TEST_F(BlockPoolTest, ExhaustedSsdPoolKeepsEveryLiveBlock) {
    auto pool = MakeSsdPool({SsdDevice("exhaust.data", 256 * 1024)});
    ASSERT_NE(pool, nullptr);

    std::vector<BlockAllocation> blocks;
    for (;;) {
        auto allocated = pool->Allocate(kBlockSize, 0);
        if (!allocated.has_value()) {
            EXPECT_EQ(allocated.error(), ErrorCode::NO_AVAILABLE_HANDLE);
            break;
        }
        const std::vector<uint8_t> payload =
            Pattern(kBlockSize, static_cast<uint8_t>(blocks.size() & 0xff));
        ASSERT_TRUE(allocated->Data().Write(0, AsBytes(payload)).has_value());
        blocks.push_back(std::move(*allocated));
    }
    ASSERT_GT(blocks.size(), 1u);

    const size_t used_at_exhaustion = pool->Usage();
    auto retry = pool->Allocate(kBlockSize, 0);
    ASSERT_FALSE(retry.has_value());
    EXPECT_EQ(pool->Usage(), used_at_exhaustion)
        << "the pool reclaimed space on its own";

    for (size_t i = 0; i < blocks.size(); ++i) {
        std::vector<uint8_t> readback(kBlockSize, 0);
        ASSERT_TRUE(
            blocks[i].Data().Read(0, AsWritableBytes(readback)).has_value());
        EXPECT_EQ(readback, Pattern(kBlockSize, static_cast<uint8_t>(i & 0xff)))
            << "block " << i << " was evicted underneath its owner";
    }
}

TEST_F(BlockPoolTest, SsdPoolSpreadsAcrossDevicesAndFallsThrough) {
    auto pool = MakeSsdPool({SsdDevice("dev_small.data", 128 * 1024),
                             SsdDevice("dev_large.data", 4 * 1024 * 1024)});
    ASSERT_NE(pool, nullptr);
    EXPECT_EQ(pool->Capacity(), 128u * 1024u + 4u * 1024u * 1024u);

    std::vector<BlockAllocation> blocks;
    std::set<uint32_t> devices_used;
    for (int i = 0; i < 16; ++i) {
        auto allocated = pool->Allocate(kBlockSize, 0);
        ASSERT_TRUE(allocated.has_value()) << "block " << i;
        devices_used.insert(allocated->Id().target_index);
        blocks.push_back(std::move(*allocated));
    }
    EXPECT_EQ(devices_used.size(), 2u) << "both devices should have been used";

    // Keep going past the small device's capacity: the pool must fall through
    // to the large one rather than reporting exhaustion.
    for (int i = 0; i < 64; ++i) {
        auto allocated = pool->Allocate(kBlockSize, 0);
        ASSERT_TRUE(allocated.has_value())
            << "fallthrough failed at block " << i;
        blocks.push_back(std::move(*allocated));
    }
}

TEST_F(BlockPoolTest, SsdFsyncOnCommitIsHonoured) {
    SSDDeviceConfig device = SsdDevice("fsync.data", 1024 * 1024);
    device.fsync_on_commit = true;
    auto pool = MakeSsdPool({device});
    ASSERT_NE(pool, nullptr);

    auto allocated = pool->Allocate(kBlockSize, 0);
    ASSERT_TRUE(allocated.has_value());
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x77);
    ASSERT_TRUE(allocated->Data().Write(0, AsBytes(payload)).has_value());
    ASSERT_TRUE(allocated->Data().Commit().has_value());

    std::vector<uint8_t> readback(kBlockSize, 0);
    ASSERT_TRUE(
        allocated->Data().Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, payload);
}

TEST_F(BlockPoolTest, SsdUsageAccountingIsExactWhenAlignmentsDiffer) {
    SSDDeviceConfig device = SsdDevice("align.data", 4 * 1024 * 1024);
    device.alignment = 512;
    auto pool = MakeSsdPool({device});
    ASSERT_NE(pool, nullptr);

    for (int i = 0; i < 32; ++i) {
        auto block = pool->Allocate(kBlockSize, 4096);
        ASSERT_TRUE(block.has_value()) << "iteration " << i;
    }
    EXPECT_EQ(pool->Usage(), 0u) << "usage drifted across allocate/free cycles";
}

TEST_F(BlockPoolTest, InvalidSsdConfigurationIsRejected) {
    SSDBlockPoolConfig empty;
    EXPECT_EQ(SSDBlockPool(empty).Init().error(), ErrorCode::INVALID_PARAMS);

    SSDBlockPoolConfig no_path;
    SSDDeviceConfig missing;
    missing.capacity_bytes = 1024 * 1024;
    no_path.devices.push_back(missing);
    EXPECT_EQ(SSDBlockPool(no_path).Init().error(), ErrorCode::INVALID_PARAMS);

    SSDBlockPoolConfig zero_capacity;
    zero_capacity.devices.push_back(SsdDevice("zero.data", 0));
    EXPECT_EQ(SSDBlockPool(zero_capacity).Init().error(),
              ErrorCode::INVALID_PARAMS);

    SSDBlockPoolConfig bad_alignment;
    SSDDeviceConfig odd = SsdDevice("odd.data", 1024 * 1024);
    odd.alignment = 3000;
    bad_alignment.devices.push_back(odd);
    EXPECT_EQ(SSDBlockPool(bad_alignment).Init().error(),
              ErrorCode::INVALID_PARAMS);
}

}  // namespace
}  // namespace mooncake::v2
