#include "p2p/client/v2/block_index.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

#include "p2p/client/v2/block.h"
#include "p2p/client/v2/block_pool.h"
#include "p2p/client/v2/block_registry.h"
#include "types.h"

namespace mooncake::v2 {
namespace {

constexpr UUID kTilerId{0xA11CE, 0xB10C};
constexpr size_t kArenaBytes = 4u << 20;
constexpr size_t kBlockBytes = 64;

std::span<const std::byte> AsBytes(std::string_view s) {
    return std::as_bytes(std::span<const char>(s.data(), s.size()));
}

std::span<std::byte> AsWritableBytes(std::string& s) {
    return std::as_writable_bytes(std::span<char>(s.data(), s.size()));
}

// Every key gets its own byte pattern so a snapshot that survives an Erase can
// be proven to still see *its* data rather than a recycled neighbour's.
std::string PayloadFor(std::string_view key) {
    std::string payload(key);
    payload.resize(kBlockBytes, '.');
    return payload;
}

class BlockIndexTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("BlockIndexTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        DramArenaConfig arena;
        arena.capacity_bytes = kArenaBytes;
        // A single arena with alignment 1 makes Usage() exactly the sum of the
        // block sizes, so the deferred-reclaim assertions can be equalities
        // instead of inequalities.
        arena.alignment = 1;
        DramBlockPoolConfig pool_config;
        pool_config.arenas.push_back(arena);

        // No TransferEngine: BlockIndex never touches one, and registering
        // memory would drag an RDMA device into a pure index test.
        pool_ = std::make_shared<DramBlockPool>(pool_config, nullptr);
        ASSERT_TRUE(pool_->Init().has_value());

        registry_ = BlockRegistry(BlockRegistryConfig{});
        index_ = std::make_unique<BlockIndex>(kTilerId, BlockIndexConfig{});
    }

    BlockRegistrationHandle Register(std::string_view key) {
        auto handle = registry_.Register(key);
        CHECK(handle.has_value()) << "test setup: Register failed";
        return handle.value();
    }

    CompletedBlock MakeCompleted(std::string_view key,
                                 std::string_view payload) {
        auto allocation = pool_->Allocate(payload.size(), /*alignment=*/1);
        CHECK(allocation.has_value()) << "test setup: Allocate failed";
        auto mutable_block =
            MutableBlock::MakeForTiler(std::move(allocation.value()));
        CHECK(mutable_block.Write(0, AsBytes(payload)).has_value());
        auto completed = std::move(mutable_block).Complete(std::string(key));
        CHECK(completed.has_value()) << "test setup: Complete failed";
        return std::move(completed.value());
    }

    tl::expected<ImmutableBlock, ErrorCode> InsertPayload(
        const BlockRegistrationHandle& registration, std::string_view payload) {
        return index_->Insert(MakeCompleted(registration.Key(), payload),
                              registration);
    }

    // Drops the snapshot Insert returns, so a test that reasons about
    // reference counts starts from "the index is the only holder".
    void InsertAndForget(const BlockRegistrationHandle& registration,
                         std::string_view payload) {
        auto inserted = InsertPayload(registration, payload);
        CHECK(inserted.has_value()) << "test setup: Insert failed, error="
                                    << toString(inserted.error());
    }

    std::string ReadAll(const ImmutableBlock& block) {
        std::string out(block.Size(), '\0');
        auto read = block.Read(0, AsWritableBytes(out));
        CHECK(read.has_value()) << "Read failed on a live snapshot";
        return out;
    }

    std::shared_ptr<DramBlockPool> pool_;
    BlockRegistry registry_;
    std::unique_ptr<BlockIndex> index_;
};

TEST_F(BlockIndexTest, InsertedBlockIsVisibleWithItsKeySizeAndRegistration) {
    const std::string payload = PayloadFor("alpha");
    auto registration = Register("alpha");

    auto inserted = InsertPayload(registration, payload);
    ASSERT_TRUE(inserted.has_value());
    EXPECT_TRUE(static_cast<bool>(inserted.value()));

    auto found = index_->Lookup(registration);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->Key(), "alpha");
    EXPECT_EQ(found->Size(), payload.size());
    EXPECT_EQ(found->Registration(), registration.Id());
    // The entry is owned by this tiler and by no other, which is what makes a
    // BlockId from another tiler's policy token impossible to confuse here.
    EXPECT_EQ(found->Id().tiler_id, kTilerId);
    EXPECT_EQ(found->Id(), inserted->Id());
    EXPECT_EQ(ReadAll(*found), payload);
    // Lookup hands out a second reference to the same entry, never a copy.
    EXPECT_EQ(found->Entry().get(), inserted->Entry().get());
}

TEST_F(BlockIndexTest, InsertConsumesTheCompletedBlockOnSuccess) {
    auto registration = Register("consumed");
    CompletedBlock block = MakeCompleted("consumed", PayloadFor("consumed"));
    ASSERT_TRUE(static_cast<bool>(block));

    auto inserted = index_->Insert(std::move(block), registration);
    ASSERT_TRUE(inserted.has_value());
    // A consumed wrapper cannot be aborted or re-inserted, so its destructor
    // will not double-free the allocation the index now owns.
    EXPECT_FALSE(static_cast<bool>(block));
}

TEST_F(BlockIndexTest,
       DuplicateRegistrationIsRejectedAndTheRejectedBlockIsReclaimed) {
    auto registration = Register("dup");
    InsertAndForget(registration, PayloadFor("dup"));
    const size_t usage_after_first = pool_->Usage();

    CompletedBlock second = MakeCompleted("dup", PayloadFor("dup-second"));
    ASSERT_TRUE(static_cast<bool>(second));
    EXPECT_GT(pool_->Usage(), usage_after_first);

    auto rejected = index_->Insert(std::move(second), registration);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    // Failure is a rollback, not a retry point: the wrapper is spent and its
    // allocation has already gone back to the pool.
    EXPECT_FALSE(static_cast<bool>(second));
    EXPECT_EQ(pool_->Usage(), usage_after_first);

    // The rejected insert must not have disturbed the winner.
    auto found = index_->Lookup(registration);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(ReadAll(*found), PayloadFor("dup"));
    EXPECT_EQ(index_->Stats().entry_count, 1u);
}

TEST_F(BlockIndexTest,
       InsertWithoutARegistrationIsRejectedAndStillConsumesTheBlock) {
    CompletedBlock block = MakeCompleted("orphan", PayloadFor("orphan"));
    ASSERT_TRUE(static_cast<bool>(block));

    auto rejected = index_->Insert(std::move(block), BlockRegistrationHandle{});
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(static_cast<bool>(block));
    EXPECT_EQ(pool_->Usage(), 0u);
    EXPECT_EQ(index_->Stats().entry_count, 0u);
}

TEST_F(BlockIndexTest, LookupOfAnUnindexedRegistrationMisses) {
    auto indexed = Register("present");
    InsertAndForget(indexed, PayloadFor("present"));

    // A registration that exists in the registry but was never inserted here
    // is the normal "this tiler does not hold the key" answer.
    auto absent = Register("absent");
    EXPECT_FALSE(index_->Lookup(absent).has_value());
    // An empty handle must miss rather than dereference anything.
    EXPECT_FALSE(index_->Lookup(BlockRegistrationHandle{}).has_value());
    EXPECT_EQ(index_->Erase(BlockRegistrationHandle{}), nullptr);
    EXPECT_EQ(index_->Erase(absent), nullptr);
}

TEST_F(BlockIndexTest, EraseDetachesTheEntryAndSubsequentLookupsMiss) {
    auto registration = Register("gone");
    InsertAndForget(registration, PayloadFor("gone"));

    BlockEntryPtr detached = index_->Erase(registration);
    ASSERT_NE(detached, nullptr);
    // Ownership moves to the caller: the physical free happens where the
    // caller drops this, outside any shard lock.
    EXPECT_EQ(detached->block.key, "gone");
    EXPECT_EQ(detached->block.registration.Id(), registration.Id());

    EXPECT_FALSE(index_->Lookup(registration).has_value());
    EXPECT_EQ(index_->Stats().entry_count, 0u);
    // Erasing twice must not hand the same entry out to two owners.
    EXPECT_EQ(index_->Erase(registration), nullptr);
}

TEST_F(BlockIndexTest, EraseWithAStaleBlockIdLeavesTheEntryInPlace) {
    auto registration = Register("guarded");
    InsertAndForget(registration, PayloadFor("guarded"));

    auto live = index_->Lookup(registration);
    ASSERT_TRUE(live.has_value());
    BlockId stale = live->Id();
    stale.local_id += 1;

    // A policy token naming a block that has already been replaced must not
    // remove whatever currently occupies the registration.
    EXPECT_EQ(index_->Erase(registration, stale), nullptr);
    EXPECT_TRUE(index_->Lookup(registration).has_value());
    EXPECT_EQ(index_->Stats().entry_count, 1u);

    BlockId wrong_generation = live->Id();
    wrong_generation.generation += 1;
    EXPECT_EQ(index_->Erase(registration, wrong_generation), nullptr);
    EXPECT_TRUE(index_->Lookup(registration).has_value());

    // The matching token is the only one allowed through.
    EXPECT_NE(index_->Erase(registration, live->Id()), nullptr);
    EXPECT_FALSE(index_->Lookup(registration).has_value());
}

TEST_F(BlockIndexTest, PhysicalMemoryIsHeldUntilTheLastSnapshotIsReleased) {
    const std::string payload = PayloadFor("deferred-snapshot");
    auto registration = Register("deferred-snapshot");
    ASSERT_EQ(pool_->Usage(), 0u);
    InsertAndForget(registration, payload);
    ASSERT_EQ(pool_->Usage(), payload.size());

    auto snapshot = index_->Lookup(registration);
    ASSERT_TRUE(snapshot.has_value());

    BlockEntryPtr detached = index_->Erase(registration);
    ASSERT_NE(detached, nullptr);
    EXPECT_FALSE(index_->Lookup(registration).has_value());
    EXPECT_EQ(pool_->Usage(), payload.size());

    // An in-flight read started before the Erase must still see its own data;
    // this is the invariant that lets Delete run without draining readers.
    EXPECT_EQ(ReadAll(*snapshot), payload);

    detached.reset();
    EXPECT_EQ(pool_->Usage(), payload.size());

    snapshot.reset();
    EXPECT_EQ(pool_->Usage(), 0u);
}

TEST_F(BlockIndexTest, PhysicalMemoryIsHeldUntilTheDetachedEntryIsReleased) {
    const std::string payload = PayloadFor("deferred-detached");
    auto registration = Register("deferred-detached");
    InsertAndForget(registration, payload);
    ASSERT_EQ(pool_->Usage(), payload.size());

    auto snapshot = index_->Lookup(registration);
    ASSERT_TRUE(snapshot.has_value());
    BlockEntryPtr detached = index_->Erase(registration);
    ASSERT_NE(detached, nullptr);

    // Mirror of the previous test with the release order swapped: neither
    // holder alone may trigger the free.
    snapshot.reset();
    EXPECT_EQ(pool_->Usage(), payload.size());

    detached.reset();
    EXPECT_EQ(pool_->Usage(), 0u);
}

TEST_F(BlockIndexTest, EveryEntryLivesOnlyInTheShardShardForSelects) {
    constexpr int kKeys = 200;
    std::vector<BlockRegistrationHandle> registrations;
    for (int i = 0; i < kKeys; ++i) {
        const std::string key = "route-" + std::to_string(i);
        registrations.push_back(Register(key));
        InsertAndForget(registrations.back(), PayloadFor(key));
    }

    // Nothing may be reachable through a shard other than the one the hash
    // names, otherwise an Erase taking a single shard lock could miss it.
    size_t seen = 0;
    for (size_t shard = 0; shard < index_->ShardCount(); ++shard) {
        for (const auto& entry : index_->SnapshotShard(shard)) {
            EXPECT_EQ(index_->ShardFor(entry->block.registration.Id()), shard);
            ++seen;
        }
    }
    EXPECT_EQ(seen, static_cast<size_t>(kKeys));

    for (const auto& registration : registrations) {
        const size_t owner = index_->ShardFor(registration.Id());
        bool found = false;
        for (const auto& entry : index_->SnapshotShard(owner)) {
            found =
                found || entry->block.registration.Id() == registration.Id();
        }
        EXPECT_TRUE(found) << "key " << registration.Key()
                           << " missing from shard " << owner;
    }
}

TEST_F(BlockIndexTest, SnapshotOfEveryShardYieldsEachEntryExactlyOnce) {
    constexpr int kKeys = 128;
    // The handles are dropped at the end of each iteration on purpose: the
    // registry only holds weak references, so the walk below is also a check
    // that the entry keeps its registration identity alive on its own.
    std::unordered_set<std::string> expected_keys;
    for (int i = 0; i < kKeys; ++i) {
        const std::string key = "walk-" + std::to_string(i);
        auto registration = Register(key);
        InsertAndForget(registration, PayloadFor(key));
        expected_keys.insert(key);
    }

    std::unordered_set<std::string> walked;
    size_t total = 0;
    for (size_t shard = 0; shard < index_->ShardCount(); ++shard) {
        for (const auto& entry : index_->SnapshotShard(shard)) {
            ++total;
            walked.insert(entry->block.key);
        }
    }
    EXPECT_EQ(total, static_cast<size_t>(kKeys));
    EXPECT_EQ(walked, expected_keys);

    // An out-of-range shard is an empty batch, not a crash: callers iterate
    // shard ids without re-reading ShardCount() under a lock.
    EXPECT_TRUE(index_->SnapshotShard(index_->ShardCount()).empty());
}

TEST_F(BlockIndexTest, StatsFollowInsertedAndErasedBytes) {
    auto stats = index_->Stats();
    EXPECT_EQ(stats.entry_count, 0u);
    EXPECT_EQ(stats.indexed_bytes, 0u);

    const std::string small(16, 'a');
    const std::string medium(128, 'b');
    const std::string large(512, 'c');
    auto small_reg = Register("small");
    auto medium_reg = Register("medium");
    auto large_reg = Register("large");
    InsertAndForget(small_reg, small);
    InsertAndForget(medium_reg, medium);
    InsertAndForget(large_reg, large);

    stats = index_->Stats();
    EXPECT_EQ(stats.entry_count, 3u);
    EXPECT_EQ(stats.indexed_bytes, small.size() + medium.size() + large.size());

    ASSERT_NE(index_->Erase(medium_reg), nullptr);
    stats = index_->Stats();
    EXPECT_EQ(stats.entry_count, 2u);
    // Erase must subtract the erased block's own size, not a running average.
    EXPECT_EQ(stats.indexed_bytes, small.size() + large.size());

    // A rejected Erase must not move the counters at all.
    BlockId stale{kTilerId, 999999, 1};
    EXPECT_EQ(index_->Erase(large_reg, stale), nullptr);
    stats = index_->Stats();
    EXPECT_EQ(stats.entry_count, 2u);
    EXPECT_EQ(stats.indexed_bytes, small.size() + large.size());
}

TEST_F(BlockIndexTest, DrainEmptiesEveryShardAndHandsBackEveryEntry) {
    constexpr int kKeys = 100;
    std::vector<BlockRegistrationHandle> registrations;
    for (int i = 0; i < kKeys; ++i) {
        const std::string key = "drain-" + std::to_string(i);
        registrations.push_back(Register(key));
        InsertAndForget(registrations.back(), PayloadFor(key));
    }
    const size_t usage_before = pool_->Usage();
    ASSERT_EQ(usage_before, static_cast<size_t>(kKeys) * kBlockBytes);

    std::vector<BlockEntryPtr> drained = index_->Drain();
    EXPECT_EQ(drained.size(), static_cast<size_t>(kKeys));

    auto stats = index_->Stats();
    EXPECT_EQ(stats.entry_count, 0u);
    EXPECT_EQ(stats.indexed_bytes, 0u);
    for (size_t shard = 0; shard < index_->ShardCount(); ++shard) {
        EXPECT_TRUE(index_->SnapshotShard(shard).empty());
    }
    for (const auto& registration : registrations) {
        EXPECT_FALSE(index_->Lookup(registration).has_value());
    }

    // Drain transfers ownership rather than destroying: the caller decides
    // when the physical memory goes back, exactly as with Erase.
    EXPECT_EQ(pool_->Usage(), usage_before);
    drained.clear();
    EXPECT_EQ(pool_->Usage(), 0u);

    EXPECT_TRUE(index_->Drain().empty());
}

TEST_F(BlockIndexTest, BlockLocalIdsAreUniqueAcrossShards) {
    constexpr int kKeys = 500;
    std::unordered_set<uint64_t> local_ids;
    for (int i = 0; i < kKeys; ++i) {
        const std::string key = "id-" + std::to_string(i);
        auto registration = Register(key);
        auto inserted = InsertPayload(registration, PayloadFor(key));
        ASSERT_TRUE(inserted.has_value());
        // Per-shard sequences are interleaved by shard id; a collision would
        // let a stale policy token match a block it never named.
        EXPECT_TRUE(local_ids.insert(inserted->Id().local_id).second)
            << "duplicate local_id " << inserted->Id().local_id;
    }
    EXPECT_EQ(local_ids.size(), static_cast<size_t>(kKeys));
}

TEST_F(BlockIndexTest,
       ConcurrentInsertsOfDistinctRegistrationsAllBecomeVisible) {
    constexpr int kThreads = 8;
    constexpr int kPerThread = 64;
    std::vector<std::string> keys;
    std::vector<BlockRegistrationHandle> registrations;
    for (int i = 0; i < kThreads * kPerThread; ++i) {
        keys.push_back("concurrent-" + std::to_string(i));
        registrations.push_back(Register(keys.back()));
    }

    std::atomic<bool> go{false};
    std::atomic<int> failures{0};
    std::vector<std::thread> writers;
    for (int t = 0; t < kThreads; ++t) {
        writers.emplace_back([&, t] {
            while (!go.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int i = 0; i < kPerThread; ++i) {
                const int index = t * kPerThread + i;
                auto inserted = index_->Insert(
                    MakeCompleted(keys[index], PayloadFor(keys[index])),
                    registrations[index]);
                if (!inserted) ++failures;
            }
        });
    }
    go.store(true, std::memory_order_release);
    for (auto& writer : writers) writer.join();

    EXPECT_EQ(failures.load(), 0);
    auto stats = index_->Stats();
    EXPECT_EQ(stats.entry_count, keys.size());
    EXPECT_EQ(stats.indexed_bytes, keys.size() * kBlockBytes);

    // Every writer's entry must be readable and carry its own bytes: a lost
    // update or a crossed allocation would show up here.
    std::unordered_set<uint64_t> local_ids;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto found = index_->Lookup(registrations[i]);
        ASSERT_TRUE(found.has_value()) << "missing key " << keys[i];
        EXPECT_EQ(ReadAll(*found), PayloadFor(keys[i]));
        EXPECT_TRUE(local_ids.insert(found->Id().local_id).second);
    }
}

TEST_F(BlockIndexTest, LookupRacingEraseNeverReadsTornOrFreedBytes) {
    constexpr int kKeys = 128;
    std::vector<std::string> keys;
    std::vector<BlockRegistrationHandle> registrations;
    for (int i = 0; i < kKeys; ++i) {
        keys.push_back("race-" + std::to_string(i));
        registrations.push_back(Register(keys.back()));
        InsertAndForget(registrations.back(), PayloadFor(keys.back()));
    }

    std::atomic<bool> go{false};
    std::atomic<int> corrupted{0};
    std::atomic<int> hits{0};
    auto reader = [&] {
        while (!go.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (int round = 0; round < 8; ++round) {
            for (int i = 0; i < kKeys; ++i) {
                auto found = index_->Lookup(registrations[i]);
                // A miss is a legal outcome of the race; a wrong hit is not.
                if (!found.has_value()) continue;
                ++hits;
                std::string out(found->Size(), '\0');
                if (!found->Read(0, AsWritableBytes(out)) ||
                    out != PayloadFor(keys[i])) {
                    ++corrupted;
                }
            }
        }
    };

    std::vector<std::thread> threads;
    threads.emplace_back(reader);
    threads.emplace_back(reader);
    threads.emplace_back([&] {
        while (!go.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        // Dropping the detached pointer immediately is the aggressive case:
        // the free lands while readers may still hold a snapshot.
        for (int i = 0; i < kKeys; ++i) {
            index_->Erase(registrations[i]);
        }
    });
    go.store(true, std::memory_order_release);
    for (auto& thread : threads) thread.join();

    EXPECT_EQ(corrupted.load(), 0);
    EXPECT_GT(hits.load(), 0) << "the race never overlapped; test is vacuous";
    EXPECT_EQ(index_->Stats().entry_count, 0u);
    EXPECT_EQ(index_->Stats().indexed_bytes, 0u);
    for (const auto& registration : registrations) {
        EXPECT_FALSE(index_->Lookup(registration).has_value());
    }
    // Nothing leaked: every snapshot taken during the race is gone by now.
    EXPECT_EQ(pool_->Usage(), 0u);
}

TEST_F(BlockIndexTest,
       ValidateBlockIndexConfigRejectsZeroShardsAndOutOfRangeLoadFactor) {
    EXPECT_TRUE(ValidateBlockIndexConfig(BlockIndexConfig{}).has_value());

    BlockIndexConfig no_shards;
    no_shards.shard_count = 0;
    auto rejected = ValidateBlockIndexConfig(no_shards);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::INVALID_PARAMS);

    BlockIndexConfig zero_load;
    zero_load.max_load_factor = 0.0F;
    EXPECT_FALSE(ValidateBlockIndexConfig(zero_load).has_value());

    BlockIndexConfig over_one;
    over_one.max_load_factor = 1.5F;
    EXPECT_FALSE(ValidateBlockIndexConfig(over_one).has_value());

    // 1.0 is the boundary and must be accepted, not rounded away.
    BlockIndexConfig exactly_one;
    exactly_one.max_load_factor = 1.0F;
    EXPECT_TRUE(ValidateBlockIndexConfig(exactly_one).has_value());
}

}  // namespace
}  // namespace mooncake::v2
