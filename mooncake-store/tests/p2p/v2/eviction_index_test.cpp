// Component tests for the per-tiler EvictionIndex (design doc sections 3.2,
// 3.3 and 9).
//
// The concrete index is private to eviction_index.cpp, so everything here goes
// through CreateEvictionIndex -- the same surface TilerManager uses. Two
// indexes are built in every fixture, one per tiler, because half of what this
// component has to get right is what it does with a token that belongs to the
// *other* one.
//
// Registrations come from a real BlockRegistry rather than a fabricated
// handle: the property being pinned is that the tokens this index hands back
// are weak. A fake handle could never fail to upgrade, and failing to upgrade
// is exactly what the evict engine has to be able to observe.

#include "p2p/client/v2/eviction_index.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "p2p/client/v2/block.h"
#include "p2p/client/v2/block_registry.h"
#include "types.h"

// ---------------------------------------------------------------------------
// Allocation-failure injection
// ---------------------------------------------------------------------------
//
// One thing this index has to survive only happens when an allocation fails
// halfway through an update: the entry map has already changed and the chain
// then throws, or the other way round. Both structures allocate through
// std::allocator, so the only way to reach that state through the public
// surface is to make the program's allocator fail on demand.
//
// Every replaceable form is defined, including the ones no test arms. Leaving
// some to libstdc++ would let the linker pull the sanitizer runtime's
// new/delete object for them under -fsanitize=address, and that object also
// defines the plain operator new -- a duplicate symbol. These definitions
// belong to this translation unit alone, which is why they are not in a
// shared header.

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

constexpr UUID kFastTiler{0xFA57, 0x0001};
constexpr UUID kSlowTiler{0x5104, 0x0002};

// 100-byte blocks against a 1000-byte tier: every "how many victims" number
// below is arithmetic rather than a rounding accident.
constexpr size_t kBlockBytes = 100;
constexpr size_t kFastCapacity = 1000;

// The shard count the snapshots below claim their producer's BlockIndex is
// using. Small on purpose, so a handful of keys reliably spans more than one
// shard.
constexpr size_t kShardCount = 4;

std::vector<std::string> KeysOf(const std::vector<BlockToken>& victims) {
    std::vector<std::string> keys;
    keys.reserve(victims.size());
    for (const auto& victim : victims) keys.push_back(victim.key);
    return keys;
}

/** For assertions about membership, where the order is not the point. */
std::vector<std::string> Sorted(std::vector<std::string> keys) {
    std::sort(keys.begin(), keys.end());
    return keys;
}

}  // namespace

class EvictionIndexTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("EvictionIndexTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        registry_ = BlockRegistry(BlockRegistryConfig{/*shard_count=*/8});
        fast_ = MakeIndex(BaseConfig(), kFastTiler);
        slow_ = MakeIndex(BaseConfig(), kSlowTiler);
    }

    static EvictionIndexConfig BaseConfig() {
        return EvictionIndexConfig{};  // tinylfu_lru, 256 candidates
    }

    std::unique_ptr<EvictionIndex> MakeIndex(const EvictionIndexConfig& config,
                                             const UUID& tiler_id) {
        auto index = CreateEvictionIndex(config, tiler_id, kFastCapacity);
        CHECK(index.has_value())
            << "test setup: CreateEvictionIndex failed, error="
            << toString(index.error());
        return std::move(index.value());
    }

    /** Mints a real registration, so the weak handle inside the token can be
     *  upgraded for exactly as long as this fixture holds the strong one. */
    BlockToken NewToken(std::string_view key, const UUID& tiler,
                        size_t size_bytes = kBlockBytes) {
        auto handle = registry_.Register(key);
        CHECK(handle.has_value()) << "test setup: Register failed";
        handles_.insert_or_assign(std::string(key), handle.value());

        BlockToken token;
        token.key = std::string(key);
        token.registration_id = handle->Id();
        token.registration = handle->Downgrade();
        token.tiler_id = tiler;
        token.block_id = BlockId{tiler, next_local_id_++, /*generation=*/1};
        token.size_bytes = size_bytes;
        tokens_.insert_or_assign(std::string(key), token);
        return token;
    }

    /** The token last minted for `key`; what a caller would replay later. */
    const BlockToken& TokenOf(std::string_view key) const {
        auto it = tokens_.find(std::string(key));
        CHECK(it != tokens_.end()) << "test bug: unknown key " << key;
        return it->second;
    }

    std::vector<std::string> CommitKeys(EvictionIndex& index, const UUID& tiler,
                                        size_t count, std::string_view prefix) {
        std::vector<std::string> keys;
        for (size_t i = 0; i < count; ++i) {
            keys.push_back(std::string(prefix) + std::to_string(i));
            index.OnCommit(NewToken(keys.back(), tiler));
        }
        return keys;
    }

    /** The same block as it would look after moving to `tiler`. */
    BlockToken MigratedToken(const BlockToken& old_token, const UUID& tiler) {
        BlockToken token = old_token;
        token.tiler_id = tiler;
        token.block_id = BlockId{tiler, next_local_id_++, /*generation=*/1};
        tokens_.insert_or_assign(token.key, token);
        return token;
    }

    /**
     * @brief A whole-tier snapshot, built the way a producer must build one:
     *        the mutation counter is sampled before any row is read.
     */
    BlockIndexSnapshot SnapshotOf(const EvictionIndex& index, const UUID& tiler,
                                  const std::vector<std::string>& keys) {
        BlockIndexSnapshot snapshot;
        snapshot.tiler_id = tiler;
        snapshot.complete = true;
        snapshot.shard_count = kShardCount;
        snapshot.observed_mutations = index.MutationCount();
        for (const auto& key : keys) snapshot.entries.push_back(TokenOf(key));
        return snapshot;
    }

    /** The one-shard form: authoritative for `shard_id` and nothing else. */
    BlockIndexSnapshot PartialSnapshotOf(const EvictionIndex& index,
                                         const UUID& tiler, size_t shard_id,
                                         const std::vector<std::string>& keys) {
        BlockIndexSnapshot snapshot = SnapshotOf(index, tiler, keys);
        snapshot.complete = false;
        snapshot.shard_id = shard_id;
        return snapshot;
    }

    /** Which of kShardCount shards the authority keeps `key` in. */
    size_t ShardOf(std::string_view key) const {
        return SnapshotShardOf(TokenOf(key).registration_id, kShardCount);
    }

    /** Releases the last strong reference to a key's registration. */
    void DropRegistration(std::string_view key) {
        handles_.erase(std::string(key));
    }

    BlockRegistry registry_;
    std::unique_ptr<EvictionIndex> fast_;
    std::unique_ptr<EvictionIndex> slow_;
    std::unordered_map<std::string, BlockRegistrationHandle> handles_;
    std::unordered_map<std::string, BlockToken> tokens_;
    uint64_t next_local_id_ = 1;
};

TEST_F(EvictionIndexTest, ShippedDefaultsAreAcceptedAndUnknownTypesAreNot) {
    // The shipped defaults are the baseline: without them a rejection below
    // could equally well mean the validator rejects everything.
    EXPECT_TRUE(ValidateEvictionIndexConfig(EvictionIndexConfig{}).has_value());
    for (const char* type : {"lru", "multi_lru", "tinylfu_lru"}) {
        EvictionIndexConfig config = BaseConfig();
        config.type = type;
        EXPECT_TRUE(ValidateEvictionIndexConfig(config).has_value()) << type;
    }

    // An unknown ordering must fail at configuration time. Falling back to a
    // default would give an operator an eviction order they never asked for
    // and no way to notice.
    EvictionIndexConfig unknown = BaseConfig();
    unknown.type = "arc";
    EXPECT_EQ(ValidateEvictionIndexConfig(unknown).error(),
              ErrorCode::INVALID_PARAMS);
    auto index = CreateEvictionIndex(unknown, kFastTiler, kFastCapacity);
    ASSERT_FALSE(index.has_value());
    EXPECT_EQ(index.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(EvictionIndexTest, ConfigTheValidatorRejectsIsNeverConstructed) {
    // Zero candidates would turn every eviction round into a no-op that still
    // reports itself healthy, and non-increasing bands would collapse two
    // heat bands into one silently.
    EvictionIndexConfig no_candidates = BaseConfig();
    no_candidates.max_victim_candidates = 0;
    EXPECT_EQ(ValidateEvictionIndexConfig(no_candidates).error(),
              ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(CreateEvictionIndex(no_candidates, kFastTiler, kFastCapacity)
                     .has_value());

    EvictionIndexConfig inverted = BaseConfig();
    inverted.band_hot_threshold = 2;  // below the default warm threshold of 3
    EXPECT_EQ(ValidateEvictionIndexConfig(inverted).error(),
              ErrorCode::INVALID_PARAMS);
}

TEST_F(EvictionIndexTest, SelectVictimsIsColdestFirstAndStopsAtTheTarget) {
    const auto keys = CommitKeys(*fast_, kFastTiler, 5, "cold/");

    // 250 bytes out of 500 resident: an eviction round must not walk the whole
    // tier to reclaim a fraction of it.
    const auto victims = fast_->SelectVictims(250);
    EXPECT_EQ(KeysOf(victims),
              (std::vector<std::string>{keys[0], keys[1], keys[2]}));

    // The rest is still reachable when the target actually asks for it.
    EXPECT_EQ(fast_->SelectVictims(5 * kBlockBytes).size(), 5u);
    EXPECT_EQ(fast_->Stats().tracked_blocks, 5u);
    EXPECT_EQ(fast_->Stats().tracked_bytes, 5 * kBlockBytes);
}

TEST_F(EvictionIndexTest, AnotherTiersIndexOrAZeroTargetSelectsNothing) {
    const auto fast_keys = CommitKeys(*fast_, kFastTiler, 3, "own/");
    const auto slow_keys = CommitKeys(*slow_, kSlowTiler, 2, "other/");

    // One index per tiler: neither may offer the other's candidates, or the
    // evict engine would ask a BlockIndex to erase blocks it has never held.
    // Both are loaded, so this fails against an index that shares state as
    // well as against one that returns nothing at all.
    EXPECT_EQ(KeysOf(fast_->SelectVictims(10 * kBlockBytes)), fast_keys);
    EXPECT_EQ(KeysOf(slow_->SelectVictims(10 * kBlockBytes)), slow_keys);
    // And a zero target must not evict "just one to be safe".
    EXPECT_TRUE(fast_->SelectVictims(0).empty());
}

TEST_F(EvictionIndexTest, EveryVictimCarriesAWeakRegistrationAndABlockId) {
    fast_->OnCommit(NewToken("token/alpha", kFastTiler));

    const auto victims = fast_->SelectVictims(kBlockBytes);
    ASSERT_EQ(victims.size(), 1u);
    const BlockToken& victim = victims.front();
    EXPECT_EQ(victim.key, "token/alpha");
    EXPECT_EQ(victim.size_bytes, kBlockBytes);
    EXPECT_EQ(victim.tiler_id, kFastTiler);
    // Both halves of the staleness check the evict engine has to run: the
    // block identity it must match, and the registration it must upgrade.
    EXPECT_EQ(victim.block_id, TokenOf("token/alpha").block_id);
    EXPECT_EQ(victim.registration_id, TokenOf("token/alpha").registration_id);
    auto upgraded = victim.registration.Lock();
    ASSERT_TRUE(upgraded.has_value());
    EXPECT_EQ(upgraded->Key(), "token/alpha");
    EXPECT_EQ(upgraded->Id(), victim.registration_id);
}

TEST_F(EvictionIndexTest, VictimRegistrationStopsUpgradingOnceTheKeyIsGone) {
    fast_->OnCommit(NewToken("weak/alpha", kFastTiler));
    DropRegistration("weak/alpha");

    // This index must never become a second source of truth about what
    // exists: dropping the last strong registration has to stay visible
    // through the token it hands out, not be masked by a strong reference
    // kept here, which would also pin the identity forever.
    const auto victims = fast_->SelectVictims(kBlockBytes);
    ASSERT_EQ(victims.size(), 1u);
    EXPECT_FALSE(victims.front().registration.Lock().has_value());
}

TEST_F(EvictionIndexTest, DeletedKeysAreNoLongerOfferedAsVictims) {
    const auto keys = CommitKeys(*fast_, kFastTiler, 3, "deleted/");

    fast_->OnDelete(TokenOf(keys[1]));

    // Offering it again would send the evict engine after a block that can
    // never be validated, and every round would waste its candidate budget on
    // the same dead name.
    EXPECT_EQ(KeysOf(fast_->SelectVictims(3 * kBlockBytes)),
              (std::vector<std::string>{keys[0], keys[2]}));
    EXPECT_EQ(fast_->Stats().tracked_blocks, 2u);
    EXPECT_EQ(fast_->Stats().tracked_bytes, 2 * kBlockBytes);
}

TEST_F(EvictionIndexTest, FrequentlyReadKeysAreOfferedAfterUntouchedOnes) {
    const auto keys = CommitKeys(*fast_, kFastTiler, 4, "heat/");
    for (int i = 0; i < 16; ++i) fast_->OnAccess(TokenOf(keys[2]));

    // Recency alone would evict the three keys nobody has read since they were
    // written last; frequency is what keeps the hot one out of reach.
    EXPECT_EQ(KeysOf(fast_->SelectVictims(4 * kBlockBytes)),
              (std::vector<std::string>{keys[0], keys[1], keys[3], keys[2]}));
}

TEST_F(EvictionIndexTest, ClearEmptiesTheVictimState) {
    CommitKeys(*fast_, kFastTiler, 3, "cleared/");
    CommitKeys(*slow_, kSlowTiler, 2, "cleared-slow/");
    ASSERT_FALSE(fast_->SelectVictims(kBlockBytes).empty());

    fast_->Clear();

    EXPECT_TRUE(fast_->SelectVictims(10 * kBlockBytes).empty());
    EXPECT_EQ(fast_->Stats().tracked_blocks, 0u);
    EXPECT_EQ(fast_->Stats().tracked_bytes, 0u);
    // Clear is scoped to one index; the other tiler is untouched.
    EXPECT_EQ(slow_->SelectVictims(10 * kBlockBytes).size(), 2u);

    // Cleared, not decommissioned: the index still accepts commits, and the
    // chain must have been emptied along with the entry map or the refilled
    // key would come back behind three names nothing can remove.
    fast_->OnCommit(NewToken("cleared/again", kFastTiler));
    EXPECT_EQ(KeysOf(fast_->SelectVictims(10 * kBlockBytes)),
              (std::vector<std::string>{"cleared/again"}));
}

TEST_F(EvictionIndexTest, TokenScopedDeleteKeepsANewerBlock) {
    const BlockToken first = NewToken("scoped/key", kFastTiler);
    fast_->OnCommit(first);
    // Same key, same registration, a different block: a rewrite that landed
    // while the caller was still holding the old name.
    const BlockToken second = NewToken("scoped/key", kFastTiler);
    ASSERT_EQ(second.registration_id, first.registration_id);
    ASSERT_FALSE(second.block_id == first.block_id);
    fast_->OnCommit(second);

    fast_->OnDelete(first);

    // Erasing on the old block's behalf would hide the live replacement from
    // eviction for the rest of the process: nothing else would ever offer it,
    // and its bytes would sit in the tier forever.
    const auto victims = fast_->SelectVictims(kBlockBytes);
    ASSERT_EQ(victims.size(), 1u);
    EXPECT_EQ(victims.front().block_id, second.block_id);
    EXPECT_EQ(fast_->Stats().tracked_blocks, 1u);
    // The delete named a block this index does not hold, which is the same
    // signal as a candidate the caller found stale.
    EXPECT_EQ(fast_->Stats().stale_candidates, 1u);
    // A delete for the block it does hold still works.
    fast_->OnDelete(second);
    EXPECT_TRUE(fast_->SelectVictims(kBlockBytes).empty());
}

TEST_F(EvictionIndexTest, AForeignTilerTokenIsRejectedNotAbsorbed) {
    const BlockToken resident = NewToken("foreign/resident", kFastTiler);
    fast_->OnCommit(resident);

    // Same block, mislabelled as living in the slow tier.
    BlockToken foreign = resident;
    foreign.tiler_id = kSlowTiler;
    fast_->OnCommit(foreign);
    fast_->OnDelete(foreign);
    fast_->OnAccess(foreign);

    // Absorbing a foreign token would let this index offer a victim to a
    // BlockIndex that has never heard of it -- and here it would also have
    // deleted a live block on the strength of a mislabelled name.
    EXPECT_EQ(fast_->Stats().tracked_blocks, 1u);
    EXPECT_EQ(KeysOf(fast_->SelectVictims(kBlockBytes)),
              (std::vector<std::string>{"foreign/resident"}));
    // Rejection is not a lost update: there is nothing here to reconcile.
    EXPECT_FALSE(fast_->NeedsReconcile());
}

TEST_F(EvictionIndexTest, ReconcileRestoresALostEntryAndClearsTheFlag) {
    const auto keys = CommitKeys(*fast_, kFastTiler, 1, "lost/");

    // An update that cannot be applied: a commit whose registration was never
    // minted cannot be filed under one.
    BlockToken unindexable = NewToken("lost/orphan", kFastTiler);
    unindexable.registration_id = RegistrationId{};
    fast_->OnCommit(unindexable);
    ASSERT_TRUE(fast_->NeedsReconcile());
    EXPECT_EQ(fast_->Stats().needs_reconcile, 1u);
    EXPECT_EQ(fast_->Stats().tracked_blocks, 1u);

    // The safety net: without it the block would be committed, invisible to
    // every eviction round, and its bytes unreclaimable for the process
    // lifetime.
    fast_->Reconcile(SnapshotOf(*fast_, kFastTiler, {keys[0], "lost/orphan"}));

    EXPECT_FALSE(fast_->NeedsReconcile());
    EXPECT_EQ(fast_->Stats().reconciles, 1u);
    EXPECT_EQ(fast_->Stats().tracked_blocks, 2u);
    // Recovered, not read: the repaired record enters at the cold end of its
    // band, so it is offered before the block this index actually watched
    // being committed. See ARecoveredEntryDoesNotOvertakeTouchedBlocks.
    EXPECT_EQ(KeysOf(fast_->SelectVictims(2 * kBlockBytes)),
              (std::vector<std::string>{"lost/orphan", keys[0]}));
}

TEST_F(EvictionIndexTest, ReconcileDropsEntriesTheSnapshotDoesNotContain) {
    const auto keys = CommitKeys(*fast_, kFastTiler, 3, "drop/");

    // The authority no longer has the middle block -- say a delete was never
    // reported here. An entry the BlockIndex cannot confirm is a candidate
    // that fails validation on every round and never leaves the chain.
    fast_->Reconcile(SnapshotOf(*fast_, kFastTiler, {keys[0], keys[2]}));

    EXPECT_EQ(KeysOf(fast_->SelectVictims(3 * kBlockBytes)),
              (std::vector<std::string>{keys[0], keys[2]}));
    EXPECT_EQ(fast_->Stats().tracked_bytes, 2 * kBlockBytes);

    // A snapshot for another tiler is a routing bug and must change nothing.
    fast_->Reconcile(SnapshotOf(*fast_, kSlowTiler, {}));
    EXPECT_EQ(fast_->Stats().tracked_blocks, 2u);
    EXPECT_EQ(fast_->Stats().reconciles, 1u);
}

TEST_F(EvictionIndexTest, OnlyAWholeTierSnapshotClearsTheReconcileFlag) {
    const auto keys = CommitKeys(*fast_, kFastTiler, 2, "partial/");

    // A lost update: the flag a reconcile pass exists to clear.
    BlockToken unfilable = NewToken("partial/orphan", kFastTiler);
    unfilable.registration_id = RegistrationId{};
    fast_->OnCommit(unfilable);
    ASSERT_TRUE(fast_->NeedsReconcile());

    fast_->Reconcile(
        PartialSnapshotOf(*fast_, kFastTiler, ShardOf(keys[0]), {keys[0]}));

    // One shard proves nothing about the others, so it cannot switch off the
    // fallback scan. It must not raise the flag either: a per-shard pass that
    // reports a fresh gap every time pins the fallback on for good.
    EXPECT_TRUE(fast_->NeedsReconcile());
    EXPECT_EQ(fast_->Stats().reconciles, 1u);

    // The whole tier, with nothing landing in between: the only thing that
    // can report this index whole again.
    fast_->Reconcile(SnapshotOf(*fast_, kFastTiler, {keys[0], keys[1]}));
    EXPECT_FALSE(fast_->NeedsReconcile());
    EXPECT_EQ(fast_->Stats().tracked_blocks, 2u);
}

TEST_F(EvictionIndexTest, OnMigrationMovesATokenBetweenTwoIndexes) {
    const BlockToken source = NewToken("moved/key", kFastTiler);
    fast_->OnCommit(source);
    const BlockToken destination = MigratedToken(source, kSlowTiler);

    // Both halves are reported to both indexes; each one acts on the half that
    // names it and ignores the other.
    fast_->OnMigration(source, destination);
    slow_->OnMigration(source, destination);

    // If the source index kept the block, eviction would keep proposing a
    // block that has already left the tier; if the destination index never
    // learned about it, the arriving bytes would be unreclaimable.
    EXPECT_TRUE(fast_->SelectVictims(kBlockBytes).empty());
    EXPECT_EQ(fast_->Stats().tracked_bytes, 0u);

    const auto victims = slow_->SelectVictims(kBlockBytes);
    ASSERT_EQ(victims.size(), 1u);
    EXPECT_EQ(victims.front().key, "moved/key");
    EXPECT_EQ(victims.front().tiler_id, kSlowTiler);
    EXPECT_EQ(victims.front().block_id, destination.block_id);
    EXPECT_EQ(victims.front().registration_id, source.registration_id);
    EXPECT_FALSE(fast_->NeedsReconcile());
    EXPECT_FALSE(slow_->NeedsReconcile());
}

TEST_F(EvictionIndexTest, PlainLruOrderingIsRecencyOnly) {
    EvictionIndexConfig config = BaseConfig();
    config.type = "lru";
    auto index = MakeIndex(config, kFastTiler);
    const auto keys = CommitKeys(*index, kFastTiler, 3, "lru/");

    for (int i = 0; i < 16; ++i) index->OnAccess(TokenOf(keys[0]));
    index->OnAccess(TokenOf(keys[1]));
    index->OnAccess(TokenOf(keys[2]));

    // An operator who asked for the simple ordering and silently got the
    // banded one would see a tier evict in an order no runbook predicts: here
    // the hottest key is also the least recently used, and "lru" must offer
    // it first. tinylfu_lru orders the same history the other way round.
    EXPECT_EQ(KeysOf(index->SelectVictims(3 * kBlockBytes)),
              (std::vector<std::string>{keys[0], keys[1], keys[2]}));
}

TEST_F(EvictionIndexTest, MultiLruBandsOnItsOwnAccessCounts) {
    EvictionIndexConfig config = BaseConfig();
    config.type = "multi_lru";
    auto index = MakeIndex(config, kFastTiler);
    const auto keys = CommitKeys(*index, kFastTiler, 3, "banded/");

    for (int i = 0; i < 16; ++i) index->OnAccess(TokenOf(keys[0]));
    index->OnAccess(TokenOf(keys[1]));
    index->OnAccess(TokenOf(keys[2]));

    // Same history as the plain-LRU test, opposite answer: banding has to come
    // from counts this index keeps itself, so a deployment without a sketch
    // still protects a hot key from a recency-only sweep.
    EXPECT_EQ(KeysOf(index->SelectVictims(3 * kBlockBytes)),
              (std::vector<std::string>{keys[1], keys[2], keys[0]}));
}

// ---------------------------------------------------------------------------
// What a snapshot is, and is not, proof of
// ---------------------------------------------------------------------------

TEST_F(EvictionIndexTest, APerShardSnapshotOnlyDropsFromItsOwnShard) {
    const auto keys = CommitKeys(*fast_, kFastTiler, 12, "shard/");

    // Split the committed keys the way the authority's shards do, so the
    // assertion below is about coverage rather than about which key is where.
    const size_t covered = ShardOf(keys[0]);
    std::vector<std::string> inside;
    std::vector<std::string> outside;
    for (const auto& key : keys) {
        (ShardOf(key) == covered ? inside : outside).push_back(key);
    }
    ASSERT_FALSE(outside.empty())
        << "test setup: every key landed in one shard, nothing to cover";

    // Exactly what TilerManager::SnapshotTokens produces: one shard, and that
    // shard no longer has the first key.
    const std::vector<std::string> present(inside.begin() + 1, inside.end());
    fast_->Reconcile(PartialSnapshotOf(*fast_, kFastTiler, covered, present));

    // The omitted block of the covered shard is gone, and every block from a
    // shard the snapshot never read survives. Dropping those would mean that
    // walking the shards in turn leaves only the last shard's blocks -- and a
    // block this index has forgotten is one nothing offers as a victim again.
    std::vector<std::string> expected = present;
    expected.insert(expected.end(), outside.begin(), outside.end());
    EXPECT_EQ(fast_->Stats().tracked_blocks, keys.size() - 1);
    EXPECT_EQ(Sorted(KeysOf(fast_->SelectVictims(12 * kBlockBytes))),
              Sorted(expected));
}

TEST_F(EvictionIndexTest, ASnapshotThatCannotNameItsShardOnlyAdds) {
    const auto keys = CommitKeys(*fast_, kFastTiler, 3, "unscoped/");

    BlockIndexSnapshot snapshot =
        PartialSnapshotOf(*fast_, kFastTiler, /*shard_id=*/0, {keys[0]});
    // A producer that cannot say which shard it read has read an unknown part
    // of the tier, so absence from it proves nothing about anything.
    snapshot.shard_count = 0;
    fast_->Reconcile(snapshot);

    EXPECT_EQ(fast_->Stats().tracked_blocks, 3u);
    EXPECT_EQ(Sorted(KeysOf(fast_->SelectVictims(3 * kBlockBytes))),
              Sorted(keys));
}

TEST_F(EvictionIndexTest, AReconcileThatRacedACommitKeepsItAndTheFlag) {
    const auto keys = CommitKeys(*fast_, kFastTiler, 1, "race/");

    // A lost update, which is what sends a reconcile pass here in the first
    // place.
    BlockToken unfilable = NewToken("race/lost", kFastTiler);
    unfilable.registration_id = RegistrationId{};
    fast_->OnCommit(unfilable);
    ASSERT_TRUE(fast_->NeedsReconcile());

    // Captured here, applied below. Reconcile holds no BlockIndex lock, and
    // TilerManager::InsertRegistered calls OnCommit on the request path, so an
    // ordinary commit gets in between -- this is traffic, not a rare race.
    const BlockIndexSnapshot snapshot =
        SnapshotOf(*fast_, kFastTiler, {keys[0], "race/lost"});
    fast_->OnCommit(NewToken("race/late", kFastTiler));

    fast_->Reconcile(snapshot);

    // The late block is live in the BlockIndex. Dropping it here makes it
    // invisible to every selection round, and reporting the index whole on top
    // of that tells the evict engine not to run the fallback scan either: its
    // bytes would never come back.
    EXPECT_EQ(fast_->Stats().tracked_blocks, 3u);
    EXPECT_TRUE(fast_->NeedsReconcile());
    EXPECT_EQ(Sorted(KeysOf(fast_->SelectVictims(3 * kBlockBytes))),
              Sorted({keys[0], "race/late", "race/lost"}));
}

TEST_F(EvictionIndexTest, ASnapshotRowThatCannotBeFiledKeepsTheFlag) {
    const auto keys = CommitKeys(*fast_, kFastTiler, 1, "unfilable/");

    BlockToken unfilable = NewToken("unfilable/orphan", kFastTiler);
    unfilable.registration_id = RegistrationId{};
    fast_->OnCommit(unfilable);
    ASSERT_TRUE(fast_->NeedsReconcile());
    const uint64_t marks = fast_->Stats().needs_reconcile;

    // The snapshot repeats the row this index could not file. Skipping it and
    // then reporting the index whole tells the same lie twice: the block stays
    // unindexed and the fallback scan is switched off on top.
    BlockIndexSnapshot snapshot = SnapshotOf(*fast_, kFastTiler, {keys[0]});
    snapshot.entries.push_back(unfilable);
    fast_->Reconcile(snapshot);

    EXPECT_TRUE(fast_->NeedsReconcile());
    EXPECT_GT(fast_->Stats().needs_reconcile, marks);
    EXPECT_EQ(fast_->Stats().tracked_blocks, 1u);

    // A row naming another tiler is no more filable, and no more proof.
    BlockIndexSnapshot mixed = SnapshotOf(*fast_, kFastTiler, {keys[0]});
    BlockToken elsewhere = TokenOf("unfilable/orphan");
    elsewhere.tiler_id = kSlowTiler;
    mixed.entries.push_back(elsewhere);
    fast_->Reconcile(mixed);
    EXPECT_TRUE(fast_->NeedsReconcile());
    EXPECT_EQ(fast_->Stats().tracked_blocks, 1u);
}

// ---------------------------------------------------------------------------
// Failure and repair
// ---------------------------------------------------------------------------

TEST_F(EvictionIndexTest, ACommitThatRunsOutOfMemoryLeavesNoUndeletableName) {
    size_t failed_commits = 0;
    // The allocation that fails has to be the one inside the chain insert, so
    // every allocation a commit makes is tried in turn rather than guessed.
    for (int64_t nth = 0; nth < 16; ++nth) {
        auto index = MakeIndex(BaseConfig(), kFastTiler);
        const std::string suffix = std::to_string(nth);
        const BlockToken doomed = NewToken("oom/doomed" + suffix, kFastTiler);

        FailAllocationAfter(nth);
        index->OnCommit(doomed);
        DisarmAllocationFailure();
        index->OnCommit(NewToken("oom/kept" + suffix, kFastTiler));
        if (index->NeedsReconcile()) ++failed_commits;

        // A chain name the entry map cannot resolve is offered by every round,
        // no delete can reach it, and it re-raises needs_reconcile each time:
        // one candidate slot of max_victim_candidates burned for the process
        // lifetime. Two rounds in a row expose it -- the count only grows when
        // a round found a name it cannot account for.
        const uint64_t marks = index->Stats().needs_reconcile;
        const auto first = index->SelectVictims(10 * kBlockBytes);
        const auto second = index->SelectVictims(10 * kBlockBytes);
        EXPECT_EQ(index->Stats().needs_reconcile, marks) << "nth=" << nth;
        EXPECT_EQ(first.size(), index->Stats().tracked_blocks) << "nth=" << nth;
        EXPECT_EQ(KeysOf(first), KeysOf(second)) << "nth=" << nth;
    }
    // Otherwise the loop above proves only that the injector never fired.
    EXPECT_GT(failed_commits, 0u);
}

TEST_F(EvictionIndexTest, ARepairIsNotCountedAsAnAccess) {
    EvictionIndexConfig config = BaseConfig();
    config.type = "multi_lru";  // bands on this index's own access counts
    auto index = MakeIndex(config, kFastTiler);
    const auto keys = CommitKeys(*index, kFastTiler, 3, "repair/");
    index->OnAccess(TokenOf(keys[0]));
    index->OnAccess(TokenOf(keys[2]));

    // The authority reports a different block under the same registration: the
    // record has to be replaced, but nobody read anything.
    const BlockId previous = TokenOf(keys[0]).block_id;
    const BlockToken replacement = NewToken(keys[0], kFastTiler);
    ASSERT_EQ(replacement.registration_id, TokenOf(keys[0]).registration_id);
    ASSERT_FALSE(replacement.block_id == previous);
    index->Reconcile(SnapshotOf(*index, kFastTiler, keys));

    // Counting the repair as a third access would push that block into the
    // warm band, behind two blocks that really were read.
    EXPECT_EQ(KeysOf(index->SelectVictims(3 * kBlockBytes)),
              (std::vector<std::string>{keys[1], keys[0], keys[2]}));
}

TEST_F(EvictionIndexTest, ARecoveredEntryDoesNotOvertakeTouchedBlocks) {
    const auto keys = CommitKeys(*fast_, kFastTiler, 2, "recovered/");
    // This index lost an entry the authority still has.
    fast_->OnDelete(TokenOf(keys[0]));
    ASSERT_EQ(KeysOf(fast_->SelectVictims(2 * kBlockBytes)),
              (std::vector<std::string>{keys[1]}));

    fast_->Reconcile(SnapshotOf(*fast_, kFastTiler, {keys[0], keys[1]}));

    // Recovered, not touched. Reconcile knows the block exists, not when it
    // was last read, so it enters at the cold end of its band: entering at the
    // MRU would evict the block that really is newer first.
    EXPECT_EQ(KeysOf(fast_->SelectVictims(2 * kBlockBytes)),
              (std::vector<std::string>{keys[0], keys[1]}));
    EXPECT_EQ(fast_->Stats().tracked_blocks, 2u);
}

// ---------------------------------------------------------------------------
// Remaining surface
// ---------------------------------------------------------------------------

TEST_F(EvictionIndexTest, ClearDoesNotEraseAKnownGap) {
    BlockToken unfilable = NewToken("cleared/orphan", kFastTiler);
    unfilable.registration_id = RegistrationId{};
    fast_->OnCommit(unfilable);
    ASSERT_TRUE(fast_->NeedsReconcile());

    fast_->Clear();

    // Clear is evidence about this index, not about what the BlockIndex still
    // holds: dropping state on purpose must not report a real gap as repaired.
    EXPECT_TRUE(fast_->NeedsReconcile());
    EXPECT_EQ(fast_->Stats().tracked_blocks, 0u);
}

TEST_F(EvictionIndexTest, ARoundInspectsAtMostTheConfiguredCandidates) {
    EvictionIndexConfig config = BaseConfig();
    config.max_victim_candidates = 2;
    auto index = MakeIndex(config, kFastTiler);
    const auto keys = CommitKeys(*index, kFastTiler, 5, "budget/");

    // The budget bounds the round even when the target asks for more: an
    // eviction round must not walk the whole tier looking for one more block.
    EXPECT_EQ(KeysOf(index->SelectVictims(5 * kBlockBytes)),
              (std::vector<std::string>{keys[0], keys[1]}));
}

TEST_F(EvictionIndexTest, MigratingABlockThisIndexNeverHadIsStale) {
    const BlockToken source = NewToken("stale/moved", kFastTiler);
    const BlockToken destination = MigratedToken(source, kSlowTiler);

    // The source index never held the block, so the mover is acting on a
    // candidate that is already gone -- the same signal as a stale victim.
    fast_->OnMigration(source, destination);

    EXPECT_EQ(fast_->Stats().stale_candidates, 1u);
    EXPECT_EQ(fast_->Stats().tracked_blocks, 0u);
    EXPECT_FALSE(fast_->NeedsReconcile());
}

TEST_F(EvictionIndexTest, ConcurrentCommitsReadsAndDeletesStayConsistent) {
    constexpr size_t kThreads = 4;
    constexpr size_t kPerThread = 64;
    // Registrations are minted up front: BlockRegistry has its own concurrency
    // tests, and this one is about the index's single mutex.
    std::vector<std::vector<BlockToken>> work(kThreads);
    for (size_t t = 0; t < kThreads; ++t) {
        for (size_t i = 0; i < kPerThread; ++i) {
            work[t].push_back(
                NewToken("busy/" + std::to_string(t) + "/" + std::to_string(i),
                         kFastTiler));
        }
    }

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (size_t t = 0; t < kThreads; ++t) {
        threads.emplace_back([this, &work, t] {
            for (const auto& token : work[t]) {
                fast_->OnCommit(token);
                fast_->OnAccess(token);
                fast_->SelectVictims(kBlockBytes);
            }
            for (const auto& token : work[t]) fast_->OnDelete(token);
        });
    }
    for (auto& thread : threads) thread.join();

    // Every commit was paired with a delete. The chain and the entry map move
    // under one lock, so they cannot end up disagreeing: a name in one and not
    // the other is offered forever and can never be removed -- which is what
    // SelectVictims reports by raising needs_reconcile.
    EXPECT_EQ(fast_->Stats().tracked_blocks, 0u);
    EXPECT_EQ(fast_->Stats().tracked_bytes, 0u);
    EXPECT_TRUE(
        fast_->SelectVictims(kThreads * kPerThread * kBlockBytes).empty());
    EXPECT_FALSE(fast_->NeedsReconcile());
}

}  // namespace mooncake::v2
