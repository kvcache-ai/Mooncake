#include "p2p/client/data_manager/v2/block_index.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "p2p/client/data_manager/v2/block_pool.h"
#include "p2p/client/data_manager/v2/block_registry.h"
#include "common_types.h"

namespace mooncake::v2 {
namespace {

constexpr UUID kFastTiler{0xFA57, 0x0001};
constexpr UUID kSlowTiler{0x5104, 0x0002};
constexpr size_t kBlockBytes = 100;
constexpr size_t kCapacity = 1u << 20;

std::vector<std::string> KeysOf(const std::vector<BlockToken>& victims) {
    std::vector<std::string> keys;
    for (const auto& victim : victims) keys.push_back(victim.key);
    return keys;
}

class BlockIndexOrderingTest : public ::testing::TestWithParam<const char*> {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("BlockIndexOrderingTest");
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
        fast_ = MakeIndex(kFastTiler);
        slow_ = MakeIndex(kSlowTiler);
    }

    std::unique_ptr<BlockIndex> MakeIndex(const UUID& tiler_id,
                                          size_t max_candidates = 256) {
        BlockIndexConfig config;
        config.type = GetParam();
        config.shard_count = 4;
        config.max_victim_candidates = max_candidates;
        auto index = CreateBlockIndex(config, tiler_id, kCapacity);
        CHECK(index) << toString(index.error());
        return std::move(*index);
    }

    CompletedBlock MakeCompleted(std::string_view key) {
        auto allocation = pool_->Allocate(kBlockBytes, 1);
        CHECK(allocation);
        auto mutable_block = MutableBlock::MakeForTiler(std::move(*allocation));
        const std::string payload(kBlockBytes, 'x');
        CHECK(mutable_block.Write(
            0, std::as_bytes(std::span<const char>(payload))));
        auto completed = std::move(mutable_block).Complete(std::string(key));
        CHECK(completed);
        return std::move(*completed);
    }

    BlockToken Insert(BlockIndex& index, std::string_view key) {
        auto registration = registry_.Register(key);
        CHECK(registration);
        handles_.insert_or_assign(std::string(key), *registration);
        auto inserted = index.Insert(MakeCompleted(key), *registration);
        CHECK(inserted) << toString(inserted.error());
        BlockToken token;
        token.key = inserted->Key();
        token.registration_id = inserted->Registration();
        token.registration = registration->Downgrade();
        token.tiler_id = index.TilerId();
        token.block_id = inserted->Id();
        token.size_bytes = inserted->Size();
        tokens_.insert_or_assign(std::string(key), token);
        return token;
    }

    std::vector<std::string> InsertKeys(BlockIndex& index, size_t count,
                                        std::string_view prefix) {
        std::vector<std::string> keys;
        for (size_t i = 0; i < count; ++i) {
            keys.push_back(std::string(prefix) + std::to_string(i));
            Insert(index, keys.back());
        }
        return keys;
    }

    const BlockToken& TokenOf(const std::string& key) const {
        return tokens_.at(key);
    }

    std::shared_ptr<DramBlockPool> pool_;
    BlockRegistry registry_;
    std::unique_ptr<BlockIndex> fast_;
    std::unique_ptr<BlockIndex> slow_;
    std::unordered_map<std::string, BlockRegistrationHandle> handles_;
    std::unordered_map<std::string, BlockToken> tokens_;
};

TEST_P(BlockIndexOrderingTest, SelectVictimsStopsAtTheByteTarget) {
    const auto keys = InsertKeys(*fast_, 5, "cold/");
    EXPECT_EQ(KeysOf(fast_->SelectVictims(250)),
              (std::vector<std::string>{keys[0], keys[1], keys[2]}));
    EXPECT_EQ(KeysOf(fast_->SelectVictims(kCapacity)), keys);
    EXPECT_EQ(fast_->Stats().entry_count, keys.size());
    EXPECT_EQ(fast_->Stats().indexed_bytes, keys.size() * kBlockBytes);
    // Selection is read-only: another round sees the same committed entries.
    EXPECT_EQ(KeysOf(fast_->SelectVictims(kCapacity)), keys);
}

TEST_P(BlockIndexOrderingTest, IndexesAreIndependentAndZeroTargetsAreEmpty) {
    const auto fast_keys = InsertKeys(*fast_, 3, "own/");
    const auto slow_keys = InsertKeys(*slow_, 2, "other/");
    EXPECT_EQ(KeysOf(fast_->SelectVictims(kCapacity)), fast_keys);
    EXPECT_EQ(KeysOf(slow_->SelectVictims(kCapacity)), slow_keys);
    EXPECT_TRUE(fast_->SelectVictims(0).empty());
}

TEST_P(BlockIndexOrderingTest,
       CandidatesCarryLiveIdentityWithoutOwningStorage) {
    const auto committed = Insert(*fast_, "token/alpha");
    const auto victims = fast_->SelectVictims(kBlockBytes);
    ASSERT_EQ(victims.size(), 1u);
    const auto& victim = victims.front();
    EXPECT_EQ(victim.key, committed.key);
    EXPECT_EQ(victim.size_bytes, kBlockBytes);
    EXPECT_EQ(victim.tiler_id, kFastTiler);
    EXPECT_EQ(victim.block_id, committed.block_id);
    EXPECT_EQ(victim.registration_id, committed.registration_id);
    ASSERT_TRUE(victim.registration.Lock());

    // Only the index and actual read snapshots retain physical allocations.
    // Keeping candidates alive must not pin either storage or registration.
    fast_->EraseById(committed.registration_id, committed.block_id).reset();
    handles_.erase(committed.key);
    EXPECT_EQ(pool_->Usage(), 0u);
    EXPECT_FALSE(victim.registration.Lock());
    EXPECT_TRUE(fast_->SelectVictims(kCapacity).empty());
}

TEST_P(BlockIndexOrderingTest, EraseAndDrainRemoveTheirOrderingNodes) {
    const auto keys = InsertKeys(*fast_, 3, "deleted/");
    InsertKeys(*slow_, 2, "other/");
    const auto token = TokenOf(keys[1]);
    ASSERT_NE(fast_->EraseById(token.registration_id, token.block_id), nullptr);
    EXPECT_EQ(KeysOf(fast_->SelectVictims(kCapacity)),
              (std::vector<std::string>{keys[0], keys[2]}));
    EXPECT_EQ(fast_->Stats().entry_count, 2u);
    EXPECT_EQ(fast_->Stats().indexed_bytes, 2 * kBlockBytes);

    auto drained = fast_->Drain();
    EXPECT_EQ(drained.size(), 2u);
    EXPECT_TRUE(fast_->SelectVictims(kCapacity).empty());
    EXPECT_EQ(fast_->Stats().entry_count, 0u);
    EXPECT_EQ(slow_->SelectVictims(kCapacity).size(), 2u);
    Insert(*fast_, "refilled");
    EXPECT_EQ(KeysOf(fast_->SelectVictims(kCapacity)),
              (std::vector<std::string>{"refilled"}));
}

TEST_P(BlockIndexOrderingTest, AccessMovesAReadBlockBehindUntouchedBlocks) {
    const auto keys = InsertKeys(*fast_, 4, "heat/");
    for (int i = 0; i < 16; ++i) fast_->OnAccess(TokenOf(keys[2]));
    EXPECT_EQ(KeysOf(fast_->SelectVictims(kCapacity)),
              (std::vector<std::string>{keys[0], keys[1], keys[3], keys[2]}));
}

TEST_P(BlockIndexOrderingTest, StrategyDistinguishesRecencyFromFrequency) {
    const auto keys = InsertKeys(*fast_, 3, "strategy/");
    for (int i = 0; i < 16; ++i) fast_->OnAccess(TokenOf(keys[0]));
    fast_->OnAccess(TokenOf(keys[1]));
    fast_->OnAccess(TokenOf(keys[2]));
    const auto expected =
        std::string_view(GetParam()) == "lru"
            ? keys
            : std::vector<std::string>{keys[1], keys[2], keys[0]};
    EXPECT_EQ(KeysOf(fast_->SelectVictims(kCapacity)), expected);
}

TEST_P(BlockIndexOrderingTest, StaleCandidatesAndAccessesPreserveAReplacement) {
    const auto old = Insert(*fast_, "rewritten");
    ASSERT_NE(fast_->EraseById(old.registration_id, old.block_id), nullptr);
    const auto replacement = Insert(*fast_, "rewritten");
    Insert(*fast_, "untouched");
    ASSERT_NE(replacement.block_id, old.block_id);
    ASSERT_EQ(replacement.registration_id, old.registration_id);

    for (int i = 0; i < 16; ++i) fast_->OnAccess(old);
    EXPECT_EQ(fast_->EraseById(old.registration_id, old.block_id), nullptr);
    auto registration = replacement.registration.Lock();
    ASSERT_TRUE(registration);
    auto found = fast_->Lookup(*registration);
    ASSERT_TRUE(found);
    EXPECT_EQ(found->Id(), replacement.block_id);
    EXPECT_EQ(KeysOf(fast_->SelectVictims(kCapacity)),
              (std::vector<std::string>{"rewritten", "untouched"}));
}

TEST_P(BlockIndexOrderingTest, ForeignTilerAccessDoesNotReorderAnEntry) {
    auto foreign = Insert(*fast_, "first");
    Insert(*fast_, "second");
    foreign.tiler_id = kSlowTiler;
    foreign.block_id.tiler_id = kSlowTiler;
    for (int i = 0; i < 16; ++i) fast_->OnAccess(foreign);
    EXPECT_EQ(KeysOf(fast_->SelectVictims(kCapacity)),
              (std::vector<std::string>{"first", "second"}));
}

TEST_P(BlockIndexOrderingTest, OneRoundRespectsTheCandidateLimit) {
    auto bounded = MakeIndex(kFastTiler, 2);
    const auto keys = InsertKeys(*bounded, 5, "bounded/");
    EXPECT_EQ(KeysOf(bounded->SelectVictims(kCapacity)),
              (std::vector<std::string>{keys[0], keys[1]}));
    EXPECT_EQ(bounded->Stats().entry_count, 5u);
}

TEST_P(BlockIndexOrderingTest, ConcurrentMutationAndSelectionRemainConsistent) {
    constexpr size_t kThreads = 4;
    constexpr size_t kRounds = 100;
    std::vector<BlockRegistrationHandle> registrations;
    for (size_t i = 0; i < kThreads; ++i) {
        auto registered = registry_.Register("racing/" + std::to_string(i));
        ASSERT_TRUE(registered);
        registrations.push_back(*registered);
    }
    std::atomic<size_t> failures{0};
    std::vector<std::thread> threads;
    for (size_t i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            const auto& registration = registrations[i];
            for (size_t round = 0; round < kRounds; ++round) {
                auto inserted = fast_->Insert(MakeCompleted(registration.Key()),
                                              registration);
                if (!inserted) {
                    ++failures;
                    continue;
                }
                BlockToken token;
                token.key = inserted->Key();
                token.registration_id = registration.Id();
                token.registration = registration.Downgrade();
                token.tiler_id = kFastTiler;
                token.block_id = inserted->Id();
                token.size_bytes = inserted->Size();
                fast_->OnAccess(token);
                if (!fast_->Lookup(registration)) ++failures;
                const auto victims = fast_->SelectVictims(kCapacity);
                bool selected = false;
                for (const auto& victim : victims) {
                    if (victim.block_id == token.block_id) selected = true;
                }
                // Each thread owns its registration, so its committed block
                // stays present throughout this selection despite other
                // writers.
                if (!selected) ++failures;
                if (!fast_->Erase(registration, token.block_id)) ++failures;
            }
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(failures.load(), 0u);
    EXPECT_EQ(fast_->Stats().entry_count, 0u);
    EXPECT_EQ(fast_->Stats().indexed_bytes, 0u);
    EXPECT_TRUE(fast_->SelectVictims(kCapacity).empty());
    EXPECT_EQ(pool_->Usage(), 0u);
}

INSTANTIATE_TEST_SUITE_P(Orderings, BlockIndexOrderingTest,
                         ::testing::Values("lru", "multi_lru", "tinylfu_lru"));

}  // namespace
}  // namespace mooncake::v2
