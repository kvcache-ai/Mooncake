// Tests for BlockRegistry, the cross-tiler key-identity layer of section 3.3.
//
// The registry owns identity and nothing else: it stores weak references only,
// hands out one canonical handle per live key, and mints a brand new identity
// once the previous one has been retired. The cases below pin exactly the
// invariants section 9.3 lists for this component -- canonical reuse, fresh
// identity after retire, automatic removal when the last strong handle dies,
// idempotent presence hints, and the pointer-identity check that keeps a late
// destructor from evicting a re-created registration.

#include "p2p/client/v2/block_registry.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "types.h"

namespace mooncake::v2 {

// Declared in mooncake (not in the anonymous namespace below) so that gtest's
// ADL finds it; otherwise every id mismatch is reported as a byte dump.
void PrintTo(const RegistrationId& id, std::ostream* os) {
    *os << "{shard=" << id.registry_shard << " seq=" << id.shard_sequence
        << "}";
}

namespace {

BlockRegistrationHandle MustRegister(const BlockRegistry& registry,
                                     std::string_view key) {
    auto result = registry.Register(key);
    if (!result.has_value()) {
        ADD_FAILURE() << "Register(" << key << ") failed: " << result.error();
        return BlockRegistrationHandle{};
    }
    return std::move(result.value());
}

// Retirement is only legal under this key's serialization point, so every
// test that kills a registration goes through the guard.
void RetireUnderGuard(const BlockRegistrationHandle& handle) {
    auto guard = handle.LockMutation();
    ASSERT_TRUE(guard.OwnsLock());
    handle.Retire(guard);
}

// Releases every worker at once so the racing window is as wide as possible.
class StartGate {
   public:
    void Wait() const {
        while (!open_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

    void Open() { open_.store(true, std::memory_order_release); }

   private:
    std::atomic<bool> open_{false};
};

class BlockRegistryTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("BlockRegistryTest");
            FLAGS_logtostderr = 1;
        });
    }

    BlockRegistry registry_{BlockRegistryConfig{}};
};

TEST_F(BlockRegistryTest, RegisterReusesTheLiveRegistrationForTheSameKey) {
    auto first = MustRegister(registry_, "key-alpha");
    auto second = MustRegister(registry_, "key-alpha");

    // Dedup is what makes the registration canonical: two tilers registering
    // the same key must end up coordinating through one identity.
    EXPECT_EQ(first.Id(), second.Id());
    EXPECT_EQ(first.IdentityPtr(), second.IdentityPtr());
    EXPECT_EQ(first.Key(), "key-alpha");
    EXPECT_EQ(second.Key(), "key-alpha");
    EXPECT_FALSE(first.IsRetired());
    EXPECT_EQ(registry_.SizeForTest(), 1u);
}

TEST_F(BlockRegistryTest, RegisterMintsAFreshIdentityForEachUnknownKey) {
    // One shard makes the minting order observable without depending on where
    // the string hash sends each key.
    BlockRegistry registry{BlockRegistryConfig{.shard_count = 1}};

    std::vector<BlockRegistrationHandle> handles;
    for (int i = 0; i < 4; ++i) {
        handles.push_back(MustRegister(registry, "key-" + std::to_string(i)));
    }

    std::unordered_set<RegistrationId, RegistrationIdHash> ids;
    uint64_t previous_sequence = 0;
    for (const auto& handle : handles) {
        // A minted id must never look like the default one an empty handle
        // reports, or a stale command could pass a staleness check by luck.
        EXPECT_NE(handle.Id(), RegistrationId{});
        EXPECT_EQ(handle.Id().registry_shard, 0u);
        EXPECT_GT(handle.Id().shard_sequence, previous_sequence);
        previous_sequence = handle.Id().shard_sequence;
        EXPECT_TRUE(ids.insert(handle.Id()).second);
    }
    EXPECT_EQ(registry.SizeForTest(), handles.size());
}

TEST_F(BlockRegistryTest, RegisterNeverReusesASequenceAfterTheEntryDisappears) {
    BlockRegistry registry{BlockRegistryConfig{.shard_count = 1}};

    RegistrationId first_id;
    {
        auto first = MustRegister(registry, "recycled");
        first_id = first.Id();
    }
    ASSERT_EQ(registry.SizeForTest(), 0u);

    // The shard sequence must move forward even though the map is empty
    // again: an async command still carrying `first_id` has to stay
    // recognizable as stale forever.
    auto second = MustRegister(registry, "recycled");
    EXPECT_NE(second.Id(), first_id);
    EXPECT_GT(second.Id().shard_sequence, first_id.shard_sequence);
    EXPECT_EQ(second.Key(), "recycled");
}

TEST_F(BlockRegistryTest, MatchMissesAnUnknownKey) {
    // A populated registry: a miss must come from the key not being there, not
    // from the shard being empty.
    auto present = MustRegister(registry_, "present");
    ASSERT_TRUE(static_cast<bool>(present));

    EXPECT_FALSE(registry_.Match("absent").has_value());
    EXPECT_FALSE(registry_.Match("").has_value());
    EXPECT_FALSE(registry_.Match("presen").has_value());
    EXPECT_FALSE(registry_.Match("present-suffix").has_value());
}

TEST_F(BlockRegistryTest, RetireMakesMatchMissWhileTheEntryIsStillStored) {
    auto handle = MustRegister(registry_, "retired-key");
    ASSERT_TRUE(registry_.Match("retired-key").has_value());

    {
        auto guard = handle.LockMutation();
        ASSERT_TRUE(guard.OwnsLock());
        EXPECT_FALSE(guard.IsRetired());
        handle.Retire(guard);
        EXPECT_TRUE(guard.IsRetired());

        // The miss has to be visible before the guard is released, otherwise a
        // concurrent Get could still adopt a key whose delete already
        // committed.
        EXPECT_FALSE(registry_.Match("retired-key").has_value());
    }

    EXPECT_TRUE(handle.IsRetired());
    EXPECT_FALSE(registry_.Match("retired-key").has_value());
    // Retiring is not removal: the entry lives as long as the identity does,
    // which is what lets the destructor's identity check work.
    EXPECT_EQ(registry_.SizeForTest(), 1u);
}

TEST_F(BlockRegistryTest, ReRegisterAfterRetireMintsADifferentIdentity) {
    auto old_handle = MustRegister(registry_, "same-key");
    const RegistrationId old_id = old_handle.Id();
    const BlockRegistrationHandleInner* old_identity = old_handle.IdentityPtr();
    RetireUnderGuard(old_handle);

    auto fresh = MustRegister(registry_, "same-key");

    // Same key string, deliberately different identity: that difference is the
    // only signal a delete-then-recreate leaves for in-flight work.
    EXPECT_EQ(fresh.Key(), old_handle.Key());
    EXPECT_NE(fresh.Id(), old_id);
    EXPECT_NE(fresh.IdentityPtr(), old_identity);
    EXPECT_FALSE(fresh.IsRetired());
    EXPECT_TRUE(old_handle.IsRetired());

    auto matched = registry_.Match("same-key");
    ASSERT_TRUE(matched.has_value());
    EXPECT_EQ(matched->Id(), fresh.Id());
    EXPECT_EQ(matched->IdentityPtr(), fresh.IdentityPtr());
    EXPECT_EQ(registry_.SizeForTest(), 1u);
}

TEST_F(BlockRegistryTest, DroppingEveryStrongHandleRemovesTheKey) {
    WeakBlockRegistrationHandle weak;
    {
        auto first = MustRegister(registry_, "weak-only");
        weak = first.Downgrade();
        {
            auto second = MustRegister(registry_, "weak-only");
            EXPECT_EQ(second.IdentityPtr(), first.IdentityPtr());
            EXPECT_EQ(registry_.SizeForTest(), 1u);
        }
        // Dropping a duplicate handle must not remove anything: the strong
        // count belongs to the identity, not to each handle copy.
        EXPECT_EQ(registry_.SizeForTest(), 1u);
        EXPECT_TRUE(weak.Lock().has_value());
    }

    EXPECT_EQ(registry_.SizeForTest(), 0u);
    EXPECT_FALSE(registry_.Match("weak-only").has_value());
}

TEST_F(BlockRegistryTest, MutationGuardKeepsTheIdentityAliveWithoutAHandle) {
    auto handle = MustRegister(registry_, "guarded");
    {
        auto guard = handle.LockMutation();
        // A mutation in progress must survive the caller dropping its handle,
        // or Delete could race the entry out from under itself.
        handle = BlockRegistrationHandle{};
        EXPECT_EQ(registry_.SizeForTest(), 1u);

        auto matched = registry_.Match("guarded");
        ASSERT_TRUE(matched.has_value());
        EXPECT_EQ(matched->Key(), "guarded");
    }

    EXPECT_EQ(registry_.SizeForTest(), 0u);
}

TEST_F(BlockRegistryTest, DestructorOfSupersededIdentityKeepsTheNewEntry) {
    auto old_handle = MustRegister(registry_, "recreated");
    RetireUnderGuard(old_handle);
    auto fresh = MustRegister(registry_, "recreated");
    ASSERT_NE(fresh.IdentityPtr(), old_handle.IdentityPtr());

    // The stale identity dies *after* the key was re-registered. Erasing by
    // key alone here would silently drop a live registration, which is the
    // exact failure the entry's identity_ptr/id check exists to prevent.
    old_handle = BlockRegistrationHandle{};

    EXPECT_EQ(registry_.SizeForTest(), 1u);
    auto matched = registry_.Match("recreated");
    ASSERT_TRUE(matched.has_value());
    EXPECT_EQ(matched->Id(), fresh.Id());
    EXPECT_EQ(matched->IdentityPtr(), fresh.IdentityPtr());
    EXPECT_TRUE(registry_.IsCanonical(fresh));
}

TEST_F(BlockRegistryTest, PresenceMarkersAreIdempotent) {
    auto handle = MustRegister(registry_, "presence");
    const UUID tiler_a{1, 11};
    const UUID tiler_b{2, 22};

    EXPECT_TRUE(handle.PresenceHint().empty());

    // Repeated marks come from repeated Put/Migrate on the same tiler; they
    // must not accumulate duplicates in the hint.
    handle.MarkPresent(tiler_a);
    handle.MarkPresent(tiler_a);
    EXPECT_EQ(handle.PresenceHint(), std::vector<UUID>{tiler_a});

    handle.MarkPresent(tiler_b);
    auto hint = handle.PresenceHint();
    std::sort(hint.begin(), hint.end());
    EXPECT_EQ(hint, (std::vector<UUID>{tiler_a, tiler_b}));

    handle.MarkAbsent(tiler_b);
    handle.MarkAbsent(tiler_b);
    EXPECT_EQ(handle.PresenceHint(), std::vector<UUID>{tiler_a});

    // Clearing a tiler that was never marked must leave the rest intact.
    handle.MarkAbsent(UUID{3, 33});
    EXPECT_EQ(handle.PresenceHint(), std::vector<UUID>{tiler_a});

    handle.MarkAbsent(tiler_a);
    EXPECT_TRUE(handle.PresenceHint().empty());
}

TEST_F(BlockRegistryTest, PresenceHintIsAHintBoundToTheIdentity) {
    auto handle = MustRegister(registry_, "hint-only");
    handle.MarkPresent(UUID{7, 7});
    EXPECT_TRUE(registry_.Match("hint-only").has_value());

    RetireUnderGuard(handle);
    // The hint is advisory, so marking a dead registration is allowed and must
    // neither resurrect it nor make Match lie.
    handle.MarkPresent(UUID{8, 8});
    EXPECT_FALSE(registry_.Match("hint-only").has_value());
    EXPECT_EQ(handle.PresenceHint().size(), 2u);

    auto fresh = MustRegister(registry_, "hint-only");
    // Presence follows the identity, not the key string: the new registration
    // starts with no claims about where the key lives.
    EXPECT_TRUE(fresh.PresenceHint().empty());
}

TEST_F(BlockRegistryTest, IsCanonicalTurnsFalseOnlyWhenSuperseded) {
    auto handle = MustRegister(registry_, "canonical");
    EXPECT_TRUE(registry_.IsCanonical(handle));

    RetireUnderGuard(handle);
    // Retired but not yet replaced: still the stored registration, which is
    // why canonicity and retirement are two separate staleness checks.
    EXPECT_TRUE(registry_.IsCanonical(handle));

    auto fresh = MustRegister(registry_, "canonical");
    EXPECT_FALSE(registry_.IsCanonical(handle));
    EXPECT_TRUE(registry_.IsCanonical(fresh));
}

TEST_F(BlockRegistryTest, WeakHandleFailsToLockOnceTheIdentityIsGone) {
    WeakBlockRegistrationHandle weak;
    RegistrationId id;
    {
        auto handle = MustRegister(registry_, "async-command");
        weak = handle.Downgrade();
        id = handle.Id();

        auto locked = weak.Lock();
        ASSERT_TRUE(locked.has_value());
        EXPECT_EQ(locked->Id(), id);
        EXPECT_EQ(locked->IdentityPtr(), handle.IdentityPtr());
    }

    // Failing to upgrade is the first of the three staleness checks an async
    // command performs.
    EXPECT_FALSE(weak.Lock().has_value());
    // The id outlives the identity so the stale command can still report what
    // it was carrying.
    EXPECT_EQ(weak.Id(), id);
}

TEST_F(BlockRegistryTest, WeakHandleOfRetiredRegistrationUpgradesAsRetired) {
    auto handle = MustRegister(registry_, "staleness");
    auto weak = handle.Downgrade();
    RetireUnderGuard(handle);

    // Upgrade succeeding is not enough: the second check (retired) is what
    // stops a queued command from acting on a deleted key.
    auto locked = weak.Lock();
    ASSERT_TRUE(locked.has_value());
    EXPECT_TRUE(locked->IsRetired());
    EXPECT_EQ(locked->Id(), weak.Id());
}

TEST_F(BlockRegistryTest, DefaultHandleIsEmptyAndReportsRetired) {
    BlockRegistrationHandle handle;

    EXPECT_FALSE(static_cast<bool>(handle));
    EXPECT_TRUE(handle.Key().empty());
    EXPECT_EQ(handle.Id(), RegistrationId{});
    // "No identity" must read as dead, so callers that only test IsRetired()
    // cannot mistake an empty handle for a usable one.
    EXPECT_TRUE(handle.IsRetired());
    EXPECT_FALSE(registry_.IsCanonical(handle));

    handle.MarkPresent(UUID{1, 1});
    EXPECT_TRUE(handle.PresenceHint().empty());
    EXPECT_FALSE(handle.Downgrade().Lock().has_value());

    auto guard = handle.LockMutation();
    EXPECT_FALSE(guard.OwnsLock());
    EXPECT_TRUE(guard.IsRetired());
}

TEST_F(BlockRegistryTest, RegisterRejectsAnEmptyKey) {
    auto result = registry_.Register("");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(registry_.SizeForTest(), 0u);
}

TEST_F(BlockRegistryTest, DefaultConstructedRegistryRefusesRegister) {
    BlockRegistry stateless;

    EXPECT_FALSE(static_cast<bool>(stateless));
    EXPECT_EQ(stateless.ShardCount(), 0u);
    EXPECT_EQ(stateless.SizeForTest(), 0u);

    auto result = stateless.Register("key");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_FALSE(stateless.Match("key").has_value());
    EXPECT_TRUE(stateless.SnapshotShard(0).empty());
    EXPECT_FALSE(stateless.IsCanonical(BlockRegistrationHandle{}));
}

TEST_F(BlockRegistryTest, ShardCountFollowsConfigAndNeverDropsToZero) {
    EXPECT_EQ(BlockRegistry(BlockRegistryConfig{.shard_count = 8}).ShardCount(),
              8u);

    // A zero-shard config would make the shard modulo divide by zero, so it is
    // clamped rather than rejected.
    BlockRegistry degenerate{BlockRegistryConfig{.shard_count = 0}};
    EXPECT_EQ(degenerate.ShardCount(), 1u);
    auto handle = MustRegister(degenerate, "still-works");
    EXPECT_EQ(handle.Id().registry_shard, 0u);
    EXPECT_TRUE(degenerate.IsCanonical(handle));
}

TEST_F(BlockRegistryTest, SnapshotShardHandsOutWeakHandlesOnly) {
    BlockRegistry registry{BlockRegistryConfig{.shard_count = 1}};
    auto handle = MustRegister(registry, "snap");

    auto snapshot = registry.SnapshotShard(0);
    ASSERT_EQ(snapshot.size(), 1u);
    EXPECT_EQ(snapshot[0].key, "snap");
    EXPECT_EQ(snapshot[0].registration.Id(), handle.Id());
    {
        auto locked = snapshot[0].registration.Lock();
        ASSERT_TRUE(locked.has_value());
        EXPECT_EQ(locked->IdentityPtr(), handle.IdentityPtr());
    }

    handle = BlockRegistrationHandle{};
    // A walk holding a snapshot must not pin registrations alive; observing a
    // vanished key mid-walk is the expected outcome, not a leak.
    EXPECT_EQ(registry.SizeForTest(), 0u);
    EXPECT_FALSE(snapshot[0].registration.Lock().has_value());
    EXPECT_TRUE(registry.SnapshotShard(1).empty());
}

TEST_F(BlockRegistryTest, ConcurrentRegisterOfTheSameKeyYieldsOneIdentity) {
    constexpr int kThreads = 16;
    constexpr int kRounds = 200;

    // Held for the whole race so no legal re-mint can happen: any divergence
    // below is a dedup bug, not a registration that expired and came back.
    auto anchor = MustRegister(registry_, "hot-key");

    StartGate gate;
    std::vector<std::vector<BlockRegistrationHandle>> observed(kThreads);
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            observed[t].reserve(kRounds);
            gate.Wait();
            for (int i = 0; i < kRounds; ++i) {
                auto result = registry_.Register("hot-key");
                if (!result.has_value()) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                observed[t].push_back(std::move(result.value()));
            }
        });
    }
    gate.Open();
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(failures.load(), 0);
    size_t divergent = 0;
    for (const auto& handles : observed) {
        EXPECT_EQ(handles.size(), static_cast<size_t>(kRounds));
        for (const auto& handle : handles) {
            if (handle.Id() != anchor.Id() ||
                handle.IdentityPtr() != anchor.IdentityPtr()) {
                ++divergent;
            }
        }
    }
    EXPECT_EQ(divergent, 0u);
    EXPECT_EQ(registry_.SizeForTest(), 1u);
}

TEST_F(BlockRegistryTest, ConcurrentRegisterOfDistinctKeysAllSucceed) {
    constexpr int kThreads = 8;
    constexpr int kKeysPerThread = 250;

    StartGate gate;
    std::vector<std::vector<BlockRegistrationHandle>> observed(kThreads);
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            observed[t].reserve(kKeysPerThread);
            gate.Wait();
            for (int i = 0; i < kKeysPerThread; ++i) {
                const std::string key =
                    "key-" + std::to_string(t) + "-" + std::to_string(i);
                auto result = registry_.Register(key);
                if (!result.has_value()) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                observed[t].push_back(std::move(result.value()));
            }
        });
    }
    gate.Open();
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(failures.load(), 0);
    // Sequences are minted per shard under that shard's lock, so ids must stay
    // globally unique even when unrelated keys race through different shards.
    std::unordered_set<RegistrationId, RegistrationIdHash> ids;
    size_t total = 0;
    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < static_cast<int>(observed[t].size()); ++i) {
            const auto& handle = observed[t][i];
            EXPECT_EQ(handle.Key(),
                      "key-" + std::to_string(t) + "-" + std::to_string(i));
            EXPECT_TRUE(ids.insert(handle.Id()).second);
            ++total;
        }
    }
    EXPECT_EQ(total, static_cast<size_t>(kThreads) * kKeysPerThread);
    EXPECT_EQ(registry_.SizeForTest(), total);
}

TEST_F(BlockRegistryTest, ConcurrentRetireAndReRegisterLeavesNoStaleEntry) {
    constexpr int kThreads = 8;
    constexpr int kRounds = 200;

    StartGate gate;
    std::atomic<int> failures{0};
    std::atomic<int> resurrected{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            gate.Wait();
            for (int i = 0; i < kRounds; ++i) {
                auto result = registry_.Register("churn");
                if (!result.has_value()) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                auto handle = std::move(result.value());
                {
                    auto guard = handle.LockMutation();
                    if (!guard.IsRetired()) {
                        handle.Retire(guard);
                    }
                }
                // Whatever Match returns now, it must never be the identity
                // this thread just retired.
                auto matched = registry_.Match("churn");
                if (matched.has_value() &&
                    matched->IdentityPtr() == handle.IdentityPtr()) {
                    resurrected.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    gate.Open();
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(resurrected.load(), 0);
    // Every identity created during the race is gone, so the destructor's
    // identity check must have erased exactly the entries it owned -- no more
    // (a leaked entry) and no fewer (an entry erased by a stale destructor
    // would have shown up as a resurrected match above).
    EXPECT_EQ(registry_.SizeForTest(), 0u);
    EXPECT_FALSE(registry_.Match("churn").has_value());
}

TEST_F(BlockRegistryTest, MutationGuardSerializesMutationsOnTheSameIdentity) {
    constexpr int kThreads = 8;
    constexpr int kRounds = 500;

    auto anchor = MustRegister(registry_, "serialized");

    StartGate gate;
    std::atomic<int> concurrent{0};
    std::atomic<int> overlaps{0};
    int guarded_counter = 0;  // deliberately unsynchronized
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            // Each thread locks through its own handle: the mutex belongs to
            // the identity, not to a particular handle copy.
            auto handle = MustRegister(registry_, "serialized");
            gate.Wait();
            for (int i = 0; i < kRounds; ++i) {
                auto guard = handle.LockMutation();
                if (concurrent.fetch_add(1, std::memory_order_acq_rel) != 0) {
                    overlaps.fetch_add(1, std::memory_order_relaxed);
                }
                ++guarded_counter;
                concurrent.fetch_sub(1, std::memory_order_acq_rel);
            }
        });
    }
    gate.Open();
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(overlaps.load(), 0);
    EXPECT_EQ(guarded_counter, kThreads * kRounds);
    EXPECT_TRUE(registry_.IsCanonical(anchor));
}

}  // namespace
}  // namespace mooncake::v2
