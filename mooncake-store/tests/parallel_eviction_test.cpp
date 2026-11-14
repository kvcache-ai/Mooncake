#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "allocator.h"
#include "eviction/evictor.h"
#include "eviction/range_executor.h"
#include "metadata/lease_eviction_index.h"
#include "metadata/namespace.h"

namespace mooncake {
namespace {

using Clock = metadata::EvictionIndex::Clock;

class ShardedLeaseIndexTest : public ::testing::Test {
   protected:
    std::shared_ptr<OffsetBufferAllocator> allocator_ =
        std::make_shared<OffsetBufferAllocator>("sharded-test", 0x100000000ULL,
                                                64 * 1024 * 1024,
                                                "sharded-test", "tcp");
    metadata::Namespace ns_;
    const Clock::time_point now_{std::chrono::seconds(1000)};

    void Publish(const std::string& key, const std::string& group = "",
                 bool hard_pin = false) {
        PublishAt(key, now_ - std::chrono::seconds(10), group, hard_pin);
    }

    void PublishAt(const std::string& key, Clock::time_point deadline,
                   const std::string& group = "", bool hard_pin = false) {
        auto guard = ns_.objects.WriteKey(key);
        std::vector<Replica> replicas;
        replicas.emplace_back(allocator_->allocate(128),
                              ReplicaStatus::COMPLETE);
        (void)guard.Publish(UUID{1, 2}, Clock::time_point{}, 128,
                            std::move(replicas), std::nullopt, hard_pin,
                            ObjectDataType::UNKNOWN, group);
        SpinLocker locker(&guard.metadata().lock);
        guard.metadata().lease_->SetDeadline(deadline);
    }
};

TEST_F(ShardedLeaseIndexTest,
       StandaloneIdentitySurvivesDeletionAndRejectsRepublication) {
    Publish("a");
    auto cursor = ns_.eviction_index().Scan(now_);
    auto entry = cursor->Next();
    ASSERT_TRUE(entry);
    {
        auto guard = ns_.objects.Write("a");
        ASSERT_TRUE(guard);
        guard->TearDown();
    }
    Publish("a");
    auto guard = ns_.objects.Write("a");
    ASSERT_TRUE(guard);
    EXPECT_FALSE(cursor->StillAt(*guard, *entry));
    EXPECT_EQ(entry->keys.front(), "a");
}

TEST_F(ShardedLeaseIndexTest,
       InPlaceStandaloneRepublicationRejectsOldEntryIdentity) {
    Publish("a");
    auto cursor = ns_.eviction_index().Scan(now_);
    auto entry = cursor->Next();
    ASSERT_TRUE(entry);
    {
        auto guard = ns_.objects.Write("a");
        ASSERT_TRUE(guard);
        guard->TearDown();
        std::vector<Replica> replicas;
        replicas.emplace_back(allocator_->allocate(128),
                              ReplicaStatus::COMPLETE);
        (void)guard->Publish(UUID{1, 2}, Clock::time_point{}, 128,
                             std::move(replicas), std::nullopt, false,
                             ObjectDataType::UNKNOWN, "");
        SpinLocker locker(&guard->metadata().lock);
        guard->metadata().lease_->SetDeadline(now_ - std::chrono::seconds(10));
    }
    auto guard = ns_.objects.Write("a");
    ASSERT_TRUE(guard);
    EXPECT_FALSE(cursor->StillAt(*guard, *entry));
}

TEST_F(ShardedLeaseIndexTest, CursorExcludesPublicationsAddedAfterScanBegan) {
    Publish("first");
    auto cursor = ns_.eviction_index().ScanRanges(now_, 1);
    ASSERT_TRUE(cursor);
    Publish("later_same_bucket");
    PublishAt("later_older_bucket", now_ - std::chrono::seconds(20));
    const auto entry = cursor->Next();
    ASSERT_TRUE(entry);
    EXPECT_EQ(entry->keys.front(), "first");
    EXPECT_FALSE(cursor->Next());
}

TEST_F(ShardedLeaseIndexTest, BusyKeyIsNotWaitedOnByTryWrite) {
    Publish("a");
    auto hold = ns_.objects.Read("a");
    auto attempt = std::async(std::launch::async, [&] {
        return ns_.objects.TryWrite("a").has_value();
    });
    const auto ready = attempt.wait_for(std::chrono::seconds(1));
    EXPECT_EQ(ready, std::future_status::ready);
    hold.reset();
    EXPECT_FALSE(attempt.get());
    EXPECT_TRUE(ns_.objects.TryWrite("a"));
}

TEST_F(ShardedLeaseIndexTest, LiveLeaseIsRevalidatedAndRefiled) {
    Publish("a");
    auto cursor = ns_.eviction_index().Scan(now_);
    auto entry = cursor->Next();
    ASSERT_TRUE(entry);
    {
        auto guard = ns_.objects.Read("a");
        guard->metadata().lease_->ExtendTo(now_ + std::chrono::seconds(20));
    }
    {
        auto guard = ns_.objects.Write("a");
        EXPECT_FALSE(cursor->StillAt(*guard, *entry));
    }
    EXPECT_FALSE(ns_.eviction_index().Scan(now_)->Next());
    EXPECT_TRUE(
        ns_.eviction_index().Scan(now_ + std::chrono::seconds(30))->Next());
}

TEST_F(ShardedLeaseIndexTest,
       ExpiredReadRenewalRefilesIntoDueBucketAndWidenReclaimsItOnce) {
    for (const bool serial : {false, true}) {
        const std::string key = serial ? "serial" : "range";
        Publish(key);
        {
            auto guard = ns_.objects.Read(key);
            guard->metadata().lease_->ExtendTo(now_ - std::chrono::seconds(1));
        }
        auto cursor = ns_.eviction_index().ScanRanges(now_, 1);
        size_t visits = 0;
        auto check = [&](const auto& entry) {
            if (entry.keys.front() != key) {
                return true;
            }
            auto guard = ns_.objects.Write(key);
            // Rejected at its stale bucket, refiled on lock release, then
            // admitted at its real due bucket once widened.
            EXPECT_EQ(cursor->StillAt(*guard, entry), visits != 0);
            ++visits;
            return true;
        };
        if (serial) {
            while (auto entry = cursor->Next()) {
                check(*entry);
            }
        } else {
            do {
                cursor->VisitRange(0, cursor->Limit(), check);
            } while (cursor->Widen());
        }
        EXPECT_EQ(visits, 2u);
        auto guard = ns_.objects.Write(key);
        guard->TearDown();
    }
}

TEST_F(ShardedLeaseIndexTest,
       NegativeEpochCutoffRoundsTowardEarlierBucketAndExcludesFutureRanks) {
    const Clock::time_point before_epoch{std::chrono::milliseconds(-250)};
    const Clock::time_point cutoff{std::chrono::milliseconds(-210)};
    const Clock::time_point later{std::chrono::milliseconds(-150)};
    PublishAt("early", before_epoch);
    PublishAt("later", later);

    auto cursor = ns_.eviction_index().Scan(cutoff);
    auto first = cursor->Next();
    ASSERT_TRUE(first);
    EXPECT_EQ(first->keys.front(), "early");
    EXPECT_EQ(first->rank, -3);
    EXPECT_FALSE(cursor->Next());
}

TEST_F(ShardedLeaseIndexTest, GroupIsOneEntryAndRecreationGetsANewIdentity) {
    Publish("a", "g");
    Publish("b", "g");
    Publish("pinned", "", true);
    auto cursor = ns_.eviction_index().Scan(now_);
    EXPECT_EQ(cursor->Total(), 2u);
    auto entry = cursor->Next();
    ASSERT_TRUE(entry);
    EXPECT_EQ(
        (std::set<std::string_view>(entry->keys.begin(), entry->keys.end())),
        (std::set<std::string_view>{"a", "b"}));
    {
        auto guard = ns_.objects.Write("a");
        guard->TearDown();
    }
    {
        auto guard = ns_.objects.Write("b");
        guard->TearDown();
    }
    Publish("a", "g");
    {
        auto guard = ns_.objects.Write("a");
        EXPECT_FALSE(cursor->StillAt(*guard, *entry));
    }
    auto next = ns_.eviction_index().Scan(now_);
    EXPECT_EQ(next->Total(), 1u);
    EXPECT_TRUE(next->Next());
    EXPECT_FALSE(next->Next());
}

TEST_F(ShardedLeaseIndexTest,
       GroupMemberJoiningDuringScanHonorsSharedLeaseAndWholeGroupSemantics) {
    // Expired group: a member joining after Scan() starts is yielded together
    // with the existing group rather than stranded behind.
    Publish("a", "g_expired");
    auto expired_scan = ns_.eviction_index().Scan(now_);
    Publish("b", "g_expired");
    auto entry = expired_scan->Next();
    ASSERT_TRUE(entry);
    EXPECT_EQ(
        (std::set<std::string_view>(entry->keys.begin(), entry->keys.end())),
        (std::set<std::string_view>{"a", "b"}));
    for (const std::string_view key : entry->keys) {
        auto guard = ns_.objects.Write(key);
        ASSERT_TRUE(guard);
        EXPECT_TRUE(expired_scan->StillAt(*guard, *entry));
        guard->TearDown();
    }

    // Live extension on join: moves the group out of due buckets if not yet
    // copied, and rejects already-copied entries via StillAt.
    Publish("c", "g_extended");
    auto scan_before_copy = ns_.eviction_index().Scan(now_);
    auto scan_after_copy = ns_.eviction_index().Scan(now_);
    auto copied = scan_after_copy->Next();
    ASSERT_TRUE(copied);
    PublishAt("d", now_ + std::chrono::seconds(20), "g_extended");
    EXPECT_FALSE(scan_before_copy->Next());
    {
        auto guard = ns_.objects.Write("c");
        ASSERT_TRUE(guard);
        EXPECT_FALSE(scan_after_copy->StillAt(*guard, *copied));
    }
}

TEST_F(ShardedLeaseIndexTest, DisjointWorkersSeeEachKeyOnceAcrossShards) {
    constexpr int kCount = 4000;
    for (int i = 0; i < kCount; ++i) {
        PublishAt("k" + std::to_string(i),
                  now_ - std::chrono::milliseconds(100 * (1 + (i % 16))));
    }
    auto cursor = ns_.eviction_index().ScanRanges(now_, 0);
    EXPECT_GT(cursor->Limit(), 1u);
    std::mutex mutex;
    std::set<std::string> seen;
    ThreadPool pool(4);
    const auto result = eviction::RunRanges(
        *cursor, pool, kCount,
        [&](uint32_t, const auto& entry, eviction::RangeStop&) {
            auto guard = ns_.objects.Write(entry.keys.front());
            EXPECT_TRUE(cursor->StillAt(*guard, entry));
            std::lock_guard lock(mutex);
            EXPECT_TRUE(seen.emplace(entry.keys.front()).second);
            return uint64_t{1};
        },
        cursor->GrainSize());
    EXPECT_EQ(result.progress, static_cast<uint64_t>(kCount));
    EXPECT_EQ(seen.size(), static_cast<size_t>(kCount));
    EXPECT_EQ(cursor->Examined(), static_cast<uint64_t>(kCount));
}

TEST_F(ShardedLeaseIndexTest,
       ConcurrentMultiThreadedWritesAndScansHaveNoRacesOrDanglingSlots) {
    for (int i = 0; i < 64; ++i) {
        Publish("k" + std::to_string(i));
    }
    std::atomic<bool> done{false};
    std::thread scanner([&] {
        while (!done.load(std::memory_order_relaxed)) {
            auto cursor = ns_.eviction_index().ScanRanges(now_, 0);
            cursor->VisitRange(0, cursor->Limit(), [&](const auto& entry) {
                auto guard = ns_.objects.TryWrite(entry.keys.front());
                if (guard) {
                    (void)cursor->StillAt(*guard, entry);
                }
                return true;
            });
        }
    });

    std::vector<std::thread> writers;
    for (int t = 0; t < 4; ++t) {
        writers.emplace_back([&, t] {
            for (int i = 0; i < 200; ++i) {
                const auto key = "k" + std::to_string(t * 16 + (i % 16));
                {
                    auto guard = ns_.objects.Write(key);
                    if (guard) {
                        guard->TearDown();
                    }
                }
                PublishAt(
                    key, now_ - std::chrono::milliseconds(100 * (1 + (i % 8))));
            }
        });
    }
    for (auto& w : writers) {
        w.join();
    }
    done.store(true, std::memory_order_relaxed);
    scanner.join();
    EXPECT_EQ(ns_.objects.ObjectCount(), 64u);
}

// A mock live range cursor: first wave exposes only 20 slots; after widening
// the remaining slots are reachable. Successful slots cannot be taken twice.
struct TwoWaves {
    unsigned wave = 0;
    std::array<std::atomic<bool>, 80> alive{};
    TwoWaves() {
        for (auto& item : alive) {
            item.store(true);
        }
    }
    uint64_t Limit() const { return alive.size(); }
    bool Widen() { return wave++ == 0; }
    template <typename Visit>
    void VisitRange(uint64_t begin, uint64_t end, Visit&& visit) {
        for (auto i = begin; i < end; ++i) {
            if ((wave == 0 && i >= 20) || !alive[i].load()) {
                continue;
            }
            if (!visit(i)) {
                return;
            }
        }
    }
};

TEST(RangeExecutorTest, RemainingGoalIsNotResetOnWiden) {
    TwoWaves cursor;
    ThreadPool pool(1);
    const auto result = eviction::RunRanges(
        cursor, pool, 60,
        [&](uint32_t, uint64_t i, eviction::RangeStop&) {
            return uint64_t(cursor.alive[i].exchange(false));
        },
        1);
    EXPECT_EQ(result.waves, 2u);
    EXPECT_EQ(result.progress, 60u);
    EXPECT_EQ(std::count_if(cursor.alive.begin(), cursor.alive.end(),
                            [](const auto& item) { return !item.load(); }),
              60);
}

TEST(RangeExecutorTest,
     ConcurrentOvershootIsOnlyInFlightEntriesNotAnExtraGoal) {
    TwoWaves cursor;
    ThreadPool pool(4);
    const auto result = eviction::RunRanges(
        cursor, pool, 60,
        [&](uint32_t, uint64_t i, eviction::RangeStop&) {
            return uint64_t(cursor.alive[i].exchange(false));
        },
        1);
    EXPECT_GE(result.progress, 60u);
    EXPECT_LE(result.progress, 63u);
}

TEST(RangeExecutorTest, VisitorStopEndsTheRunBeforeTheGoal) {
    TwoWaves cursor;
    ThreadPool pool(1);
    size_t visits = 0;
    const auto result = eviction::RunRanges(
        cursor, pool, 60,
        [&](uint32_t, uint64_t i, eviction::RangeStop& stop) {
            if (++visits == 5) {
                stop.Stop();
            }
            return uint64_t(cursor.alive[i].exchange(false));
        },
        1);
    EXPECT_EQ(result.waves, 1u);
    EXPECT_EQ(result.progress, 5u);
    EXPECT_EQ(visits, 5u);
}

TEST(RangeExecutorTest, AdmissionFailureDoesNotWaitOnAnUnadmittedFuture) {
    TwoWaves cursor;
    ThreadPool pool(2);
    pool.stop();
    EXPECT_THROW(
        eviction::RunRanges(cursor, pool, 60,
                            [](uint32_t, uint64_t, eviction::RangeStop&) {
                                return uint64_t{1};
                            }),
        std::runtime_error);
}

TEST(RangeExecutorTest, SerialExceptionPropagatesWithoutAnotherWave) {
    TwoWaves cursor;
    ThreadPool pool(1);
    size_t visits = 0;
    EXPECT_THROW(eviction::RunRanges(
                     cursor, pool, 60,
                     [&](uint32_t, uint64_t, eviction::RangeStop&) -> uint64_t {
                         ++visits;
                         throw std::runtime_error("injected");
                     },
                     1),
                 std::runtime_error);
    EXPECT_EQ(visits, 1u);
}

TEST(RangeExecutorTest, ExceptionJoinsWorkersBeforeReturning) {
    TwoWaves cursor;
    ThreadPool pool(4);
    std::atomic<int> active{0};
    EXPECT_THROW(
        eviction::RunRanges(
            cursor, pool, 60,
            [&](uint32_t, uint64_t i, eviction::RangeStop&) -> uint64_t {
                struct Scope {
                    std::atomic<int>& n;
                    explicit Scope(std::atomic<int>& n) : n(n) { ++n; }
                    ~Scope() { --n; }
                } scope(active);
                if (i == 3) {
                    throw std::runtime_error("injected");
                }
                return uint64_t(cursor.alive[i].exchange(false));
            },
            1),
        std::runtime_error);
    EXPECT_EQ(active.load(), 0);
}

class ParallelPolicy final : public eviction::EvictionPolicy {
   public:
    bool parallel_safe = true;

    bool SupportsParallel() const override { return parallel_safe; }

    tl::expected<eviction::Plan, eviction::SkipReason> Decide(
        const eviction::Candidate& c) const override {
        if (c.key.starts_with("soft") && !c.fallback) {
            return tl::make_unexpected(eviction::SkipReason::kDeferred);
        }
        eviction::Plan plan;
        for (const auto& replica : c.metadata.GetAllReplicas()) {
            plan.release.push_back(replica.id());
        }
        return plan;
    }
};

class ParallelReclaimer final : public eviction::Reclaimer {
   public:
    bool fail = false;
    bool parallel_safe = true;

    bool SupportsParallel() const override { return parallel_safe; }

    tl::expected<eviction::Effect, ErrorCode> Apply(
        metadata::Namespace&, const route::WriteGuard& hold,
        const eviction::Plan&, const eviction::Report&) override {
        if (fail) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        eviction::Effect effect;
        effect.detached =
            hold.metadata().PopReplicas([](const Replica&) { return true; });
        effect.released_bytes =
            effect.detached.size() * hold.metadata().TotalLength();
        effect.needs_teardown = true;
        effect.outcome = eviction::Outcome::kDropped;
        return effect;
    }

    void TearDown(metadata::Namespace&,
                  const route::WriteGuard& hold) override {
        hold.TearDown();
    }
};

class ParallelEvictorNativeTest : public ::testing::Test {
   protected:
    std::shared_ptr<OffsetBufferAllocator> allocator_ =
        std::make_shared<OffsetBufferAllocator>(
            "parallel", 0x100000000ULL, 64 * 1024 * 1024, "parallel", "tcp");
    metadata::NamespaceTable namespaces_{[](const TenantId& id) {
        return std::make_unique<metadata::Namespace>(id);
    }};
    ParallelPolicy policy_;
    ParallelReclaimer reclaimer_;

    void Publish(const std::string& key, const std::string& group = "",
                 bool live = false) {
        auto& ns = namespaces_.GetOrCreate(TenantId{});
        auto guard = ns.objects.WriteKey(key);
        std::vector<Replica> replicas;
        replicas.emplace_back(allocator_->allocate(128),
                              ReplicaStatus::COMPLETE);
        (void)guard.Publish(UUID{1, 2}, Clock::time_point{}, 128,
                            std::move(replicas), std::nullopt, false,
                            ObjectDataType::UNKNOWN, group);
        SpinLocker locker(&guard.metadata().lock);
        guard.metadata().lease_->SetDeadline(
            live ? Clock::now() + std::chrono::hours(1) : Clock::time_point{});
    }
};

TEST_F(ParallelEvictorNativeTest, AutomaticWorkersGrowAndShrinkWithFiledKeys) {
    class PopulationIndex final : public metadata::EvictionIndex {
        class EmptyCursor final : public RangeCursor {
           public:
            explicit EmptyCursor(size_t total) : total_(total) {}
            std::optional<Entry> Next() override { return std::nullopt; }
            bool StillAt(const route::WriteGuard&,
                         const Entry&) const override {
                return false;
            }
            size_t Total() const override { return total_; }
            uint64_t Limit() const override { return 0; }
            void VisitRange(uint64_t, uint64_t, const Visitor&) const override {
            }
            bool Widen() override { return false; }
            uint64_t Examined() const override { return 0; }

           private:
            size_t total_;
        };

       public:
        explicit PopulationIndex(const size_t& population)
            : EvictionIndex(false), population_(population) {}
        void OnRead(const route::ReadGuard&) override {}
        void OnWrite(const route::WriteGuard&) override {}
        std::unique_ptr<Cursor> Scan(Clock::time_point now) const override {
            return EvictionIndex::ScanRanges(now, 0);
        }
        std::unique_ptr<RangeCursor> ScanRanges(
            Clock::time_point, const TargetForTotal&) const override {
            return std::make_unique<EmptyCursor>(population_);
        }

       private:
        const size_t& population_;
    };

    size_t population = 0;
    metadata::NamespaceTable namespaces([&](const TenantId& id) {
        return std::make_unique<metadata::Namespace>(
            id, nullptr, nullptr,
            std::make_unique<PopulationIndex>(population));
    });
    (void)namespaces.GetOrCreate(TenantId{});
    eviction::Evictor evictor(namespaces, policy_, reclaimer_, {});
    const std::pair<size_t, uint32_t> cases[] = {
        {0, 1},
        {16000000, 1},
        {16000001, 2},
        {200000000, 13},
        {500000000, 32},
        {1024000001, 64},
        {std::numeric_limits<size_t>::max(), 64},
        {200000000, 13},
        {0, 1}};
    for (const auto& [filed, workers] : cases) {
        population = filed;
        const auto report =
            evictor.Run(TenantId{}, eviction::ByteGoal{.target_bytes = 1});
        EXPECT_EQ(report.workers, workers) << filed;
    }
    population = 500000000;
    eviction::Evictor fixed(namespaces, policy_, reclaimer_, {}, 4);
    EXPECT_EQ(
        fixed.Run(TenantId{}, eviction::ByteGoal{.target_bytes = 1}).workers,
        4u);
    reclaimer_.parallel_safe = false;
    eviction::Evictor serial(namespaces, policy_, reclaimer_, {});
    EXPECT_EQ(
        serial.Run(TenantId{}, eviction::ByteGoal{.target_bytes = 1}).workers,
        1u);
}

TEST_F(ParallelEvictorNativeTest, ConcurrentRoundHonorsGoalAndKeepsLiveLeases) {
    for (int i = 0; i < 33000; ++i) {
        Publish(std::to_string(i), "", i < 1000);
    }
    eviction::Evictor evictor(namespaces_, policy_, reclaimer_, {}, 4);
    const auto report =
        evictor.Run(eviction::Scope::All(), {.target_ratio = .1});
    EXPECT_GE(report.evicted_objects, 3300u);
    EXPECT_LE(report.evicted_objects, 3303u);
    EXPECT_EQ(report.released_bytes, report.evicted_objects * 128);
    EXPECT_GT(report.scanned_slots, 0u);
    auto& ns = namespaces_.GetOrCreate(TenantId{});
    for (int i = 0; i < 1000; ++i) {
        EXPECT_TRUE(ns.objects.Read(std::to_string(i)));
    }
}

TEST_F(ParallelEvictorNativeTest, GroupIsNotSplitByGoal) {
    for (int i = 0; i < 10; ++i) {
        Publish("g" + std::to_string(i), "g");
    }
    for (int i = 0; i < 90; ++i) {
        auto& ns = namespaces_.GetOrCreate(TenantId{});
        auto guard = ns.objects.WriteKey("k" + std::to_string(i));
        std::vector<Replica> replicas;
        replicas.emplace_back(allocator_->allocate(128),
                              ReplicaStatus::COMPLETE);
        (void)guard.Publish(UUID{1, 2}, Clock::time_point{}, 128,
                            std::move(replicas), std::nullopt, false,
                            ObjectDataType::UNKNOWN, "");
        SpinLocker locker(&guard.metadata().lock);
        guard.metadata().lease_->SetDeadline(
            Clock::time_point{metadata::LeaseEvictionIndex::kBucketWidth});
    }
    eviction::Evictor evictor(namespaces_, policy_, reclaimer_, {}, 4);
    const auto report =
        evictor.Run(eviction::Scope::All(), {.target_ratio = .01});
    EXPECT_EQ(report.evicted_objects, 10u);
    EXPECT_EQ(report.Count(eviction::Outcome::kDropped), 10u);
    auto& ns = namespaces_.GetOrCreate(TenantId{});
    for (int i = 0; i < 90; ++i) {
        EXPECT_TRUE(ns.objects.Read("k" + std::to_string(i)));
    }
}

TEST_F(ParallelEvictorNativeTest,
       OlderRankSkippedWidensIntoNextRankWithoutSkippingIntermediateRank) {
    auto& ns = namespaces_.GetOrCreate(TenantId{});
    auto publish_at = [&](const std::string& key, Clock::time_point deadline) {
        auto guard = ns.objects.WriteKey(key);
        std::vector<Replica> replicas;
        replicas.emplace_back(allocator_->allocate(128),
                              ReplicaStatus::COMPLETE);
        (void)guard.Publish(UUID{1, 2}, Clock::time_point{}, 128,
                            std::move(replicas), std::nullopt, false,
                            ObjectDataType::UNKNOWN, "");
        SpinLocker locker(&guard.metadata().lock);
        guard.metadata().lease_->SetDeadline(deadline);
    };
    const auto w = metadata::LeaseEvictionIndex::kBucketWidth;
    for (int i = 0; i < 5; ++i) {
        publish_at("r0_" + std::to_string(i), Clock::time_point{});
        auto read = ns.objects.Read("r0_" + std::to_string(i));
        read->metadata().lease_->ExtendTo(Clock::now() + std::chrono::hours(1));
    }
    for (int i = 0; i < 5; ++i) {
        publish_at("r1_" + std::to_string(i), Clock::time_point{w});
    }
    for (int i = 0; i < 10; ++i) {
        publish_at("r2_" + std::to_string(i), Clock::time_point{2 * w});
    }

    eviction::Evictor evictor(namespaces_, policy_, reclaimer_, {}, 4);
    const auto report =
        evictor.Run(eviction::Scope::All(), {.target_ratio = .1});
    EXPECT_EQ(report.evicted_objects, 2u);
    EXPECT_EQ(report.scan_waves, 2u);
    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(ns.objects.Read("r2_" + std::to_string(i)));
    }
}

TEST_F(ParallelEvictorNativeTest,
       DeferredGroupIsVisitedOnceAndFallbackWaitsForWorkers) {
    for (int i = 0; i < 10; ++i) {
        Publish("soft" + std::to_string(i), "soft-group");
    }
    eviction::Evictor evictor(namespaces_, policy_, reclaimer_, {}, 4);
    const auto report = evictor.Run(eviction::Scope::All(),
                                    {.target_ratio = .5, .min_ratio = .1});
    EXPECT_EQ(report.evicted_objects, 10u);
    EXPECT_EQ(report.visited, 20u);
}

TEST_F(ParallelEvictorNativeTest, ByteGoalCountsFreedBytes) {
    for (int i = 0; i < 2000; ++i) {
        Publish(std::to_string(i));
    }
    eviction::Evictor evictor(namespaces_, policy_, reclaimer_, {}, 4);
    const auto report =
        evictor.Run(TenantId{}, eviction::ByteGoal{.target_bytes = 128000});
    EXPECT_GE(report.released_bytes, 128000u);
    EXPECT_LE(report.released_bytes, 128000u + 3 * 128);
    EXPECT_EQ(report.released_bytes, report.evicted_objects * 128);
}

TEST_F(ParallelEvictorNativeTest, ByteGoalReclaimsExpiredReadRenewalsInOneRun) {
    for (const uint32_t workers : {1u, 4u}) {
        for (const bool parallel_safe : {false, true}) {
            const std::string suffix = std::to_string(workers) +
                                       (parallel_safe ? "parallel" : "serial");
            const std::string expired = "expired-" + suffix;
            const std::string live = "live-" + suffix;
            Publish(expired);
            Publish(live);
            auto& ns = namespaces_.GetOrCreate(TenantId{});
            {
                auto guard = ns.objects.Read(expired);
                guard->metadata().lease_->ExtendTo(Clock::now() -
                                                   std::chrono::seconds(1));
            }
            {
                auto guard = ns.objects.Read(live);
                guard->metadata().lease_->ExtendTo(Clock::now() +
                                                   std::chrono::hours(1));
            }
            reclaimer_.parallel_safe = parallel_safe;
            eviction::Evictor evictor(namespaces_, policy_, reclaimer_, {},
                                      workers);
            const auto report = evictor.Run(
                TenantId{}, eviction::ByteGoal{.target_bytes = 128});
            EXPECT_EQ(report.evicted_objects, 1u);
            EXPECT_EQ(report.released_bytes, 128u);
            EXPECT_FALSE(ns.objects.Read(expired));
            EXPECT_TRUE(ns.objects.Read(live));
            if (auto guard = ns.objects.Write(expired)) {
                guard->TearDown();
            }
            if (auto guard = ns.objects.Write(live)) {
                guard->TearDown();
            }
        }
    }
}

TEST_F(ParallelEvictorNativeTest,
       RenewedKeysAreRefiledOnceAndNoProgressTerminates) {
    for (int i = 0; i < 50; ++i) {
        Publish(std::to_string(i));
    }
    auto& ns = namespaces_.GetOrCreate(TenantId{});
    for (int i = 0; i < 50; ++i) {
        auto guard = ns.objects.Read(std::to_string(i));
        guard->metadata().lease_->ExtendTo(Clock::now() +
                                           std::chrono::hours(1));
    }
    eviction::Evictor evictor(namespaces_, policy_, reclaimer_, {}, 4);
    auto report = evictor.Run(eviction::Scope::All(), {.target_ratio = .5});
    EXPECT_EQ(report.evicted_objects, 0u);
    EXPECT_EQ(report.moved, 50u);
    EXPECT_EQ(report.scan_waves, 1u);
    report = evictor.Run(eviction::Scope::All(), {.target_ratio = .5});
    EXPECT_EQ(report.visited, 0u);
    EXPECT_EQ(report.evicted_objects, 0u);
    EXPECT_LE(report.scan_waves, 1u);
}

TEST_F(ParallelEvictorNativeTest, ReclaimFailureCancelsWorkers) {
    for (int i = 0; i < 50; ++i) {
        Publish(std::to_string(i));
    }
    reclaimer_.fail = true;
    eviction::Evictor evictor(namespaces_, policy_, reclaimer_, {}, 4);
    const auto report =
        evictor.Run(eviction::Scope::All(), {.target_ratio = .5});
    EXPECT_TRUE(report.stopped);
    EXPECT_EQ(report.evicted_objects, 0u);
    EXPECT_LT(report.visited, 50u);
}

TEST_F(ParallelEvictorNativeTest,
       APolicyWithoutParallelCapabilityKeepsSerialExecution) {
    policy_.parallel_safe = false;
    for (int i = 0; i < 50; ++i) {
        Publish(std::to_string(i));
    }
    eviction::Evictor evictor(namespaces_, policy_, reclaimer_, {}, 4);
    const auto report =
        evictor.Run(eviction::Scope::All(), {.target_ratio = .1});
    EXPECT_EQ(report.evicted_objects, 5u);
    EXPECT_EQ(report.workers, 1u);
    EXPECT_EQ(report.scan_waves, 0u);
}

TEST_F(ParallelEvictorNativeTest,
       AReclaimerWithoutParallelCapabilityKeepsSerialExecution) {
    reclaimer_.parallel_safe = false;
    for (int i = 0; i < 50; ++i) {
        Publish(std::to_string(i));
    }
    eviction::Evictor evictor(namespaces_, policy_, reclaimer_, {}, 4);
    const auto report =
        evictor.Run(eviction::Scope::All(), {.target_ratio = .1});
    EXPECT_EQ(report.evicted_objects, 5u);
    EXPECT_EQ(report.workers, 1u);
    EXPECT_EQ(report.scan_waves, 0u);
}

TEST_F(ParallelEvictorNativeTest,
       AnObserverWithoutParallelCapabilityKeepsSerialExecution) {
    struct DefaultSerialObserver : eviction::EvictionObserver {
        uint64_t n = 0;
        void OnApplied(const eviction::ObjectView&,
                       const eviction::Effect&) override {
            ++n;
        }
    } serial_observer;
    for (int i = 0; i < 50; ++i) {
        Publish(std::to_string(i));
    }
    eviction::Evictor serial_evictor(namespaces_, policy_, reclaimer_,
                                     {&serial_observer}, 4);
    const auto serial_report =
        serial_evictor.Run(eviction::Scope::All(), {.target_ratio = .1});
    EXPECT_EQ(serial_report.evicted_objects, 5u);
    EXPECT_EQ(serial_observer.n, 5u);
    EXPECT_EQ(serial_report.scan_waves, 0u);

    struct ParallelObserver : eviction::EvictionObserver {
        std::atomic<uint64_t> n{0};
        bool SupportsParallel() const override { return true; }
        void OnApplied(const eviction::ObjectView&,
                       const eviction::Effect&) override {
            n.fetch_add(1, std::memory_order_relaxed);
        }
    } parallel_observer;
    eviction::Evictor parallel_evictor(namespaces_, policy_, reclaimer_,
                                       {&parallel_observer}, 4);
    const auto parallel_report =
        parallel_evictor.Run(eviction::Scope::All(), {.target_ratio = .1});
    EXPECT_GE(parallel_report.evicted_objects, 5u);
    EXPECT_EQ(parallel_observer.n.load(), parallel_report.evicted_objects);
    EXPECT_GE(parallel_report.scan_waves, 1u);
}

}  // namespace
}  // namespace mooncake
