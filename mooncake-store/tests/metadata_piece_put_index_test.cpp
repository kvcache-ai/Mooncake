#include "metadata/namespace.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake::metadata {
namespace {

std::unique_ptr<PiecePutRuntime> Runtime(std::atomic<size_t>& rounds) {
    rounds.fetch_add(1, std::memory_order_relaxed);
    return std::make_unique<PiecePutRuntime>(
        PiecePutState("index-test", PiecePutState::TimePoint::max()),
        SegmentWriteHold{}, rounds);
}

std::vector<std::string> Sorted(std::vector<std::string> keys) {
    std::sort(keys.begin(), keys.end());
    return keys;
}

TEST(PiecePutIndexTest, PublicationListsPiecePutOnlyWhenItsGuardReleases) {
    std::atomic<size_t> rounds{0};
    Namespace ns;
    {
        auto ordinary = ns.objects.WriteKey("ordinary");
        (void)test::PublishEnvelope(ordinary);
        ordinary.state().is_processing = true;
    }
    {
        auto piece = ns.objects.WriteKey("piece");
        (void)test::PublishEnvelope(piece);
        piece.state().is_processing = true;
        piece.state().piece_put = Runtime(rounds);
        EXPECT_TRUE(ns.PiecePutInFlightKeys().empty());
        EXPECT_EQ(ns.InFlightKeys(), (std::vector<std::string>{"ordinary"}));
    }
    EXPECT_EQ(ns.PiecePutInFlightKeys(), (std::vector<std::string>{"piece"}));
    EXPECT_EQ(Sorted(ns.InFlightKeys()),
              (std::vector<std::string>{"ordinary", "piece"}));
}

TEST(PiecePutIndexTest, KindChangesWhileTheObjectRemainsInFlight) {
    std::atomic<size_t> rounds{0};
    Namespace ns;
    ASSERT_NE(test::PublishObject(ns.objects, "key"), 0u);
    {
        auto guard = ns.objects.Write("key");
        guard->state().is_processing = true;
    }
    EXPECT_TRUE(ns.PiecePutInFlightKeys().empty());
    {
        auto guard = ns.objects.Write("key");
        guard->state().piece_put = Runtime(rounds);
    }
    EXPECT_EQ(ns.PiecePutInFlightKeys(), (std::vector<std::string>{"key"}));
    EXPECT_EQ(ns.InFlightKeys(), (std::vector<std::string>{"key"}));
    {
        auto guard = ns.objects.Write("key");
        guard->state().piece_put.reset();
        // Publication can replace PiecePut with ordinary background work.
        guard->state().is_processing = false;
        guard->state().offloading_task =
            OffloadingTask{1, std::chrono::system_clock::now(), {}};
    }
    EXPECT_TRUE(ns.PiecePutInFlightKeys().empty());
    EXPECT_EQ(ns.InFlightKeys(), (std::vector<std::string>{"key"}));
    {
        auto guard = ns.objects.Write("key");
        guard->state().offloading_task.reset();
    }
    EXPECT_TRUE(ns.InFlightKeys().empty());
    EXPECT_EQ(rounds.load(), 0u);
}

TEST(PiecePutIndexTest, RuntimeRemainsIndexedWithoutThePrimaryProcessingBit) {
    std::atomic<size_t> rounds{0};
    Namespace ns;
    {
        auto guard = ns.objects.WriteKey("piece");
        (void)test::PublishEnvelope(guard);
        guard.state().piece_put = Runtime(rounds);
        EXPECT_TRUE(guard.state().HasWorkInFlight());
    }
    EXPECT_EQ(ns.PiecePutInFlightKeys(), (std::vector<std::string>{"piece"}));
    EXPECT_EQ(ns.InFlightKeys(), (std::vector<std::string>{"piece"}));
    {
        auto guard = ns.objects.Write("piece");
        guard->state().piece_put.reset();
    }
    EXPECT_TRUE(ns.PiecePutInFlightKeys().empty());
    EXPECT_TRUE(ns.InFlightKeys().empty());
}

TEST(PiecePutIndexTest, EraseAndRepublishReclassifyTheNewGeneration) {
    std::atomic<size_t> rounds{0};
    Namespace ns;
    route::Generation original = 0;
    {
        auto guard = ns.objects.WriteKey("piece");
        original = test::PublishEnvelope(guard);
        guard.state().is_processing = true;
        guard.state().piece_put = Runtime(rounds);
    }
    const auto snapshot = ns.PiecePutInFlightKeys();
    {
        auto guard = ns.objects.Write("piece");
        guard->TearDown();
    }
    EXPECT_TRUE(ns.PiecePutInFlightKeys().empty());
    EXPECT_TRUE(ns.InFlightKeys().empty());
    route::Generation replacement = 0;
    {
        auto guard = ns.objects.WriteKey(snapshot.front());
        replacement = test::PublishEnvelope(guard);
        guard.state().is_processing = true;
    }
    EXPECT_NE(original, replacement);
    EXPECT_TRUE(ns.PiecePutInFlightKeys().empty());
    EXPECT_EQ(ns.InFlightKeys(), snapshot);
    {
        auto guard = ns.objects.Write(snapshot.front());
        EXPECT_EQ(guard->generation(), replacement);
        guard->TearDown();
        EXPECT_NE(test::PublishEnvelope(*guard), replacement);
        guard->state().piece_put = Runtime(rounds);
    }
    EXPECT_EQ(ns.PiecePutInFlightKeys(), snapshot);
    EXPECT_EQ(ns.InFlightKeys(), snapshot);
    {
        auto guard = ns.objects.Write(snapshot.front());
        const auto before = guard->generation();
        guard->TearDown();
        EXPECT_NE(test::PublishEnvelope(*guard), before);
        guard->state().piece_put = Runtime(rounds);
    }
    // The same slot stays PiecePut across replacement; it must remain linked
    // exactly once even though both its runtime and generation changed.
    EXPECT_EQ(ns.PiecePutInFlightKeys(), snapshot);
    EXPECT_EQ(ns.InFlightKeys(), snapshot);
}

TEST(PiecePutIndexTest, ConcurrentKindChangesAndSnapshotsKeepOneHookPerKey) {
    std::atomic<size_t> rounds{0};
    Namespace ns;
    constexpr size_t kThreads = 4;
    constexpr size_t kKeysPerThread = 24;
    std::atomic<bool> start{false};
    std::atomic<size_t> violations{0};
    std::vector<std::thread> writers;
    for (size_t thread = 0; thread < kThreads; ++thread) {
        writers.emplace_back([&, thread] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (size_t round = 0; round < 40; ++round) {
                for (size_t key = 0; key < kKeysPerThread; ++key) {
                    auto guard =
                        ns.objects.WriteKey("t" + std::to_string(thread) + "-" +
                                            std::to_string(key));
                    if (guard.has_object() && round % 3 == 0) {
                        guard.TearDown();
                    }
                    if (!guard.has_object()) (void)test::PublishEnvelope(guard);
                    guard.state().is_processing = true;
                    if (round == 39 ? key % 2 == 0 : round % 2 == 0) {
                        if (!guard.state().piece_put) {
                            guard.state().piece_put = Runtime(rounds);
                        }
                    } else {
                        guard.state().piece_put.reset();
                    }
                }
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (size_t iteration = 0; iteration < 240; ++iteration) {
        auto snapshot = Sorted(ns.PiecePutInFlightKeys());
        if (std::adjacent_find(snapshot.begin(), snapshot.end()) !=
            snapshot.end()) {
            violations.fetch_add(1, std::memory_order_relaxed);
        }
        for (const auto& key : snapshot) {
            auto guard = ns.objects.Read(key);
            if (guard && guard->key() != key) {
                violations.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }
    for (auto& writer : writers) writer.join();
    std::vector<std::string> expected;
    for (size_t thread = 0; thread < kThreads; ++thread) {
        for (size_t key = 0; key < kKeysPerThread; key += 2) {
            expected.push_back("t" + std::to_string(thread) + "-" +
                               std::to_string(key));
        }
    }
    EXPECT_EQ(violations.load(), 0u);
    EXPECT_EQ(Sorted(ns.PiecePutInFlightKeys()), Sorted(expected));
    EXPECT_EQ(ns.InFlightKeys().size(), kThreads * kKeysPerThread);
    EXPECT_EQ(rounds.load(), expected.size());
}

TEST(PiecePutIndexTest, DestructionUnlinksBothListsBeforeDestroyingSlots) {
    std::atomic<size_t> rounds{0};
    {
        Namespace ns;
        for (const auto& key : {"ordinary", "piece"}) {
            auto guard = ns.objects.WriteKey(key);
            (void)test::PublishEnvelope(guard);
            guard.state().is_processing = true;
            if (guard.key() == "piece")
                guard.state().piece_put = Runtime(rounds);
        }
        EXPECT_EQ(ns.InFlightKeys().size(), 2u);
        EXPECT_EQ(ns.PiecePutInFlightKeys().size(), 1u);
    }
    EXPECT_EQ(rounds.load(), 0u);
}

}  // namespace
}  // namespace mooncake::metadata
