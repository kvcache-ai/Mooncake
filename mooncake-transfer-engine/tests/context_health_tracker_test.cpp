// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Hardware-free unit tests for ContextHealthTracker (the context-level
// circuit-breaker state). The tracker takes an injected clock, so the
// trip/TTL/half-open logic is exercised deterministically with no RDMA device
// and no real sleeps. The header is inline-only, so the test links just gtest.
//
// Incident background these tests pin down: a single mooncake store replica
// restarting must never trip the breaker (SinglePeerStreakNeverTrips), and a
// tripped breaker must self-heal after its TTL instead of latching the local
// context inactive until process restart (ReactivationDueAfterTtl).

#include "transport/rdma_transport/context_health_tracker.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

using mooncake::ContextHealthTracker;

namespace {

constexpr int kThreshold = 32;
constexpr int kMinPeers = 2;
constexpr uint64_t kTtlNs = 5000ull * 1000000ull;  // 5000 ms
constexpr auto kSubmit =
    ContextHealthTracker::FailureSource::kAllRailsUnavailable;
constexpr auto kLocal = ContextHealthTracker::FailureSource::kLocalCompletion;

// Manually-advanced clock so TTL transitions are deterministic.
struct FakeClock {
    std::atomic<uint64_t> now{0};
    uint64_t operator()() const { return now.load(std::memory_order_relaxed); }
};

ContextHealthTracker makeTracker(std::shared_ptr<FakeClock> clk) {
    return ContextHealthTracker([clk] { return (*clk)(); });
}

std::unordered_set<std::string> peers(std::initializer_list<const char *> ps) {
    std::unordered_set<std::string> out;
    for (auto *p : ps) out.insert(p);
    return out;
}

// The incident shape: every failed batch targets the same (dead) store. No
// matter how long the streak runs, one peer is not evidence of local RNIC
// failure and the breaker must not trip.
TEST(ContextHealthTracker, SinglePeerStreakNeverTrips) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    for (int i = 0; i < 100; ++i) {
        auto rec =
            t.recordFailure(kSubmit, peers({"storeA"}), kThreshold, kMinPeers);
        EXPECT_FALSE(rec.tripped_now) << "iteration " << i;
    }
    EXPECT_FALSE(t.tripped());
    EXPECT_EQ(t.streak(kSubmit), 100);
    EXPECT_EQ(t.distinctPeerCount(kSubmit), 1u);
}

TEST(ContextHealthTracker, MultiPeerStreakTripsAtThreshold) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    for (int i = 0; i < kThreshold - 1; ++i) {
        auto rec =
            t.recordFailure(kSubmit, peers({"storeA"}), kThreshold, kMinPeers);
        EXPECT_FALSE(rec.tripped_now);
    }
    auto rec =
        t.recordFailure(kSubmit, peers({"storeB"}), kThreshold, kMinPeers);
    EXPECT_TRUE(rec.tripped_now);
    EXPECT_EQ(rec.streak, kThreshold);
    EXPECT_EQ(rec.distinct_peers, 2u);
    EXPECT_FALSE(rec.peer_sample.empty());
    EXPECT_TRUE(t.tripped());
}

TEST(ContextHealthTracker, TripsOnlyOnceAndStragglersDontRetrip) {
    auto clk = std::make_shared<FakeClock>();
    clk->now = 777;
    auto t = makeTracker(clk);
    for (int i = 0; i < kThreshold - 1; ++i)
        t.recordFailure(kSubmit, peers({"a"}), kThreshold, kMinPeers);
    EXPECT_TRUE(t.recordFailure(kSubmit, peers({"b"}), kThreshold, kMinPeers)
                    .tripped_now);
    const uint64_t trip_ts = t.tripTimeNs();
    EXPECT_EQ(trip_ts, 777u);

    // In-flight straggler batches keep arriving after the trip: no re-trip,
    // no trip-timestamp movement (which would push out the reactivation).
    clk->now = 999;
    for (int i = 0; i < 10; ++i) {
        auto rec =
            t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers);
        EXPECT_FALSE(rec.tripped_now);
    }
    EXPECT_EQ(t.tripTimeNs(), trip_ts);
}

// A second peer joining the streak after the count already passed the
// threshold trips on that batch (streak > threshold, peers reach min).
TEST(ContextHealthTracker, LateSecondPeerTripsPastThreshold) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    for (int i = 0; i < 40; ++i) {
        auto rec =
            t.recordFailure(kSubmit, peers({"a"}), kThreshold, kMinPeers);
        EXPECT_FALSE(rec.tripped_now);
    }
    auto rec = t.recordFailure(kSubmit, peers({"b"}), kThreshold, kMinPeers);
    EXPECT_TRUE(rec.tripped_now);
    EXPECT_EQ(rec.streak, 41);
}

// min_peers == 1 restores the legacy behavior: a single-peer streak trips.
TEST(ContextHealthTracker, MinPeersOneIsLegacyBehavior) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    for (int i = 0; i < kThreshold - 1; ++i)
        EXPECT_FALSE(
            t.recordFailure(kSubmit, peers({"a"}), kThreshold, 1).tripped_now);
    EXPECT_TRUE(
        t.recordFailure(kSubmit, peers({"a"}), kThreshold, 1).tripped_now);
}

// Regression for the reviewer-reported preload: weaker submit-side evidence
// must not count toward the stronger local-completion threshold.
TEST(ContextHealthTracker, SubmitFailuresDoNotPreloadLocalFailure) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    for (int i = 0; i < kThreshold - 1; ++i) {
        EXPECT_FALSE(
            t.recordFailure(kSubmit, peers({"peerA"}), kThreshold, kMinPeers)
                .tripped_now);
    }

    auto local = t.recordFailure(kLocal, peers({"peerA"}), kThreshold, 1);
    EXPECT_FALSE(local.tripped_now);
    EXPECT_FALSE(t.tripped());
    EXPECT_EQ(t.streak(kSubmit), kThreshold - 1);
    EXPECT_EQ(t.streak(kLocal), 1);

    // The submit channel retained only its own evidence and can trip on its
    // next qualified batch.
    EXPECT_TRUE(
        t.recordFailure(kSubmit, peers({"peerB"}), kThreshold, kMinPeers)
            .tripped_now);
}

TEST(ContextHealthTracker, LocalFailuresDoNotPreloadSubmitFailure) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    for (int i = 0; i < kThreshold - 1; ++i) {
        EXPECT_FALSE(t.recordFailure(kLocal, peers({"peerA"}), kThreshold, 1)
                         .tripped_now);
    }

    auto submit = t.recordFailure(kSubmit, peers({"peerA", "peerB"}),
                                  kThreshold, kMinPeers);
    EXPECT_FALSE(submit.tripped_now);
    EXPECT_FALSE(t.tripped());
    EXPECT_EQ(t.streak(kLocal), kThreshold - 1);
    EXPECT_EQ(t.streak(kSubmit), 1);

    // The local channel likewise retained only its own evidence.
    EXPECT_TRUE(
        t.recordFailure(kLocal, peers({"peerA"}), kThreshold, 1).tripped_now);
}

TEST(ContextHealthTracker, LocalCompletionTripsAtOwnThreshold) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    for (int i = 0; i < kThreshold - 1; ++i) {
        EXPECT_FALSE(t.recordFailure(kLocal, peers({"peerA"}), kThreshold, 1)
                         .tripped_now);
    }
    EXPECT_TRUE(
        t.recordFailure(kLocal, peers({"peerA"}), kThreshold, 1).tripped_now);
    EXPECT_TRUE(t.tripped());
}

TEST(ContextHealthTracker, SuccessResetsBothFailureSources) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    for (int i = 0; i < kThreshold - 1; ++i)
        t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers);
    for (int i = 0; i < 3; ++i)
        t.recordFailure(kLocal, peers({"a"}), kThreshold, 1);
    t.recordSuccess();
    EXPECT_EQ(t.streak(kSubmit), 0);
    EXPECT_EQ(t.distinctPeerCount(kSubmit), 0u);
    EXPECT_EQ(t.streak(kLocal), 0);
    EXPECT_EQ(t.distinctPeerCount(kLocal), 0u);

    // The streak restarts: another threshold-1 batches don't trip...
    for (int i = 0; i < kThreshold - 1; ++i) {
        auto rec =
            t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers);
        EXPECT_FALSE(rec.tripped_now);
    }
    // ...and the next one does.
    EXPECT_TRUE(
        t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers)
            .tripped_now);
}

TEST(ContextHealthTracker, ReactivationDueAfterTtl) {
    auto clk = std::make_shared<FakeClock>();
    clk->now = 1000;
    auto t = makeTracker(clk);
    for (int i = 0; i < 3; ++i)
        t.recordFailure(kLocal, peers({"a"}), kThreshold, 1);
    for (int i = 0; i < kThreshold; ++i)
        t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers);
    ASSERT_TRUE(t.tripped());

    clk->now = 1000 + kTtlNs - 1;
    EXPECT_FALSE(t.tryReactivate(kTtlNs));  // one ns early
    EXPECT_TRUE(t.tripped());

    clk->now = 1000 + kTtlNs;
    EXPECT_TRUE(t.tryReactivate(kTtlNs));  // due -- clears once
    EXPECT_FALSE(t.tripped());
    EXPECT_EQ(t.streak(kSubmit), 0);
    EXPECT_EQ(t.streak(kLocal), 0);
    EXPECT_FALSE(t.tryReactivate(kTtlNs));  // second call: nothing to clear
}

// ttl == 0 is the legacy latch: the breaker never auto-reactivates.
TEST(ContextHealthTracker, ZeroTtlNeverReactivates) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    for (int i = 0; i < kThreshold; ++i)
        t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers);
    ASSERT_TRUE(t.tripped());
    clk->now = 1ull << 40;  // far future
    EXPECT_FALSE(t.tryReactivate(0));
    EXPECT_TRUE(t.tripped());
}

// Half-open semantics: after reactivation the streak is reset, and a
// genuinely-dead NIC (still failing across peers) re-trips after another
// full threshold's worth of failed batches.
TEST(ContextHealthTracker, HalfOpenRetripsAfterReactivation) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    for (int i = 0; i < kThreshold; ++i)
        t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers);
    ASSERT_TRUE(t.tripped());

    clk->now = kTtlNs;
    ASSERT_TRUE(t.tryReactivate(kTtlNs));

    for (int i = 0; i < kThreshold - 1; ++i) {
        auto rec =
            t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers);
        EXPECT_FALSE(rec.tripped_now);
    }
    auto rec =
        t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers);
    EXPECT_TRUE(rec.tripped_now);
    EXPECT_EQ(t.tripTimeNs(), kTtlNs);  // fresh trip timestamp
}

// A fatal async event takes ownership of the context's inactive state via
// reset(): the pending TTL reactivation must be cancelled so the breaker
// never resurrects an event-deactivated context.
TEST(ContextHealthTracker, ResetClearsTripAndCancelsReactivation) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    for (int i = 0; i < 3; ++i)
        t.recordFailure(kLocal, peers({"a"}), kThreshold, 1);
    for (int i = 0; i < kThreshold; ++i)
        t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers);
    ASSERT_TRUE(t.tripped());

    t.reset();  // fatal event (or PORT_ACTIVE) owns the state now
    EXPECT_FALSE(t.tripped());
    EXPECT_EQ(t.streak(kSubmit), 0);
    EXPECT_EQ(t.streak(kLocal), 0);

    clk->now = 1ull << 40;                  // far past any TTL
    EXPECT_FALSE(t.tryReactivate(kTtlNs));  // nothing pending to reactivate
}

// The under-lock callbacks drive an external flag (the RdmaContext active
// flag in production) atomically with the trip state: on_trip fires exactly
// once per trip, tryReactivate/reset fire theirs when clearing.
TEST(ContextHealthTracker, CallbacksRunOnTransitions) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    bool active = true;
    int trip_calls = 0;
    auto deactivate = [&] {
        active = false;
        ++trip_calls;
    };
    auto activate = [&] { active = true; };

    for (int i = 0; i < kThreshold - 1; ++i)
        t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers,
                        deactivate);
    EXPECT_TRUE(active);  // no trip yet, callback not run
    EXPECT_EQ(trip_calls, 0);

    t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers,
                    deactivate);
    EXPECT_FALSE(active);  // tripped -> deactivated under the lock
    EXPECT_EQ(trip_calls, 1);

    // Stragglers after the trip never re-run the trip callback.
    t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers,
                    deactivate);
    EXPECT_EQ(trip_calls, 1);

    clk->now = kTtlNs;
    EXPECT_TRUE(t.tryReactivate(kTtlNs, activate));
    EXPECT_TRUE(active);  // reactivated atomically with the clear

    // reset() (fatal event / PORT_ACTIVE in production) runs its callback
    // unconditionally, tripped or not.
    active = false;
    t.reset(activate);
    EXPECT_TRUE(active);
}

// P1 regression: a submitter-thread trip (tripped_ = true + flag = false)
// racing the monitor thread's PORT_ACTIVE recovery (reset + flag = true)
// must never strand the external flag at false while the tracker is
// untripped -- that state has no armed trip, so TTL recovery would never
// run and the context would be skipped forever. With every transition's
// flag update inside the tracker mutex, tripped() == (flag == false) holds
// at quiescence regardless of interleaving. Run under TSan as well.
TEST(ContextHealthTracker, ConcurrentTripAndResetKeepFlagConsistent) {
    for (int round = 0; round < 50; ++round) {
        auto clk = std::make_shared<FakeClock>();
        clk->now = 1;
        auto t = makeTracker(clk);
        std::atomic<bool> active{true};
        auto deactivate = [&] {
            active.store(false, std::memory_order_relaxed);
        };
        auto activate = [&] { active.store(true, std::memory_order_relaxed); };

        std::vector<std::thread> threads;
        // Submitter threads: drive streaks toward trips (min_peers 1 and a
        // threshold of 4 make trips frequent, maximizing the race window).
        for (int i = 0; i < 3; ++i)
            threads.emplace_back([&t, &deactivate] {
                for (int k = 0; k < 500; ++k)
                    (void)t.recordFailure(
                        kSubmit, {std::string("peer") + std::to_string(k % 3)},
                        4, 1, deactivate);
            });
        // Monitor thread: PORT_ACTIVE-style recovery plus TTL reactivation.
        threads.emplace_back([&t, &activate, clk] {
            for (int k = 0; k < 500; ++k) {
                t.reset(activate);
                clk->now.fetch_add(10, std::memory_order_relaxed);
                (void)t.tryReactivate(1, activate);
            }
        });
        for (auto &th : threads) th.join();

        // Atomic transitions guarantee the invariant at quiescence; the
        // pre-fix code (flag flipped outside the tracker lock) allows
        // tripped() == false with active == false, the permanent-skip state.
        EXPECT_EQ(t.tripped(), !active.load()) << "round " << round;
    }
}

// Late CQE stragglers polled after the trip must not clear it: reactivation
// is exclusively the monitor thread's tryReactivate()/reset().
TEST(ContextHealthTracker, SuccessWhileTrippedDoesNotReactivate) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    for (int i = 0; i < kThreshold; ++i)
        t.recordFailure(kSubmit, peers({"a", "b"}), kThreshold, kMinPeers);
    ASSERT_TRUE(t.tripped());
    t.recordSuccess();
    EXPECT_TRUE(t.tripped());
}

// Hammer all entry points concurrently; primarily a ThreadSanitizer target
// (run under -fsanitize=thread). Mirrors the production thread layout:
// several submitter threads recording failures, transfer workers recording
// successes and reading tripped(), one monitor thread reactivating/resetting.
TEST(ContextHealthTracker, ConcurrentAccessIsRaceFree) {
    auto clk = std::make_shared<FakeClock>();
    clk->now = 1;
    auto t = makeTracker(clk);
    constexpr int kIters = 5000;
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i)
        threads.emplace_back([&t, i] {
            auto ps = peers({i % 2 ? "peerA" : "peerB", "peerC"});
            auto source = i % 2 ? kSubmit : kLocal;
            int min_peers = source == kSubmit ? kMinPeers : 1;
            for (int k = 0; k < kIters; ++k)
                (void)t.recordFailure(source, ps, kThreshold, min_peers);
        });
    for (int i = 0; i < 4; ++i)
        threads.emplace_back([&t] {
            for (int k = 0; k < kIters; ++k) {
                t.recordSuccess();
                (void)t.tripped();
            }
        });
    threads.emplace_back([&t, clk] {
        for (int k = 0; k < kIters; ++k) {
            // Advance the clock past the tiny TTL so a tripped breaker is
            // periodically due, exercising the reactivation path under load.
            clk->now.fetch_add(10, std::memory_order_relaxed);
            (void)t.tryReactivate(1);
            if (k % 100 == 0) t.reset();
        }
    });
    for (auto &th : threads) th.join();
    SUCCEED();  // TSan asserts the absence of data races
}

}  // namespace
