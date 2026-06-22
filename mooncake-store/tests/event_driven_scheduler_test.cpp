#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <chrono>
#include <cstring>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/tiers/cache_tier.h"  // TempDRAMBuffer
#include "utils/common.h"                    // InitTieredBackendForTest

namespace mooncake {
namespace {

constexpr size_t kMB = 1024 * 1024;

// Two local DRAM tiers (fast = higher priority). DRAM<->DRAM copies are plain
// memcpy, so no TransferEngine / Master is needed (same as tiered_backend_test).
Json::Value MakeConfig(size_t fast_cap, size_t slow_cap,
                       double evict_wm_high = 0.90, double evict_wm_low = 0.70) {
    Json::Value cfg;
    Json::Value& sched = cfg["scheduler"];
    sched["type"] = "event_driven";
    sched["loop_interval_ms"] = 50;
    sched["offload_freq_threshold"] = 2;
    sched["onboard_freq_threshold"] = 2;
    sched["onboard_fast_threshold"] = 0.95;
    sched["evict_watermark_high"] = evict_wm_high;
    sched["evict_watermark_low"] = evict_wm_low;
    sched["limit_watermark"] = 0.95;
    sched["sketch_capacity"] = 4096;
    sched["queue_capacity"] = 256;
    sched["thread_count"] = 2;

    Json::Value tiers(Json::arrayValue);
    Json::Value fast;
    fast["type"] = "DRAM";
    fast["capacity"] = static_cast<Json::UInt64>(fast_cap);
    fast["priority"] = 20;
    fast["allocator_type"] = "OFFSET";
    Json::Value slow;
    slow["type"] = "DRAM";
    slow["capacity"] = static_cast<Json::UInt64>(slow_cap);
    slow["priority"] = 10;
    slow["allocator_type"] = "OFFSET";
    tiers.append(fast);
    tiers.append(slow);
    cfg["tiers"] = tiers;
    return cfg;
}

class EventDrivenSchedulerTest : public ::testing::Test {
   protected:
    static UUID FastTier(const TieredBackend& b) {
        UUID best{};
        int best_prio = std::numeric_limits<int>::min();
        for (const auto& v : b.GetTierViews()) {
            if (v.priority > best_prio) {
                best_prio = v.priority;
                best = v.id;
            }
        }
        return best;
    }

    static UUID SlowTier(const TieredBackend& b) {
        UUID worst{};
        int worst_prio = std::numeric_limits<int>::max();
        for (const auto& v : b.GetTierViews()) {
            if (v.priority < worst_prio) {
                worst_prio = v.priority;
                worst = v.id;
            }
        }
        return worst;
    }

    static size_t TierUsage(const TieredBackend& b, UUID tier) {
        for (const auto& v : b.GetTierViews()) {
            if (v.id == tier) {
                return v.usage;
            }
        }
        return 0;
    }

    static bool Put(TieredBackend& b, const std::string& key, UUID tier,
                    size_t size) {
        auto alloc = b.Allocate(size, tier, /*strict=*/true);
        if (!alloc.has_value()) {
            return false;
        }
        DataSource src;
        auto buf = std::make_unique<char[]>(size);
        std::memset(buf.get(), 'x', size);
        src.buffer = std::make_unique<TempDRAMBuffer>(std::move(buf), size);
        src.type = MemoryType::DRAM;
        if (!b.Write(src, alloc.value()).has_value()) {
            return false;
        }
        return b.Commit(key, alloc.value()).has_value();
    }

    // A recorded Get on a specific tier (drives the OnAccess hook).
    static void Access(TieredBackend& b, const std::string& key, UUID tier) {
        (void)b.Get(key, tier, /*record_access=*/true);
    }

    template <class Pred>
    static bool WaitUntil(Pred pred, int timeout_ms) {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            if (pred()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return pred();
    }
};

TEST_F(EventDrivenSchedulerTest, OffloadCreatesSlowReplicaKeepingFast) {
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, MakeConfig(16 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);
    ASSERT_NE(fast, slow);

    ASSERT_TRUE(Put(backend, "k", fast, 64 * 1024));
    // Drive frequency past the offload threshold (>2) via fast-tier hits.
    for (int i = 0; i < 5; ++i) {
        Access(backend, "k", fast);
    }
    // The offload pre-demotes a replica to the slow tier...
    EXPECT_TRUE(WaitUntil([&] { return backend.Exist("k", slow); }, 5000));
    // ...while RETAINING the fast-tier copy.
    EXPECT_TRUE(backend.Exist("k", fast));
}

TEST_F(EventDrivenSchedulerTest, OnboardPromotesAndDropsSlow) {
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, MakeConfig(16 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);

    ASSERT_TRUE(Put(backend, "k", slow, 64 * 1024));
    for (int i = 0; i < 5; ++i) {
        Access(backend, "k", slow);
    }
    EXPECT_TRUE(WaitUntil([&] { return backend.Exist("k", fast); }, 5000));
    // After a successful promotion the slow replica is dropped.
    EXPECT_TRUE(WaitUntil([&] { return !backend.Exist("k", slow); }, 5000));
}

TEST_F(EventDrivenSchedulerTest, OnboardCopyFailureKeepsSlowReplica) {
    TieredBackend backend;
    // Fast capacity is SMALLER than the item, so onboard's copy to fast fails.
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, MakeConfig(1 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);

    const size_t big = 2 * kMB;  // > fast capacity
    ASSERT_TRUE(Put(backend, "k", slow, big));
    for (int i = 0; i < 5; ++i) {
        Access(backend, "k", slow);
    }
    // Onboard is attempted (fast occupancy is under the threshold) but the copy
    // to fast cannot land, so the slow replica must survive.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_TRUE(backend.Exist("k", slow));
    EXPECT_FALSE(backend.Exist("k", fast));
}

TEST_F(EventDrivenSchedulerTest, EvictReclaimsFastTierAboveWatermark) {
    TieredBackend backend;
    // Low watermarks so the fill clearly exceeds the trigger.
    ASSERT_TRUE(InitTieredBackendForTest(
                    backend, MakeConfig(4 * kMB, 64 * kMB,
                                        /*evict_wm_high=*/0.50,
                                        /*evict_wm_low=*/0.40))
                    .has_value());
    const UUID fast = FastTier(backend);

    std::vector<std::string> keys;
    for (int i = 0; i < 14; ++i) {
        const std::string k = "e" + std::to_string(i);
        if (Put(backend, k, fast, 256 * 1024)) {
            keys.push_back(k);
        }
    }
    ASSERT_FALSE(keys.empty());
    // The background evict pass reclaims the fast tier by dropping cold copies
    // (no synchronous migrate), so some committed keys leave the fast tier.
    EXPECT_TRUE(WaitUntil(
        [&] {
            size_t on_fast = 0;
            for (const auto& k : keys) {
                if (backend.Exist(k, fast)) {
                    ++on_fast;
                }
            }
            return on_fast < keys.size();
        },
        5000));
}

TEST_F(EventDrivenSchedulerTest, NonFastTierAllocationFailureDoesNotForceReclaim) {
    // D2-B: the event-driven scheduler force-reclaims ONLY the fast tier. A
    // strict allocation on a full SLOW tier must fail without evicting any of
    // its residents (slow tiers own their eviction inside their own Allocate).
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, MakeConfig(16 * kMB, 1 * kMB))
            .has_value());
    const UUID slow = SlowTier(backend);

    std::vector<std::string> slow_keys;
    for (int i = 0; i < 3; ++i) {
        const std::string k = "slow" + std::to_string(i);
        if (Put(backend, k, slow, 256 * 1024)) {
            slow_keys.push_back(k);
        }
    }
    ASSERT_FALSE(slow_keys.empty());
    const size_t usage_before = TierUsage(backend, slow);

    // Request that cannot fit in the remaining slow-tier space.
    auto alloc = backend.Allocate(512 * 1024, slow, /*strict=*/true);
    EXPECT_FALSE(alloc.has_value());  // declined, not force-reclaimed
    for (const auto& k : slow_keys) {
        EXPECT_TRUE(backend.Exist(k, slow))
            << k << " was evicted: slow tier must not be force-reclaimed";
    }
    EXPECT_EQ(TierUsage(backend, slow), usage_before);
}

TEST_F(EventDrivenSchedulerTest, StrictDramPutEvictsInsteadOfSpilling) {
    // P1-4: the local-Put path now allocates DRAM strictly. Writing far more
    // data than the fast (DRAM) tier holds must keep succeeding by evicting
    // cold fast-tier copies and retrying on DRAM — never spilling the local
    // replica down to the slow tier.
    TieredBackend backend;
    ASSERT_TRUE(InitTieredBackendForTest(
                    backend, MakeConfig(2 * kMB, 64 * kMB,
                                        /*evict_wm_high=*/0.50,
                                        /*evict_wm_low=*/0.40))
                    .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);
    ASSERT_EQ(backend.GetDramTierId(), fast);  // fast role is the DRAM tier

    constexpr size_t kItem = 256 * 1024;  // 8 of these fill the 2MB fast tier
    int ok = 0;
    for (int i = 0; i < 24; ++i) {
        if (Put(backend, "k" + std::to_string(i), fast, kItem)) {
            ++ok;
        }
    }
    // Far more than the ~8 that fit at once succeeded => strict alloc evicted
    // and retried rather than failing.
    EXPECT_GT(ok, 8);
    // And nothing spilled onto the slow tier.
    EXPECT_EQ(TierUsage(backend, slow), 0u)
        << "strict DRAM Put must not spill the local replica to the slow tier";
}

TEST_F(EventDrivenSchedulerTest, GetHotKeyStatsZeroReturnsAllBeyondDefaultCap) {
    // P1-1: HA recovery passes hot_key_num=0 (== all) through DataManager ->
    // TieredBackend so Phase 1 recovers the entire hot working set, not just the
    // scheduler's default cap. This guards the TieredBackend layer that
    // DataManager::GetHotKeyStats forwards verbatim.
    auto cfg = MakeConfig(16 * kMB, 64 * kMB);
    cfg["scheduler"]["hot_key_num"] = 64;  // explicit small cap for the test
    TieredBackend backend;
    ASSERT_TRUE(InitTieredBackendForTest(backend, cfg).has_value());
    const UUID fast = FastTier(backend);

    constexpr int kKeys = 100;  // > the configured cap of 64
    int committed = 0;
    for (int i = 0; i < kKeys; ++i) {
        if (Put(backend, "h" + std::to_string(i), fast, 4 * 1024)) {
            ++committed;
        }
    }
    ASSERT_EQ(committed, kKeys);

    // Default (nullopt) truncates to scheduler.hot_key_num (64)...
    EXPECT_EQ(backend.GetHotKeyStats().hot_keys.size(), 64u);
    // ...while 0 returns the full set.
    EXPECT_EQ(backend.GetHotKeyStats(0).hot_keys.size(),
              static_cast<size_t>(kKeys));
}

TEST_F(EventDrivenSchedulerTest, StrictCopyDataDoesNotFallBackToOtherTier) {
    // P2-1: migrate copies strictly, so a copy that cannot land on dest fails
    // outright instead of silently falling back to another tier (which made the
    // "did it land on dest?" accounting unreliable).
    TieredBackend backend;
    // Fast tier is too small to ever hold the item, even after eviction.
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, MakeConfig(1 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);

    const size_t big = 2 * kMB;  // > fast capacity
    ASSERT_TRUE(Put(backend, "src", slow, big));
    uint64_t version = 0;
    auto src = backend.Get("src", slow, /*record_access=*/false, &version);
    ASSERT_TRUE(src.has_value());

    auto copy = backend.CopyData("src", src.value()->loc.data, fast, version,
                                 /*record_access=*/false);
    EXPECT_FALSE(copy.has_value());            // strict: fails, no fallback
    EXPECT_FALSE(backend.Exist("src", fast));  // nothing landed on fast
}

TEST_F(EventDrivenSchedulerTest, StopUnderLoadIsClean) {
    // Exercises Stop()/teardown with the evict thread and pool busy. Under
    // ASAN/TSAN this guards against UAF / leaks on shutdown.
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, MakeConfig(16 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    for (int i = 0; i < 50; ++i) {
        const std::string k = "s" + std::to_string(i);
        Put(backend, k, fast, 64 * 1024);
        for (int j = 0; j < 5; ++j) {
            Access(backend, k, fast);
        }
    }
    // backend goes out of scope here -> TieredBackend::Stop()/dtor ->
    // scheduler Stop() drains the pool and joins the evict thread cleanly.
    SUCCEED();
}

}  // namespace
}  // namespace mooncake
