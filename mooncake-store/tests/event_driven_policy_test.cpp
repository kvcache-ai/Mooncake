#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <chrono>
#include <cstring>
#include <limits>
#include <optional>
#include <string>

#include "tiered_cache/event_driven_scheduler/multi_lru_policy.h"
#include "tiered_cache/scheduler/scheduler_factory.h"  // detail::ReadMultiLRUConfig
#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/tiers/cache_tier.h"  // TempDRAMBuffer
#include "utils/common.h"                    // InitTieredBackendForTest

namespace mooncake {
namespace {

constexpr size_t kMB = 1024 * 1024;

// Two local DRAM tiers (fast = higher priority). No scheduler section, so the
// backend's own scheduler is the (inert, for this test) Legacy one; we drive a
// standalone MultiLRUPolicy directly and let it query the real backend.
Json::Value TwoTierConfig(size_t fast_cap, size_t slow_cap) {
    Json::Value cfg;
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

UUID FastTier(const TieredBackend& b) {
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

UUID SlowTier(const TieredBackend& b) {
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

bool Put(TieredBackend& b, const std::string& key, UUID tier, size_t size) {
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

MultiLRUPolicy::Config TestConfig() {
    MultiLRUPolicy::Config c;
    c.offload_freq_threshold = 2;
    c.onboard_freq_threshold = 2;
    c.onboard_fast_threshold = 0.95;
    c.sketch_capacity = 4096;
    return c;
}

// NOTE: the TEST bodies live inside this anonymous namespace so that the
// test-local kMB shadows storage_backend.h's mooncake::kMB (pulled in via
// utils/common.h) — otherwise references to kMB would be ambiguous.

TEST(MultiLRUPolicyTest, OnAccessReturnsOffloadForHotFastKey) {
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, TwoTierConfig(16 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);
    MultiLRUPolicy policy(TestConfig());
    policy.Init(&backend, fast, slow);

    ASSERT_TRUE(Put(backend, "k", fast, 4096));
    policy.OnCommit(CommitContext{"k", fast, 4096, 0, false});

    std::optional<MovementRequest> mv;
    for (int i = 0; i < 5; ++i) {
        mv = policy.OnAccess(AccessContext{"k", fast, 4096});
    }
    ASSERT_TRUE(mv.has_value());
    EXPECT_EQ(mv->kind, MovementRequest::Kind::kReplicate);  // offload
    EXPECT_EQ(mv->source_tier, fast);
    EXPECT_EQ(mv->dest_tier, slow);
}

TEST(MultiLRUPolicyTest, NoOffloadWhenAlreadyOnSlow) {
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, TwoTierConfig(16 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);
    MultiLRUPolicy policy(TestConfig());
    policy.Init(&backend, fast, slow);

    ASSERT_TRUE(Put(backend, "k", fast, 4096));
    ASSERT_TRUE(Put(backend, "k", slow, 4096));  // second replica on slow
    policy.OnCommit(CommitContext{"k", fast, 4096, 0, false});

    std::optional<MovementRequest> mv;
    for (int i = 0; i < 5; ++i) {
        mv = policy.OnAccess(AccessContext{"k", fast, 4096});
    }
    EXPECT_FALSE(mv.has_value());  // already on slow -> nothing to offload
}

TEST(MultiLRUPolicyTest, OnAccessReturnsOnboardForHotSlowKey) {
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, TwoTierConfig(16 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);
    MultiLRUPolicy policy(TestConfig());
    policy.Init(&backend, fast, slow);

    ASSERT_TRUE(Put(backend, "k", slow, 4096));
    policy.OnCommit(CommitContext{"k", slow, 4096, 0, false});

    std::optional<MovementRequest> mv;
    for (int i = 0; i < 5; ++i) {
        mv = policy.OnAccess(AccessContext{"k", slow, 4096});
    }
    ASSERT_TRUE(mv.has_value());
    EXPECT_EQ(mv->kind, MovementRequest::Kind::kMigrate);  // onboard (move)
    EXPECT_EQ(mv->source_tier, slow);
    EXPECT_EQ(mv->dest_tier, fast);
}

TEST(MultiLRUPolicyTest, DecideEvictAboveWatermarkEmitsEvicts) {
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, TwoTierConfig(4 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);
    MultiLRUPolicy::Config cfg = TestConfig();
    cfg.evict_watermark_high = 0.50;
    cfg.evict_watermark_low = 0.40;
    MultiLRUPolicy policy(cfg);
    policy.Init(&backend, fast, slow);

    for (int i = 0; i < 14; ++i) {
        const std::string k = "e" + std::to_string(i);
        ASSERT_TRUE(Put(backend, k, fast, 256 * 1024));
        policy.OnCommit(CommitContext{k, fast, 256 * 1024, 0, false});
    }

    const auto movements = policy.DecideEvict(/*min_reclaim_bytes=*/0);
    ASSERT_FALSE(movements.empty());
    for (const auto& mv : movements) {
        EXPECT_EQ(mv.source_tier, fast);
        // Eviction only drops fast-tier copies; it never migrates synchronously.
        EXPECT_EQ(mv.kind, MovementRequest::Kind::kEvict);
    }
}

TEST(MultiLRUPolicyTest, DecideEvictBelowWatermarkEmpty) {
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, TwoTierConfig(64 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);
    MultiLRUPolicy policy(TestConfig());  // default bounds [0.70, 0.90]
    policy.Init(&backend, fast, slow);

    ASSERT_TRUE(Put(backend, "k", fast, 4096));
    policy.OnCommit(CommitContext{"k", fast, 4096, 0, false});

    EXPECT_TRUE(policy.DecideEvict(0).empty());  // ~0% usage, far below trigger
}

TEST(MultiLRUPolicyTest, InitialWatermarkStartsAtLowBound) {
    // Startup pessimistically assumes high write load: the trigger is born at
    // the LOW bound so the fast tier starts with maximum headroom and avoids an
    // immediate allocation-failure fallback before any load has been measured.
    const MultiLRUPolicy::Config cfg = TestConfig();
    MultiLRUPolicy policy(cfg);
    EXPECT_NEAR(policy.evict_wm(), cfg.evict_watermark_low, 1e-6);
}

TEST(MultiLRUPolicyTest, WatermarkFloatsDownWithWriteLoad) {
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, TwoTierConfig(4 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);
    MultiLRUPolicy::Config cfg = TestConfig();  // low 0.70, high 0.90
    MultiLRUPolicy policy(cfg);
    policy.Init(&backend, fast, slow);

    policy.DecideEvict(0);  // load 0 -> watermark floats up to the high bound
    EXPECT_NEAR(policy.evict_wm(), cfg.evict_watermark_high, 1e-6);

    // A full-capacity worth of writes floats the trigger down to the low bound
    // (evict earlier / keep more headroom under write pressure).
    policy.OnCommit(CommitContext{"x", fast, 4 * kMB, 0, false});
    policy.DecideEvict(0);
    EXPECT_NEAR(policy.evict_wm(), cfg.evict_watermark_low, 1e-6);
    EXPECT_LT(policy.evict_wm(), cfg.limit_watermark);
}

TEST(MultiLRUPolicyTest, HysteresisBandBelowFloor) {
    MultiLRUPolicy::Config cfg;
    EXPECT_LT(cfg.onboard_fast_threshold, cfg.evict_watermark_low);
}

// The trigger must be able to traverse the FULL [low, high] range under
// realistic load: the periodic loop fires often and commits only a sliver of
// capacity per pass, but the time-decayed load integrator lets sustained writes
// accumulate to pull the trigger all the way to the low bound (and relax back).
TEST(MultiLRUPolicyTest, SustainedWriteLoadDrivesTriggerAcrossFullRange) {
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, TwoTierConfig(4 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);
    MultiLRUPolicy::Config cfg = TestConfig();  // low 0.70, high 0.90
    cfg.evict_load_window_s = 1.0;

    // Controllable monotonic clock so the test is timing-deterministic.
    auto now = std::chrono::steady_clock::time_point{};
    MultiLRUPolicy policy(cfg, [&now] { return now; });
    policy.Init(&backend, fast, slow);

    // One periodic pass 50ms later, optionally committing `bytes` first.
    auto tick = [&](size_t bytes) {
        if (bytes) {
            policy.OnCommit(CommitContext{"w", fast, bytes, 0, false});
        }
        now += std::chrono::milliseconds(50);
        policy.DecideEvict(0);
    };
    const size_t slice = (4 * kMB) / 20;  // 5% of capacity per pass

    // No load -> rests at the high bound.
    tick(0);
    EXPECT_NEAR(policy.evict_wm(), cfg.evict_watermark_high, 1e-6);

    // A SINGLE small pass barely moves it — exactly the symptom a fast loop
    // would otherwise be pinned at.
    tick(slice);
    EXPECT_GT(policy.evict_wm(), cfg.evict_watermark_high - 0.02);

    // SUSTAINED writes (~one capacity per 1s window) accumulate and pull the
    // trigger all the way down to the low bound.
    for (int i = 0; i < 120; ++i) {
        tick(slice);
    }
    EXPECT_NEAR(policy.evict_wm(), cfg.evict_watermark_low, 1e-6);

    // Writes stop: over several windows the trigger relaxes back to high.
    for (int i = 0; i < 200; ++i) {
        tick(0);
    }
    EXPECT_NEAR(policy.evict_wm(), cfg.evict_watermark_high, 1e-3);
}

// P2-2: the allocation-failure path (min_reclaim_bytes > 0) must NOT consume
// the write-load accumulator, so a concurrent periodic pass still observes the
// load and floats the trigger watermark down to the low bound.
TEST(MultiLRUPolicyTest, AllocFailurePathDoesNotConsumeWriteLoad) {
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, TwoTierConfig(4 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);
    MultiLRUPolicy::Config cfg = TestConfig();  // low 0.70, high 0.90
    MultiLRUPolicy policy(cfg);
    policy.Init(&backend, fast, slow);

    // A full-capacity worth of writes' worth of load.
    policy.OnCommit(CommitContext{"x", fast, 4 * kMB, 0, false});

    // Allocation-failure pass first: must leave the accumulator intact.
    policy.DecideEvict(/*min_reclaim_bytes=*/1);
    // Periodic pass still sees the load and floats the trigger DOWN to the low
    // bound (it would rest at the high bound had the alloc-failure pass consumed
    // the accumulator).
    policy.DecideEvict(0);
    EXPECT_NEAR(policy.evict_wm(), cfg.evict_watermark_low, 1e-6);
}

// P2-3: a zero-size commit must never enter the MultiLRU, so it cannot become a
// victim that the eviction loop emits without advancing reclaim.
TEST(MultiLRUPolicyTest, ZeroSizeCommitProducesNoVictim) {
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, TwoTierConfig(4 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);
    MultiLRUPolicy policy(TestConfig());
    policy.Init(&backend, fast, slow);

    policy.OnCommit(CommitContext{"empty", fast, 0, 0, false});

    // Backpressure wants to reclaim, but the zero-size key never entered the
    // LRU, so there is nothing to evict.
    const auto movements = policy.DecideEvict(/*min_reclaim_bytes=*/1);
    EXPECT_TRUE(movements.empty());
}

// --- Factory config wiring (P0-1: candidate_scan_limit key) -----------------

TEST(SchedulerFactoryConfigTest, CandidateScanLimitUsesOwnKey) {
    Json::Value cfg;
    cfg["scheduler"]["candidate_scan_limit"] = 128;
    const auto c = detail::ReadMultiLRUConfig(cfg);
    EXPECT_EQ(c.candidate_scan_limit, 128u);
}

TEST(SchedulerFactoryConfigTest, LegacyStatsSnapshotLimitDoesNotPolluteScanLimit) {
    // stats_snapshot_limit is a *legacy* scheduler knob (0 == "snapshot all").
    // The event-driven policy must ignore it and keep its own default, rather
    // than inheriting 0 and mapping it to SIZE_MAX (a full scan every pass).
    Json::Value cfg;
    cfg["scheduler"]["stats_snapshot_limit"] = 0;
    const auto c = detail::ReadMultiLRUConfig(cfg);
    EXPECT_EQ(c.candidate_scan_limit,
              MultiLRUPolicy::Config{}.candidate_scan_limit);
    EXPECT_NE(c.candidate_scan_limit, std::numeric_limits<size_t>::max());
}

TEST(SchedulerFactoryConfigTest, CandidateScanLimitZeroMeansUnlimited) {
    Json::Value cfg;
    cfg["scheduler"]["candidate_scan_limit"] = 0;
    const auto c = detail::ReadMultiLRUConfig(cfg);
    EXPECT_EQ(c.candidate_scan_limit, std::numeric_limits<size_t>::max());
}

TEST(SchedulerFactoryConfigTest, ClampsInvalidWatermarkOrdering) {
    Json::Value cfg;
    cfg["scheduler"]["evict_watermark_low"] = 0.80;
    cfg["scheduler"]["evict_watermark_high"] = 0.60;  // high < low
    cfg["scheduler"]["limit_watermark"] = 0.50;       // limit < high
    const auto c = detail::ReadMultiLRUConfig(cfg);
    EXPECT_GE(c.evict_watermark_high, c.evict_watermark_low);
    EXPECT_GE(c.limit_watermark, c.evict_watermark_high);
}

TEST(SchedulerFactoryConfigTest, RejectsNonPositiveLoadWindow) {
    Json::Value cfg;
    cfg["scheduler"]["evict_load_window_s"] = 0.0;
    const auto c = detail::ReadMultiLRUConfig(cfg);
    EXPECT_GT(c.evict_load_window_s, 0.0);
}

}  // namespace
}  // namespace mooncake
