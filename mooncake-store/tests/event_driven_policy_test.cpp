#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <cstring>
#include <limits>
#include <optional>
#include <string>

#include "tiered_cache/event_driven_scheduler/multi_lru_policy.h"
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
    cfg.evict_watermark = 0.50;
    cfg.user_floor = 0.40;
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
    MultiLRUPolicy policy(TestConfig());  // evict_watermark 0.90
    policy.Init(&backend, fast, slow);

    ASSERT_TRUE(Put(backend, "k", fast, 4096));
    policy.OnCommit(CommitContext{"k", fast, 4096, 0, false});

    EXPECT_TRUE(policy.DecideEvict(0).empty());  // ~0% usage << 90%
}

TEST(MultiLRUPolicyTest, WatermarkFloatsDownWithWriteLoad) {
    TieredBackend backend;
    ASSERT_TRUE(
        InitTieredBackendForTest(backend, TwoTierConfig(4 * kMB, 64 * kMB))
            .has_value());
    const UUID fast = FastTier(backend);
    const UUID slow = SlowTier(backend);
    MultiLRUPolicy::Config cfg = TestConfig();  // seed 0.90, floor 0.70
    MultiLRUPolicy policy(cfg);
    policy.Init(&backend, fast, slow);

    policy.DecideEvict(0);  // load 0 -> watermark == seed
    EXPECT_NEAR(policy.evict_watermark(), 0.90, 1e-6);

    // A full-capacity worth of writes floats the trigger down to the floor.
    policy.OnCommit(CommitContext{"x", fast, 4 * kMB, 0, false});
    policy.DecideEvict(0);
    EXPECT_NEAR(policy.evict_watermark(), cfg.user_floor, 1e-6);
    EXPECT_LT(policy.evict_watermark(), cfg.limit_watermark);
}

TEST(MultiLRUPolicyTest, HysteresisBandBelowFloor) {
    MultiLRUPolicy::Config cfg;
    EXPECT_LT(cfg.onboard_fast_threshold, cfg.user_floor);
}

}  // namespace
}  // namespace mooncake
