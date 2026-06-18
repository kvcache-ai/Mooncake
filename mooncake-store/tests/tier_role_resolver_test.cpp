#include <glog/logging.h>
#include <gtest/gtest.h>

#include <vector>

#include "tiered_cache/event_driven_scheduler/tier_roles.h"
#include "tiered_cache/tiered_backend.h"  // TierView

namespace mooncake {
namespace {

// Synthetic TierView with an arbitrary (deliberately non-DRAM) memory type, to
// prove role resolution never consults the type.
TierView MakeView(UUID id, int priority, MemoryType type = MemoryType::NVME,
                  std::vector<std::string> tags = {}) {
    TierView v;
    v.id = id;
    v.type = type;
    v.capacity = 1000;
    v.usage = 0;
    v.free_space = 1000;
    v.priority = priority;
    v.tags = std::move(tags);
    return v;
}

TEST(TierRoleResolverTest, AutoPicksHighestPriorityAsFast) {
    UUID a{1, 1}, b{2, 2}, c{3, 3};
    std::vector<TierView> views = {MakeView(a, 10), MakeView(b, 20),
                                   MakeView(c, 5)};
    auto roles = ResolveTierRoles(views, TierRoleConfig{});
    EXPECT_EQ(roles.fast, b);  // priority 20 is highest
    ASSERT_TRUE(roles.slow.has_value());
    EXPECT_EQ(*roles.slow, a);  // priority 10 is next
}

TEST(TierRoleResolverTest, NoTypeDependencyAllNonDram) {
    // Neither tier is DRAM; roles must still come purely from priority.
    UUID a{1, 1}, b{2, 2};
    std::vector<TierView> views = {MakeView(a, 1, MemoryType::ASCEND_NPU),
                                   MakeView(b, 99, MemoryType::NVME)};
    auto roles = ResolveTierRoles(views, TierRoleConfig{});
    EXPECT_EQ(roles.fast, b);  // highest priority, despite being NVME
    ASSERT_TRUE(roles.slow.has_value());
    EXPECT_EQ(*roles.slow, a);
}

TEST(TierRoleResolverTest, SingleTierHasNoSlow) {
    UUID a{7, 7};
    std::vector<TierView> views = {MakeView(a, 10)};
    auto roles = ResolveTierRoles(views, TierRoleConfig{});
    EXPECT_EQ(roles.fast, a);
    EXPECT_FALSE(roles.slow.has_value());  // offload/onboard auto-disabled
}

TEST(TierRoleResolverTest, ManualTagsOverridePriority) {
    UUID a{1, 1}, b{2, 2}, c{3, 3};
    std::vector<TierView> views = {
        MakeView(a, 100, MemoryType::NVME, {"fastpool"}),
        MakeView(b, 50, MemoryType::NVME, {"slowpool"}),
        MakeView(c, 200, MemoryType::NVME, {})};  // highest priority, untagged
    TierRoleConfig cfg;
    cfg.mode = TierRoleMode::kManual;
    cfg.fast_tier_tag = "fastpool";
    cfg.slow_tier_tag = "slowpool";
    auto roles = ResolveTierRoles(views, cfg);
    EXPECT_EQ(roles.fast, a);  // tag wins over c's higher priority
    ASSERT_TRUE(roles.slow.has_value());
    EXPECT_EQ(*roles.slow, b);
}

TEST(TierRoleResolverTest, ManualMissFallsBackToAuto) {
    UUID a{1, 1}, b{2, 2};
    std::vector<TierView> views = {MakeView(a, 10), MakeView(b, 20)};
    TierRoleConfig cfg;
    cfg.mode = TierRoleMode::kManual;
    cfg.fast_tier_tag = "nonexistent";
    cfg.slow_tier_tag = "alsomissing";
    auto roles = ResolveTierRoles(views, cfg);
    EXPECT_EQ(roles.fast, b);  // fall back to highest priority
    ASSERT_TRUE(roles.slow.has_value());
    EXPECT_EQ(*roles.slow, a);
}

TEST(TierRoleResolverTest, EmptyViewsYieldNoRoles) {
    auto roles = ResolveTierRoles({}, TierRoleConfig{});
    EXPECT_FALSE(roles.slow.has_value());
}

}  // namespace
}  // namespace mooncake
