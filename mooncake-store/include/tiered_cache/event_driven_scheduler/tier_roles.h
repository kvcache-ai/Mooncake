#pragma once

#include <glog/logging.h>

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

#include "tiered_cache/tiered_backend.h"  // TierView
#include "types.h"                         // UUID

namespace mooncake {

/**
 * @file tier_roles.h
 * @brief Pure, header-only resolution of fast/slow tier ROLES from runtime tier
 *        topology — by `priority` and `tags` ONLY, never by tier type.
 *
 * The event-driven scheduler / collector / policy depend only on the two roles
 * (fast, slow), not on DRAM/SSD/etc. This is the single place that maps the
 * concrete tier set to those roles. It deliberately does NOT look at
 * CacheTier::GetMemoryType() / RTTI (contrast LegacyClientScheduler, which keys
 * "fast" off MemoryType==DRAM). Swapping in a new tier type (CXL, NVMe-oF,
 * remote DRAM, NPU, ...) requires zero changes here: give it a priority (and
 * optionally a tag) and the role falls out.
 */

enum class TierRoleMode {
    kAuto,    // roles follow TierView.priority
    kManual,  // roles follow fast_tier_tag / slow_tier_tag (fallback to auto)
};

struct TierRoleConfig {
    TierRoleMode mode = TierRoleMode::kAuto;
    std::string fast_tier_tag;  // empty => highest-priority tier
    std::string slow_tier_tag;  // empty => next-highest-priority tier
};

struct TierRoles {
    UUID fast{};                // fast role (priority-highest by default)
    std::optional<UUID> slow;   // slow role; nullopt => offload/onboard disabled
};

namespace tier_roles_detail {

// Pointers into `views`, sorted by priority descending (ties broken by id for
// determinism). Valid only while `views` is alive.
inline std::vector<const TierView*> SortByPriorityDesc(
    const std::vector<TierView>& views) {
    std::vector<const TierView*> sorted;
    sorted.reserve(views.size());
    for (const auto& v : views) {
        sorted.push_back(&v);
    }
    std::sort(sorted.begin(), sorted.end(),
              [](const TierView* a, const TierView* b) {
                  if (a->priority != b->priority) {
                      return a->priority > b->priority;
                  }
                  if (a->id.first != b->id.first) {
                      return a->id.first < b->id.first;
                  }
                  return a->id.second < b->id.second;
              });
    return sorted;
}

inline bool HasTag(const TierView& v, const std::string& tag) {
    return std::find(v.tags.begin(), v.tags.end(), tag) != v.tags.end();
}

// Highest-priority tier whose id differs from `fast` (nullptr if none).
inline const TierView* HighestExcept(
    const std::vector<const TierView*>& sorted, UUID fast) {
    for (const auto* v : sorted) {
        if (v->id != fast) {
            return v;
        }
    }
    return nullptr;
}

}  // namespace tier_roles_detail

/**
 * @brief Resolve fast/slow tier roles from the current tier topology.
 *
 * auto:   fast = highest priority; slow = next-highest (nullopt if only one).
 * manual: fast = first tier (priority order) tagged `fast_tier_tag` (empty tag
 *         or a miss falls back to auto-fast); slow = first OTHER tier tagged
 *         `slow_tier_tag` (empty tag or a miss falls back to auto-slow).
 *
 * Reads only priority/tags/id — no MemoryType / dynamic_cast.
 */
inline TierRoles ResolveTierRoles(const std::vector<TierView>& views,
                                  const TierRoleConfig& cfg) {
    TierRoles roles;
    if (views.empty()) {
        return roles;  // no tiers: caller must guard (fast stays zero UUID)
    }

    const auto sorted = tier_roles_detail::SortByPriorityDesc(views);

    // Auto baseline.
    const TierView* fast = sorted.front();
    const TierView* slow = sorted.size() >= 2 ? sorted[1] : nullptr;

    if (cfg.mode == TierRoleMode::kManual) {
        // Fast override by tag (miss keeps auto-fast).
        if (!cfg.fast_tier_tag.empty()) {
            for (const auto* v : sorted) {
                if (tier_roles_detail::HasTag(*v, cfg.fast_tier_tag)) {
                    fast = v;
                    break;
                }
            }
        }
        // Slow override by tag, excluding the chosen fast (miss / empty falls
        // back to highest-priority tier other than fast).
        slow = nullptr;
        if (!cfg.slow_tier_tag.empty()) {
            for (const auto* v : sorted) {
                if (v->id != fast->id &&
                    tier_roles_detail::HasTag(*v, cfg.slow_tier_tag)) {
                    slow = v;
                    break;
                }
            }
        }
        if (slow == nullptr) {
            slow = tier_roles_detail::HighestExcept(sorted, fast->id);
        }
        // Sanity: the fast role must outrank the slow role. A tag set that
        // lands 'fast' on a low-priority tier (and lets 'slow' fall back to a
        // higher-priority one) would invert the roles and silently demote hot
        // data to the higher-priority tier. Reject the manual mapping and use
        // the priority-based auto roles instead.
        if (slow != nullptr && slow->priority >= fast->priority) {
            LOG(WARNING) << "Manual tier roles invert priority (fast role @"
                         << fast->priority << " <= slow role @"
                         << slow->priority
                         << "); falling back to auto priority-based roles";
            fast = sorted.front();
            slow = sorted.size() >= 2 ? sorted[1] : nullptr;
        }
    }

    roles.fast = fast->id;
    if (slow != nullptr && slow->id != fast->id) {
        roles.slow = slow->id;
    }
    return roles;
}

}  // namespace mooncake
