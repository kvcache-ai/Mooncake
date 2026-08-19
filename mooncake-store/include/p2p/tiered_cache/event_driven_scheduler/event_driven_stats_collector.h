#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

#include "tiered_cache/scheduler/stats_collector.h"  // AccessStats
#include "types.h"                                   // UUID

namespace mooncake {

/**
 * @class EventDrivenStatsCollector
 * @brief Statistics interface for the event-driven scheduler.
 *
 * This is a DELIBERATELY INDEPENDENT interface — it does NOT inherit from
 * StatsCollector. The event-driven collector is owned by the
 * scheduler by concrete type; no caller holds it as a StatsCollector*, and the
 * policy receives plain KeyContext/TierStats values rather than the collector.
 * Inheriting StatsCollector would add nothing and force a pile of no-op
 * overrides plus overload hiding. We reuse only AccessStats/AccessStatEntry as
 * a return-value data carrier — not the StatsCollector contract or algorithm.
 *
 * Crucially, this interface has NO tier-agnostic RemoveKey: fast-tier residency
 * is maintained solely by the tier-aware OnDelete below.
 */
class EventDrivenStatsCollector {
   public:
    virtual ~EventDrivenStatsCollector() = default;

    // Inject the fast-tier role id. The MultiLRU only tracks keys resident in
    // this tier (see "tier role abstraction"). The collector needs only the
    // fast-tier role; the slow tier's identity is supplied by the policy.
    virtual void SetFastTier(UUID fast_tier_id) = 0;

    // Access event, carrying the tier that served this Get.
    virtual void OnAccess(std::string_view key, UUID served_tier_id) = 0;

    // Commit event. Only commits to the fast tier enter the MultiLRU.
    virtual void OnCommit(std::string_view key, UUID tier_id,
                          size_t size_bytes) = 0;

    // Tier-scoped delete. When tier_id == fast tier (or nullopt
    // for a full key delete), the key is removed from the fast-tier MultiLRU.
    // Deletes on other tiers do not affect fast-tier residency.
    virtual void OnDelete(std::string_view key,
                          std::optional<UUID> tier_id = std::nullopt) = 0;

    // Frequency query.
    virtual uint64_t GetAccessFrequency(std::string_view key) const = 0;

    // Top-N hottest keys from the MultiLRU bands — NOT enumerated from the
    // sketch (a Count-Min sketch is not enumerable).
    virtual AccessStats GetHotKeyStats(size_t hot_key_num) const = 0;
};

}  // namespace mooncake
