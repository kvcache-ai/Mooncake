#pragma once

#include <cstddef>
#include <optional>
#include <string_view>

#include "tiered_cache/event_driven_scheduler/scheduler_context.h"
#include "tiered_cache/scheduler/stats_collector.h"  // AccessStats
#include "types.h"                                   // UUID

namespace mooncake {

class CacheTier;  // Forward declaration

/**
 * @class IClientScheduler
 * @brief Abstract interface for the TieredBackend's scheduling layer.
 *
 * Implementations coordinate statistics collection, policy decisions, and data
 * movement across tiers. The data plane (TieredBackend) drives the scheduler
 * purely through this interface, so concrete schedulers (LegacyClientScheduler,
 * EventDrivenClientScheduler) can be swapped via configuration.
 */
class IClientScheduler {
   public:
    virtual ~IClientScheduler() = default;

    // Lifecycle management.
    virtual void Start() = 0;
    virtual void Stop() = 0;

    // Register a managed tier.
    virtual void RegisterTier(CacheTier* tier) = 0;

    // --- Incoming event hooks (thread-safe), Context-carrying ---
    // All hook payloads are bundled into Context structs so the payload can
    // grow (e.g. served_tier_id) without churning signatures or call sites. The
    // data plane always invokes these Context variants.
    virtual void OnAccess(const AccessContext& ctx) = 0;
    virtual void OnCommit(const CommitContext& ctx) = 0;
    virtual void OnDelete(const DeleteContext& ctx) = 0;

    // Called when allocation fails due to insufficient space.
    // Returns true if reclaim freed enough space for an immediate retry.
    virtual bool OnAllocationFailure(const AllocationFailureContext& ctx) = 0;

    /**
     * @brief Get current hot key statistics (e.g. for HA recovery
     *        prioritization).
     * @param hot_key_num Number of hottest keys to return.
     *        - nullopt (default): use the startup config
     * `scheduler.hot_key_num` (defaults to 64). NOTE: this differs from the
     * historical no-arg behavior which returned *all* tracked keys. Deployments
     * that need the old "return everything" semantics should set
     *          `scheduler.hot_key_num = 0`.
     *        - explicit value: override for this call.
     *        - resolves to 0: return all (subject to the snapshot limit).
     */
    virtual AccessStats GetHotKeyStats(
        std::optional<size_t> hot_key_num = std::nullopt) const = 0;
};

}  // namespace mooncake
