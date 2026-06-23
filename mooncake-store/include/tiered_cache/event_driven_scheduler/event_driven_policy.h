#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "tiered_cache/event_driven_scheduler/scheduler_context.h"
#include "tiered_cache/scheduler/stats_collector.h"  // AccessStats
#include "types.h"                                   // UUID

namespace mooncake {

class TieredBackend;  // data plane (injected via Init)

/**
 * @struct MovementRequest
 * @brief A generic data-plane movement the scheduler should execute.
 *
 * The policy expresses ALL of its decisions in terms of these primitives; the
 * scheduler knows nothing about offload/onboard/heat — it just executes the
 * requested movement. This is what keeps the scheduler policy-agnostic.
 */
struct MovementRequest {
    enum class Kind {
        kReplicate,  // copy source -> dest, KEEP source (e.g. pre-demote)
        kMigrate,    // copy source -> dest, verify landed, then DELETE source
        kEvict,      // delete source (drop a redundant copy)
    };

    Kind kind = Kind::kEvict;
    std::string key;
    UUID source_tier{};
    UUID dest_tier{};       // ignored for kEvict
    size_t size_bytes = 0;  // best-effort, for reclaim accounting
};

/**
 * @class EventDrivenPolicy
 * @brief The pluggable "brain" of the event-driven scheduler.
 *
 * A policy owns whatever statistics it needs and makes ALL movement decisions.
 * The generic EventDrivenClientScheduler only feeds it events, enqueues the
 * movements it returns, and executes them. Implementations must be thread-safe:
 * OnAccess/OnCommit/OnDelete are called from request threads, while DecideEvict
 * runs on the scheduler's background thread.
 */
class EventDrivenPolicy {
   public:
    virtual ~EventDrivenPolicy() = default;

    // Wire up the data plane and the resolved fast/slow tier roles. Called once
    // before the scheduler starts. slow_tier == nullopt disables
    // offload/onboard and demotion-on-evict.
    virtual void Init(TieredBackend* backend, UUID fast_tier,
                      std::optional<UUID> slow_tier) = 0;

    // Event hooks. The policy updates its own statistics. OnAccess may return a
    // movement to enqueue (e.g. promote/pre-demote a hot key);
    // OnCommit/OnDelete update bookkeeping only.
    virtual std::optional<MovementRequest> OnAccess(
        const AccessContext& ctx) = 0;
    virtual void OnCommit(const CommitContext& ctx) = 0;
    virtual void OnDelete(const DeleteContext& ctx) = 0;

    // Decide what to reclaim. Returns movements in execution order (coldest
    // first for an LRU-style policy). `min_reclaim_bytes` is a lower bound for
    // the allocation-failure backpressure path; pass 0 for the periodic pass
    // (the policy then uses its own watermark-driven target).
    virtual std::vector<MovementRequest> DecideEvict(
        size_t min_reclaim_bytes) = 0;

    // Hottest keys for HA recovery prioritization. `hot_key_num` is a concrete
    // count (the scheduler resolves "all" to a large value before calling).
    virtual AccessStats GetHotKeyStats(size_t hot_key_num) const = 0;
};

}  // namespace mooncake
