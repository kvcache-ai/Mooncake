#pragma once

// EvictEngine: the only path that reclaims space in V2.
//
// It serves two callers with one victim-selection and removal path: the
// background watermark loop, and the synchronous retry inside
// AllocateWithPolicy. It decides nothing about *whether* to run -- rounds,
// deadline and the try_evict switch all live in AllocateWithPolicy -- so it
// can never loop on its own.
//
// Reclamation is tier-local, and deliberately so (design section 4.1). This
// engine never looks at another tier, never asks whether a replica exists
// elsewhere, never demotes a block to a slower tier and never waits for an
// offload. Cross-tier copies and durability belong entirely to the offload
// pipeline, which runs on its own thread with its own budget; putting any of
// it on the reclaim path would make a request thread that is merely short of
// space wait for a whole block copy.
//
// The consequence is accepted, not overlooked: if the offload pipeline has not
// yet copied a block to a slower tier when its tier fills up, reclaiming that
// block destroys the object. `EvictStats::victims_sole_replica` counts exactly
// that, and each one is logged, because a trade-off nobody can measure is
// indistinguishable from a bug.
//
// The distinction it must never blur: detaching an entry from an index is a
// logical removal, and the physical bytes only come back when the last reader
// snapshot is released. Reporting detached bytes as reclaimed would let a
// caller believe an allocation should now succeed when nothing was freed.

#include <chrono>
#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <cstdint>
#include <mutex>

#include <ylt/util/tl/expected.hpp>

#include "p2p/client/data_manager_types.h"
#include "p2p/client/v2/block_registry.h"
#include "p2p/client/v2/event_center.h"
#include "p2p/client/v2/tiler_manager.h"
#include "p2p/client/v2/v2_common.h"
#include "types.h"

namespace mooncake::v2 {

/**
 * @struct ReclaimRequest
 */
struct ReclaimRequest {
    UUID tiler_id{0, 0};
    AllocationSource source = AllocationSource::kPut;
    size_t allocation_size = 0;
    size_t reclaim_target_bytes = 0;
    uint32_t round = 0;
    Clock::time_point deadline{};
};

/**
 * @struct ReclaimResult
 * @brief Deliberately reports logical and physical reclaim separately: only
 *        the second can make an allocation succeed.
 */
struct ReclaimResult {
    size_t logically_detached_bytes = 0;
    size_t physically_reclaimed_bytes = 0;
    size_t free_bytes_before = 0;
    size_t free_bytes_after = 0;
    uint32_t candidates_examined = 0;
    bool deadline_reached = false;
};

/**
 * @struct EvictStats
 */
struct EvictStats {
    uint64_t rounds = 0;
    uint64_t victims_detached = 0;
    uint64_t victims_stale = 0;
    uint64_t deadline_reached = 0;
    /**
     * Evictions that removed the last replica of an object, so the object is
     * gone. Not a failure of this engine -- it is what section 4.1 asks for --
     * but the one number that says how much data the offload pipeline is
     * failing to keep ahead of. A non-zero value here means offload is not
     * keeping up, or its threshold is too close to the reclaim watermark.
     */
    uint64_t victims_sole_replica = 0;
};

/**
 * @class EvictEngine
 */
class EvictEngine {
   public:
    /**
     * @param on_evicted Invoked, outside every lock, with a key whose last
     *        replica has just gone. DataManagerV2 uses it to stop tracking the
     *        key's access frequency; without it the tracker grows for the
     *        process lifetime and reports keys that no longer exist.
     */
    EvictEngine(MultiTiler* tilers, BlockRegistry* registry,
                MetadataCallbacks* callbacks, std::shared_ptr<Clock> clock,
                std::function<void(const std::string&)> on_evicted = {});

    /**
     * @brief One bounded pass over the coldest candidates on a tiler.
     *
     * Never loops: the caller decides whether to run another round, so a
     * request thread cannot get stuck here.
     */
    tl::expected<ReclaimResult, ErrorCode> ReclaimOneRound(
        const ReclaimRequest& request);

    /**
     * @brief Reclaim one named block.
     *
     * Takes a BlockToken rather than a command: reclaiming space is a
     * tiler-local operation, not a cross-tier movement, so it does not
     * belong in the movement pipeline and never needs a destination.
     */
    tl::expected<void, ErrorCode> EvictOne(const BlockToken& token);

    EvictStats Stats() const;

   private:
    /**
     * @brief Reclaim one victim if it is still exactly what the token claims.
     *
     * Removal is unconditional once the identity checks pass. There is no
     * "is it safe to drop this" question here by design: answering it would
     * mean reading other tiers, and acting on the answer would mean copying a
     * block on the reclaim path.
     */
    struct EvictOutcome {
        /** Bytes freed on `tiler`; 0 means nothing was reclaimed. */
        size_t bytes = 0;
        /**
         * True when this eviction removed the object's last replica.
         *
         * Observed after the removal, from the presence set the registration
         * already maintains -- not a check that changed the decision, and not
         * a walk of the other tilers. It drives the access-tracker trim (a key
         * with no replicas left should stop being tracked) and the
         * sole-replica counter.
         */
        bool was_sole_replica = false;
    };

    EvictOutcome EvictVictim(TilerManager& tiler, const BlockToken& victim);

    MultiTiler* tilers_ = nullptr;
    BlockRegistry* registry_ = nullptr;
    MetadataCallbacks* callbacks_ = nullptr;
    std::shared_ptr<Clock> clock_;
    std::function<void(const std::string&)> on_evicted_;

    mutable std::mutex stats_mu_;
    EvictStats stats_;
};

}  // namespace mooncake::v2
