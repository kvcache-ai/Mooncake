#pragma once

// The two stateless consumers that decide cross-tier movement.
//
// Between them they replace the monolithic PlacementPolicy, and the thing that
// makes them different is what they do NOT hold. Neither keeps an access
// count, a residency clock, a cooldown, a dedup set or a record of what exists
// where. Every one of those already has an owner:
//
//   how hot is this key      -> FrequencyTracker
//   does the block exist     -> the destination tiler's BlockIndex
//   may it move again yet    -> MovementTracker
//   which tier may it go to  -> TierPlacementPolicy over the TierGraph
//
// A consumer that cached any of it would be a second copy of state that
// already diverges on every path that does not run through the consumer -- a
// delete, a shutdown, a command the executor dropped as stale.
//
// Both are event consumers, so they see facts in per-key order and produce at
// most one decision per fact. Neither blocks: they look things up, decide, and
// hand a command to the sink.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "p2p/client/v2/block_registry.h"
#include "p2p/client/v2/event_center.h"
#include "p2p/client/v2/frequency_tracker.h"
#include "p2p/client/v2/movement_tracker.h"
#include "p2p/client/v2/tier_graph.h"
#include "p2p/client/v2/tiler_manager.h"
#include "types.h"

namespace mooncake::v2 {

/**
 * @struct MovementConsumerConfig
 */
struct MovementConsumerConfig {
    /**
     * Fraction of a tier's capacity above which committing another block
     * starts offloading. Must leave room before reclamation begins: with a
     * tier-local reclaim path, offload is the only thing keeping a
     * single-tier object alive.
     */
    double offload_high_watermark = 0.9;

    /**
     * Read heat a block on a slow tier needs before it is worth copying up.
     * 0 disables onboarding. Reads only -- a write is not demand.
     */
    double onboard_min_read_heat = 8.0;

    /** Deadline stamped on a command, so a stale one is dropped not run. */
    std::chrono::milliseconds movement_deadline{30000};
};

tl::expected<void, ErrorCode> ValidateMovementConsumerConfig(
    const MovementConsumerConfig& config);

/**
 * @struct MovementConsumerDeps
 * @brief Everything the consumers borrow. All of it outlives them.
 */
struct MovementConsumerDeps {
    MultiTiler* tilers = nullptr;
    BlockRegistry* registry = nullptr;
    const TierPlacementPolicy* placement = nullptr;
    FrequencyTracker* frequency = nullptr;
    MovementTracker* movement = nullptr;
    MovementSink* sink = nullptr;
    std::shared_ptr<Clock> clock;
};

/**
 * @brief Offload: a tier that has just grown past its watermark sheds its
 *        coldest block towards a slower neighbour.
 *
 * Subscribes to kCommit only. The committed block is not necessarily the one
 * that moves -- the tier's own eviction ordering picks the victim, because
 * "what should leave" is a property of the tier's contents and not of whatever
 * happened to arrive last.
 */
tl::expected<std::unique_ptr<EventConsumer>, ErrorCode> CreateOffloadConsumer(
    const MovementConsumerConfig& config, const MovementConsumerDeps& deps);

/**
 * @brief Onboard: a block being read on a slow tier is copied to a faster one.
 *
 * Subscribes to kAccess only, and reads its frequency from the tracker rather
 * than counting for itself. The distinction matters: the old policy's sketch
 * was bumped on commit as well as on access, so writing a key counted as
 * demand for it.
 */
tl::expected<std::unique_ptr<EventConsumer>, ErrorCode> CreateOnboardConsumer(
    const MovementConsumerConfig& config, const MovementConsumerDeps& deps);

}  // namespace mooncake::v2
