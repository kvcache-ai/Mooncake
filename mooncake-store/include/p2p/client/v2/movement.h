#pragma once

// Commands: what the movement pipeline is asked to do.
//
// Split out of event_center.h because facts and commands have different
// lifetimes and different owners. A BlockEvent is published, broadcast to
// consumers and released; a MovementRequest is produced by a consumer, queued
// per route, batched, executed and settled. Keeping them in one header forced
// the tier graph and the migration engine to depend on the event queue.
//
// A request is a *proposal*, never a capability. It carries a weak
// registration and a source BlockId, both of which the executor re-validates
// before and after the copy: by the time a batch runs, the key may have been
// deleted, recreated or rewritten.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include <boost/functional/hash.hpp>

#include "p2p/client/v2/block.h"
#include "p2p/client/v2/block_registry.h"
#include "p2p/client/v2/copier.h"
#include "p2p/client/v2/v2_common.h"
#include "types.h"

namespace mooncake::v2 {

/**
 * @enum MovementKind
 * @brief kReplicate keeps the source, kMigrate removes it once the destination
 *        is visible. There is deliberately no kEvict: reclaiming space is a
 *        tiler-local operation owned by that tiler's EvictEngine, not a
 *        cross-tier command that travels through this pipeline.
 */
enum class MovementKind : uint8_t {
    kReplicate,
    kMigrate,
};

const char* ToString(MovementKind kind);

/**
 * @enum MovementPriority
 * @brief Foreground work has a caller waiting on capacity; background work is
 *        speculative warming. A route scheduler must not let a large warm-up
 *        batch delay the reclamation a writer is blocked on.
 */
enum class MovementPriority : uint8_t {
    kForeground,
    kBackground,
};

const char* ToString(MovementPriority priority);

/**
 * @struct MovementRoute
 * @brief The queue a request belongs to.
 *
 * The copy domains are part of the identity, not just the tiler pair: two
 * tilers can be connected by more than one edge (a staged path and a direct
 * one), and they queue, batch and throttle independently.
 */
struct MovementRoute {
    MovementKind kind = MovementKind::kReplicate;
    UUID source_tiler{0, 0};
    UUID destination_tiler{0, 0};
    CopyDomain source_domain = CopyDomain::kOpaque;
    CopyDomain destination_domain = CopyDomain::kOpaque;
    bool operator==(const MovementRoute&) const = default;
};

struct MovementRouteHash {
    size_t operator()(const MovementRoute& route) const noexcept;
};

/** Stable, low-cardinality label for metrics. Never contains a key. */
std::string ToLabel(const MovementRoute& route);

/**
 * @struct MovementRequest
 * @brief One proposed movement of one block.
 *
 * Copyable on purpose: the dedup lease that makes it unique lives beside it in
 * the queue, not inside it, so a test can build and compare requests freely.
 */
struct MovementRequest {
    MovementKind kind = MovementKind::kReplicate;
    std::string key;

    /**
     * Weak: a proposal must never keep a deleted key alive.
     *
     * It also carries the canonical registration identity as of planning time
     * (`registration.Id()`), which the executor re-checks against the registry
     * before copying: a delete-and-recreate mints a new identity, and this one
     * would then name a detached block. The identity is deliberately NOT
     * duplicated into a separate field -- two copies of the same fact can
     * disagree, and the weak handle is the one the executor has to upgrade
     * anyway.
     */
    WeakBlockRegistrationHandle registration;

    UUID source_tiler{0, 0};
    UUID destination_tiler{0, 0};
    /** The block as it was when the proposal was made, generation included. */
    BlockId source_block_id;

    MovementRoute route;
    size_t length = 0;
    MovementPriority priority = MovementPriority::kBackground;
    std::optional<Clock::time_point> deadline;

    /** How many times this request has already been retried. */
    uint32_t attempt = 0;
};

}  // namespace mooncake::v2
