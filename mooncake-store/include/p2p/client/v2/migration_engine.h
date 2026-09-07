#pragma once

// MigrationEngine: executes Replicate and Migrate commands.
//
// A command is a proposal made from approximate policy state, possibly a while
// ago. Before it copies anything the engine has to prove the proposal still
// refers to the same logical object, in three steps:
//
//   1. upgrade the weak registration -- failure means the key is gone;
//   2. confirm it is still the canonical registration for that key -- failure
//      means the key was deleted and recreated;
//   3. match the source BlockId exactly -- failure means the block was
//      replaced under the same key.
//
// After the copy it re-checks all three under the key's mutation guard, since
// any of them can change while a large block is being copied. Only then does a
// Migrate delete its source, and only once the destination is matchable.

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "p2p/client/data_manager_types.h"
#include "p2p/client/v2/block.h"
#include "p2p/client/v2/block_registry.h"
#include "p2p/client/v2/event_center.h"
#include "p2p/client/v2/local_copy_engine.h"
#include "p2p/client/v2/movement_tracker.h"
#include "p2p/client/v2/tiler_manager.h"
#include "p2p/client/v2/v2_common.h"
#include "types.h"

namespace mooncake::v2 {

/**
 * @brief Allocate on a named tiler through DataManagerV2's policy-aware path,
 *        so a migration target obeys the same allocation-failure rules as any
 *        other allocation.
 */
using AllocateBlockCallback =
    std::function<tl::expected<MutableBlock, ErrorCode>(
        const UUID& tiler_id, size_t size, size_t alignment,
        AllocationSource source)>;

/**
 * @struct MigrationSchedulerConfig
 * @brief What the route queues do. Section 10's MigrationEngine block.
 */
struct MigrationSchedulerConfig {
    /** A batch is submitted once any one of these is reached. */
    size_t max_batch_items = 16;
    size_t max_batch_bytes = 64ULL * 1024 * 1024;
    /**
     * Measured from the OLDEST queued request's arrival, not from the end of
     * the previous batch. Timing it from the previous batch is what starves a
     * low-traffic route: its one request waits behind whatever the busy routes
     * are doing and its own timer keeps restarting.
     */
    std::chrono::milliseconds max_batch_delay{20};

    /** Concurrent batches allowed on one route, and on one destination tier. */
    size_t max_inflight_per_route = 2;
    size_t max_inflight_per_device = 4;

    /** Bounded retry for transient failures only. */
    uint32_t retry_limit = 2;
    std::chrono::milliseconds retry_backoff{50};

    /** Total queued requests across all routes. 0 means unbounded. */
    size_t max_queued_requests = 8192;
};

tl::expected<void, ErrorCode> ValidateMigrationSchedulerConfig(
    const MigrationSchedulerConfig& config);

/**
 * @struct RouteStats
 * @brief Per-route queue depth, labelled by route only -- never by key.
 */
struct RouteStats {
    /** The route itself, so a caller can tell two routes apart. */
    MovementRoute route;
    std::string label;
    size_t queued_items = 0;
    size_t queued_bytes = 0;
    /** Age of the oldest queued request. */
    std::chrono::milliseconds oldest_age{0};
    size_t inflight = 0;
};

/**
 * @struct MigrationStats
 */
struct MigrationStats {
    uint64_t executed = 0;
    uint64_t succeeded = 0;
    /** Rejected because the proposal no longer matched reality. */
    uint64_t stale = 0;
    uint64_t copy_failed = 0;
    uint64_t allocate_failed = 0;
    uint64_t register_failed = 0;
    /** Abandoned because its deadline had passed. */
    uint64_t deadline_exceeded = 0;
    /** Refused at submission because the queue was full. */
    uint64_t submissions_rejected = 0;
    /** Batches formed, and the reason each one was formed. */
    uint64_t batches = 0;
    uint64_t batches_by_items = 0;
    uint64_t batches_by_bytes = 0;
    uint64_t batches_by_delay = 0;
    /** Retries attempted after a transient failure. */
    uint64_t retries = 0;
};

/**
 * @class MigrationEngine
 */
class MigrationEngine : public MovementSink {
   public:
    /**
     * @param clock Every deadline is read through it. A movement deadline that
     *        consulted steady_clock directly could not be tested without real
     *        sleeps, which is how it ended up unenforced in the first place.
     */
    MigrationEngine(MultiTiler* tilers, BlockRegistry* registry,
                    LocalCopyEngine* copy_engine, MetadataCallbacks* callbacks,
                    AllocateBlockCallback allocate_block,
                    std::shared_ptr<Clock> clock = nullptr,
                    const MigrationSchedulerConfig& scheduler = {});

    /**
     * @param request kReplicate keeps the source; kMigrate removes it once the
     *        destination is matchable.
     * @return OBJECT_NOT_FOUND when the proposal is stale, which is an
     *         expected outcome rather than an error worth alarming on.
     */
    tl::expected<void, ErrorCode> Execute(const MovementRequest& request);

    /**
     * @brief Run a whole batch, one independent outcome per item.
     *
     * Independent is the point: one item failing must not re-run the ones that
     * already succeeded, and a stale item must not abort its neighbours.
     */
    std::vector<tl::expected<void, ErrorCode>> ExecuteBatch(
        const std::vector<MovementRequest>& requests);

    // --- scheduling ---

    /**
     * @brief Queue a command on its route.
     *
     * The lease travels with the request and is settled when the request
     * leaves the pipeline, however it leaves: executed, dropped at shutdown,
     * or refused here. That is the only way "settle on every exit path" can be
     * true of a queue that can also be discarded wholesale.
     */
    bool Enqueue(MovementRequest request, MovementLease lease) override;

    /**
     * @brief Take one ready batch and run it. Blocks until there is one.
     *
     * Returns the number of items executed; 0 means the engine is stopping.
     * The worker loop lives in DataManagerV2, which owns the threads: the
     * engines must outlive them, and member order is what guarantees it.
     */
    size_t RunOnce();

    /** Stop accepting work, wake the workers and settle what is queued. */
    void Stop();
    bool IsStopped() const;

    std::vector<RouteStats> Routes() const;
    size_t QueuedCount() const;

    MigrationStats Stats() const;

   private:
    MultiTiler* tilers_ = nullptr;
    BlockRegistry* registry_ = nullptr;
    LocalCopyEngine* copy_engine_ = nullptr;
    MetadataCallbacks* callbacks_ = nullptr;
    AllocateBlockCallback allocate_block_;
    std::shared_ptr<Clock> clock_;
    MigrationSchedulerConfig scheduler_;

    /** One queued command: the proposal plus the lease that must be settled. */
    struct QueuedMovement {
        MovementRequest request;
        MovementLease lease;
        Clock::time_point queued_at;
    };

    struct RouteQueue {
        MovementRoute route;
        std::deque<QueuedMovement> items;
        size_t queued_bytes = 0;
        size_t inflight = 0;
    };

    /** Caller holds `queue_mu_`. Is this route ready to form a batch? */
    bool ReadyLocked(const RouteQueue& queue, Clock::time_point now,
                     const char** reason) const;
    /** Caller holds `queue_mu_`. Quota check for one route. */
    bool AdmissibleLocked(const RouteQueue& queue) const;
    /** Caller holds `queue_mu_`. Pick the next route to serve, or nullptr. */
    RouteQueue* SelectRouteLocked(Clock::time_point now, const char** reason);

    mutable std::mutex queue_mu_;
    std::condition_variable queue_cv_;
    std::unordered_map<MovementRoute, RouteQueue, MovementRouteHash> routes_;
    /** Round-robin cursor, so one hot route cannot hold both workers. */
    std::vector<MovementRoute> route_order_;
    size_t next_route_ = 0;
    size_t queued_total_ = 0;
    std::unordered_map<UUID, size_t, boost::hash<UUID>> device_inflight_;
    bool stopped_ = false;

    mutable std::mutex stats_mu_;
    MigrationStats stats_;
};

}  // namespace mooncake::v2
