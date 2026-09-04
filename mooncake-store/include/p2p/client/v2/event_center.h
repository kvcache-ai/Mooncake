#pragma once

// Facts and commands (section 3.8).
//
// A BlockEvent states something that already happened (a block was read,
// committed, deleted; an allocation failed). A MovementRequest is a command
// produced by a policy from those facts (replicate, migrate, evict). Nothing
// on the event side may move a block: a command carries a weak registration
// plus a source BlockId, and the executor re-validates both before acting.
//
// Layering note: producers depend on the EventSink interface, not on
// EventCenter. That keeps TilerManager free of any queue/worker dependency and
// lets a component test inject a recording sink. EventCenter (the bounded
// queues, the coalescing and the movement dispatch) implements EventSink and
// arrives with the policy engine.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "p2p/client/v2/block.h"
#include "p2p/client/v2/block_registry.h"
#include "p2p/client/v2/movement.h"
#include "p2p/client/v2/movement_tracker.h"
#include "types.h"

namespace mooncake::v2 {

/**
 * @enum EventType
 * @brief Facts. Access may be sampled or coalesced; lifecycle events
 *        (kCommit, kDelete) must not be silently lost.
 */
enum class EventType : uint8_t {
    kAccess,
    kCommit,
    kDelete,
    kAllocationFailure,
    kWatermark,
};

const char* ToString(EventType type);

/**
 * @struct BlockEvent
 */
struct BlockEvent {
    EventType type = EventType::kAccess;
    std::string key;
    UUID tiler_id{0, 0};
    // Absent for an allocation failure: there is no block yet.
    std::optional<BlockId> block_id;
    std::optional<WeakBlockRegistrationHandle> registration;
    size_t size_bytes = 0;
    // Set for kAllocationFailure: the original allocator error.
    std::optional<ErrorCode> error;
};

/**
 * @enum QueuePushMode
 */
enum class QueuePushMode : uint8_t {
    kBestEffort,  // Access: may be dropped or coalesced
    kReliable,    // lifecycle: must take effect, one way or another
};

/**
 * @enum QueuePushResult
 */
enum class QueuePushResult : uint8_t {
    kEnqueued,
    kCoalesced,
    kDropped,
    kClosed,
    kCancelled,
};

const char* ToString(QueuePushResult result);

/**
 * @enum ConsumeResult
 * @brief What a consumer did with an event. Every one of these is a terminal
 *        answer: once a consumer returns, that delivery is complete.
 */
enum class ConsumeResult : uint8_t {
    /** State was updated; nothing further follows. */
    kApplied,
    /** A command was reliably handed to its queue. */
    kCommandEnqueued,
    /** Checked and deliberately no-op -- not an error and not a drop. */
    kIgnored,
    /** Consumption failed. Counted and alarmed on, never silently swallowed. */
    kFailed,
};

const char* ToString(ConsumeResult result);

/**
 * @enum DeliveryMode
 * @brief How an event reached the consumer.
 *
 * kInline means the shard queue was full and the producer is applying the
 * event on its own thread -- a writer's thread. A consumer must then update
 * its state and return, and must not produce commands: doing so would push
 * queueing, and on a full command queue a whole scheduling decision, into the
 * write path. Losing the commands is the accepted cost; losing the state
 * change is not.
 */
enum class DeliveryMode : uint8_t {
    kQueued,
    kInline,
};

/**
 * @struct Subscription
 * @brief Which event types a consumer wants, and what to call it in metrics.
 */
struct Subscription {
    /** Stable, low-cardinality label. Never derived from a key. */
    std::string name;
    /** Bitmask over EventType. */
    uint32_t event_mask = 0;

    static constexpr uint32_t MaskOf(EventType type) {
        return 1U << static_cast<uint32_t>(type);
    }
    bool Wants(EventType type) const {
        return (event_mask & MaskOf(type)) != 0;
    }
};

/** Convenience: build a mask from a list of types. */
uint32_t SubscriptionMask(std::initializer_list<EventType> types);

/**
 * @class EventConsumer
 * @brief A registered reader of facts.
 *
 * Consumers are registered during initialization and the set is frozen when
 * the center starts, so the fan-out target of any event is fixed and a
 * consumer cannot appear halfway through a key's history.
 *
 * Consume must not block: it may look things up, decide, and hand a command to
 * a queue, but it runs on the shard worker that is also delivering that key's
 * next event, and on the inline path it runs on a writer's thread.
 */
class EventConsumer {
   public:
    virtual ~EventConsumer() = default;
    virtual Subscription SubscriptionInfo() const = 0;
    virtual ConsumeResult Consume(const BlockEvent& event,
                                  DeliveryMode mode) = 0;
};

/**
 * @class MovementSink
 * @brief Where a consumer hands the commands it decides on.
 *
 * Separate from the consumer interface so a consumer can be tested without a
 * queue, and so the queue can be replaced -- by per-route queues -- without
 * touching any consumer.
 */
class MovementSink {
   public:
    virtual ~MovementSink() = default;
    /**
     * @brief Hand over a command and the lease that keeps it unique.
     *
     * The lease travels with the request because the sink is the only thing
     * that knows when the request finally leaves the pipeline -- executed,
     * discarded at shutdown, or refused here. A lease left behind on any of
     * those paths wedges that key's dedup slot for the process lifetime.
     *
     * @return false when the sink refused it. The lease is settled either way.
     */
    virtual bool Enqueue(MovementRequest request, MovementLease lease) = 0;
};

/**
 * @class EventSink
 * @brief Where facts go. Implemented by EventCenter in production and by a
 *        recorder in component tests.
 */
class EventSink {
   public:
    virtual ~EventSink() = default;
    virtual QueuePushResult Publish(BlockEvent event) = 0;
};

/**
 * @class EventPublisher
 * @brief The producer-side handle. Holding it weakly means a producer can
 *        outlive the sink during shutdown without dangling.
 */
class EventPublisher {
   public:
    EventPublisher() = default;
    explicit EventPublisher(std::weak_ptr<EventSink> sink)
        : sink_(std::move(sink)) {}

    /** kClosed when no sink is attached or it has already been destroyed. */
    QueuePushResult Publish(BlockEvent event) const;

    bool IsAttached() const { return !sink_.expired(); }

   private:
    std::weak_ptr<EventSink> sink_;
};

/**
 * @class BoundedQueue
 * @brief An MPSC queue with a hard capacity and an optional coalescer.
 *
 * Distinct from the existing bounded_dedup_queue: that one is try-push only,
 * dedupes by key and drops when full. What the event pipeline needs is a
 * configurable coalescer plus a *deterministic* answer when a reliable push
 * finds the queue full -- kDropped, so the producer can apply the event inline
 * instead of blocking. Blocking would push a stalled policy worker straight
 * into every write path.
 */
template <typename T>
class BoundedQueue {
   public:
    /** Returns true if `incoming` was folded into `existing`. */
    using Coalescer = std::function<bool(T& existing, const T& incoming)>;

    /**
     * @param reserved Slots only a kReliable push may use.
     *
     * Without it, a read burst fills the queue with best-effort events and the
     * next lifecycle event is pushed onto the inline path -- where it is
     * applied immediately, ahead of the events already queued for the same
     * key. The consumer then sees that key's Delete before its Commit. The
     * reserve makes the inline fallback what it is supposed to be: a last
     * resort for a flood of lifecycle events, not a consequence of reads.
     */
    explicit BoundedQueue(size_t capacity, Coalescer coalescer = {},
                          size_t reserved = 0)
        : capacity_(capacity == 0 ? 1 : capacity),
          // Capped at a quarter of the queue. A fixed reserve is meant for a
          // queue of thousands; applied literally to a small one it would turn
          // most of the queue over to lifecycle events and start dropping the
          // access stream the ordering depends on.
          reserved_(std::min(reserved, (capacity == 0 ? 1 : capacity) / 4)),
          coalescer_(std::move(coalescer)) {}

    QueuePushResult Push(T value, QueuePushMode mode) {
        std::lock_guard<std::mutex> lock(mu_);
        if (closed_) return QueuePushResult::kClosed;

        const size_t limit = mode == QueuePushMode::kReliable
                                 ? capacity_
                                 : capacity_ - reserved_;
        if (queue_.size() >= limit) {
            if (coalescer_) {
                for (auto& queued : queue_) {
                    if (coalescer_(queued, value)) {
                        ++coalesced_;
                        return QueuePushResult::kCoalesced;
                    }
                }
            }
            // Full. Best-effort events are simply lost; reliable ones are
            // handed back to the producer to apply inline. Neither blocks.
            ++dropped_;
            return QueuePushResult::kDropped;
        }
        queue_.push_back(std::move(value));
        not_empty_.notify_one();
        return QueuePushResult::kEnqueued;
    }

    /** Blocks until an item is available or the queue closes. */
    std::optional<T> Pop() {
        std::unique_lock<std::mutex> lock(mu_);
        not_empty_.wait(lock, [this] { return closed_ || !queue_.empty(); });
        if (queue_.empty()) return std::nullopt;
        T value = std::move(queue_.front());
        queue_.pop_front();
        return value;
    }

    /** Non-blocking; used to drain what is already queued. */
    std::optional<T> TryPop() {
        std::lock_guard<std::mutex> lock(mu_);
        if (queue_.empty()) return std::nullopt;
        T value = std::move(queue_.front());
        queue_.pop_front();
        return value;
    }

    void Close() {
        std::lock_guard<std::mutex> lock(mu_);
        closed_ = true;
        not_empty_.notify_all();
    }

    /** Discard everything queued. Used by a cancelling shutdown. */
    void Clear() {
        std::lock_guard<std::mutex> lock(mu_);
        queue_.clear();
    }

    size_t Size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return queue_.size();
    }
    size_t Capacity() const { return capacity_; }
    uint64_t DroppedCount() const {
        std::lock_guard<std::mutex> lock(mu_);
        return dropped_;
    }
    uint64_t CoalescedCount() const {
        std::lock_guard<std::mutex> lock(mu_);
        return coalesced_;
    }

    size_t Reserved() const { return reserved_; }

   private:
    const size_t capacity_;
    const size_t reserved_;
    mutable std::mutex mu_;
    std::condition_variable not_empty_;
    std::deque<T> queue_;
    Coalescer coalescer_;
    bool closed_ = false;
    uint64_t dropped_ = 0;
    uint64_t coalesced_ = 0;
};

/**
 * @struct EventCenterConfig
 */
struct EventCenterConfig {
    size_t shard_count = 8;
    size_t event_queue_capacity = 4096;
    /**
     * Slots in each shard queue that only a lifecycle event may take. See
     * BoundedQueue's constructor: this is what stops a read burst from pushing
     * a Commit or a Delete onto the inline path and out of order.
     */
    size_t lifecycle_reserve = 64;
};

tl::expected<void, ErrorCode> ValidateEventCenterConfig(
    const EventCenterConfig& config);

/**
 * @struct EventCenterMetrics
 */
struct ConsumerMetrics {
    std::string name;
    uint64_t applied = 0;
    uint64_t command_enqueued = 0;
    uint64_t ignored = 0;
    uint64_t failed = 0;
};

struct EventCenterMetrics {
    uint64_t events_enqueued = 0;
    uint64_t events_coalesced = 0;
    /** Best-effort events the queue refused. Sampling made visible. */
    uint64_t events_dropped = 0;
    /** Lifecycle events applied by the producer because a queue was full. */
    uint64_t lifecycle_event_inline_applied = 0;
    /** Events delivered to at least one consumer. */
    uint64_t events_broadcast = 0;
    /** Individual consumer deliveries. */
    uint64_t consumer_deliveries = 0;
    size_t event_queue_depth = 0;
    /** One entry per registered consumer, in registration order. */
    std::vector<ConsumerMetrics> consumers;
};

enum class EventCenterStopMode : uint8_t {
    kDrain,
    kCancel,
};

/**
 * @class EventCenter
 * @brief A pure queue: facts in, commands out.
 *
 * It deliberately owns no execution component. The movement workers that
 * consume its output belong to DataManagerV2, because they call the migration
 * and eviction engines: if the EventCenter owned them, member destruction
 * order would tear those engines down first and a destructor that skipped
 * Stop() would be a use-after-free (section 5.2).
 */
class EventCenter final : public EventSink,
                          public std::enable_shared_from_this<EventCenter> {
   public:
    explicit EventCenter(const EventCenterConfig& config);
    ~EventCenter() override;

    /**
     * @brief Add a consumer. Initialization only.
     *
     * @return INVALID_PARAMS for a null consumer, an empty subscription mask,
     *         a duplicate name, or a call after Start(). The set is frozen at
     *         Start so the fan-out target of an event is fixed and no consumer
     *         can appear part-way through a key's history.
     */
    tl::expected<void, ErrorCode> RegisterConsumer(EventConsumer* consumer);

    /** Freeze the consumer set and start one worker per shard. */
    void Start();

    /**
     * @brief Enqueue a fact.
     *
     * A full queue returns kDropped for a lifecycle event, and the caller must
     * then apply it inline -- which Publish does for its own callers via
     * ApplyInlineIfDropped, so producers never have to remember.
     */
    QueuePushResult Publish(BlockEvent event) override;

    EventPublisher Publisher();

    void Stop(EventCenterStopMode mode);
    bool IsStopped() const;

    EventCenterMetrics Metrics() const;

    /** Apply everything queued right now, for deterministic tests. */
    void DrainForTest();

   private:
    void WorkerMain();
    /** Fan the event out to every consumer subscribed to its type. */
    void ProcessEvent(const BlockEvent& event, DeliveryMode mode);
    void CountResult(size_t consumer_index, ConsumeResult result);
    /** Apply the backlog on the calling thread. Only safe with no workers. */
    void PumpQueuedEvents();
    size_t ShardFor(std::string_view key) const;

    struct ConsumerSlot {
        EventConsumer* consumer = nullptr;
        Subscription subscription;
        std::atomic<uint64_t> applied{0};
        std::atomic<uint64_t> command_enqueued{0};
        std::atomic<uint64_t> ignored{0};
        std::atomic<uint64_t> failed{0};
    };

    EventCenterConfig config_;
    /**
     * Registered before Start and never mutated after, so the delivery path
     * reads it without a lock. Held by unique_ptr because the slots carry
     * atomics and the vector grows during registration.
     */
    std::vector<std::unique_ptr<ConsumerSlot>> consumers_;
    /** Per event type, the indices into consumers_ that want it. */
    std::array<std::vector<size_t>,
               static_cast<size_t>(EventType::kWatermark) + 1>
        subscribers_;
    std::atomic<bool> started_{false};

    std::vector<std::unique_ptr<BoundedQueue<BlockEvent>>> event_queues_;

    std::atomic<bool> stopped_{false};
    std::atomic<uint64_t> events_enqueued_{0};
    std::atomic<uint64_t> events_broadcast_{0};
    std::atomic<uint64_t> consumer_deliveries_{0};
    std::atomic<uint64_t> inline_applied_{0};
    /** Events currently inside ProcessEvent; lets DrainForTest wait them out.
     */
    std::atomic<uint64_t> processing_{0};
    std::vector<std::thread> workers_;
};

}  // namespace mooncake::v2
