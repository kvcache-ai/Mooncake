#include "p2p/client/v2/event_center.h"

#include <algorithm>
#include <utility>

#include <boost/functional/hash.hpp>
#include <glog/logging.h>

#include "utils.h"

namespace mooncake::v2 {

const char* ToString(EventType type) {
    switch (type) {
        case EventType::kAccess:
            return "access";
        case EventType::kCommit:
            return "commit";
        case EventType::kDelete:
            return "delete";
        case EventType::kAllocationFailure:
            return "allocation_failure";
        case EventType::kWatermark:
            return "watermark";
    }
    return "unknown";
}

const char* ToString(ConsumeResult result) {
    switch (result) {
        case ConsumeResult::kApplied:
            return "applied";
        case ConsumeResult::kCommandEnqueued:
            return "command_enqueued";
        case ConsumeResult::kIgnored:
            return "ignored";
        case ConsumeResult::kFailed:
            return "failed";
    }
    return "unknown";
}

uint32_t SubscriptionMask(std::initializer_list<EventType> types) {
    uint32_t mask = 0;
    for (EventType type : types) mask |= Subscription::MaskOf(type);
    return mask;
}

const char* ToString(QueuePushResult result) {
    switch (result) {
        case QueuePushResult::kEnqueued:
            return "enqueued";
        case QueuePushResult::kCoalesced:
            return "coalesced";
        case QueuePushResult::kDropped:
            return "dropped";
        case QueuePushResult::kClosed:
            return "closed";
        case QueuePushResult::kCancelled:
            return "cancelled";
    }
    return "unknown";
}

tl::expected<void, ErrorCode> ValidateEventCenterConfig(
    const EventCenterConfig& config) {
    if (config.shard_count == 0 || config.event_queue_capacity == 0) {
        LOG(ERROR) << "events.shard_count and events.event_queue_capacity "
                      "must both be greater than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

EventCenter::EventCenter(const EventCenterConfig& config) : config_(config) {
    const size_t shards = std::max<size_t>(1, config.shard_count);
    event_queues_.reserve(shards);
    for (size_t i = 0; i < shards; ++i) {
        // Access events on the same key and tier collapse: they are a
        // sampling signal, and keeping every one of them would let a read
        // burst evict the lifecycle events queued behind it.
        event_queues_.push_back(std::make_unique<BoundedQueue<BlockEvent>>(
            config.event_queue_capacity,
            [](BlockEvent& existing, const BlockEvent& incoming) {
                if (existing.type != EventType::kAccess ||
                    incoming.type != EventType::kAccess) {
                    return false;
                }
                return existing.key == incoming.key &&
                       existing.tiler_id == incoming.tiler_id;
            },
            config.lifecycle_reserve));
    }
}

EventCenter::~EventCenter() { Stop(EventCenterStopMode::kCancel); }

tl::expected<void, ErrorCode> EventCenter::RegisterConsumer(
    EventConsumer* consumer) {
    if (started_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "EventCenter consumers are frozen once it has started";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (consumer == nullptr) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    Subscription subscription = consumer->SubscriptionInfo();
    if (subscription.event_mask == 0) {
        LOG(ERROR) << "EventConsumer '" << subscription.name
                   << "' subscribes to nothing";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    for (const auto& slot : consumers_) {
        if (slot->subscription.name == subscription.name) {
            // Names are metric labels; two consumers sharing one would merge
            // their counters and make both unreadable.
            LOG(ERROR) << "Duplicate EventConsumer name '" << subscription.name
                       << "'";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    const size_t index = consumers_.size();
    auto slot = std::make_unique<ConsumerSlot>();
    slot->consumer = consumer;
    slot->subscription = subscription;
    consumers_.push_back(std::move(slot));

    // Indexed here rather than at Start(): a center with no workers is a
    // legitimate configuration -- a test drains it on the calling thread --
    // and building the index at Start would leave that mode with no
    // subscribers at all, silently delivering nothing.
    for (size_t type = 0; type < subscribers_.size(); ++type) {
        if (subscription.Wants(static_cast<EventType>(type))) {
            subscribers_[type].push_back(index);
        }
    }
    return {};
}

void EventCenter::Start() {
    // Freezing the set is what makes the per-type index safe to read without a
    // lock for the rest of the process: an event's fan-out target cannot
    // change while it is being delivered.
    started_.store(true, std::memory_order_release);
    workers_.reserve(event_queues_.size());
    for (size_t i = 0; i < event_queues_.size(); ++i) {
        workers_.emplace_back([this, i] {
            // One worker per shard, so a slow policy call on one shard cannot
            // stall the others. Exactly one consumer per shard, which is what
            // keeps two events for the same key in the order they were
            // published.
            for (;;) {
                auto event = event_queues_[i]->Pop();
                if (!event.has_value()) return;
                processing_.fetch_add(1, std::memory_order_acq_rel);
                ProcessEvent(*event, DeliveryMode::kQueued);
                processing_.fetch_sub(1, std::memory_order_acq_rel);
            }
        });
    }
}

size_t EventCenter::ShardFor(std::string_view key) const {
    return StringHash{}(key) % event_queues_.size();
}

void EventCenter::CountResult(size_t consumer_index, ConsumeResult result) {
    ConsumerSlot& slot = *consumers_[consumer_index];
    switch (result) {
        case ConsumeResult::kApplied:
            slot.applied.fetch_add(1, std::memory_order_relaxed);
            break;
        case ConsumeResult::kCommandEnqueued:
            slot.command_enqueued.fetch_add(1, std::memory_order_relaxed);
            break;
        case ConsumeResult::kIgnored:
            slot.ignored.fetch_add(1, std::memory_order_relaxed);
            break;
        case ConsumeResult::kFailed:
            // Never swallowed: a consumer that cannot apply a fact is a
            // correctness problem somewhere, and the counter is what an
            // operator alarms on.
            slot.failed.fetch_add(1, std::memory_order_relaxed);
            LOG(WARNING) << "EventConsumer '" << slot.subscription.name
                         << "' failed to consume an event";
            break;
    }
}

QueuePushResult EventCenter::Publish(BlockEvent event) {
    if (stopped_.load(std::memory_order_acquire)) {
        return QueuePushResult::kClosed;
    }
    const bool reliable =
        event.type == EventType::kCommit || event.type == EventType::kDelete;
    const size_t shard = ShardFor(event.key);

    // Copied before the push so the inline fallback still has the event: Push
    // moves its argument on the enqueued path.
    BlockEvent fallback;
    if (reliable) fallback = event;

    const QueuePushResult result = event_queues_[shard]->Push(
        std::move(event),
        reliable ? QueuePushMode::kReliable : QueuePushMode::kBestEffort);

    if (result == QueuePushResult::kEnqueued) {
        events_enqueued_.fetch_add(1, std::memory_order_relaxed);
        return result;
    }
    if (reliable && result == QueuePushResult::kDropped) {
        // A lifecycle event may not be lost. Applying it here keeps every
        // consumer's state correct at the cost of not producing commands for
        // it, which is strictly better than blocking the writer that published
        // it. The shard's lifecycle reserve is what makes this rare rather
        // than a consequence of a read burst.
        ProcessEvent(fallback, DeliveryMode::kInline);
        inline_applied_.fetch_add(1, std::memory_order_relaxed);
    }
    return result;
}

void EventCenter::ProcessEvent(const BlockEvent& event, DeliveryMode mode) {
    const size_t type = static_cast<size_t>(event.type);
    if (type >= subscribers_.size()) return;
    const std::vector<size_t>& targets = subscribers_[type];
    if (targets.empty()) return;

    // Every subscriber of this type, exactly once, in registration order. The
    // event outlives the loop, so it is released only after the last consumer
    // has returned -- which is what "delivered to all subscribers" has to mean
    // for a consumer that reads the event's fields.
    events_broadcast_.fetch_add(1, std::memory_order_relaxed);
    for (size_t index : targets) {
        const ConsumeResult result =
            consumers_[index]->consumer->Consume(event, mode);
        consumer_deliveries_.fetch_add(1, std::memory_order_relaxed);
        CountResult(index, result);
    }
}

EventPublisher EventCenter::Publisher() {
    return EventPublisher(
        std::static_pointer_cast<EventSink>(shared_from_this()));
}

void EventCenter::Stop(EventCenterStopMode mode) {
    if (stopped_.exchange(true, std::memory_order_acq_rel)) return;

    if (mode == EventCenterStopMode::kCancel) {
        for (auto& queue : event_queues_) queue->Clear();
    }
    // Closed first, then joined. Pop keeps handing out queued items after
    // Close and only reports nothing once the queue is empty, so a draining
    // stop finishes the backlog through the workers -- in order, and with the
    // single consumer per shard that ordering depends on. Draining alongside
    // them would race a Commit against the Delete that follows it.
    for (auto& queue : event_queues_) queue->Close();
    for (auto& worker : workers_) {
        if (worker.joinable()) worker.join();
    }
    workers_.clear();

    if (mode == EventCenterStopMode::kDrain) {
        // No workers were running (a test, or a very early Stop), so the
        // backlog is applied here. Safe now: the joins above guarantee this
        // thread is the only consumer.
        PumpQueuedEvents();
    }
}

bool EventCenter::IsStopped() const {
    return stopped_.load(std::memory_order_acquire);
}

void EventCenter::PumpQueuedEvents() {
    for (auto& queue : event_queues_) {
        for (;;) {
            auto event = queue->TryPop();
            if (!event.has_value()) break;
            ProcessEvent(*event, DeliveryMode::kQueued);
        }
    }
}

void EventCenter::DrainForTest() {
    if (workers_.empty()) {
        // Nothing else is consuming, so pumping here is both safe and the
        // only way the backlog gets applied.
        PumpQueuedEvents();
        return;
    }
    // Workers own the queues. Consuming alongside them would add a second
    // consumer per shard and let two events for one key be applied out of
    // order, so this waits for them instead.
    for (int attempt = 0; attempt < 2000; ++attempt) {
        bool idle = processing_.load(std::memory_order_acquire) == 0;
        for (const auto& queue : event_queues_) {
            if (queue->Size() != 0) idle = false;
        }
        if (idle) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    LOG(WARNING) << "EventCenter::DrainForTest gave up waiting for its workers";
}

EventCenterMetrics EventCenter::Metrics() const {
    EventCenterMetrics metrics;
    metrics.events_enqueued = events_enqueued_.load(std::memory_order_relaxed);
    metrics.events_broadcast =
        events_broadcast_.load(std::memory_order_relaxed);
    metrics.consumer_deliveries =
        consumer_deliveries_.load(std::memory_order_relaxed);
    metrics.lifecycle_event_inline_applied =
        inline_applied_.load(std::memory_order_relaxed);
    for (const auto& queue : event_queues_) {
        metrics.events_coalesced += queue->CoalescedCount();
        metrics.event_queue_depth += queue->Size();
        metrics.events_dropped += queue->DroppedCount();
    }
    metrics.consumers.reserve(consumers_.size());
    for (const auto& slot : consumers_) {
        ConsumerMetrics consumer;
        consumer.name = slot->subscription.name;
        consumer.applied = slot->applied.load(std::memory_order_relaxed);
        consumer.command_enqueued =
            slot->command_enqueued.load(std::memory_order_relaxed);
        consumer.ignored = slot->ignored.load(std::memory_order_relaxed);
        consumer.failed = slot->failed.load(std::memory_order_relaxed);
        metrics.consumers.push_back(std::move(consumer));
    }
    return metrics;
}

QueuePushResult EventPublisher::Publish(BlockEvent event) const {
    // Weak on purpose: a producer may still be running while the sink is being
    // torn down, and losing an event during shutdown is preferable to keeping
    // the sink alive from the producer side.
    auto sink = sink_.lock();
    if (!sink) return QueuePushResult::kClosed;
    return sink->Publish(std::move(event));
}

}  // namespace mooncake::v2
