#pragma once

#ifndef MOONCAKE_KV_EVENT_TYPES_H
#define MOONCAKE_KV_EVENT_TYPES_H

#include "kv_event/kv_event.hpp"
#include "thread_safe_queue.h"

#include <future>
#include <memory>
#include <optional>
#include <type_traits>

namespace mooncake {

/**
 * @brief Type trait concept for events derived from KVCacheEvent
 * Ensures compile-time type checking for event types used in templates
 */
template <typename Event>
concept DerivedFromKVCacheEvent = std::is_base_of_v<KVCacheEvent, Event>;

/**
 * @brief Type alias for KVCacheEvent shared pointer
 * Standardized pointer type for event objects throughout the system
 */
using KVEventPtr = std::shared_ptr<KVCacheEvent>;

/**
 * @brief Structure representing an item in the event queue
 * Combines an event with its associated promise for notification of
 * transmission completion The promise is fulfilled by the consumer when the
 * event is successfully published
 */
struct QueuedItem {
    KVEventPtr event;  // Shared pointer to the event
    std::shared_ptr<std::promise<bool>>
        promise;  // Promise to notify publishing result
};

/**
 * @brief Type alias for thread-safe event queue
 * Queue element is optional to support sentinel values for shutdown signaling
 * Uses ThreadSafeQueue implementation with configurable capacity
 */
using KVEventQueue = ThreadSafeQueue<std::optional<QueuedItem>>;

}  // namespace mooncake

#endif  // MOONCAKE_KV_EVENT_TYPES_H