#pragma once

#ifndef MOONCAKE_KV_EVENT_SYSTEM_H
#define MOONCAKE_KV_EVENT_SYSTEM_H

#include "kv_event/kv_event.hpp"
#include "kv_event/kv_event_publisher_config.h"
#include "kv_event/kv_event_types.h"

#include <future>
#include <atomic>
#include <memory>

namespace mooncake {

class KVEventProducer;
class KVEventConsumer;

/**
 * @brief Event system facade class
 * Provides a simplified interface for using the event system's
 * publish/subscribe functionality Implements the Facade pattern to hide complex
 * interactions between producers and consumers
 */
class KVEventSystem {
   public:
    /**
     * @brief Constructor
     * @param config System configuration parameters
     * Initializes event queue, producer, and consumer, and starts the event
     * processing pipeline
     */
    explicit KVEventSystem(
        const KVEventPublisherConfig& config = KVEventPublisherConfig{});

    /**
     * @brief Destructor
     * Automatically calls shutdown() to ensure proper resource release
     */
    ~KVEventSystem();

    // Disable copy and move operations to ensure singleton-like behavior
    KVEventSystem(const KVEventSystem&) = delete;
    KVEventSystem& operator=(const KVEventSystem&) = delete;
    KVEventSystem(KVEventSystem&&) = delete;
    KVEventSystem& operator=(KVEventSystem&&) = delete;

    /**
     * @brief Stop the event system
     * Gracefully shuts down producer and consumer, ensuring all events are
     * processed
     */
    void shutdown();

    /**
     * @brief Check if the system is running
     * @return System running status
     */
    bool is_running() const noexcept {
        return running_.load(std::memory_order_acquire);
    };

    /**
     * @brief Generic event publishing interface
     * @tparam Event Event type, must derive from KVCacheEvent
     * @tparam Args Event constructor parameter types
     * @param args Arguments passed to the event constructor
     * @return Future containing the publication result (true for success, false
     * for failure)
     */
    template <DerivedFromKVCacheEvent Event, typename... Args>
    std::future<bool> publish(Args&&... args) {
        return producer_->publish<Event>(std::forward<Args>(args)...);
    };

    /**
     * @brief Queue statistics structure
     * Monitors event queue status for performance tuning and capacity planning
     */
    struct QueueStats {
        size_t queue_remain_events =
            0;                      // Number of pending events in the queue
        size_t queue_capacity = 0;  // Total queue capacity

        friend std::ostream& operator<<(std::ostream& os,
                                        const QueueStats& stats);
    };

    /**
     * @brief Complete system statistics
     * Aggregates statistics from producer, consumer, and queue
     */
    struct Stats {
        KVEventProducer::Stats producer_stats;
        KVEventConsumer::Stats consumer_stats;
        QueueStats event_queue_stats;

        double success_rate{0.0};

        bool has_data() const;
        void calculate_derived_metrics();

        friend std::ostream& operator<<(std::ostream& os, const Stats& stats);
    };

    /**
     * @brief Get complete system statistics
     * @return Stats object containing all statistical information
     * Note: Frequent calls may impact performance
     */
    Stats get_stats() const;

    /**
     * @brief Get producer statistics
     * @return Producer statistics
     */
    KVEventProducer::Stats get_producer_stats() const {
        return producer_->get_stats();
    };

    /**
     * @brief Get consumer statistics
     * @return Consumer statistics
     */
    KVEventConsumer::Stats get_consumer_stats() const {
        return consumer_->get_stats();
    };

    /**
     * @brief Get queue statistics
     * @return Queue statistics
     */
    QueueStats get_queue_stats() const;

    /**
     * @brief Get internal producer instance
     * @return Shared pointer to event producer
     * Used for advanced usage to directly access producer interface
     */
    std::shared_ptr<KVEventProducer> get_producer() const { return producer_; };

    /**
     * @brief Get internal consumer instance
     * @return Shared pointer to event consumer
     * Used for advanced usage to directly access consumer interface
     */
    std::shared_ptr<KVEventConsumer> get_publisher() const {
        return consumer_;
    };

   private:
    KVEventPublisherConfig config_;
    std::shared_ptr<KVEventQueue> event_queue_;
    std::shared_ptr<KVEventProducer> producer_;
    std::shared_ptr<KVEventConsumer> consumer_;
    std::atomic<bool> running_{false};
};

}  // namespace mooncake

#endif  // MOONCAKE_KV_EVENT_SYSTEM_H