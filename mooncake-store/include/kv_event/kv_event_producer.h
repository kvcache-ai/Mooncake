#pragma once

#ifndef MOONCAKE_KV_EVENT_PRODUCER_H
#define MOONCAKE_KV_EVENT_PRODUCER_H

#include "kv_event/kv_event.hpp"
#include "kv_event/kv_event_types.h"
#include "thread_pool.h"

#include <future>
#include <memory>

namespace mooncake {

class KVEventSystem;

/**
 * @brief Event producer
 * Responsible for creating events and asynchronously pushing them to the event
 * queue
 */
class KVEventProducer {
   public:
    /**
     * @brief Producer configuration structure
     */
    struct Config {
        size_t enqueue_thread_pool_size{
            1};  // Number of enqueue thread pool workers
        std::chrono::milliseconds enqueue_timeout{
            10};                         // Enqueue timeout duration
        size_t enqueue_max_retries{10};  // Maximum enqueue retry attempts
    };

    /**
     * @brief Producer statistics
     */
    struct Stats {
        size_t events_created{0};
        size_t enqueue_failed{0};
        double success_rate{0.0};

        size_t store_event{0};
        size_t update_event{0};
        size_t remove_all_event{0};

        bool has_data() const { return events_created > 0; };

        void calculate_derived_metrics();

        friend std::ostream& operator<<(std::ostream& os, const Stats& stats);
    };

    /**
     * @brief Constructor
     * @param event_queue Shared event queue
     * @param config Producer configuration
     * Initializes thread pool and configures the shared queue
     */
    explicit KVEventProducer(const std::shared_ptr<KVEventQueue> event_queue,
                             const Config& config);

    /**
     * @brief Destructor
     * Automatically calls shutdown() to ensure thread pool is properly closed
     */
    ~KVEventProducer();

    // Prohibit copy and move operations
    KVEventProducer(const KVEventProducer&) = delete;
    KVEventProducer& operator=(const KVEventProducer&) = delete;
    KVEventProducer(KVEventProducer&&) = delete;
    KVEventProducer& operator=(KVEventProducer&&) = delete;

    bool is_running() const noexcept {
        return running_.load(std::memory_order_acquire);
    };

    /**
     * @brief Increment counter based on event type
     * @tparam Event Event type
     */
    template <DerivedFromKVCacheEvent Event>
    void increment_event_count() {
        if constexpr (std::is_same_v<Event, BlockStoreEvent>) {
            store_event_.fetch_add(1, std::memory_order_relaxed);
        } else if constexpr (std::is_same_v<Event, BlockUpdateEvent>) {
            update_event_.fetch_add(1, std::memory_order_relaxed);
        } else if constexpr (std::is_same_v<Event, RemoveAllEvent>) {
            remove_all_event_.fetch_add(1, std::memory_order_relaxed);
        } else {
            static_assert(std::is_same_v<Event, void>,
                          "Unknown event type! Please add handling for this "
                          "event type in Stats::increment_event_count()");
        }
        events_created_.fetch_add(1, std::memory_order_relaxed);
    };

    /**
     * @brief Publish an event
     * @tparam Event Event type, must derive from KVCacheEvent
     * @tparam Args Event constructor parameter types
     * @param args Arguments passed to the event constructor
     * @return std::future<bool> Future containing the publication result
     * Creates and enqueues events asynchronously
     */
    template <DerivedFromKVCacheEvent Event, typename... Args>
    std::future<bool> publish(Args&&... args) {
        if (!is_running()) {
            auto promise = std::make_shared<std::promise<bool>>();
            promise->set_value(false);
            return promise->get_future();
        }

        auto event = std::make_shared<Event>(std::forward<Args>(args)...);
        increment_event_count<Event>();
        return publish_event_async(std::move(event));
    };

    /**
     * @brief Asynchronously publish a pre-created event object
     * @param event Shared pointer to event
     * @return std::future<bool> Future containing the publication result
     * Submits the event to the thread pool for enqueuing operation
     */
    std::future<bool> publish_event_async(KVEventPtr event);

    /**
     * @brief Get the event queue
     * @return Shared pointer to the event queue
     */
    std::shared_ptr<KVEventQueue> get_queue() const { return event_queue_; };

    /**
     * @brief Get statistics
     * @return Producer statistics
     */
    Stats get_stats() const;

    /**
     * @brief Stop the producer
     * Shuts down thread pool and stops accepting new events
     */
    void shutdown();

   private:
    Config config_;

    std::atomic<bool> running_{false};
    std::shared_ptr<KVEventQueue> event_queue_;
    std::unique_ptr<ThreadPool> enqueue_pool_;  // Enqueuing thread pool

    std::atomic<size_t> events_created_{0};
    std::atomic<size_t> enqueue_failed_{0};

    std::atomic<size_t> store_event_{0};
    std::atomic<size_t> update_event_{0};
    std::atomic<size_t> remove_all_event_{0};
};

}  // namespace mooncake

#endif  // MOONCAKE_KV_EVENT_PRODUCER_H