#pragma once

#ifndef MOONCAKE_KV_EVENT_ZMQ_PUBLISHER_H
#define MOONCAKE_KV_EVENT_ZMQ_PUBLISHER_H

#include "kv_event/kv_event.hpp"
#include "kv_event/kv_event_publisher_config.h"
#include "thread_safe_queue.h"
#include "thread_pool.h"

#include <zmq.hpp>
#include <zmq_addon.hpp>

#include <future>
#include <atomic>
#include <memory>
#include <optional>
#include <iostream>
#include <algorithm>

namespace mooncake {

/**
 * @brief ZeroMQ-based asynchronous event publisher with batch processing
 *
 * This class implements a high-performance event publishing system using ZeroMQ
 * for message distribution. It provides asynchronous publishing with automatic
 * batching, configurable queue management, and optional event replay
 * capabilities. The publisher operates with separate threads for event
 * enqueuing and message transmission to maximize throughput and minimize
 * blocking in producer code.
 */
class ZmqEventPublisher {
   public:
    /**
     * @brief Statistical metrics for monitoring publisher performance
     *
     * Contains counters for queue status, event processing statistics,
     * and derived performance metrics. All metrics are calculated on-demand
     * and can be safely accessed while the publisher is running.
     */
    struct Stats {
        size_t queue_remain_events = 0;
        size_t queue_capacity = 0;
        size_t total_events = 0;
        size_t total_batches = 0;
        size_t failed_events = 0;
        size_t store_event = 0;
        size_t update_event = 0;
        size_t remove_all_event = 0;
        size_t replay_requests = 0;
        double events_per_batch = 0.0;
        double success_rate = 0.0;

        /**
         * @brief Check if any events have been processed
         * @return true if at least one event has been received, false otherwise
         */
        bool has_data() const { return total_events > 0; }

        /**
         * @brief Calculate derived performance metrics from raw counters
         *
         * Computes events_per_batch and success_rate based on the current
         * values of total_events, total_batches, and failed_events.
         * Should be called before accessing the derived metrics.
         */
        void calculate_derived_metrics();

        /**
         * @brief Output formatted statistics to an output stream
         * @param os Output stream to write formatted statistics
         * @param stats Statistics object to format
         * @return Reference to the output stream
         */
        friend std::ostream& operator<<(std::ostream& os, const Stats& stats);
    };

    /**
     * @brief Construct a new ZeroMQ event publisher
     * @param config Configuration parameters for publisher behavior
     * @throws std::runtime_error if configuration validation fails
     *
     * Initializes the publisher with the provided configuration, validates
     * all parameters, and starts the internal publishing thread. The
     * constructor performs the following setup:
     * 1. Validates configuration parameters
     * 2. Creates ZeroMQ context and sockets
     * 3. Initializes the thread-safe event queue
     * 4. Starts the publishing thread for message transmission
     * 5. Sets up the thread pool for asynchronous event enqueuing
     */
    explicit ZmqEventPublisher(
        const KVEventPublisherConfig& config = KVEventPublisherConfig{});

    /**
     * @brief Destroy the publisher and release all resources
     *
     * If the publisher is still running, automatically calls shutdown()
     * to gracefully terminate all threads and clean up resources.
     */
    ~ZmqEventPublisher();

    // Prohibit copy and move operations
    ZmqEventPublisher(const ZmqEventPublisher&) = delete;
    ZmqEventPublisher& operator=(const ZmqEventPublisher&) = delete;
    ZmqEventPublisher(ZmqEventPublisher&&) = delete;
    ZmqEventPublisher& operator=(ZmqEventPublisher&&) = delete;

    /**
     * @brief Asynchronously publish a block storage event
     * @param mooncake_key Key identifier for the stored block
     * @param replicas Vector of replica descriptors for block placement
     * @param store_event_info Metadata about the storage operation
     * @return std::future<bool> representing the operation's completion status
     *
     * Creates a BlockStoreEvent with the provided parameters and submits it
     * for asynchronous publishing. The returned future can be used to wait
     * for transmission completion and check success/failure status.
     */
    std::future<bool> publish_block_store(
        const std::string& mooncake_key,
        const std::vector<Replica::Descriptor>& replicas,
        const StoreEventInfo& store_event_info) {
        store_event_.fetch_add(1, std::memory_order_relaxed);
        return publish_event_impl<BlockStoreEvent>(mooncake_key, replicas,
                                                   store_event_info);
    }

    /**
     * @brief Asynchronously publish a block update event
     * @param mooncake_key Key identifier for the updated block
     * @param replicas Vector of replica descriptors for the update
     * @return std::future<bool> representing the operation's completion status
     *
     * Creates a BlockUpdateEvent with the provided parameters and submits it
     * for asynchronous publishing. Used to notify subscribers about block
     * metadata changes or content modifications.
     */
    std::future<bool> publish_block_update(
        const std::string& mooncake_key,
        const std::vector<Replica::Descriptor>& replicas) {
        update_event_.fetch_add(1, std::memory_order_relaxed);
        return publish_event_impl<BlockUpdateEvent>(mooncake_key, replicas);
    }

    /**
     * @brief Asynchronously publish a remove all event
     * @return std::future<bool> representing the operation's completion status
     *
     * Creates a RemoveAllEvent and submits it for asynchronous publishing.
     * This event type signals subscribers to clear all cached data,
     * typically used during system resets or cache invalidation scenarios.
     */
    std::future<bool> publish_remove_all() {
        remove_all_event_.fetch_add(1, std::memory_order_relaxed);
        return publish_event_impl<RemoveAllEvent>();
    }

    /**
     * @brief Generic interface for asynchronous event publishing
     * @param event Pre-constructed event object to publish
     * @return std::future<bool> representing the operation's completion status
     *
     * Accepts any KVCacheEvent-derived object for publishing. This method
     * provides flexibility for custom event types beyond the standard
     * block operations. The event is submitted to the internal queue
     * and transmitted as part of the next batch.
     */
    std::future<bool> publish_event_async(std::shared_ptr<KVCacheEvent> event);

    /**
     * @brief Gracefully shutdown the publisher
     *
     * Initiates a controlled shutdown sequence that:
     * 1. Stops accepting new events
     * 2. Signals the publishing thread to exit
     * 3. Shuts down the thread pool
     * 4. Closes all network connections
     * 5. Logs final statistics
     *
     * Any events remaining in the queue will be processed before shutdown
     * completes. The method is idempotent and can be safely called multiple
     * times.
     */
    void shutdown();

    /**
     * @brief Get current publisher statistics
     * @return Stats object containing all performance metrics
     *
     * Collects real-time statistics about queue status, event counts,
     * success rates, and replay requests. Note that accessing queue size
     * requires acquiring a lock, so frequent calls may impact performance
     * in high-throughput scenarios.
     */
    Stats get_stats() const;

    /**
     * @brief Check if the publisher is currently running
     * @return true if publisher is active and accepting events,
     *         false if shutdown has been initiated or completed
     */
    bool is_running() const noexcept { return running_; }

   private:
    /**
     * @brief Internal container for queued events
     *
     * Combines an event with its associated promise for notifying
     * the caller when transmission completes. Stored in the thread-safe
     * queue until processed by the publishing thread.
     */
    struct QueuedItem {
        std::shared_ptr<KVCacheEvent> event;
        std::shared_ptr<std::promise<bool>> promise;
    };

    /**
     * @brief Entry in the replay buffer for event recovery
     *
     * Stores recently transmitted events with their sequence numbers
     * to support replay requests from subscribers that join late or
     * need to recover from network interruptions.
     */
    struct ReplayEntry {
        uint64_t seq;
        msgpack::sbuffer payload;

        ReplayEntry(uint64_t s, msgpack::sbuffer p)
            : seq(s), payload(std::move(p)) {}
    };

    /**
     * @brief Resources exclusive to the publishing thread
     *
     * Contains all thread-local resources including ZeroMQ sockets,
     * replay buffer, and sequence counter. This structure ensures
     * proper resource lifetime management and thread safety by
     * keeping thread-specific resources isolated.
     */
    struct ThreadResources {
        std::unique_ptr<zmq::socket_t> pub_socket;
        std::unique_ptr<zmq::socket_t> replay_socket;
        std::deque<ReplayEntry> replay_buffer;
        uint64_t next_seq = 0;

        /**
         * @brief Construct thread resources with configuration
         * @param ctx ZeroMQ context for socket creation
         * @param config Publisher configuration for socket options
         *
         * Initializes PUB socket for event broadcasting and optionally
         * a ROUTER socket for replay requests based on configuration.
         */
        explicit ThreadResources(zmq::context_t& ctx,
                                 const KVEventPublisherConfig& config);
    };

    /**
     * @brief Template method for creating and publishing specific event types
     * @tparam Event Event type derived from KVCacheEvent
     * @tparam Args Argument types for event constructor
     * @param args Arguments forwarded to the event constructor
     * @return std::future<bool> for tracking publication completion
     *
     * Template implementation that creates events of the specified type
     * and forwards them to the asynchronous publishing pipeline. Provides
     * compile-time type safety while maintaining a uniform interface
     * for all event types.
     */
    template <DerivedFromKVCacheEvent Event, typename... Args>
    std::future<bool> publish_event_impl(Args&&... args) {
        if (!running_) {
            auto promise = std::make_shared<std::promise<bool>>();
            promise->set_value(false);
            return promise->get_future();
        }

        auto event = std::make_shared<Event>(std::forward<Args>(args)...);
        return publish_event_async(std::move(event));
    }

    /**
     * @brief Process event enqueuing with retry logic
     * @param item Event and promise pair to enqueue
     *
     * Implements retry logic for adding events to the thread-safe queue
     * when it's full. Respects the maximum retry count and timeout
     * configuration, eventually failing the promise if all retries are
     * exhausted. This method runs in the enqueuing thread pool.
     */
    void process_enqueue_task_with_retry(QueuedItem item);

    /**
     * @brief Main publishing thread function
     * @param stop_token Stop token for cooperative thread cancellation
     *
     * Continuously processes events from the queue with the following workflow:
     * 1. Checks for replay requests and services them
     * 2. Waits for the configured send interval (if any)
     * 3. Batches available events up to max_batch_size
     * 4. Serializes batches and transmits via ZeroMQ
     * 5. Updates replay buffer with sent events
     * 6. Fulfills promises for successfully transmitted events
     *
     * The thread runs until stop is requested or the publisher is shutdown.
     */
    void publisher_thread(std::stop_token stop_token);

    /**
     * @brief Set up ZeroMQ sockets for publishing and replay
     * @param resources Thread-specific resources to initialize
     * @throws std::runtime_error if socket setup fails
     *
     * Configures and binds/connects the PUB and optional ROUTER sockets
     * based on endpoint configuration. Implements automatic port selection
     * for TCP endpoints when the configured port is unavailable.
     */
    void setup_sockets(ThreadResources& resources);

    /**
     * @brief Service replay requests from subscribers
     * @param resources Thread resources containing the replay socket
     *
     * Handles incoming replay requests on the ROUTER socket by:
     * 1. Reading the requested starting sequence number
     * 2. Sending all buffered events with equal or higher sequence numbers
     * 3. Sending an end marker to indicate completion
     *
     * Only processes one request per call to avoid blocking the main
     * publishing loop for extended periods.
     */
    void service_replay(ThreadResources& resources);

    /**
     * @brief Handle errors during publishing operations
     * @param e Exception that was caught
     * @param context Description of where the error occurred
     *
     * Logs error details and implements a brief sleep to prevent
     * tight error loops while maintaining responsiveness. This method
     * centralizes error handling logic for consistent behavior across
     * all publishing operations.
     */
    void handle_error(const std::exception& e, const std::string& context);

    /// Timeout for graceful shutdown in seconds
    static constexpr double SHUTDOWN_TIMEOUT = 2.0;

    /// Magic sequence number indicating end of replay transmission
    static constexpr std::array<uint8_t, 8> END_SEQ = {0xFF, 0xFF, 0xFF, 0xFF,
                                                       0xFF, 0xFF, 0xFF, 0xFF};

    KVEventPublisherConfig config_;
    zmq::context_t context_;

    using EventQueue = ThreadSafeQueue<std::optional<QueuedItem>>;
    std::unique_ptr<EventQueue> event_queue_;
    std::unique_ptr<ThreadPool> enqueue_pool_;
    std::jthread publisher_thread_;
    std::atomic<bool> running_{false};

    std::atomic<size_t> total_events_{0};
    std::atomic<size_t> total_batches_{0};
    std::atomic<size_t> failed_events_{0};
    std::atomic<size_t> store_event_{0};
    std::atomic<size_t> update_event_{0};
    std::atomic<size_t> remove_all_event_{0};
    std::atomic<size_t> replay_requests_{0};
};  // class ZmqEventPublisher

/**
 * @brief Factory for creating event publisher instances
 *
 * Provides a registry-based factory pattern for creating different types
 * of event publishers. Currently supports only "zmq" publisher type but
 * designed for extensibility with additional publisher implementations.
 */
class EventPublisherFactory {
   public:
    /// Function type for publisher constructors
    using PublisherConstructor =
        std::function<std::unique_ptr<ZmqEventPublisher>(
            const KVEventPublisherConfig&)>;

    /**
     * @brief Register a new publisher type in the factory registry
     * @param name Unique identifier for the publisher type
     * @param constructor Factory function that creates the publisher
     * @throws std::invalid_argument if a publisher with the same name
     *         is already registered
     *
     * Enables extending the system with new publisher implementations
     * without modifying core code. Registration is thread-unsafe and
     * should typically be done during application initialization.
     */
    static void register_publisher(const std::string& name,
                                   PublisherConstructor constructor);

    /**
     * @brief Create a publisher instance of the specified type
     * @param publisher_type Type identifier of publisher to create
     * @param config Configuration parameters for the publisher
     * @return Unique pointer to the created publisher instance
     * @throws std::invalid_argument if publisher_type is not registered
     *
     * Looks up the constructor for the specified publisher type and
     * uses it to create a new instance. The default type is "zmq".
     */
    static std::unique_ptr<ZmqEventPublisher> create(
        const std::string& publisher_type = "zmq",
        const KVEventPublisherConfig& config = KVEventPublisherConfig{});

    /**
     * @brief Create a ZeroMQ publisher with default type
     * @param config Configuration parameters for the publisher
     * @return Unique pointer to a ZeroMQ event publisher
     *
     * Convenience overload that creates a ZeroMQ publisher without
     * requiring explicit type specification.
     */
    static std::unique_ptr<ZmqEventPublisher> create(
        const KVEventPublisherConfig& config) {
        return create("zmq", config);
    }

   private:
    /**
     * @brief Get the publisher registry
     * @return Reference to the static publisher registry map
     *
     * The registry is a static map that persists for the lifetime of
     * the program. It is lazily initialized on first access.
     */
    static std::unordered_map<std::string, PublisherConstructor>& registry();

    // Factory is not instantiable
    EventPublisherFactory() = delete;
    ~EventPublisherFactory() = delete;
};  // class EventPublisherFactory

}  // namespace mooncake

#endif  // MOONCAKE_KV_EVENT_ZMQ_PUBLISHER_H