#pragma once

#ifndef MOONCAKE_KV_EVENT_CONSUMER_H
#define MOONCAKE_KV_EVENT_CONSUMER_H

#include "kv_event/kv_event.hpp"
#include "kv_event/kv_event_types.h"

#include <zmq.hpp>
#include <zmq_addon.hpp>

#include <future>
#include <memory>
#include <optional>
#include <algorithm>

namespace mooncake {

class KVEventSystem;

/**
 * @brief ZeroMQ event consumer
 * Consumes events from the event queue and publishes them via ZeroMQ
 * Supports event batch processing and replay functionality
 */
class KVEventConsumer {
   public:
    /**
     * @brief Consumer configuration structure
     */
    struct Config {
        // Network endpoint configuration
        std::string endpoint{"tcp://*:19997"};
        std::optional<std::string> replay_endpoint = std::nullopt;

        // Performance configuration
        size_t buffer_steps{10000};  // Replay buffer size
        int hwm{100000};             // ZeroMQ high-water mark
        std::chrono::milliseconds send_interval{0};

        // Batching configuration
        size_t max_batch_size{50};  // Maximum event batch size
        std::chrono::milliseconds pop_timeout{100};

        // Message configuration
        std::string topic{"mooncake"};

        // Auto-port switching configuration
        bool auto_port{true};  // Whether to enable auto-port switching
        size_t max_port_attempts{10};
    };

    /**
     * @brief Consumer statistics
     */
    struct Stats {
        size_t total_events{0};
        size_t total_batches{0};

        size_t failed_events{0};
        size_t replay_requests{0};

        double success_rate{0.0};
        double events_per_batch{0.0};

        /**
         * @brief Check if any events have been processed
         * @return true if at least one event has been received, false otherwise
         */
        bool has_data() const { return total_events > 0; }

        void calculate_derived_metrics();

        friend std::ostream& operator<<(std::ostream& os, const Stats& stats);
    };

    /**
     * @brief Constructor
     * @param event_queue Shared event queue
     * @param config Consumer configuration
     * Initializes ZeroMQ context, publishing thread and configures the shared
     * queue
     */
    explicit KVEventConsumer(const std::shared_ptr<KVEventQueue> event_queue,
                             const Config& config);

    /**
     * @brief Destructor
     * Automatically calls shutdown() to ensure resource release
     */
    ~KVEventConsumer();

    // Prohibit copy and move operations
    KVEventConsumer(const KVEventConsumer&) = delete;
    KVEventConsumer& operator=(const KVEventConsumer&) = delete;
    KVEventConsumer(KVEventConsumer&&) = delete;
    KVEventConsumer& operator=(KVEventConsumer&&) = delete;

    /**
     * @brief Gracefully shutdown the consumer
     * 1. Stop accepting new events
     * 2. Stop publishing thread
     * 3. Close all network connections
     */
    void shutdown();

    /**
     * @brief Get current statistics
     * @return Stats object containing all performance metrics
     * Note: Frequent calls may impact performance
     */
    Stats get_stats() const;

    /**
     * @brief Check if consumer is running
     * @return true if consumer is active, false if shutdown
     */
    bool is_running() const noexcept {
        return running_.load(std::memory_order_acquire);
    };

   private:
    /**
     * @brief Replay buffer entry
     * Stores recently transmitted events with their sequence numbers to support
     * replay for late subscribers
     */
    struct ReplayEntry {
        uint64_t seq;              // Sequence number for ordering
        msgpack::sbuffer payload;  // Serialized event data

        ReplayEntry(uint64_t s, msgpack::sbuffer p)
            : seq(s), payload(std::move(p)) {}
    };

    /**
     * @brief Thread-specific resources
     * Contains all resources required by the publishing thread to ensure thread
     * safety
     */
    struct ThreadResources {
        std::unique_ptr<zmq::socket_t> pub_socket;
        std::unique_ptr<zmq::socket_t> replay_socket;
        std::deque<ReplayEntry> replay_buffer;
        uint64_t next_seq = 0;

        /**
         * @brief Constructor
         * @param ctx ZeroMQ context
         * @param config Consumer configuration
         */
        explicit ThreadResources(zmq::context_t& ctx, const Config& config);
    };

    /**
     * @brief Publishing thread main function
     * @param stop_token Stop token for cooperative thread cancellation
     * Continuous event processing workflow:
     * 1. Check and service replay requests
     * 2. Wait for configured send interval
     * 3. Batch events (up to max_batch_size)
     * 4. Serialize batches and send via ZeroMQ
     * 5. Update replay buffer
     * 6. Fulfill promises for successfully transmitted events
     */
    void publisher_thread(std::stop_token stop_token);

    /**
     * @brief Set up ZeroMQ sockets
     * @param resources Thread resources reference
     * @throws std::runtime_error if socket setup fails
     * Performs automatic port selection based on configuration
     */
    void setup_sockets(ThreadResources& resources);

    /**
     * @brief Service replay requests
     * @param resources Thread resources
     * Handles replay requests:
     * 1. Reads requested starting sequence number
     * 2. Sends all buffered events with equal or higher sequence numbers
     * 3. Sends an end marker
     * Processes only one request per call to avoid blocking the main publishing
     * loop
     */
    void service_replay(ThreadResources& resources);

    /**
     * @brief Handle errors during publishing operations
     * @param e Exception that was caught
     * @param context Description of where the error occurred
     * Logs error details and implements brief sleep to prevent tight error
     * loops
     */
    void handle_error(const std::exception& e, const std::string& context);

    // Graceful shutdown timeout in seconds
    static constexpr double SHUTDOWN_TIMEOUT = 2.0;

    // Magic sequence number indicating end of replay transmission
    static constexpr std::array<uint8_t, 8> END_SEQ = {0xFF, 0xFF, 0xFF, 0xFF,
                                                       0xFF, 0xFF, 0xFF, 0xFF};

    Config config_;
    zmq::context_t context_;

    std::shared_ptr<KVEventQueue> event_queue_;
    std::jthread publisher_thread_;  // Publishing thread
    std::atomic<bool> running_{false};

    std::atomic<size_t> total_events_{0};
    std::atomic<size_t> total_batches_{0};

    std::atomic<size_t> failed_events_{0};
    std::atomic<size_t> replay_requests_{0};
};  // class KVEventConsumer

}  // namespace mooncake

#endif  // MOONCAKE_KV_EVENT_CONSUMER_H