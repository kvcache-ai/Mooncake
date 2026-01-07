#pragma once

#ifndef MOONCAKE_KV_EVENT_PUBLISHER_CONFIG_H
#define MOONCAKE_KV_EVENT_PUBLISHER_CONFIG_H

#include <string>
#include <atomic>
#include <chrono>
#include <optional>

namespace mooncake {

struct KVEventPublisherConfig {
    // Network endpoint configuration
    std::string endpoint{"tcp://*:19997"};
    std::optional<std::string> replay_endpoint = std::nullopt;

    // Performance configuration
    size_t buffer_steps{10000};  // Replay buffer size
    int hwm{100000};             // ZeroMQ high-water mark
    size_t max_queue_size{100000};
    std::chrono::milliseconds send_interval{0};

    // Batching configuration
    size_t max_batch_size{50};  // Maximum event batch size
    std::chrono::milliseconds batch_timeout{200};
    std::chrono::milliseconds pop_timeout{100};

    // Thread pool configuration
    size_t enqueue_thread_pool_size{1};
    std::chrono::milliseconds enqueue_timeout{10};
    size_t enqueue_max_retries = 10;

    // Message configuration
    std::string topic{"mooncake"};

    // Auto-port switching configuration
    bool auto_port{true};  // Whether to enable auto-port switching
    size_t max_port_attempts{10};

    // Validate configuration validity
    bool validate() const noexcept;
};

}  // namespace mooncake

#endif  // MOONCAKE_KV_EVENT_PUBLISHER_CONFIG_H