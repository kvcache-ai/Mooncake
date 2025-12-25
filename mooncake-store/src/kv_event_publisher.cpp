#include "kv_event_publisher.h"

#include <zmq_addon.hpp>
#include <glog/logging.h>

#include <regex>
#include <random>
#include <chrono>
#include <algorithm>
#include <iostream>

namespace mooncake {
namespace {

enum class EndpointType { TCP, IPC, INPROC, UNKNOWN };

struct EndpointInfo {
    EndpointType type = EndpointType::UNKNOWN;
    std::string protocol;
    std::string host;
    int port = 0;
    std::string path;

    static EndpointInfo parse(const std::string& endpoint) {
        EndpointInfo info;

        if (endpoint.find("tcp://") == 0) {
            info.type = EndpointType::TCP;
            info.protocol = "tcp";

            std::string rest = endpoint.substr(6);
            size_t colon_pos = rest.find_last_of(':');

            if (colon_pos != std::string::npos) {
                info.host = rest.substr(0, colon_pos);
                std::string port_str = rest.substr(colon_pos + 1);
                try {
                    info.port = std::stoi(port_str);
                } catch (...) {
                    info.port = 0;
                }
            }
        } else if (endpoint.find("ipc://") == 0) {
            info.type = EndpointType::IPC;
            info.protocol = "ipc";
            info.path = endpoint.substr(6);
        } else if (endpoint.find("inproc://") == 0) {
            info.type = EndpointType::INPROC;
            info.protocol = "inproc";
            info.path = endpoint.substr(9);
        } else {
            info.type = EndpointType::UNKNOWN;
        }

        return info;
    }

    std::string to_string() const {
        switch (type) {
            case EndpointType::TCP:
                return protocol + "://" + host + ":" + std::to_string(port);
            case EndpointType::IPC:
                return protocol + "://" + path;
            case EndpointType::INPROC:
                return protocol + "://" + path;
            default:
                return "";
        }
    }

    std::string to_string_with_port(int new_port) const {
        if (type != EndpointType::TCP) {
            return to_string();
        }
        return protocol + "://" + host + ":" + std::to_string(new_port);
    }
};

std::pair<bool, std::string> smart_bind(zmq::socket_t& socket,
                                        const std::string& endpoint,
                                        const KVEventConsumer::Config& config) {
    EndpointInfo info = EndpointInfo::parse(endpoint);

    if (info.type == EndpointType::UNKNOWN) {
        LOG(ERROR) << "Unknown endpoint type: " << endpoint;
        return {false, endpoint};
    }

    try {
        socket.bind(endpoint);
        LOG(INFO) << "Bound to: " << endpoint;
        return {true, endpoint};
    } catch (const zmq::error_t& e) {
        if (e.num() != EADDRINUSE) {
            LOG(ERROR) << "Failed to bind to " << endpoint << ": " << e.what();
            return {false, endpoint};
        }

        if (info.type != EndpointType::TCP || !config.auto_port) {
            LOG(ERROR) << "Port in use and auto_port not enabled: " << endpoint;
            return {false, endpoint};
        }

        LOG(WARNING) << "Port {" << info.port
                     << "} was in use, trying random ports";
    }

    static constexpr std::array<int, 4> PORT_ERRORS = {
        EADDRINUSE,     // Address already in use
        EACCES,         // Permission denied
        EADDRNOTAVAIL,  // Address not available
        EINVAL          // Invalid argument
    };

    std::random_device rd;
    std::mt19937_64 rng(rd());
    std::uniform_int_distribution<int> dist(1024, 65535);

    std::string random_endpoint;
    random_endpoint.reserve(endpoint.size() + 10);

    for (size_t attempts = 0; attempts < config.max_port_attempts; ++attempts) {
        int random_port = dist(rng);
        random_endpoint = info.to_string_with_port(random_port);

        try {
            socket.bind(random_endpoint);

            LOG(WARNING) << "WARNING: Failed to bind to in-use port {"
                         << info.port << "}"
                         << ", successfully switched to port {" << random_port
                         << "} (attempt " << (attempts + 1) << ")";

            return {true, random_endpoint};

        } catch (const zmq::error_t& e) {
            bool is_port_unavailable = false;
            for (int error_code : PORT_ERRORS) {
                if (e.num() == error_code) {
                    is_port_unavailable = true;
                    break;
                }
            }

            if (!is_port_unavailable) {
                LOG(ERROR) << "Failed to bind to " << random_endpoint << ": "
                           << e.what();
                return {false, endpoint};
            }
        }
    }

    LOG(ERROR) << "Failed to find an available port after "
               << config.max_port_attempts << " random attempts";
    return {false, endpoint};
}

}  // namespace

// KVEventPublisherConfig Implementation
bool KVEventPublisherConfig::validate() const noexcept {
    if (endpoint.empty() || topic.empty()) {
        LOG(ERROR) << "Endpoint and topic cannot be empty";
        return false;
    }

    auto is_valid_endpoint = [](const std::string& ep) -> bool {
        if (ep.find("://") == std::string::npos) {
            LOG(ERROR) << "Endpoint missing protocol: " << ep;
            return false;
        }

        if (ep.find("tcp://") != 0 && ep.find("ipc://") != 0 &&
            ep.find("inproc://") != 0) {
            LOG(ERROR) << "Unsupported protocol in endpoint: " << ep;
            return false;
        }

        return true;
    };

    if (!is_valid_endpoint(endpoint)) {
        return false;
    }

    if (replay_endpoint.has_value()) {
        if (!is_valid_endpoint(*replay_endpoint)) {
            return false;
        }
    }

    if (max_queue_size == 0 || max_queue_size > 10000000) {
        LOG(ERROR) << "max_queue_size out of range: " << max_queue_size;
        return false;
    }

    if (max_batch_size == 0 || max_batch_size > 100) {
        LOG(ERROR) << "max_batch_size out of range: " << max_batch_size;
        return false;
    }

    if (hwm < 0) {
        LOG(ERROR) << "hwm cannot be negative: " << hwm;
        return false;
    }

    if (buffer_steps == 0 || buffer_steps > 1000000) {
        LOG(ERROR) << "buffer_steps out of range: " << buffer_steps;
        return false;
    }

    if (max_port_attempts == 0 || max_port_attempts > 1000) {
        LOG(ERROR) << "max_port_attempts out of range: " << max_port_attempts;
        return false;
    }

    if (enqueue_max_retries == 0 || enqueue_max_retries > 1000) {
        LOG(ERROR) << "enqueue_max_retries out of range: "
                   << enqueue_max_retries;
        return false;
    }

    if (batch_timeout.count() < 0 || pop_timeout.count() < 0 ||
        enqueue_timeout.count() < 0) {
        LOG(ERROR) << "Timeout values cannot be negative";
        return false;
    }

    if (topic.size() > 255) {
        LOG(ERROR) << "Topic too long: " << topic.size();
        return false;
    }

    return true;
}

// KVEventSystem Implementation
KVEventSystem::KVEventSystem(const KVEventPublisherConfig& config)
    : config_(config) {
    if (!config_.validate()) {
        throw std::runtime_error("Invalid KV Event System configuration");
    }

    if (config_.max_batch_size > 100) {
        config_.max_batch_size = 100;
        LOG(WARNING)
            << "KV Event System Config"
            << " max_batch_size cannot be negative or greater than 100;\n"
            << "KV Event System Config max_batch_size has been reset to: "
            << config_.max_batch_size;
    }

    if (config_.send_interval.count() < 0) {
        config_.send_interval = std::chrono::milliseconds(0);
        ;
        LOG(WARNING) << "KV Event System Config"
                     << " send_interval has been reset to: "
                     << config_.send_interval.count();
    }

    event_queue_ = std::make_shared<KVEventQueue>(config_.max_queue_size);

    KVEventProducer::Config producer_config{
        .enqueue_thread_pool_size = config_.enqueue_thread_pool_size,
        .enqueue_timeout = config_.enqueue_timeout,
        .enqueue_max_retries = config_.enqueue_max_retries};

    KVEventConsumer::Config consumer_config{
        .endpoint = config_.endpoint,
        .replay_endpoint = config_.replay_endpoint,
        .buffer_steps = config_.buffer_steps,
        .hwm = config_.hwm,
        .send_interval = config_.send_interval,
        .max_batch_size = config_.max_batch_size,
        .pop_timeout = config_.pop_timeout,
        .topic = config_.topic,
        .auto_port = config_.auto_port,
        .max_port_attempts = config_.max_port_attempts,
    };

    producer_ =
        std::make_shared<KVEventProducer>(event_queue_, producer_config);
    consumer_ =
        std::make_shared<KVEventConsumer>(event_queue_, consumer_config);

    running_.store(true, std::memory_order_release);

    LOG(INFO) << "KV Event Publish System started.";
    LOG(INFO) << "  Endpoint" << (config_.auto_port ? "(mutable)" : "") << ": "
              << config_.endpoint;
    LOG(INFO) << "  Max batch size: " << config_.max_batch_size;
    LOG(INFO) << "  Send interval: " << config_.send_interval.count() << "ms";
    LOG(INFO) << "  " << get_stats();
}

KVEventSystem::~KVEventSystem() {
    if (!is_running()) {
        return;
    }
    shutdown();
}

void KVEventSystem::shutdown() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false,
                                          std::memory_order_release,
                                          std::memory_order_relaxed)) {
        return;
    }

    LOG(INFO) << "Shutting down KV Event System...";

    producer_->shutdown();
    event_queue_->shutdown();
    consumer_->shutdown();

    LOG(INFO) << "KV Event System shutdown complete: " << get_stats();
}

bool KVEventSystem::Stats::has_data() const {
    return producer_stats.has_data();
}

void KVEventSystem::Stats::calculate_derived_metrics() {
    auto succeed_events =
        consumer_stats.total_events - consumer_stats.failed_events;
    auto total_events = producer_stats.events_created;

    success_rate =
        total_events > 0 ? succeed_events * 100.0 / total_events : 0.0;
}

KVEventSystem::Stats KVEventSystem::get_stats() const {
    Stats stats{.producer_stats = get_producer_stats(),
                .consumer_stats = get_consumer_stats(),
                .event_queue_stats = get_queue_stats()};
    stats.calculate_derived_metrics();
    return stats;
}

KVEventSystem::QueueStats KVEventSystem::get_queue_stats() const {
    return QueueStats{
        .queue_remain_events =
            event_queue_->size(),  // Expensive operation, avoid frequent calls
        .queue_capacity = event_queue_->capacity()};
}

std::ostream& operator<<(std::ostream& os,
                         const KVEventSystem::QueueStats& stats) {
    os << "Queue(pending/cap): " << stats.queue_remain_events << "/"
       << stats.queue_capacity;
    return os;
}

std::ostream& operator<<(std::ostream& os, const KVEventSystem::Stats& stats) {
    os << "KV Event System: Succ=";
    if (stats.has_data()) {
        os << std::fixed << std::setprecision(1) << stats.success_rate << "%";
    } else {
        os << "--/--";
    }

    os << " | " << stats.event_queue_stats << " | " << stats.producer_stats
       << " | " << stats.consumer_stats;
    return os;
}

// KVEventProducer Implementation
KVEventProducer::KVEventProducer(
    const std::shared_ptr<KVEventQueue> event_queue,
    const Config& config = Config{})
    : config_(config), event_queue_(event_queue) {
    enqueue_pool_ =
        std::make_unique<ThreadPool>(config_.enqueue_thread_pool_size);
    running_.store(true, std::memory_order_release);
}

KVEventProducer::~KVEventProducer() {
    if (is_running()) {
        shutdown();
    }
}

void KVEventProducer::shutdown() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false,
                                          std::memory_order_release,
                                          std::memory_order_relaxed)) {
        return;
    }

    event_queue_->push(std::nullopt, std::chrono::milliseconds(100));

    if (enqueue_pool_) {
        LOG(INFO) << "Stopping enqueue thread pool...";
        enqueue_pool_->stop();
    }
}

KVEventProducer::Stats KVEventProducer::get_stats() const {
    Stats stats{
        .events_created = events_created_.load(std::memory_order_relaxed),
        .enqueue_failed = enqueue_failed_.load(std::memory_order_relaxed),
        .store_event = store_event_.load(std::memory_order_relaxed),
        .update_event = update_event_.load(std::memory_order_relaxed),
        .remove_all_event = remove_all_event_.load(std::memory_order_relaxed),
    };
    stats.calculate_derived_metrics();
    return stats;
}

void KVEventProducer::Stats::calculate_derived_metrics() {
    success_rate =
        has_data() ? (events_created - enqueue_failed) * 100.0 / events_created
                   : 0.0;
}

std::ostream& operator<<(std::ostream& os,
                         const KVEventProducer::Stats& stats) {
    os << "SuccEqueue: ";

    if (stats.has_data()) {
        os << std::fixed << std::setprecision(1) << stats.success_rate << "%";
    } else {
        os << "--/--";
    }

    os << "(Total=" << stats.events_created << ", Fail=" << stats.enqueue_failed
       << ")";

    os << " | Evt Types: Store=" << stats.store_event
       << ", Update=" << stats.update_event
       << ", RemoveAll=" << stats.remove_all_event;

    return os;
}

std::future<bool> KVEventProducer::publish_event_async(
    std::shared_ptr<KVCacheEvent> event) {
    auto promise = std::make_shared<std::promise<bool>>();
    auto future = promise->get_future();

    QueuedItem item{std::move(event), promise};

    auto task = [this, item = std::move(item), promise]() mutable {
        if (!this->is_running()) {
            promise->set_value(false);
            enqueue_failed_.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        // Retry logic
        for (size_t attempt = 0; attempt < config_.enqueue_max_retries;
             ++attempt) {
            if (!this->is_running()) {
                break;
            }

            // Attempt fast enqueue
            if (event_queue_->try_push(item)) {
                return;  // Success, promise is set by the consumer
            }

            // Attempt enqueue with timeout
            if (event_queue_->push(item, config_.enqueue_timeout)) {
                return;  // Success, promise is set by the consumer
            }

            // Retry interval
            if (attempt < config_.enqueue_max_retries - 1) {
                std::this_thread::sleep_for(std::chrono::microseconds(5));
            }
        }

        LOG(ERROR) << "Failed to enqueue event after "
                   << config_.enqueue_max_retries << " retries";
        promise->set_value(false);
        enqueue_failed_.fetch_add(1, std::memory_order_relaxed);
    };

    try {
        enqueue_pool_->enqueue(std::move(task));
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to enqueue task: " << e.what();
        promise->set_value(false);
        enqueue_failed_.fetch_add(1, std::memory_order_relaxed);
    } catch (...) {
        LOG(ERROR) << "Failed to enqueue task: unknown exception";
        promise->set_exception(std::current_exception());
        enqueue_failed_.fetch_add(1, std::memory_order_relaxed);
    }

    return future;
}

// KVEventConsumer Implementation
KVEventConsumer::KVEventConsumer(
    const std::shared_ptr<KVEventQueue> event_queue,
    const Config& config = Config{})
    : config_(config), event_queue_(event_queue) {
    context_ = zmq::context_t(1);

    publisher_thread_ = std::jthread([this](std::stop_token token) {
        this->publisher_thread(std::move(token));
    });

    running_.store(true, std::memory_order_release);
}

KVEventConsumer::~KVEventConsumer() {
    if (is_running()) {
        shutdown();
    }
}

// Shutdown
void KVEventConsumer::shutdown() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false,
                                          std::memory_order_release,
                                          std::memory_order_relaxed)) {
        return;
    }

    // Wait for consumer thread to finish
    if (publisher_thread_.joinable()) {
        publisher_thread_.request_stop();
        publisher_thread_.join();
    }
}

// Publisher thread
void KVEventConsumer::publisher_thread(std::stop_token stop_token) {
    ThreadResources resources(context_, config_);
    setup_sockets(resources);

    auto last_send_time = std::chrono::steady_clock::now();

    while (is_running() && !stop_token.stop_requested()) {
        try {
            // Check replay requests
            if (resources.replay_socket) {
                zmq::pollitem_t items[] = {
                    {*resources.replay_socket, 0, ZMQ_POLLIN, 0}};
                if (zmq::poll(items, 1, std::chrono::milliseconds(0)) > 0) {
                    service_replay(resources);
                }
            }

            if (config_.send_interval.count() > 0) {
                auto now = std::chrono::steady_clock::now();
                auto time_since_last_send =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - last_send_time);

                if (time_since_last_send < config_.send_interval) {
                    // Wait until next send window
                    auto wait_time =
                        config_.send_interval - time_since_last_send;

                    // Handle replay requests while waiting
                    if (resources.replay_socket) {
                        zmq::pollitem_t items[] = {
                            {*resources.replay_socket, 0, ZMQ_POLLIN, 0}};
                        int poll_timeout = static_cast<int>(wait_time.count());

                        if (zmq::poll(items, 1,
                                      std::chrono::milliseconds(poll_timeout)) >
                            0) {
                            service_replay(resources);
                        }
                    } else {
                        std::this_thread::sleep_for(wait_time);
                    }

                    // Update last send time
                    last_send_time = std::chrono::steady_clock::now();
                }
            }

            auto effective_timeout = config_.pop_timeout;
            if (config_.send_interval.count() > 0) {
                effective_timeout =
                    std::min(config_.pop_timeout, std::chrono::milliseconds(1));
            }

            // Batch pop events from queue
            auto result = event_queue_->pop_batch(config_.max_batch_size,
                                                  effective_timeout);

            if (!result.has_value()) {
                last_send_time = std::chrono::steady_clock::now();
                continue;
            }

            auto batch_items = *result;

            if (batch_items.empty()) {
                // Timeout or empty queue, continue
                last_send_time = std::chrono::steady_clock::now();
                continue;
            }

            // Prepare events and promises
            std::vector<std::shared_ptr<KVCacheEvent>> events;
            std::vector<std::shared_ptr<std::promise<bool>>> promises;
            events.reserve(batch_items.size());
            promises.reserve(batch_items.size());

            for (auto& item : batch_items) {
                if (!item) continue;  // Sentinel value
                events.push_back(std::move(item->event));
                promises.push_back(std::move(item->promise));
            }

            if (events.empty()) {
                last_send_time = std::chrono::steady_clock::now();
                continue;
            }

            total_events_.fetch_add(events.size(), std::memory_order_relaxed);
            // Create batch and serialize
            auto event_batch = std::make_shared<EventBatch>(std::move(events));
            uint64_t seq = resources.next_seq++;
            auto payload = event_batch->serialize();
            total_batches_.fetch_add(1, std::memory_order_relaxed);

            // Prepare ZeroMQ messages
            std::vector<zmq::message_t> messages;

            // Topic part
            if (!config_.topic.empty()) {
                messages.emplace_back(config_.topic.data(),
                                      config_.topic.size());
            } else {
                messages.emplace_back();
            }

            // Sequence number part
            uint64_t seq_be = htobe64(seq);
            messages.emplace_back(&seq_be, sizeof(seq_be));

            // Payload part
            messages.emplace_back(payload.data(), payload.size());

            // Send messages
            try {
                zmq::send_multipart(*resources.pub_socket, messages,
                                    zmq::send_flags::dontwait);

                // Store in replay buffer
                if (resources.replay_buffer.size() >= config_.buffer_steps) {
                    resources.replay_buffer.pop_front();
                }
                resources.replay_buffer.emplace_back(seq, std::move(payload));

                // Set all promises to success
                for (auto& promise : promises) {
                    promise->set_value(true);
                }

            } catch (const zmq::error_t& e) {
                handle_error(e, "send_multipart");
                for (auto& promise : promises) {
                    promise->set_value(false);
                }
                failed_events_.fetch_add(promises.size(),
                                         std::memory_order_relaxed);
            }

            last_send_time = std::chrono::steady_clock::now();

        } catch (const std::exception& e) {
            handle_error(e, "publisher_thread");
        }
    }
}

// ThreadResources constructor
KVEventConsumer::ThreadResources::ThreadResources(zmq::context_t& ctx,
                                                  const Config& config) {
    pub_socket = std::make_unique<zmq::socket_t>(ctx, zmq::socket_type::pub);
    pub_socket->set(zmq::sockopt::sndhwm, config.hwm);

    if (config.replay_endpoint) {
        replay_socket =
            std::make_unique<zmq::socket_t>(ctx, zmq::socket_type::router);
    }
}

// Statistics
KVEventConsumer::Stats KVEventConsumer::get_stats() const {
    Stats stats{
        .total_events = total_events_.load(std::memory_order_relaxed),
        .total_batches = total_batches_.load(std::memory_order_relaxed),
        .failed_events = failed_events_.load(std::memory_order_relaxed),
        .replay_requests = replay_requests_.load(std::memory_order_relaxed),
    };
    stats.calculate_derived_metrics();
    return stats;
}

void KVEventConsumer::Stats::calculate_derived_metrics() {
    if (has_data()) {
        events_per_batch =
            total_batches > 0
                ? static_cast<double>(total_events) / total_batches
                : 0.0;

        success_rate = (total_events - failed_events) * 100.0 / total_events;
    } else {
        events_per_batch = 0.0;
        success_rate = 0.0;
    }
}

std::ostream& operator<<(std::ostream& os,
                         const KVEventConsumer::Stats& stats) {
    os << "Publish Evts: " << stats.total_events
       << " (Batch=" << stats.total_batches << ", Avg/Batch=";

    if (stats.has_data() && stats.total_batches > 0) {
        os << std::fixed << std::setprecision(1) << stats.events_per_batch;
    } else {
        os << "--/--";
    }

    os << ")"
       << " | Succ: ";

    if (stats.has_data()) {
        os << std::fixed << std::setprecision(1) << stats.success_rate << "%";
    } else {
        os << "--/--";
    }

    os << " (Fail: " << stats.failed_events << ")"
       << " | Replay: " << stats.replay_requests;

    return os;
}

// Socket setup
void KVEventConsumer::setup_sockets(ThreadResources& resources) {
    bool should_bind = (config_.endpoint.find('*') != std::string::npos ||
                        config_.endpoint.find("::") != std::string::npos ||
                        config_.endpoint.find("ipc://") == 0 ||
                        config_.endpoint.find("inproc://") == 0);

    try {
        if (should_bind) {
            auto [success, bound_endpoint] =
                smart_bind(*resources.pub_socket, config_.endpoint, config_);

            if (!success) {
                throw std::runtime_error("Failed to bind PUB socket");
            }

            // Update configuration
            if (bound_endpoint != config_.endpoint) {
                LOG(WARNING) << "Updated PUB endpoint from " << config_.endpoint
                             << " to " << bound_endpoint;
                config_.endpoint = bound_endpoint;
            }
        } else {
            resources.pub_socket->connect(config_.endpoint);
            LOG(INFO) << "Connected PUB socket to: " << config_.endpoint;
        }
    } catch (const zmq::error_t& e) {
        throw std::runtime_error("Failed to setup PUB socket: " +
                                 std::string(e.what()));
    }

    // Setup replay ROUTER socket
    if (resources.replay_socket && config_.replay_endpoint) {
        try {
            auto [success, bound_endpoint] = smart_bind(
                *resources.replay_socket, *config_.replay_endpoint, config_);

            if (!success) {
                throw std::runtime_error("Failed to bind ROUTER socket");
            }

            // Update configuration
            if (bound_endpoint != *config_.replay_endpoint) {
                LOG(WARNING)
                    << "Updated replay endpoint from "
                    << *config_.replay_endpoint << " to " << bound_endpoint;
                config_.replay_endpoint = bound_endpoint;
            }
        } catch (const zmq::error_t& e) {
            throw std::runtime_error("Failed to setup ROUTER socket: " +
                                     std::string(e.what()));
        }
    }
}

// Replay service
void KVEventConsumer::service_replay(ThreadResources& resources) {
    if (!resources.replay_socket) return;

    try {
        std::vector<zmq::message_t> frames;
        if (!zmq::recv_multipart(*resources.replay_socket,
                                 std::back_inserter(frames))) {
            return;
        }

        if (frames.size() != 3) {
            LOG(ERROR) << "Invalid replay request: " << frames.size()
                       << " frames";
            return;
        }

        zmq::message_t client_id_frame = std::move(frames[0]);
        zmq::message_t empty_frame = std::move(frames[1]);
        zmq::message_t start_seq_frame = std::move(frames[2]);

        if (start_seq_frame.size() != 8) {
            LOG(ERROR) << "Invalid replay sequence number size";
            return;
        }

        replay_requests_.fetch_add(1, std::memory_order_relaxed);

        uint64_t start_seq =
            be64toh(*reinterpret_cast<const uint64_t*>(frames[2].data()));

        for (const auto& entry : resources.replay_buffer) {
            if (entry.seq >= start_seq) {
                std::vector<zmq::message_t> reply;
                zmq::message_t reply_client_id(client_id_frame.data(),
                                               client_id_frame.size());
                reply.push_back(std::move(reply_client_id));

                reply.emplace_back();

                uint64_t seq_be = htobe64(entry.seq);
                reply.emplace_back(&seq_be, sizeof(seq_be));
                reply.emplace_back(entry.payload.data(), entry.payload.size());

                try {
                    zmq::send_multipart(*resources.replay_socket, reply,
                                        zmq::send_flags::dontwait);
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Error sending replay event: " << e.what();
                    break;
                }
            }
        }

        std::vector<zmq::message_t> end_reply;
        zmq::message_t end_client_id(client_id_frame.data(),
                                     client_id_frame.size());
        end_reply.push_back(std::move(end_client_id));

        end_reply.emplace_back();
        end_reply.emplace_back(END_SEQ.data(), END_SEQ.size());
        end_reply.emplace_back();

        zmq::send_multipart(*resources.replay_socket, end_reply,
                            zmq::send_flags::dontwait);

    } catch (const std::exception& e) {
        LOG(ERROR) << "Error in replay service: " << e.what();
    }
}

// Error handling
void KVEventConsumer::handle_error(const std::exception& e,
                                   const std::string& context) {
    LOG(ERROR) << "Error in " << context << ": " << e.what();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

}  // namespace mooncake