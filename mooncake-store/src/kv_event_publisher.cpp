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
                                        const KVEventPublisherConfig& config) {
    EndpointInfo info = EndpointInfo::parse(endpoint);

    if (info.type == EndpointType::UNKNOWN) {
        LOG(ERROR) << "Unknown endpoint type: " << endpoint;
        return {false, endpoint};
    }

    if (info.type != EndpointType::TCP || !config.auto_port) {
        try {
            socket.bind(endpoint);
            LOG(INFO) << "Bound to: " << endpoint;
            return {true, endpoint};
        } catch (const zmq::error_t& e) {
            LOG(ERROR) << "Failed to bind to " << endpoint << ": " << e.what();
            return {false, endpoint};
        }
    }

    int original_port = info.port;
    int attempts = 0;

    std::random_device rd;
    std::mt19937_64 rng(rd());
    std::uniform_int_distribution<int> dist(1024, 65535);
    constexpr std::string_view YELLOW = "\033[93m";
    constexpr std::string_view GREEN{"\033[92m"};
    constexpr std::string_view RESET{"\033[0m"};

    while (attempts < config.max_port_attempts) {
        int random_port = dist(rng);
        std::string random_endpoint = info.to_string_with_port(random_port);

        try {
            socket.bind(random_endpoint);

            LOG(WARNING) << "Port {" << YELLOW << original_port << RESET
                         << "} was in use, randomly switched to port {" << GREEN
                         << random_port << RESET << "} (attempt "
                         << (attempts + 1) << ")";

            return {true, random_endpoint};
        } catch (const zmq::error_t& e) {
            if (e.num() == EADDRINUSE) {
                attempts++;
                continue;
            } else {
                LOG(ERROR) << "Failed to bind to " << random_endpoint << ": "
                           << e.what();
                return {false, random_endpoint};
            }
        }
    }

    LOG(ERROR) << "Failed to find an available port after "
               << config.max_port_attempts << " random attempts";
    return {false, info.to_string_with_port(original_port)};
}

}  // namespace

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

    if (max_port_attempts <= 0 || max_port_attempts > 1000) {
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

// ZmqEventPublisher Implementation
ZmqEventPublisher::ZmqEventPublisher(const KVEventPublisherConfig& config)
    : config_(config) {
    if (!config_.validate()) {
        throw std::runtime_error("Invalid ZmqEventPublisher configuration");
    }

    if (config_.max_batch_size < 0 || config_.max_batch_size > 100) {
        config_.max_batch_size = 100;
        LOG(WARNING)
            << "KV Event Publisher Config"
            << " max_batch_size cannot be negative or greater than 100;\n"
            << "KV Event Publisher Config max_batch_size has been reset to: "
            << config_.max_batch_size;
    }

    if (config_.send_interval.count() < 0) {
        config_.send_interval = std::chrono::milliseconds(0);
        ;
        LOG(WARNING) << "KV Event Publisher Config"
                     << " send_interval has been reset to: "
                     << config_.send_interval.count();
    }

    context_ = zmq::context_t(1);

    event_queue_ = std::make_unique<EventQueue>(config_.max_queue_size);

    enqueue_pool_ =
        std::make_unique<ThreadPool>(config_.enqueue_thread_pool_size);

    running_ = true;

    publisher_thread_ = std::jthread([this](std::stop_token token) {
        this->publisher_thread(std::move(token));
    });
}

ZmqEventPublisher::~ZmqEventPublisher() {
    if (running_) {
        shutdown();
    }
}

// Publisher thread
void ZmqEventPublisher::publisher_thread(std::stop_token stop_token) {
    ThreadResources resources(context_, config_);
    setup_sockets(resources);

    LOG(INFO) << "KV Event Publisher started with async batch processing";
    LOG(INFO) << "  Endpoint: " << config_.endpoint;
    LOG(INFO) << "  Max batch size: " << config_.max_batch_size;
    LOG(INFO) << "  Send interval: " << config_.send_interval.count() << "ms";
    LOG(INFO) << "  " << get_stats();

    auto last_send_time = std::chrono::steady_clock::now();

    while (running_ && !stop_token.stop_requested()) {
        try {
            // Check replay requests
            if (resources.replay_socket) {
                zmq::pollitem_t items[] = {
                    {*resources.replay_socket, 0, ZMQ_POLLIN, 0}};
                if (zmq::poll(items, 1, 0) > 0) {
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

                        if (zmq::poll(items, 1, poll_timeout) > 0) {
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

            // Create batch and serialize
            auto event_batch = std::make_shared<EventBatch>(std::move(events));
            uint64_t seq = resources.next_seq++;
            auto payload = event_batch->serialize();
            total_batches_++;

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
                failed_events_ += promises.size();
            }

            last_send_time = std::chrono::steady_clock::now();

        } catch (const std::exception& e) {
            handle_error(e, "publisher_thread");
        }
    }

    LOG(INFO) << "Publisher thread exiting. Remaining queue size: "
              << event_queue_->size();
}

// ThreadResources constructor
ZmqEventPublisher::ThreadResources::ThreadResources(
    zmq::context_t& ctx, const KVEventPublisherConfig& config) {
    pub_socket = std::make_unique<zmq::socket_t>(ctx, zmq::socket_type::pub);
    pub_socket->set(zmq::sockopt::sndhwm, config.hwm);

    if (config.replay_endpoint) {
        replay_socket =
            std::make_unique<zmq::socket_t>(ctx, zmq::socket_type::router);
    }
}

std::future<bool> ZmqEventPublisher::publish_event_async(
    std::shared_ptr<KVCacheEvent> event) {
    QueuedItem item{std::move(event), std::make_shared<std::promise<bool>>()};
    auto future = item.promise->get_future();

    try {
        enqueue_pool_->enqueue([this, item = std::move(item)]() mutable {
            this->process_enqueue_task_with_retry(std::move(item));
        });
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to enqueue task: " << e.what();
        failed_events_++;
    }

    return future;
}

void ZmqEventPublisher::process_enqueue_task_with_retry(QueuedItem item) {
    size_t retry_count = 0;

    while (running_.load(std::memory_order_acquire)) {
        // Check retry count
        retry_count++;
        if (retry_count >= config_.enqueue_max_retries) {
            LOG(ERROR) << "Failed to enqueue event after " << retry_count
                       << " retries";
            item.promise->set_value(false);
            failed_events_++;
            return;
        }

        if (event_queue_->try_push(item)) {
            total_events_++;
            return;
        }

        if (event_queue_->push(item, config_.enqueue_timeout)) {
            total_events_++;
            return;
        }

        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    item.promise->set_value(false);
    failed_events_++;
}

// Shutdown
void ZmqEventPublisher::shutdown() {
    if (!running_.exchange(false)) {
        return;
    }

    LOG(INFO) << "Shutting down ZmqEventPublisher...";

    // Send sentinel value
    event_queue_->push(std::nullopt, std::chrono::milliseconds(100));

    // Stop producer thread pool
    if (enqueue_pool_) {
        LOG(INFO) << "Stopping enqueue thread pool...";
        enqueue_pool_->stop();
    }

    // Wait for consumer thread to finish
    if (publisher_thread_.joinable()) {
        publisher_thread_.request_stop();
        publisher_thread_.join();
    }

    // Shutdown queue
    event_queue_->shutdown();

    LOG(INFO) << "ZmqEventPublisher shutdown complete: " << get_stats();
}

// Statistics
ZmqEventPublisher::Stats ZmqEventPublisher::get_stats() const {
    Stats stats;

    stats.queue_remain_events =
        event_queue_->size();  // Expensive operation, avoid frequent calls
    stats.queue_capacity = event_queue_->capacity();

    stats.total_events = total_events_.load(std::memory_order_relaxed);
    stats.total_batches = total_batches_.load(std::memory_order_relaxed);
    stats.failed_events = failed_events_.load(std::memory_order_relaxed);

    stats.store_event = store_event_.load(std::memory_order_relaxed);
    stats.update_event = update_event_.load(std::memory_order_relaxed);
    stats.remove_all_event = remove_all_event_.load(std::memory_order_relaxed);

    stats.replay_requests = replay_requests_.load(std::memory_order_relaxed);

    stats.calculate_derived_metrics();

    return stats;
}

void ZmqEventPublisher::Stats::calculate_derived_metrics() {
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
                         const ZmqEventPublisher::Stats& stats) {
    ZmqEventPublisher::Stats mutable_stats = stats;
    mutable_stats.calculate_derived_metrics();

    os << "Queue(pending/cap): " << mutable_stats.queue_remain_events << "/"
       << mutable_stats.queue_capacity
       << " | Evts: " << mutable_stats.total_events
       << " (Batch: " << mutable_stats.total_batches << ", Avg/Batch=";

    if (mutable_stats.has_data() && mutable_stats.total_batches > 0) {
        os << std::fixed << std::setprecision(1)
           << mutable_stats.events_per_batch;
    } else {
        os << "--/--";
    }

    os << ")"
       << " | Succ: ";

    if (mutable_stats.has_data()) {
        os << std::fixed << std::setprecision(1) << mutable_stats.success_rate
           << "%";
    } else {
        os << "--/--";
    }

    os << " (Fail: " << mutable_stats.failed_events << ")"
       << " | Evt Types: Store=" << mutable_stats.store_event
       << ", Update=" << mutable_stats.update_event
       << ", RemoveAll=" << mutable_stats.remove_all_event
       << " | Replay: " << mutable_stats.replay_requests;

    return os;
}

// Socket setup
void ZmqEventPublisher::setup_sockets(ThreadResources& resources) {
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
void ZmqEventPublisher::service_replay(ThreadResources& resources) {
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
void ZmqEventPublisher::handle_error(const std::exception& e,
                                     const std::string& context) {
    LOG(ERROR) << "Error in " << context << ": " << e.what();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

// EventPublisherFactory Implementation
std::unordered_map<std::string, EventPublisherFactory::PublisherConstructor>&
EventPublisherFactory::registry() {
    static std::unordered_map<std::string, PublisherConstructor> reg = {
        {"zmq", [](const KVEventPublisherConfig& config) {
             return std::make_unique<ZmqEventPublisher>(config);
         }}};
    return reg;
}

void EventPublisherFactory::register_publisher(
    const std::string& name, PublisherConstructor constructor) {
    auto& reg = registry();
    if (reg.contains(name)) {
        throw std::invalid_argument("Publisher '" + name +
                                    "' already registered");
    }
    reg[name] = std::move(constructor);
}

std::unique_ptr<ZmqEventPublisher> EventPublisherFactory::create(
    const std::string& publisher_type, const KVEventPublisherConfig& config) {
    auto& reg = registry();
    auto it = reg.find(publisher_type);
    if (it == reg.end()) {
        throw std::invalid_argument("Unknown event publisher: " +
                                    publisher_type);
    }

    return it->second(config);
}

}  // namespace mooncake