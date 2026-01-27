#include "patterns.h"
#include <glog/logging.h>

namespace mooncake {

// ============================================================================
// REQ/REP Pattern Implementation
// ============================================================================

ReqRepPattern::ReqRepPattern(
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools,
    coro_rpc::coro_rpc_server* server, bool is_requester)
    : is_requester_(is_requester) {
    client_pools_ = client_pools;
    server_ = server;
}

ReqRepPattern::~ReqRepPattern() = default;

bool ReqRepPattern::bind(const std::string& endpoint) {
    if (is_requester_) {
        LOG(ERROR) << "REQ socket cannot bind";
        return false;
    }

    bound_endpoint_ = endpoint;
    LOG(INFO) << "REP socket bound to " << endpoint;
    return true;
}

void ReqRepPattern::registerHandlers(coro_rpc::coro_rpc_server* server) {
    if (!server || is_requester_) return;

    server_ = server;
    server_->register_handler<&ReqRepPattern::handleRequest,
                              &ReqRepPattern::handleTensorRequest>(this);
    LOG(INFO) << "REP handlers registered";
}

bool ReqRepPattern::connect(const std::string& endpoint) {
    if (!is_requester_) {
        LOG(ERROR) << "REP socket cannot connect";
        return false;
    }

    connected_endpoints_.push_back(endpoint);
    LOG(INFO) << "REQ socket connected to " << endpoint;
    return true;
}

async_simple::coro::Lazy<RpcResult> ReqRepPattern::sendAsync(
    const std::string& target_endpoint, const void* data, size_t data_size,
    const std::optional<std::string>& topic) {
    if (!is_requester_) {
        LOG(ERROR) << "REP socket cannot send requests";
        co_return RpcResult{-1, "REP socket cannot send requests"};
    }

    uint64_t seq_id = sequence_id_.fetch_add(1);
    const size_t ATTACHMENT_THRESHOLD = 1024;

    std::string endpoint =
        target_endpoint.empty() && !connected_endpoints_.empty()
            ? connected_endpoints_[0]
            : target_endpoint;

    // Capture shared_ptr to ensure Pattern object lifetime during async
    // operation
    auto self = shared_from_this();

    auto message =
        std::make_shared<std::string>(MessageCodec::encodeDataMessage(
            ZmqSocketType::REQ, data, data_size, topic, seq_id));
    auto result = co_await client_pools_->send_request(
        endpoint,
        [message, attachment_threshold = ATTACHMENT_THRESHOLD,
         self](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<std::string> {
            std::string_view message_view(*message);
            if (message->size() >= attachment_threshold) {
                client.set_req_attachment(message_view);
                auto rpc_result =
                    co_await client.call<&ReqRepPattern::handleRequest>(
                        std::string_view{});
                if (!rpc_result.has_value()) {
                    LOG(ERROR) << "RPC call failed: " << rpc_result.error().msg;
                    co_return std::string{};
                }
                co_return rpc_result.value();
            } else {
                auto rpc_result =
                    co_await client.call<&ReqRepPattern::handleRequest>(
                        message_view);
                if (!rpc_result.has_value()) {
                    LOG(ERROR) << "RPC call failed: " << rpc_result.error().msg;
                    co_return std::string{};
                }
                co_return rpc_result.value();
            }
        });

    RpcResult res;
    if (result.has_value()) {
        res.code = 0;
        res.response_data = result.value();
    } else {
        res.code = -1;
        res.message = "Request failed";
    }

    co_return res;
}

async_simple::coro::Lazy<int> ReqRepPattern::sendTensorAsync(
    const std::string& target_endpoint, const TensorInfo& tensor,
    const std::optional<std::string>& topic) {
    if (!is_requester_) {
        LOG(ERROR) << "REP socket cannot send requests";
        co_return -1;
    }

    uint64_t seq_id = sequence_id_.fetch_add(1);
    std::string header = MessageCodec::encodeTensorMessage(
        ZmqSocketType::REQ, tensor, topic, seq_id);

    std::string endpoint =
        target_endpoint.empty() && !connected_endpoints_.empty()
            ? connected_endpoints_[0]
            : target_endpoint;

    // Capture shared_ptr to ensure Pattern object lifetime during async
    // operation
    auto self = shared_from_this();

    auto header_ptr = std::make_shared<std::string>(std::move(header));
    auto result = co_await client_pools_->send_request(
        endpoint,
        [header_ptr, data_ptr = tensor.data_ptr,
         total_bytes = tensor.total_bytes,
         self](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<std::string> {
            // Create string_view inside lambda to point to captured header
            std::string_view header_view(*header_ptr);
            std::string_view tensor_view(static_cast<const char*>(data_ptr),
                                         total_bytes);
            client.set_req_attachment(tensor_view);

            auto rpc_result =
                co_await client.call<&ReqRepPattern::handleTensorRequest>(
                    header_view);

            if (!rpc_result.has_value()) {
                LOG(ERROR) << "Tensor RPC failed: " << rpc_result.error().msg;
                co_return std::string{};
            }
            co_return rpc_result.value();
        });

    co_return result.has_value() ? 0 : -1;
}

void ReqRepPattern::setReceiveCallback(
    std::function<void(std::string_view, std::string_view,
                       const std::optional<std::string>&)>
        callback) {
    receive_callback_ = callback;
}

void ReqRepPattern::setTensorReceiveCallback(
    std::function<void(std::string_view, const TensorInfo&,
                       const std::optional<std::string>&)>
        callback) {
    tensor_callback_ = callback;
}

std::string ReqRepPattern::handleRequest(std::string_view data) {
    LOG(INFO) << "REP: Handling request, data size: " << data.size();

    auto decoded = MessageCodec::decodeMessage(data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode request";
        return "";
    }

    if (receive_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        receive_callback_("", decoded->data, topic);
    }

    std::lock_guard lock(reply_mutex_);
    std::string reply = pending_reply_;
    pending_reply_.clear();

    return reply;
}

std::string ReqRepPattern::handleTensorRequest(std::string_view header_data) {
    LOG(INFO) << "REP: Handling tensor request";

    auto decoded = MessageCodec::decodeTensorMessage(header_data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode tensor request";
        return "";
    }

    if (tensor_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        tensor_callback_("", decoded->tensor, topic);
    }

    std::lock_guard lock(reply_mutex_);
    std::string reply = pending_reply_;
    pending_reply_.clear();

    return reply;
}

void ReqRepPattern::sendReply(const void* data, size_t data_size) {
    std::lock_guard lock(reply_mutex_);
    pending_reply_ = MessageCodec::encodeDataMessage(
        ZmqSocketType::REP, data, data_size, std::nullopt, 0);
}

void ReqRepPattern::sendReplyTensor(const TensorInfo& tensor) {
    std::lock_guard lock(reply_mutex_);
    pending_reply_ = MessageCodec::encodeTensorMessage(ZmqSocketType::REP,
                                                       tensor, std::nullopt, 0);
}

// ============================================================================
// PUB/SUB Pattern Implementation
// ============================================================================

PubSubPattern::PubSubPattern(
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools,
    coro_rpc::coro_rpc_server* server, bool is_publisher)
    : is_publisher_(is_publisher) {
    client_pools_ = client_pools;
    server_ = server;
}

PubSubPattern::~PubSubPattern() = default;

bool PubSubPattern::bind(const std::string& endpoint) {
    if (!is_publisher_) {
        LOG(ERROR) << "SUB socket cannot bind";
        return false;
    }

    LOG(INFO) << "PUB socket bound to " << endpoint;
    return true;
}

bool PubSubPattern::connect(const std::string& endpoint) {
    std::lock_guard lock(mutex_);

    if (is_publisher_) {
        subscriber_endpoints_.push_back(endpoint);
        LOG(INFO) << "PUB socket connected to subscriber " << endpoint;
    } else {
        if (server_) {
            server_->register_handler<&PubSubPattern::handlePublish,
                                      &PubSubPattern::handleTensorPublish>(
                this);
        }
        LOG(INFO) << "SUB socket connected to publisher " << endpoint;
    }

    return true;
}

bool PubSubPattern::subscribe(const std::string& topic) {
    if (is_publisher_) {
        LOG(ERROR) << "PUB socket cannot subscribe";
        return false;
    }

    std::lock_guard lock(mutex_);
    subscribed_topics_.insert(topic);
    LOG(INFO) << "Subscribed to topic: " << topic;
    return true;
}

bool PubSubPattern::unsubscribe(const std::string& topic) {
    if (is_publisher_) {
        LOG(ERROR) << "PUB socket cannot unsubscribe";
        return false;
    }

    std::lock_guard lock(mutex_);
    subscribed_topics_.erase(topic);
    LOG(INFO) << "Unsubscribed from topic: " << topic;
    return true;
}

bool PubSubPattern::matchesTopic(std::string_view received_topic) {
    std::lock_guard lock(mutex_);

    if (subscribed_topics_.empty()) {
        return true;
    }

    for (const auto& pattern : subscribed_topics_) {
        if (received_topic.size() >= pattern.size() &&
            received_topic.substr(0, pattern.size()) == pattern) {
            return true;
        }
    }

    return false;
}

async_simple::coro::Lazy<RpcResult> PubSubPattern::sendAsync(
    const std::string& target_endpoint, const void* data, size_t data_size,
    const std::optional<std::string>& topic) {
    if (!is_publisher_) {
        LOG(ERROR) << "SUB socket cannot publish";
        co_return RpcResult{-1, "SUB socket cannot publish"};
    }

    if (!topic.has_value()) {
        LOG(ERROR) << "PUB requires a topic";
        co_return RpcResult{-1, "Topic required"};
    }

    auto message =
        std::make_shared<std::string>(MessageCodec::encodeDataMessage(
            ZmqSocketType::PUB, data, data_size, topic, 0));

    std::vector<std::string> subscribers;
    {
        std::lock_guard lock(mutex_);
        if (target_endpoint.empty()) {
            subscribers = subscriber_endpoints_;
        } else {
            subscribers.push_back(target_endpoint);
        }
    }

    // Capture shared_ptr to ensure Pattern object lifetime during async
    // operation
    auto self = shared_from_this();

    int success_count = 0;
    for (const auto& endpoint : subscribers) {
        auto result = co_await client_pools_->send_request(
            endpoint,
            [message, self](coro_rpc::coro_rpc_client& client)
                -> async_simple::coro::Lazy<void> {
                client.set_req_attachment(std::string_view(*message));
                auto rpc_result =
                    co_await client.call<&PubSubPattern::handlePublish>(
                        std::string_view{});

                if (!rpc_result.has_value()) {
                    LOG(WARNING) << "Publish failed to one subscriber";
                }
            });

        if (result.has_value()) {
            success_count++;
        }
    }

    co_return RpcResult{
        success_count,
        "Published to " + std::to_string(success_count) + " subscribers"};
}

async_simple::coro::Lazy<int> PubSubPattern::sendTensorAsync(
    const std::string& target_endpoint, const TensorInfo& tensor,
    const std::optional<std::string>& topic) {
    if (!is_publisher_) {
        LOG(ERROR) << "SUB socket cannot publish";
        co_return -1;
    }

    if (!topic.has_value()) {
        LOG(ERROR) << "PUB requires a topic";
        co_return -1;
    }

    auto header_ptr =
        std::make_shared<std::string>(MessageCodec::encodeTensorMessage(
            ZmqSocketType::PUB, tensor, topic, 0));

    std::vector<std::string> subscribers;
    {
        std::lock_guard lock(mutex_);
        if (target_endpoint.empty()) {
            subscribers = subscriber_endpoints_;
        } else {
            subscribers.push_back(target_endpoint);
        }
    }

    // Capture shared_ptr to ensure Pattern object lifetime during async
    // operation
    auto self = shared_from_this();

    int success_count = 0;
    for (const auto& endpoint : subscribers) {
        auto result = co_await client_pools_->send_request(
            endpoint,
            [header_ptr, data_ptr = tensor.data_ptr,
             total_bytes = tensor.total_bytes,
             self](coro_rpc::coro_rpc_client& client)
                -> async_simple::coro::Lazy<void> {
                std::string_view tensor_view(static_cast<const char*>(data_ptr),
                                             total_bytes);
                client.set_req_attachment(tensor_view);

                auto rpc_result =
                    co_await client.call<&PubSubPattern::handleTensorPublish>(
                        std::string_view(*header_ptr));

                if (!rpc_result.has_value()) {
                    LOG(WARNING) << "Tensor publish failed to one subscriber";
                }
            });

        if (result.has_value()) {
            success_count++;
        }
    }

    co_return success_count;
}

void PubSubPattern::setReceiveCallback(
    std::function<void(std::string_view, std::string_view,
                       const std::optional<std::string>&)>
        callback) {
    receive_callback_ = callback;
}

void PubSubPattern::setTensorReceiveCallback(
    std::function<void(std::string_view, const TensorInfo&,
                       const std::optional<std::string>&)>
        callback) {
    tensor_callback_ = callback;
}

void PubSubPattern::handlePublish(std::string_view data) {
    LOG(INFO) << "SUB: Received publish, data size: " << data.size();

    auto decoded = MessageCodec::decodeMessage(data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode publish message";
        return;
    }

    if (!matchesTopic(decoded->topic)) {
        LOG(INFO) << "Topic not matched: " << decoded->topic;
        return;
    }

    if (receive_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        receive_callback_("", decoded->data, topic);
    }
}

void PubSubPattern::handleTensorPublish(std::string_view header_data) {
    LOG(INFO) << "SUB: Received tensor publish";

    auto decoded = MessageCodec::decodeTensorMessage(header_data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode tensor publish";
        return;
    }

    if (!matchesTopic(decoded->topic)) {
        LOG(INFO) << "Tensor topic not matched: " << decoded->topic;
        return;
    }

    if (tensor_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        tensor_callback_("", decoded->tensor, topic);
    }
}

// ============================================================================
// PUSH/PULL Pattern Implementation
// ============================================================================

PushPullPattern::PushPullPattern(
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools,
    coro_rpc::coro_rpc_server* server, bool is_pusher)
    : is_pusher_(is_pusher) {
    client_pools_ = client_pools;
    server_ = server;
}

PushPullPattern::~PushPullPattern() = default;

bool PushPullPattern::bind(const std::string& endpoint) {
    if (is_pusher_) {
        LOG(ERROR) << "PUSH socket cannot bind";
        return false;
    }

    if (server_) {
        server_->register_handler<&PushPullPattern::handlePush,
                                  &PushPullPattern::handleTensorPush>(this);
    }

    LOG(INFO) << "PULL socket bound to " << endpoint;
    return true;
}

bool PushPullPattern::connect(const std::string& endpoint) {
    if (!is_pusher_) {
        LOG(ERROR) << "PULL socket cannot connect";
        return false;
    }

    std::lock_guard lock(mutex_);
    puller_endpoints_.push_back(endpoint);
    LOG(INFO) << "PUSH socket connected to puller " << endpoint;
    return true;
}

std::string PushPullPattern::selectNextPuller() {
    std::lock_guard lock(mutex_);

    if (puller_endpoints_.empty()) {
        return "";
    }

    size_t index = round_robin_index_.fetch_add(1) % puller_endpoints_.size();
    return puller_endpoints_[index];
}

async_simple::coro::Lazy<RpcResult> PushPullPattern::sendAsync(
    const std::string& target_endpoint, const void* data, size_t data_size,
    const std::optional<std::string>& topic) {
    if (!is_pusher_) {
        LOG(ERROR) << "PULL socket cannot push";
        co_return RpcResult{-1, "PULL socket cannot push"};
    }

    std::string endpoint =
        target_endpoint.empty() ? selectNextPuller() : target_endpoint;

    if (endpoint.empty()) {
        LOG(ERROR) << "No PULL endpoints available";
        co_return RpcResult{-1, "No PULL endpoints"};
    }

    auto message =
        std::make_shared<std::string>(MessageCodec::encodeDataMessage(
            ZmqSocketType::PUSH, data, data_size, topic, 0));

    // Capture shared_ptr to ensure Pattern object lifetime during async
    // operation
    auto self = shared_from_this();

    auto result = co_await client_pools_->send_request(
        endpoint,
        [message, self](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<void> {
            client.set_req_attachment(std::string_view(*message));
            auto rpc_result =
                co_await client.call<&PushPullPattern::handlePush>(
                    std::string_view{});

            if (!rpc_result.has_value()) {
                LOG(ERROR) << "Push RPC failed: " << rpc_result.error().msg;
            }
        });

    RpcResult res;
    res.code = result.has_value() ? 0 : -1;
    res.message = result.has_value() ? "Pushed successfully" : "Push failed";

    co_return res;
}

async_simple::coro::Lazy<int> PushPullPattern::sendTensorAsync(
    const std::string& target_endpoint, const TensorInfo& tensor,
    const std::optional<std::string>& topic) {
    if (!is_pusher_) {
        LOG(ERROR) << "PULL socket cannot push";
        co_return -1;
    }

    std::string endpoint =
        target_endpoint.empty() ? selectNextPuller() : target_endpoint;

    if (endpoint.empty()) {
        LOG(ERROR) << "No PULL endpoints available";
        co_return -1;
    }

    auto header_ptr =
        std::make_shared<std::string>(MessageCodec::encodeTensorMessage(
            ZmqSocketType::PUSH, tensor, topic, 0));

    // Capture shared_ptr to ensure Pattern object lifetime during async
    // operation
    auto self = shared_from_this();

    auto result = co_await client_pools_->send_request(
        endpoint,
        [header_ptr, data_ptr = tensor.data_ptr,
         total_bytes = tensor.total_bytes,
         self](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<void> {
            std::string_view tensor_view(static_cast<const char*>(data_ptr),
                                         total_bytes);
            client.set_req_attachment(tensor_view);

            auto rpc_result =
                co_await client.call<&PushPullPattern::handleTensorPush>(
                    std::string_view(*header_ptr));

            if (!rpc_result.has_value()) {
                LOG(ERROR) << "Tensor push RPC failed: "
                           << rpc_result.error().msg;
            }
        });

    co_return result.has_value() ? 0 : -1;
}

void PushPullPattern::setReceiveCallback(
    std::function<void(std::string_view, std::string_view,
                       const std::optional<std::string>&)>
        callback) {
    receive_callback_ = callback;
}

void PushPullPattern::setTensorReceiveCallback(
    std::function<void(std::string_view, const TensorInfo&,
                       const std::optional<std::string>&)>
        callback) {
    tensor_callback_ = callback;
}

void PushPullPattern::handlePush(std::string_view data) {
    LOG(INFO) << "PULL: Received push, data size: " << data.size();

    auto decoded = MessageCodec::decodeMessage(data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode push message";
        return;
    }

    if (receive_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        receive_callback_("", decoded->data, topic);
    }
}

void PushPullPattern::handleTensorPush(std::string_view header_data) {
    LOG(INFO) << "PULL: Received tensor push";

    auto decoded = MessageCodec::decodeTensorMessage(header_data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode tensor push";
        return;
    }

    if (tensor_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        tensor_callback_("", decoded->tensor, topic);
    }
}

// ============================================================================
// PAIR Pattern Implementation
// ============================================================================

PairPattern::PairPattern(
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools,
    coro_rpc::coro_rpc_server* server) {
    client_pools_ = client_pools;
    server_ = server;
}

PairPattern::~PairPattern() = default;

bool PairPattern::bind(const std::string& endpoint) {
    std::lock_guard lock(mutex_);

    if (is_connected_) {
        LOG(ERROR) << "PAIR socket already connected";
        return false;
    }

    if (server_) {
        server_->register_handler<&PairPattern::handleMessage,
                                  &PairPattern::handleTensorMessage>(this);
    }

    is_connected_ = true;
    LOG(INFO) << "PAIR socket bound to " << endpoint;
    return true;
}

bool PairPattern::connect(const std::string& endpoint) {
    std::lock_guard lock(mutex_);

    if (is_connected_) {
        LOG(ERROR) << "PAIR socket already connected";
        return false;
    }

    peer_endpoint_ = endpoint;
    is_connected_ = true;

    LOG(INFO) << "PAIR socket connected to " << endpoint;
    return true;
}

async_simple::coro::Lazy<RpcResult> PairPattern::sendAsync(
    const std::string& target_endpoint, const void* data, size_t data_size,
    const std::optional<std::string>& topic) {
    std::string endpoint;
    {
        std::lock_guard lock(mutex_);
        endpoint = target_endpoint.empty() ? peer_endpoint_ : target_endpoint;
    }

    if (endpoint.empty()) {
        LOG(ERROR) << "PAIR socket not connected";
        co_return RpcResult{-1, "Not connected"};
    }

    auto message =
        std::make_shared<std::string>(MessageCodec::encodeDataMessage(
            ZmqSocketType::PAIR, data, data_size, topic, 0));

    // Capture shared_ptr to ensure Pattern object lifetime during async
    // operation
    auto self = shared_from_this();

    auto result = co_await client_pools_->send_request(
        endpoint,
        [message, self](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<void> {
            client.set_req_attachment(std::string_view(*message));
            auto rpc_result = co_await client.call<&PairPattern::handleMessage>(
                std::string_view{});

            if (!rpc_result.has_value()) {
                LOG(ERROR) << "PAIR send RPC failed: "
                           << rpc_result.error().msg;
            }
        });

    RpcResult res;
    res.code = result.has_value() ? 0 : -1;
    res.message = result.has_value() ? "Sent successfully" : "Send failed";

    co_return res;
}

async_simple::coro::Lazy<int> PairPattern::sendTensorAsync(
    const std::string& target_endpoint, const TensorInfo& tensor,
    const std::optional<std::string>& topic) {
    std::string endpoint;
    {
        std::lock_guard lock(mutex_);
        endpoint = target_endpoint.empty() ? peer_endpoint_ : target_endpoint;
    }

    if (endpoint.empty()) {
        LOG(ERROR) << "PAIR socket not connected";
        co_return -1;
    }

    auto header_ptr =
        std::make_shared<std::string>(MessageCodec::encodeTensorMessage(
            ZmqSocketType::PAIR, tensor, topic, 0));

    // Capture shared_ptr to ensure Pattern object lifetime during async
    // operation
    auto self = shared_from_this();

    auto result = co_await client_pools_->send_request(
        endpoint,
        [header_ptr, data_ptr = tensor.data_ptr,
         total_bytes = tensor.total_bytes,
         self](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<void> {
            std::string_view tensor_view(static_cast<const char*>(data_ptr),
                                         total_bytes);
            client.set_req_attachment(tensor_view);

            auto rpc_result =
                co_await client.call<&PairPattern::handleTensorMessage>(
                    std::string_view(*header_ptr));

            if (!rpc_result.has_value()) {
                LOG(ERROR) << "PAIR tensor RPC failed: "
                           << rpc_result.error().msg;
            }
        });

    co_return result.has_value() ? 0 : -1;
}

void PairPattern::setReceiveCallback(
    std::function<void(std::string_view, std::string_view,
                       const std::optional<std::string>&)>
        callback) {
    receive_callback_ = callback;
}

void PairPattern::setTensorReceiveCallback(
    std::function<void(std::string_view, const TensorInfo&,
                       const std::optional<std::string>&)>
        callback) {
    tensor_callback_ = callback;
}

void PairPattern::handleMessage(std::string_view data) {
    LOG(INFO) << "PAIR: Received message, data size: " << data.size();

    auto decoded = MessageCodec::decodeMessage(data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode PAIR message";
        return;
    }

    if (receive_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        receive_callback_("", decoded->data, topic);
    }
}

void PairPattern::handleTensorMessage(std::string_view header_data) {
    LOG(INFO) << "PAIR: Received tensor message";

    auto decoded = MessageCodec::decodeTensorMessage(header_data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode PAIR tensor message";
        return;
    }

    if (tensor_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        tensor_callback_("", decoded->tensor, topic);
    }
}

}  // namespace mooncake
