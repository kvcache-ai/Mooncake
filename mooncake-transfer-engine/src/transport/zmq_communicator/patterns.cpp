#include "patterns.h"
#include <glog/logging.h>
#include <memory>
#include <unordered_map>
#include <vector>

namespace {
thread_local std::unordered_map<const mooncake::ReqRepPattern*, std::string>
    reqrep_pending_replies;
}  // namespace

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

    if (server_) {
        registerHandlers(server_);
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

    std::string endpoint =
        target_endpoint.empty() && !connected_endpoints_.empty()
            ? connected_endpoints_[0]
            : target_endpoint;

    // Hold shared_ptr in this coroutine to keep object alive
    auto self = shared_from_this();

    // Move msg_buf into lambda only—do not keep it in coroutine scope. Keeping
    // both causes use-after-free: our msg_buf and the lambda's copy can destroy
    // the shared control block in wrong order when the frame tears down.
    std::string message = MessageCodec::encodeDataMessage(
        ZmqSocketType::REQ, data, data_size, topic, seq_id);
    auto msg_buf =
        std::make_shared<std::vector<char>>(message.begin(), message.end());
    const size_t message_size = msg_buf->size();
    auto op = [msg_buf = std::move(msg_buf), message_size,
               self = self](coro_rpc::coro_rpc_client& client)
        -> async_simple::coro::Lazy<std::string> {
        std::string_view message_view(msg_buf->data(), message_size);
        client.set_req_attachment(message_view);
        auto rpc_result = co_await client.call<&ReqRepPattern::handleRequest>();
        if (!rpc_result.has_value()) {
            LOG(ERROR) << "RPC call failed: " << rpc_result.error().msg;
            co_return std::string{};
        }
        std::string response;
        response.assign(rpc_result.value().data(), rpc_result.value().size());
        co_return response;
    };
    auto result = co_await client_pools_->send_request(endpoint, std::move(op));

    // Process result with value semantics
    RpcResult res;
    if (result.has_value()) {
        res.code = 0;
        // Ensure response_data is copied, not moved from temporary
        res.response_data = std::string(result.value());
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

    // Hold shared_ptr in this coroutine to keep object alive
    auto self = shared_from_this();

    const size_t header_size = header.size();
    // Capture tensor data by value - note: data_ptr must remain valid
    // TensorInfo should ensure data_ptr lifetime, but we capture by value for
    // safety
    // Use copy capture (not move) to prevent double-free when lambda is copied
    auto op = [header, header_size, data_ptr = tensor.data_ptr,  // Copy capture
               total_bytes = tensor.total_bytes,
               self = self](coro_rpc::coro_rpc_client& client)
        -> async_simple::coro::Lazy<std::string> {
        // Create string_view inside lambda to point to captured header
        std::string_view header_view(header.data(), header_size);
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
        // Response is sent via context attachment (handler returns void)
        std::string response(client.get_resp_attachment());
        co_return response;
    };
    auto result = co_await client_pools_->send_request(endpoint, std::move(op));

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

std::string ReqRepPattern::handleRequest(coro_rpc::context<void> context) {
    auto* ctx_info = context.get_context_info();
    auto attachment =
        ctx_info ? ctx_info->get_request_attachment() : std::string_view{};
    LOG(INFO) << "REP: Handling request, data size: " << attachment.size();

    auto decoded = MessageCodec::decodeMessage(attachment);
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

    auto it = reqrep_pending_replies.find(this);
    if (it == reqrep_pending_replies.end()) {
        return "";
    }
    std::string reply = std::move(it->second);
    reqrep_pending_replies.erase(it);
    return reply;
}

void ReqRepPattern::handleTensorRequest(coro_rpc::context<void> context,
                                        std::string_view header_data) {
    LOG(INFO) << "REP: Handling tensor request";

    auto* ctx_info = context.get_context_info();
    std::string reply;

    auto decoded = MessageCodec::decodeTensorMessage(header_data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode tensor request";
        ctx_info->set_response_attachment(std::string_view{});
        context.response_msg();
        return;
    }

    if (tensor_callback_) {
        auto attachment =
            ctx_info ? ctx_info->get_request_attachment() : std::string_view{};

        TensorInfo tensor = decoded->tensor;
        if (!attachment.empty()) {
            if (attachment.size() != tensor.total_bytes) {
                LOG(WARNING) << "Tensor attachment size mismatch: expected "
                             << tensor.total_bytes << " bytes, got "
                             << attachment.size() << " bytes";
            }
            tensor.data_ptr = attachment.data();  // valid during handler
        } else if (tensor.total_bytes != 0) {
            LOG(WARNING) << "Tensor request missing attachment (expected "
                         << tensor.total_bytes << " bytes)";
        }

        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        tensor_callback_("", tensor, topic);
    }

    auto it = reqrep_pending_replies.find(this);
    if (it != reqrep_pending_replies.end()) {
        reply = std::move(it->second);
        reqrep_pending_replies.erase(it);
    }

    ctx_info->set_response_attachment(reply);
    context.response_msg();
}

void ReqRepPattern::sendReply(const void* data, size_t data_size) {
    reqrep_pending_replies[this] = MessageCodec::encodeDataMessage(
        ZmqSocketType::REP, data, data_size, std::nullopt, 0);
}

void ReqRepPattern::sendReplyTensor(const TensorInfo& tensor) {
    reqrep_pending_replies[this] = MessageCodec::encodeTensorMessage(
        ZmqSocketType::REP, tensor, std::nullopt, 0);
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
        // SUB binds to a local endpoint; register handlers on the server
        // created for that endpoint (by the communicator) so receives work.
        if (server_) {
            server_->register_handler<&PubSubPattern::handlePublish,
                                      &PubSubPattern::handleTensorPublish>(
                this);
        }
        LOG(INFO) << "SUB socket bound to " << endpoint;
        return true;
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
        // Handlers are already registered in bind() for SUB.
        LOG(INFO)
            << "SUB socket connected to publisher " << endpoint
            << " (note: PUB must connect to this SUB endpoint to publish)";
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

    // Hold shared_ptr in this coroutine to keep object alive
    auto self = shared_from_this();

    std::string message = MessageCodec::encodeDataMessage(
        ZmqSocketType::PUB, data, data_size, topic, 0);
    const size_t message_size = message.size();

    std::vector<std::string> subscribers;
    {
        std::lock_guard lock(mutex_);
        if (target_endpoint.empty()) {
            subscribers = subscriber_endpoints_;
        } else {
            subscribers.push_back(target_endpoint);
        }
    }

    int success_count = 0;
    for (const auto& endpoint : subscribers) {
        std::string message_copy = message;
        auto op = [message = std::move(message_copy), message_size,
                   self = self](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<void> {
            client.set_req_attachment(
                std::string_view(message.data(), message_size));
            auto rpc_result =
                co_await client.call<&PubSubPattern::handlePublish>();

            if (!rpc_result.has_value()) {
                LOG(WARNING) << "Publish failed to one subscriber";
            }
        };
        auto result =
            co_await client_pools_->send_request(endpoint, std::move(op));

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

    // Hold shared_ptr in this coroutine to keep object alive
    auto self = shared_from_this();

    std::string header =
        MessageCodec::encodeTensorMessage(ZmqSocketType::PUB, tensor, topic, 0);
    const size_t header_size = header.size();

    std::vector<std::string> subscribers;
    {
        std::lock_guard lock(mutex_);
        if (target_endpoint.empty()) {
            subscribers = subscriber_endpoints_;
        } else {
            subscribers.push_back(target_endpoint);
        }
    }

    int success_count = 0;
    for (const auto& endpoint : subscribers) {
        std::string header_copy = header;
        auto op = [header = std::move(header_copy), header_size,
                   data_ptr = tensor.data_ptr, total_bytes = tensor.total_bytes,
                   self = self](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<void> {
            std::string_view tensor_view(static_cast<const char*>(data_ptr),
                                         total_bytes);
            client.set_req_attachment(tensor_view);

            auto rpc_result =
                co_await client.call<&PubSubPattern::handleTensorPublish>(
                    std::string_view(header.data(), header_size));

            if (!rpc_result.has_value()) {
                LOG(WARNING) << "Tensor publish failed to one subscriber";
            }
        };
        auto result =
            co_await client_pools_->send_request(endpoint, std::move(op));

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

void PubSubPattern::handlePublish(coro_rpc::context<void> context) {
    auto* ctx_info = context.get_context_info();
    auto attachment =
        ctx_info ? ctx_info->get_request_attachment() : std::string_view{};

    LOG(INFO) << "SUB: Received publish, data size: " << attachment.size();

    auto decoded = MessageCodec::decodeMessage(attachment);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode publish message";
        context.response_msg();
        return;
    }

    if (!matchesTopic(decoded->topic)) {
        LOG(INFO) << "Topic not matched: " << decoded->topic;
        context.response_msg();
        return;
    }

    if (receive_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        receive_callback_("", decoded->data, topic);
    }

    context.response_msg();
}

void PubSubPattern::handleTensorPublish(coro_rpc::context<void> context,
                                        std::string_view header_data) {
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
        auto* ctx_info = context.get_context_info();
        auto attachment =
            ctx_info ? ctx_info->get_request_attachment() : std::string_view{};

        TensorInfo tensor = decoded->tensor;
        if (!attachment.empty()) {
            if (attachment.size() != tensor.total_bytes) {
                LOG(WARNING) << "Tensor attachment size mismatch: expected "
                             << tensor.total_bytes << " bytes, got "
                             << attachment.size() << " bytes";
            }
            tensor.data_ptr = attachment.data();  // valid during handler
        } else if (tensor.total_bytes != 0) {
            LOG(WARNING) << "Tensor publish missing attachment (expected "
                         << tensor.total_bytes << " bytes)";
        }

        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        tensor_callback_("", tensor, topic);
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

    // Hold shared_ptr in this coroutine to keep object alive
    auto self = shared_from_this();

    std::string message = MessageCodec::encodeDataMessage(
        ZmqSocketType::PUSH, data, data_size, topic, 0);
    const size_t message_size = message.size();

    auto op = [message = std::move(message), message_size,
               self = self](coro_rpc::coro_rpc_client& client)
        -> async_simple::coro::Lazy<void> {
        client.set_req_attachment(
            std::string_view(message.data(), message_size));
        auto rpc_result = co_await client.call<&PushPullPattern::handlePush>();

        if (!rpc_result.has_value()) {
            LOG(ERROR) << "Push RPC failed: " << rpc_result.error().msg;
        }
    };
    auto result = co_await client_pools_->send_request(endpoint, std::move(op));

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

    // Hold shared_ptr in this coroutine to keep object alive
    auto self = shared_from_this();

    std::string header = MessageCodec::encodeTensorMessage(ZmqSocketType::PUSH,
                                                           tensor, topic, 0);
    const size_t header_size = header.size();

    auto op = [header = std::move(header), header_size,
               data_ptr = tensor.data_ptr, total_bytes = tensor.total_bytes,
               self = self](coro_rpc::coro_rpc_client& client)
        -> async_simple::coro::Lazy<void> {
        std::string_view tensor_view(static_cast<const char*>(data_ptr),
                                     total_bytes);
        client.set_req_attachment(tensor_view);

        auto rpc_result =
            co_await client.call<&PushPullPattern::handleTensorPush>(
                std::string_view(header.data(), header_size));

        if (!rpc_result.has_value()) {
            LOG(ERROR) << "Tensor push RPC failed: " << rpc_result.error().msg;
        }
    };
    auto result = co_await client_pools_->send_request(endpoint, std::move(op));

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

void PushPullPattern::handlePush(coro_rpc::context<void> context) {
    auto* ctx_info = context.get_context_info();
    auto attachment =
        ctx_info ? ctx_info->get_request_attachment() : std::string_view{};
    LOG(INFO) << "PULL: Received push, data size: " << attachment.size();

    auto decoded = MessageCodec::decodeMessage(attachment);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode push message";
        context.response_msg();
        return;
    }

    if (receive_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        receive_callback_("", decoded->data, topic);
    }
    context.response_msg();
}

void PushPullPattern::handleTensorPush(coro_rpc::context<void> context,
                                       std::string_view header_data) {
    LOG(INFO) << "PULL: Received tensor push";

    auto decoded = MessageCodec::decodeTensorMessage(header_data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode tensor push";
        return;
    }

    if (tensor_callback_) {
        auto* ctx_info = context.get_context_info();
        auto attachment =
            ctx_info ? ctx_info->get_request_attachment() : std::string_view{};

        TensorInfo tensor = decoded->tensor;
        if (!attachment.empty()) {
            if (attachment.size() != tensor.total_bytes) {
                LOG(WARNING) << "Tensor attachment size mismatch: expected "
                             << tensor.total_bytes << " bytes, got "
                             << attachment.size() << " bytes";
            }
            tensor.data_ptr = attachment.data();  // valid during handler
        } else if (tensor.total_bytes != 0) {
            LOG(WARNING) << "Tensor push missing attachment (expected "
                         << tensor.total_bytes << " bytes)";
        }

        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        tensor_callback_("", tensor, topic);
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

    if (is_bound_) {
        LOG(ERROR) << "PAIR socket already bound";
        return false;
    }

    if (server_) {
        server_->register_handler<&PairPattern::handleMessage,
                                  &PairPattern::handleTensorMessage>(this);
    }

    is_bound_ = true;
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

    // Hold shared_ptr in this coroutine to keep object alive
    auto self = shared_from_this();

    std::string message = MessageCodec::encodeDataMessage(
        ZmqSocketType::PAIR, data, data_size, topic, 0);
    const size_t message_size = message.size();

    auto op = [message = std::move(message), message_size,
               self = self](coro_rpc::coro_rpc_client& client)
        -> async_simple::coro::Lazy<void> {
        client.set_req_attachment(
            std::string_view(message.data(), message_size));
        auto rpc_result = co_await client.call<&PairPattern::handleMessage>();

        if (!rpc_result.has_value()) {
            LOG(ERROR) << "PAIR send RPC failed: " << rpc_result.error().msg;
        }
    };
    auto result = co_await client_pools_->send_request(endpoint, std::move(op));

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

    // Hold shared_ptr in this coroutine to keep object alive
    auto self = shared_from_this();

    std::string header = MessageCodec::encodeTensorMessage(ZmqSocketType::PAIR,
                                                           tensor, topic, 0);
    const size_t header_size = header.size();

    auto op = [header = std::move(header), header_size,
               data_ptr = tensor.data_ptr, total_bytes = tensor.total_bytes,
               self = self](coro_rpc::coro_rpc_client& client)
        -> async_simple::coro::Lazy<void> {
        std::string_view tensor_view(static_cast<const char*>(data_ptr),
                                     total_bytes);
        client.set_req_attachment(tensor_view);

        auto rpc_result =
            co_await client.call<&PairPattern::handleTensorMessage>(
                std::string_view(header.data(), header_size));

        if (!rpc_result.has_value()) {
            LOG(ERROR) << "PAIR tensor RPC failed: " << rpc_result.error().msg;
        }
    };
    auto result = co_await client_pools_->send_request(endpoint, std::move(op));

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

void PairPattern::handleMessage(coro_rpc::context<void> context) {
    auto* ctx_info = context.get_context_info();
    auto attachment =
        ctx_info ? ctx_info->get_request_attachment() : std::string_view{};
    LOG(INFO) << "PAIR: Received message, data size: " << attachment.size();

    auto decoded = MessageCodec::decodeMessage(attachment);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode PAIR message";
        context.response_msg();
        return;
    }

    if (receive_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        receive_callback_("", decoded->data, topic);
    }
    context.response_msg();
}

void PairPattern::handleTensorMessage(coro_rpc::context<void> context,
                                      std::string_view header_data) {
    LOG(INFO) << "PAIR: Received tensor message";

    auto decoded = MessageCodec::decodeTensorMessage(header_data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode PAIR tensor message";
        return;
    }

    if (tensor_callback_) {
        auto* ctx_info = context.get_context_info();
        auto attachment =
            ctx_info ? ctx_info->get_request_attachment() : std::string_view{};

        TensorInfo tensor = decoded->tensor;
        if (!attachment.empty()) {
            if (attachment.size() != tensor.total_bytes) {
                LOG(WARNING) << "Tensor attachment size mismatch: expected "
                             << tensor.total_bytes << " bytes, got "
                             << attachment.size() << " bytes";
            }
            tensor.data_ptr = attachment.data();  // valid during handler
        } else if (tensor.total_bytes != 0) {
            LOG(WARNING) << "PAIR tensor message missing attachment (expected "
                         << tensor.total_bytes << " bytes)";
        }

        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        tensor_callback_("", tensor, topic);
    }
}

}  // namespace mooncake
