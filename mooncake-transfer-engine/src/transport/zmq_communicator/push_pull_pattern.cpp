#include "push_pull_pattern.h"
#include <glog/logging.h>

namespace mooncake {

PushPullPattern::PushPullPattern(
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>> client_pools,
    coro_rpc::coro_rpc_server* server,
    bool is_pusher
) : is_pusher_(is_pusher) {
    client_pools_ = client_pools;
    server_ = server;
}

PushPullPattern::~PushPullPattern() = default;

bool PushPullPattern::bind(const std::string& endpoint) {
    if (is_pusher_) {
        LOG(ERROR) << "PUSH socket cannot bind";
        return false;
    }
    
    // Register RPC handlers
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
    const std::string& target_endpoint,
    const void* data,
    size_t data_size,
    const std::optional<std::string>& topic
) {
    if (!is_pusher_) {
        LOG(ERROR) << "PULL socket cannot push";
        co_return RpcResult{-1, "PULL socket cannot push"};
    }
    
    // Select target endpoint (round-robin if not specified)
    std::string endpoint = target_endpoint.empty() 
        ? selectNextPuller() : target_endpoint;
    
    if (endpoint.empty()) {
        LOG(ERROR) << "No PULL endpoints available";
        co_return RpcResult{-1, "No PULL endpoints"};
    }
    
    // Encode message
    std::string message = MessageCodec::encodeDataMessage(
        ZmqSocketType::PUSH, data, data_size, topic, 0
    );
    
    std::string_view message_view(message);
    
    auto result = co_await client_pools_->send_request(
        endpoint,
        [message_view](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<void> {
            
            client.set_req_attachment(message_view);
            auto rpc_result = co_await client.call<&PushPullPattern::handlePush>(
                std::string_view{}
            );
            
            if (!rpc_result.has_value()) {
                LOG(ERROR) << "Push RPC failed: " << rpc_result.error().msg;
            }
        }
    );
    
    RpcResult res;
    res.code = result.has_value() ? 0 : -1;
    res.message = result.has_value() ? "Pushed successfully" : "Push failed";
    
    co_return res;
}

async_simple::coro::Lazy<int> PushPullPattern::sendTensorAsync(
    const std::string& target_endpoint,
    const TensorInfo& tensor,
    const std::optional<std::string>& topic
) {
    if (!is_pusher_) {
        LOG(ERROR) << "PULL socket cannot push";
        co_return -1;
    }
    
    // Select target endpoint
    std::string endpoint = target_endpoint.empty()
        ? selectNextPuller() : target_endpoint;
    
    if (endpoint.empty()) {
        LOG(ERROR) << "No PULL endpoints available";
        co_return -1;
    }
    
    // Encode tensor header
    std::string header = MessageCodec::encodeTensorMessage(
        ZmqSocketType::PUSH, tensor, topic, 0
    );
    
    auto result = co_await client_pools_->send_request(
        endpoint,
        [header, tensor](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<void> {
            
            std::string_view tensor_view(
                static_cast<const char*>(tensor.data_ptr),
                tensor.total_bytes
            );
            client.set_req_attachment(tensor_view);
            
            auto rpc_result = co_await client.call<&PushPullPattern::handleTensorPush>(
                std::string_view(header)
            );
            
            if (!rpc_result.has_value()) {
                LOG(ERROR) << "Tensor push RPC failed: " << rpc_result.error().msg;
            }
        }
    );
    
    co_return result.has_value() ? 0 : -1;
}

void PushPullPattern::setReceiveCallback(
    std::function<void(std::string_view, std::string_view,
                      const std::optional<std::string>&)> callback
) {
    receive_callback_ = callback;
}

void PushPullPattern::setTensorReceiveCallback(
    std::function<void(std::string_view, const TensorInfo&,
                      const std::optional<std::string>&)> callback
) {
    tensor_callback_ = callback;
}

void PushPullPattern::handlePush(std::string_view data) {
    LOG(INFO) << "PULL: Received push, data size: " << data.size();
    
    // Decode message
    auto decoded = MessageCodec::decodeMessage(data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode push message";
        return;
    }
    
    // Call user callback
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
    
    // Decode tensor header
    auto decoded = MessageCodec::decodeTensorMessage(header_data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode tensor push";
        return;
    }
    
    // Call user callback
    if (tensor_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        tensor_callback_("", decoded->tensor, topic);
    }
}

}  // namespace mooncake

