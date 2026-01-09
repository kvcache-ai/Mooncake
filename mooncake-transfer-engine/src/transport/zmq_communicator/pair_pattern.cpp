#include "pair_pattern.h"
#include <glog/logging.h>

namespace mooncake {

PairPattern::PairPattern(
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>> client_pools,
    coro_rpc::coro_rpc_server* server
) {
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
    
    // Register RPC handlers
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
    const std::string& target_endpoint,
    const void* data,
    size_t data_size,
    const std::optional<std::string>& topic
) {
    std::string endpoint;
    {
        std::lock_guard lock(mutex_);
        endpoint = target_endpoint.empty() ? peer_endpoint_ : target_endpoint;
    }
    
    if (endpoint.empty()) {
        LOG(ERROR) << "PAIR socket not connected";
        co_return RpcResult{-1, "Not connected"};
    }
    
    // Encode message
    std::string message = MessageCodec::encodeDataMessage(
        ZmqSocketType::PAIR, data, data_size, topic, 0
    );
    
    std::string_view message_view(message);
    
    auto result = co_await client_pools_->send_request(
        endpoint,
        [message_view](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<void> {
            
            client.set_req_attachment(message_view);
            auto rpc_result = co_await client.call<&PairPattern::handleMessage>(
                std::string_view{}
            );
            
            if (!rpc_result.has_value()) {
                LOG(ERROR) << "PAIR send RPC failed: " << rpc_result.error().msg;
            }
        }
    );
    
    RpcResult res;
    res.code = result.has_value() ? 0 : -1;
    res.message = result.has_value() ? "Sent successfully" : "Send failed";
    
    co_return res;
}

async_simple::coro::Lazy<int> PairPattern::sendTensorAsync(
    const std::string& target_endpoint,
    const TensorInfo& tensor,
    const std::optional<std::string>& topic
) {
    std::string endpoint;
    {
        std::lock_guard lock(mutex_);
        endpoint = target_endpoint.empty() ? peer_endpoint_ : target_endpoint;
    }
    
    if (endpoint.empty()) {
        LOG(ERROR) << "PAIR socket not connected";
        co_return -1;
    }
    
    // Encode tensor header
    std::string header = MessageCodec::encodeTensorMessage(
        ZmqSocketType::PAIR, tensor, topic, 0
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
            
            auto rpc_result = co_await client.call<&PairPattern::handleTensorMessage>(
                std::string_view(header)
            );
            
            if (!rpc_result.has_value()) {
                LOG(ERROR) << "PAIR tensor RPC failed: " << rpc_result.error().msg;
            }
        }
    );
    
    co_return result.has_value() ? 0 : -1;
}

void PairPattern::setReceiveCallback(
    std::function<void(std::string_view, std::string_view,
                      const std::optional<std::string>&)> callback
) {
    receive_callback_ = callback;
}

void PairPattern::setTensorReceiveCallback(
    std::function<void(std::string_view, const TensorInfo&,
                      const std::optional<std::string>&)> callback
) {
    tensor_callback_ = callback;
}

void PairPattern::handleMessage(std::string_view data) {
    LOG(INFO) << "PAIR: Received message, data size: " << data.size();
    
    // Decode message
    auto decoded = MessageCodec::decodeMessage(data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode PAIR message";
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

void PairPattern::handleTensorMessage(std::string_view header_data) {
    LOG(INFO) << "PAIR: Received tensor message";
    
    // Decode tensor header
    auto decoded = MessageCodec::decodeTensorMessage(header_data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode PAIR tensor message";
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

