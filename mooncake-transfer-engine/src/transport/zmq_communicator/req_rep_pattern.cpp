#include "req_rep_pattern.h"
#include <glog/logging.h>

namespace mooncake {

ReqRepPattern::ReqRepPattern(
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>> client_pools,
    coro_rpc::coro_rpc_server* server,
    bool is_requester
) : is_requester_(is_requester) {
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
    
    // Register RPC handlers
    if (server_) {
        server_->register_handler<&ReqRepPattern::handleRequest,
                                  &ReqRepPattern::handleTensorRequest>(this);
    }
    
    LOG(INFO) << "REP socket bound to " << endpoint;
    return true;
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
    const std::string& target_endpoint,
    const void* data,
    size_t data_size,
    const std::optional<std::string>& topic
) {
    if (!is_requester_) {
        LOG(ERROR) << "REP socket cannot send requests";
        co_return RpcResult{-1, "REP socket cannot send requests"};
    }
    
    // Generate sequence ID
    uint64_t seq_id = sequence_id_.fetch_add(1);
    
    // Encode message
    std::string message = MessageCodec::encodeDataMessage(
        ZmqSocketType::REQ, data, data_size, topic, seq_id
    );
    
    std::string_view message_view(message);
    const size_t ATTACHMENT_THRESHOLD = 1024;
    
    std::string endpoint = target_endpoint.empty() && !connected_endpoints_.empty()
        ? connected_endpoints_[0] : target_endpoint;
    
    auto result = co_await client_pools_->send_request(
        endpoint,
        [message, message_view, data_size, this](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<std::string> {
            
            if (data_size >= ATTACHMENT_THRESHOLD) {
                // Use attachment for large data
                client.set_req_attachment(message_view);
                auto rpc_result = co_await client.call<&ReqRepPattern::handleRequest>(
                    std::string_view{}
                );
                if (!rpc_result.has_value()) {
                    LOG(ERROR) << "RPC call failed: " << rpc_result.error().msg;
                    co_return std::string{};
                }
                co_return rpc_result.value();
            } else {
                // Use parameter for small data
                auto rpc_result = co_await client.call<&ReqRepPattern::handleRequest>(
                    message_view
                );
                if (!rpc_result.has_value()) {
                    LOG(ERROR) << "RPC call failed: " << rpc_result.error().msg;
                    co_return std::string{};
                }
                co_return rpc_result.value();
            }
        }
    );
    
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
    const std::string& target_endpoint,
    const TensorInfo& tensor,
    const std::optional<std::string>& topic
) {
    if (!is_requester_) {
        LOG(ERROR) << "REP socket cannot send requests";
        co_return -1;
    }
    
    uint64_t seq_id = sequence_id_.fetch_add(1);
    
    // Encode tensor message header
    std::string header = MessageCodec::encodeTensorMessage(
        ZmqSocketType::REQ, tensor, topic, seq_id
    );
    
    std::string endpoint = target_endpoint.empty() && !connected_endpoints_.empty()
        ? connected_endpoints_[0] : target_endpoint;
    
    auto result = co_await client_pools_->send_request(
        endpoint,
        [header, tensor](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<std::string> {
            
            // Set tensor data as attachment (zero-copy)
            std::string_view tensor_view(
                static_cast<const char*>(tensor.data_ptr),
                tensor.total_bytes
            );
            client.set_req_attachment(tensor_view);
            
            // Send header
            auto rpc_result = co_await client.call<&ReqRepPattern::handleTensorRequest>(
                std::string_view(header)
            );
            
            if (!rpc_result.has_value()) {
                LOG(ERROR) << "Tensor RPC failed: " << rpc_result.error().msg;
                co_return std::string{};
            }
            co_return rpc_result.value();
        }
    );
    
    co_return result.has_value() ? 0 : -1;
}

void ReqRepPattern::setReceiveCallback(
    std::function<void(std::string_view, std::string_view,
                      const std::optional<std::string>&)> callback
) {
    receive_callback_ = callback;
}

void ReqRepPattern::setTensorReceiveCallback(
    std::function<void(std::string_view, const TensorInfo&,
                      const std::optional<std::string>&)> callback
) {
    tensor_callback_ = callback;
}

std::string ReqRepPattern::handleRequest(std::string_view data) {
    LOG(INFO) << "REP: Handling request, data size: " << data.size();
    
    // Decode message
    auto decoded = MessageCodec::decodeMessage(data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode request";
        return "";
    }
    
    // Call user callback
    if (receive_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        receive_callback_("", decoded->data, topic);
    }
    
    // Wait for reply to be set
    std::lock_guard lock(reply_mutex_);
    std::string reply = pending_reply_;
    pending_reply_.clear();
    
    return reply;
}

std::string ReqRepPattern::handleTensorRequest(std::string_view header_data) {
    LOG(INFO) << "REP: Handling tensor request";
    
    // Decode tensor header
    auto decoded = MessageCodec::decodeTensorMessage(header_data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode tensor request";
        return "";
    }
    
    // Call user callback
    if (tensor_callback_) {
        std::optional<std::string> topic;
        if (!decoded->topic.empty()) {
            topic = decoded->topic;
        }
        tensor_callback_("", decoded->tensor, topic);
    }
    
    // Wait for reply
    std::lock_guard lock(reply_mutex_);
    std::string reply = pending_reply_;
    pending_reply_.clear();
    
    return reply;
}

void ReqRepPattern::sendReply(const void* data, size_t data_size) {
    std::lock_guard lock(reply_mutex_);
    pending_reply_ = MessageCodec::encodeDataMessage(
        ZmqSocketType::REP, data, data_size, std::nullopt, 0
    );
}

void ReqRepPattern::sendReplyTensor(const TensorInfo& tensor) {
    std::lock_guard lock(reply_mutex_);
    pending_reply_ = MessageCodec::encodeTensorMessage(
        ZmqSocketType::REP, tensor, std::nullopt, 0
    );
}

}  // namespace mooncake

