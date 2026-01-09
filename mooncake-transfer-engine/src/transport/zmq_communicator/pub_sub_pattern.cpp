#include "pub_sub_pattern.h"
#include <glog/logging.h>

namespace mooncake {

PubSubPattern::PubSubPattern(
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>> client_pools,
    coro_rpc::coro_rpc_server* server,
    bool is_publisher
) : is_publisher_(is_publisher) {
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
        // SUB connects to publisher
        // Register handlers
        if (server_) {
            server_->register_handler<&PubSubPattern::handlePublish,
                                      &PubSubPattern::handleTensorPublish>(this);
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

bool PubSubPattern::matchesTopic(const std::string& received_topic) {
    std::lock_guard lock(mutex_);
    
    // Empty subscription means subscribe to all
    if (subscribed_topics_.empty()) {
        return true;
    }
    
    // Check prefix matching
    for (const auto& pattern : subscribed_topics_) {
        if (received_topic.find(pattern) == 0) {
            return true;
        }
    }
    
    return false;
}

async_simple::coro::Lazy<RpcResult> PubSubPattern::sendAsync(
    const std::string& target_endpoint,
    const void* data,
    size_t data_size,
    const std::optional<std::string>& topic
) {
    if (!is_publisher_) {
        LOG(ERROR) << "SUB socket cannot publish";
        co_return RpcResult{-1, "SUB socket cannot publish"};
    }
    
    if (!topic.has_value()) {
        LOG(ERROR) << "PUB requires a topic";
        co_return RpcResult{-1, "Topic required"};
    }
    
    // Encode message
    std::string message = MessageCodec::encodeDataMessage(
        ZmqSocketType::PUB, data, data_size, topic, 0
    );
    
    std::string_view message_view(message);
    
    // Get subscriber list
    std::vector<std::string> subscribers;
    {
        std::lock_guard lock(mutex_);
        if (target_endpoint.empty()) {
            subscribers = subscriber_endpoints_;
        } else {
            subscribers.push_back(target_endpoint);
        }
    }
    
    // Publish to all subscribers (fire-and-forget)
    int success_count = 0;
    for (const auto& endpoint : subscribers) {
        auto result = co_await client_pools_->send_request(
            endpoint,
            [message_view](coro_rpc::coro_rpc_client& client)
                -> async_simple::coro::Lazy<void> {
                
                client.set_req_attachment(message_view);
                auto rpc_result = co_await client.call<&PubSubPattern::handlePublish>(
                    std::string_view{}
                );
                
                if (!rpc_result.has_value()) {
                    LOG(WARNING) << "Publish failed to one subscriber";
                }
            }
        );
        
        if (result.has_value()) {
            success_count++;
        }
    }
    
    co_return RpcResult{success_count, "Published to " + std::to_string(success_count) + " subscribers"};
}

async_simple::coro::Lazy<int> PubSubPattern::sendTensorAsync(
    const std::string& target_endpoint,
    const TensorInfo& tensor,
    const std::optional<std::string>& topic
) {
    if (!is_publisher_) {
        LOG(ERROR) << "SUB socket cannot publish";
        co_return -1;
    }
    
    if (!topic.has_value()) {
        LOG(ERROR) << "PUB requires a topic";
        co_return -1;
    }
    
    // Encode tensor header
    std::string header = MessageCodec::encodeTensorMessage(
        ZmqSocketType::PUB, tensor, topic, 0
    );
    
    // Get subscriber list
    std::vector<std::string> subscribers;
    {
        std::lock_guard lock(mutex_);
        if (target_endpoint.empty()) {
            subscribers = subscriber_endpoints_;
        } else {
            subscribers.push_back(target_endpoint);
        }
    }
    
    // Publish to all subscribers
    int success_count = 0;
    for (const auto& endpoint : subscribers) {
        auto result = co_await client_pools_->send_request(
            endpoint,
            [header, tensor](coro_rpc::coro_rpc_client& client)
                -> async_simple::coro::Lazy<void> {
                
                std::string_view tensor_view(
                    static_cast<const char*>(tensor.data_ptr),
                    tensor.total_bytes
                );
                client.set_req_attachment(tensor_view);
                
                auto rpc_result = co_await client.call<&PubSubPattern::handleTensorPublish>(
                    std::string_view(header)
                );
                
                if (!rpc_result.has_value()) {
                    LOG(WARNING) << "Tensor publish failed to one subscriber";
                }
            }
        );
        
        if (result.has_value()) {
            success_count++;
        }
    }
    
    co_return success_count;
}

void PubSubPattern::setReceiveCallback(
    std::function<void(std::string_view, std::string_view,
                      const std::optional<std::string>&)> callback
) {
    receive_callback_ = callback;
}

void PubSubPattern::setTensorReceiveCallback(
    std::function<void(std::string_view, const TensorInfo&,
                      const std::optional<std::string>&)> callback
) {
    tensor_callback_ = callback;
}

void PubSubPattern::handlePublish(std::string_view data) {
    LOG(INFO) << "SUB: Received publish, data size: " << data.size();
    
    // Decode message
    auto decoded = MessageCodec::decodeMessage(data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode publish message";
        return;
    }
    
    // Check topic matching
    if (!matchesTopic(decoded->topic)) {
        LOG(INFO) << "Topic not matched: " << decoded->topic;
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

void PubSubPattern::handleTensorPublish(std::string_view header_data) {
    LOG(INFO) << "SUB: Received tensor publish";
    
    // Decode tensor header
    auto decoded = MessageCodec::decodeTensorMessage(header_data);
    if (!decoded) {
        LOG(ERROR) << "Failed to decode tensor publish";
        return;
    }
    
    // Check topic matching
    if (!matchesTopic(decoded->topic)) {
        LOG(INFO) << "Tensor topic not matched: " << decoded->topic;
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

