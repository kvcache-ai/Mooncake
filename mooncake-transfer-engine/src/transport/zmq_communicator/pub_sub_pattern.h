#pragma once

#include "base_pattern.h"
#include <set>
#include <mutex>

namespace mooncake {

class PubSubPattern : public BasePattern {
public:
    PubSubPattern(
        std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>> client_pools,
        coro_rpc::coro_rpc_server* server,
        bool is_publisher
    );
    
    ~PubSubPattern() override;
    
    // PUB: publish to all subscribers
    async_simple::coro::Lazy<RpcResult> sendAsync(
        const std::string& target_endpoint,
        const void* data,
        size_t data_size,
        const std::optional<std::string>& topic = std::nullopt
    ) override;
    
    async_simple::coro::Lazy<int> sendTensorAsync(
        const std::string& target_endpoint,
        const TensorInfo& tensor,
        const std::optional<std::string>& topic = std::nullopt
    ) override;
    
    void setReceiveCallback(
        std::function<void(std::string_view, std::string_view,
                          const std::optional<std::string>&)> callback
    ) override;
    
    void setTensorReceiveCallback(
        std::function<void(std::string_view, const TensorInfo&,
                          const std::optional<std::string>&)> callback
    ) override;
    
    bool bind(const std::string& endpoint) override;
    bool connect(const std::string& endpoint) override;
    
    ZmqSocketType getType() const override {
        return is_publisher_ ? ZmqSocketType::PUB : ZmqSocketType::SUB;
    }
    
    // SUB specific: subscribe to topic
    bool subscribe(const std::string& topic);
    bool unsubscribe(const std::string& topic);
    
private:
    // RPC handlers (SUB side)
    void handlePublish(std::string_view data);
    void handleTensorPublish(std::string_view header_data);
    
    // Check if topic matches subscription
    bool matchesTopic(const std::string& received_topic);
    
    bool is_publisher_;
    std::vector<std::string> subscriber_endpoints_;
    std::set<std::string> subscribed_topics_;
    
    std::function<void(std::string_view, std::string_view,
                      const std::optional<std::string>&)> receive_callback_;
    std::function<void(std::string_view, const TensorInfo&,
                      const std::optional<std::string>&)> tensor_callback_;
    
    size_t high_water_mark_ = 1000;
    std::mutex mutex_;
};

}  // namespace mooncake

