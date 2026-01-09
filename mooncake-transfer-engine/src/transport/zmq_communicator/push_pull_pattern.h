#pragma once

#include "base_pattern.h"
#include <atomic>
#include <mutex>

namespace mooncake {

class PushPullPattern : public BasePattern {
public:
    PushPullPattern(
        std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>> client_pools,
        coro_rpc::coro_rpc_server* server,
        bool is_pusher
    );
    
    ~PushPullPattern() override;
    
    // PUSH: round-robin to PULL endpoints
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
        return is_pusher_ ? ZmqSocketType::PUSH : ZmqSocketType::PULL;
    }
    
private:
    // RPC handlers (PULL side)
    void handlePush(std::string_view data);
    void handleTensorPush(std::string_view header_data);
    
    // Select next puller (round-robin)
    std::string selectNextPuller();
    
    bool is_pusher_;
    std::vector<std::string> puller_endpoints_;
    std::atomic<size_t> round_robin_index_{0};
    
    std::function<void(std::string_view, std::string_view,
                      const std::optional<std::string>&)> receive_callback_;
    std::function<void(std::string_view, const TensorInfo&,
                      const std::optional<std::string>&)> tensor_callback_;
    
    std::mutex mutex_;
};

}  // namespace mooncake

