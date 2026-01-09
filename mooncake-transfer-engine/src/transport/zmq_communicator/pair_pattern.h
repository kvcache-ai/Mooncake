#pragma once

#include "base_pattern.h"
#include <mutex>

namespace mooncake {

class PairPattern : public BasePattern {
public:
    PairPattern(
        std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>> client_pools,
        coro_rpc::coro_rpc_server* server
    );
    
    ~PairPattern() override;
    
    // Bidirectional send
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
    
    ZmqSocketType getType() const override { return ZmqSocketType::PAIR; }
    
private:
    // RPC handlers
    void handleMessage(std::string_view data);
    void handleTensorMessage(std::string_view header_data);
    
    std::string peer_endpoint_;
    bool is_connected_ = false;
    
    std::function<void(std::string_view, std::string_view,
                      const std::optional<std::string>&)> receive_callback_;
    std::function<void(std::string_view, const TensorInfo&,
                      const std::optional<std::string>&)> tensor_callback_;
    
    std::mutex mutex_;
};

}  // namespace mooncake

