#pragma once

#include "base_pattern.h"
#include <atomic>
#include <mutex>
#include <optional>

namespace mooncake {

class ReqRepPattern : public BasePattern {
public:
    ReqRepPattern(
        std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>> client_pools,
        coro_rpc::coro_rpc_server* server,
        bool is_requester
    );
    
    ~ReqRepPattern() override;
    
    // Send request and wait for reply (REQ)
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
        return is_requester_ ? ZmqSocketType::REQ : ZmqSocketType::REP;
    }
    
    // REP specific: send reply
    void sendReply(const void* data, size_t data_size);
    void sendReplyTensor(const TensorInfo& tensor);
    
private:
    // RPC handlers (REP side)
    std::string handleRequest(std::string_view data);
    std::string handleTensorRequest(std::string_view header_data);
    
    bool is_requester_;
    std::string bound_endpoint_;
    std::vector<std::string> connected_endpoints_;
    
    std::function<void(std::string_view, std::string_view,
                      const std::optional<std::string>&)> receive_callback_;
    std::function<void(std::string_view, const TensorInfo&,
                      const std::optional<std::string>&)> tensor_callback_;
    
    std::atomic<uint64_t> sequence_id_{0};
    
    // For REP: store reply data
    std::string pending_reply_;
    std::mutex reply_mutex_;
};

}  // namespace mooncake

