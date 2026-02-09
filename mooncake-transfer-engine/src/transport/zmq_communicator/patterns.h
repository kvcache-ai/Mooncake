#pragma once

#include "base_pattern.h"
#include <atomic>
#include <mutex>
#include <optional>
#include <set>

namespace mooncake {

// ============================================================================
// REQ/REP Pattern
// ============================================================================

class ReqRepPattern : public BasePattern {
   public:
    ReqRepPattern(
        std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
            client_pools,
        coro_rpc::coro_rpc_server* server, bool is_requester);

    ~ReqRepPattern() override;

    async_simple::coro::Lazy<RpcResult> sendAsync(
        const std::string& target_endpoint, const void* data, size_t data_size,
        const std::optional<std::string>& topic = std::nullopt) override;

    async_simple::coro::Lazy<int> sendTensorAsync(
        const std::string& target_endpoint, const TensorInfo& tensor,
        const std::optional<std::string>& topic = std::nullopt) override;

    void setReceiveCallback(
        std::function<void(std::string_view, std::string_view,
                           const std::optional<std::string>&)>
            callback) override;

    void setTensorReceiveCallback(
        std::function<void(std::string_view, const TensorInfo&,
                           const std::optional<std::string>&)>
            callback) override;

    bool bind(const std::string& endpoint) override;
    bool connect(const std::string& endpoint) override;

    ZmqSocketType getType() const override {
        return is_requester_ ? ZmqSocketType::REQ : ZmqSocketType::REP;
    }

    void registerHandlers(coro_rpc::coro_rpc_server* server);
    void sendReply(const void* data, size_t data_size);
    void sendReplyTensor(const TensorInfo& tensor);

   private:
    std::string handleRequest(std::string_view data);
    void handleTensorRequest(coro_rpc::context<void> context,
                             std::string_view header_data);

    bool is_requester_;
    std::string bound_endpoint_;
    std::vector<std::string> connected_endpoints_;

    std::function<void(std::string_view, std::string_view,
                       const std::optional<std::string>&)>
        receive_callback_;
    std::function<void(std::string_view, const TensorInfo&,
                       const std::optional<std::string>&)>
        tensor_callback_;

    std::atomic<uint64_t> sequence_id_{0};
};

// ============================================================================
// PUB/SUB Pattern
// ============================================================================

class PubSubPattern : public BasePattern {
   public:
    PubSubPattern(
        std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
            client_pools,
        coro_rpc::coro_rpc_server* server, bool is_publisher);

    ~PubSubPattern() override;

    async_simple::coro::Lazy<RpcResult> sendAsync(
        const std::string& target_endpoint, const void* data, size_t data_size,
        const std::optional<std::string>& topic = std::nullopt) override;

    async_simple::coro::Lazy<int> sendTensorAsync(
        const std::string& target_endpoint, const TensorInfo& tensor,
        const std::optional<std::string>& topic = std::nullopt) override;

    void setReceiveCallback(
        std::function<void(std::string_view, std::string_view,
                           const std::optional<std::string>&)>
            callback) override;

    void setTensorReceiveCallback(
        std::function<void(std::string_view, const TensorInfo&,
                           const std::optional<std::string>&)>
            callback) override;

    bool bind(const std::string& endpoint) override;
    bool connect(const std::string& endpoint) override;

    ZmqSocketType getType() const override {
        return is_publisher_ ? ZmqSocketType::PUB : ZmqSocketType::SUB;
    }

    bool subscribe(const std::string& topic);
    bool unsubscribe(const std::string& topic);

   private:
    void handlePublish(coro_rpc::context<void> context);
    void handleTensorPublish(coro_rpc::context<void> context,
                             std::string_view header_data);
    bool matchesTopic(std::string_view received_topic);

    bool is_publisher_;
    std::vector<std::string> subscriber_endpoints_;
    std::set<std::string> subscribed_topics_;

    std::function<void(std::string_view, std::string_view,
                       const std::optional<std::string>&)>
        receive_callback_;
    std::function<void(std::string_view, const TensorInfo&,
                       const std::optional<std::string>&)>
        tensor_callback_;

    size_t high_water_mark_ = 1000;
    std::mutex mutex_;
};

// ============================================================================
// PUSH/PULL Pattern
// ============================================================================

class PushPullPattern : public BasePattern {
   public:
    PushPullPattern(
        std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
            client_pools,
        coro_rpc::coro_rpc_server* server, bool is_pusher);

    ~PushPullPattern() override;

    async_simple::coro::Lazy<RpcResult> sendAsync(
        const std::string& target_endpoint, const void* data, size_t data_size,
        const std::optional<std::string>& topic = std::nullopt) override;

    async_simple::coro::Lazy<int> sendTensorAsync(
        const std::string& target_endpoint, const TensorInfo& tensor,
        const std::optional<std::string>& topic = std::nullopt) override;

    void setReceiveCallback(
        std::function<void(std::string_view, std::string_view,
                           const std::optional<std::string>&)>
            callback) override;

    void setTensorReceiveCallback(
        std::function<void(std::string_view, const TensorInfo&,
                           const std::optional<std::string>&)>
            callback) override;

    bool bind(const std::string& endpoint) override;
    bool connect(const std::string& endpoint) override;

    ZmqSocketType getType() const override {
        return is_pusher_ ? ZmqSocketType::PUSH : ZmqSocketType::PULL;
    }

   private:
    void handlePush(std::string_view data);
    void handleTensorPush(coro_rpc::context<void> context,
                          std::string_view header_data);
    std::string selectNextPuller();

    bool is_pusher_;
    std::vector<std::string> puller_endpoints_;
    std::atomic<size_t> round_robin_index_{0};

    std::function<void(std::string_view, std::string_view,
                       const std::optional<std::string>&)>
        receive_callback_;
    std::function<void(std::string_view, const TensorInfo&,
                       const std::optional<std::string>&)>
        tensor_callback_;

    std::mutex mutex_;
};

// ============================================================================
// PAIR Pattern
// ============================================================================

class PairPattern : public BasePattern {
   public:
    PairPattern(
        std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
            client_pools,
        coro_rpc::coro_rpc_server* server);

    ~PairPattern() override;

    async_simple::coro::Lazy<RpcResult> sendAsync(
        const std::string& target_endpoint, const void* data, size_t data_size,
        const std::optional<std::string>& topic = std::nullopt) override;

    async_simple::coro::Lazy<int> sendTensorAsync(
        const std::string& target_endpoint, const TensorInfo& tensor,
        const std::optional<std::string>& topic = std::nullopt) override;

    void setReceiveCallback(
        std::function<void(std::string_view, std::string_view,
                           const std::optional<std::string>&)>
            callback) override;

    void setTensorReceiveCallback(
        std::function<void(std::string_view, const TensorInfo&,
                           const std::optional<std::string>&)>
            callback) override;

    bool bind(const std::string& endpoint) override;
    bool connect(const std::string& endpoint) override;

    ZmqSocketType getType() const override { return ZmqSocketType::PAIR; }

   private:
    void handleMessage(std::string_view data);
    void handleTensorMessage(coro_rpc::context<void> context,
                             std::string_view header_data);

    std::string peer_endpoint_;
    bool is_bound_ = false;
    bool is_connected_ = false;

    std::function<void(std::string_view, std::string_view,
                       const std::optional<std::string>&)>
        receive_callback_;
    std::function<void(std::string_view, const TensorInfo&,
                       const std::optional<std::string>&)>
        tensor_callback_;

    std::mutex mutex_;
};

}  // namespace mooncake
