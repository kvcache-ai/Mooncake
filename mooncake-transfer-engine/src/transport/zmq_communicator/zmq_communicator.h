#pragma once

#include "zmq_types.h"
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace mooncake {

class BasePattern;

class ZmqCommunicator {
   public:
    ZmqCommunicator();
    ~ZmqCommunicator();

    // Initialize
    bool initialize(const ZmqConfig& config);
    void shutdown();

    // Socket management
    int createSocket(ZmqSocketType type);
    bool closeSocket(int socket_id);

    // Address binding and connection
    bool bind(int socket_id, const std::string& endpoint);
    bool connect(int socket_id, const std::string& endpoint);
    bool unbind(int socket_id, const std::string& endpoint);
    bool disconnect(int socket_id, const std::string& endpoint);

    // Socket options
    bool setSocketOption(int socket_id, ZmqSocketOption option, int64_t value);
    bool getSocketOption(int socket_id, ZmqSocketOption option, int64_t& value);
    bool setRoutingId(int socket_id, const std::string& routing_id);
    std::string getRoutingId(int socket_id);

    // Socket state queries
    bool isBound(int socket_id);
    bool isConnected(int socket_id);
    std::vector<std::string> getConnectedEndpoints(int socket_id);
    std::string getBoundEndpoint(int socket_id);
    ZmqSocketType getSocketType(int socket_id);

    // Send data
    async_simple::coro::Lazy<RpcResult> sendDataAsync(
        int socket_id, const void* data, size_t data_size,
        const std::optional<std::string>& topic = std::nullopt,
        const std::optional<std::string>& target_endpoint = std::nullopt);

    // Send tensor
    async_simple::coro::Lazy<int> sendTensorAsync(
        int socket_id, const TensorInfo& tensor,
        const std::optional<std::string>& topic = std::nullopt,
        const std::optional<std::string>& target_endpoint = std::nullopt);

    // Set callbacks
    void setReceiveCallback(
        int socket_id,
        std::function<void(std::string_view source, std::string_view data,
                           const std::optional<std::string>& topic)>
            callback);

    void setTensorReceiveCallback(
        int socket_id,
        std::function<void(std::string_view source, const TensorInfo& tensor,
                           const std::optional<std::string>& topic)>
            callback);

    // Pattern-specific operations
    bool subscribe(int socket_id, const std::string& topic);
    bool unsubscribe(int socket_id, const std::string& topic);

    // REP specific: send reply
    void sendReply(int socket_id, const void* data, size_t data_size);
    void sendReplyTensor(int socket_id, const TensorInfo& tensor);

    // Start server for a bound socket
    bool startServer(int socket_id);

   private:
    // Socket information
    struct SocketInfo {
        int id;
        ZmqSocketType type;
        std::string local_endpoint;
        std::vector<std::string> remote_endpoints;
        std::shared_ptr<BasePattern> pattern;
        bool is_bound = false;
        bool is_server_started = false;
        std::string routing_id;
        std::unordered_map<ZmqSocketOption, int64_t> options;
    };

    std::unordered_map<int, SocketInfo> sockets_;
    std::atomic<int> next_socket_id_{1};
    std::mutex sockets_mutex_;

    // Transport components
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;
    std::unordered_map<std::string, std::unique_ptr<coro_rpc::coro_rpc_server>>
        servers_;
    std::unordered_map<std::string, size_t> server_refcounts_;

    ZmqConfig config_;

    // Helper methods
    std::shared_ptr<BasePattern> createPattern(ZmqSocketType type,
                                               const std::string& endpoint);
    coro_rpc::coro_rpc_server* getOrCreateServer(const std::string& endpoint);
    SocketInfo* getSocketInfo(int socket_id);
};

}  // namespace mooncake
