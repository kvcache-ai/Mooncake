#pragma once

#include "zmq_types.h"
#include "message_codec.h"
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_io/client_pool.hpp>
#include "async_simple/coro/Lazy.h"

namespace mooncake {

class BasePattern : public std::enable_shared_from_this<BasePattern> {
   public:
    virtual ~BasePattern() = default;

    // Send data asynchronously
    virtual async_simple::coro::Lazy<RpcResult> sendAsync(
        const std::string& target_endpoint, const void* data, size_t data_size,
        const std::optional<std::string>& topic = std::nullopt) = 0;

    // Send tensor asynchronously
    virtual async_simple::coro::Lazy<int> sendTensorAsync(
        const std::string& target_endpoint, const TensorInfo& tensor,
        const std::optional<std::string>& topic = std::nullopt) = 0;

    // Set data receive callback
    virtual void setReceiveCallback(
        std::function<void(std::string_view source, std::string_view data,
                           const std::optional<std::string>& topic)>
            callback) = 0;

    // Set tensor receive callback
    virtual void setTensorReceiveCallback(
        std::function<void(std::string_view source, const TensorInfo& tensor,
                           const std::optional<std::string>& topic)>
            callback) = 0;

    // Bind to local address
    virtual bool bind(const std::string& endpoint) = 0;

    // Connect to remote address
    virtual bool connect(const std::string& endpoint) = 0;

    // Get socket type
    virtual ZmqSocketType getType() const = 0;

   protected:
    // Shared transport components
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;
    coro_rpc::coro_rpc_server* server_ = nullptr;
};

}  // namespace mooncake
