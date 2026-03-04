#pragma once

#include <csignal>
#include <memory>
#include <string>
#include <vector>
#include <future>
#include <unordered_map>
#include <functional>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_io/coro_io.hpp>
#include <async_simple/coro/Lazy.h>
#include "config.h"

namespace mooncake {

struct TensorInfo {
    void* data_ptr = nullptr;
    std::vector<size_t> shape;
    std::string dtype;
    size_t total_bytes = 0;
};

struct RpcResult {
    int code = 0;
    std::string err_msg;
};

class RpcCommunicator {
   public:
    RpcCommunicator();
    ~RpcCommunicator();

    bool initialize(const RpcCommunicatorConfig& config);
    bool startServerImpl(bool is_async = true);
    bool startServer();
    bool startServerAsync();
    void stopServer();

    int sendData(const std::string& target_address, const void* data,
                 size_t data_size);
    async_simple::coro::Lazy<RpcResult> sendDataAsync(
        const std::string& target_address, const void* data, size_t data_size);

    int sendTensor(const std::string& target_address,
                   const pybind11::object& tensor);
    async_simple::coro::Lazy<int> sendTensorAsync(
        const std::string& target_address, const TensorInfo& tensor);

    int receiveData(const std::string& source_address, void* buffer,
                    size_t buffer_size, int timeout_ms = -1);
    async_simple::coro::Lazy<std::string> receiveDataAsync(
        const std::string& source_address, int timeout_ms = -1);

    void setDataReceiveCallback(
        std::function<void(std::string_view, std::string_view)> callback);

   private:
    // Handler methods for RPC calls
    void handleDataTransfer(coro_rpc::context<void> context,
                            std::string_view data);
    void handleTensorTransfer(coro_rpc::context<void> context);
    void handleDataTransferWithAttachment(coro_rpc::context<void> context,
                                          std::string_view data);
    void handleTensorTransferWithAttachment(coro_rpc::context<void> context);

    // Implementation members
    RpcCommunicatorConfig config_;
    bool is_server_started_ = false;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::function<void(std::string_view, std::string_view)>
        data_receive_callback_;
    pybind11::handle py_callback_;
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;
};

}  // namespace mooncake