#pragma once

#include <memory>
#include <string>
#include <vector>
#include <future>
#include <unordered_map>
#include <functional>
#include <pybind11/pybind11.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_io/client_pool.hpp>

namespace mooncake {

struct TensorInfo {
    void* data_ptr = nullptr;
    std::vector<size_t> shape;
    std::string dtype;
    size_t total_bytes = 0;
};

struct result {
    int code = 0;
    std::string err_msg;
};

struct Config {
    std::string listen_address;
    size_t thread_count = 0;
    size_t timeout_seconds = 30;
    size_t pool_size = 10;
};

template<typename T>
struct SimpleContext {
    coro_rpc::context<T> context_;
    void response_msg() { context_.response_msg(); }
};

class CoroRPCCommunicator {
public:
    class Impl {
    public:
        Config config;
        bool is_server_started = false;
        
        std::unique_ptr<coro_rpc::coro_rpc_server> server_;
        std::unordered_map<std::string, std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>> client_pools_;
        
        std::function<void(const std::string&, const std::string&)> data_receive_callback;
        
        void handleDataTransfer(coro_rpc::context<void> context, std::string_view data);
        void handleTensorTransfer(coro_rpc::context<void> context);
        void handleDataTransferWithAttachment(coro_rpc::context<void> context, std::string_view data);
        void handleTensorTransferWithAttachment(coro_rpc::context<void> context);
    };

    CoroRPCCommunicator();
    ~CoroRPCCommunicator();

    bool initialize(const Config& config);
    bool startServer();
    bool startServerAsync();
    void stopServer();
    
    bool addRemoteConnection(const std::string& remote_address);
    void removeRemoteConnection(const std::string& remote_address);
    bool isConnected(const std::string& remote_address);
    
    int sendData(const std::string& target_address, const void* data, size_t data_size);
    std::future<result> sendDataAsync(const std::string& target_address, const void* data, size_t data_size);
    
    int sendTensor(const std::string& target_address, const pybind11::object& tensor);
    std::future<int> sendTensorAsync(const std::string& target_address, const TensorInfo& tensor);
    
    int receiveData(const std::string& source_address, void* buffer, size_t buffer_size, int timeout_ms = -1);
    std::future<std::string> receiveDataAsync(const std::string& source_address, int timeout_ms = -1);

    void setDataReceiveCallback(std::function<void(const std::string&, const std::string&)> callback);

    std::shared_ptr<Impl> getImpl() { return impl_; }

private:
    std::shared_ptr<Impl> impl_;
};

std::unique_ptr<CoroRPCCommunicator> createClientPool(size_t pool_size = 10, size_t timeout_seconds = 30);
std::unique_ptr<CoroRPCCommunicator> createServer(const std::string& listen_address, size_t thread_count = 0);

} // namespace mooncake