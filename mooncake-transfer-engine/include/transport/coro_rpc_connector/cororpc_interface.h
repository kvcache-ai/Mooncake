#pragma once

#include <memory>
#include <string>
#include <vector>
#include <pybind11/pybind11.h>

namespace mooncake {

struct Config;

class CoroRPCInterface {
public:
    struct ReceivedData {
        std::string source_address;
        std::string data;
        size_t data_size = 0;
        
        pybind11::bytes getBytes() const {
            return pybind11::bytes(data);
        }
    };
    
    struct ReceivedTensor {
        std::string source_address;
        std::string data;
        std::vector<size_t> shape;
        std::string dtype;
        size_t total_bytes = 0;
        
        pybind11::object rebuildTensor() const;
        
        // Safe method to get data size without triggering string decoding
        size_t getDataSize() const { return data.size(); }
        
        // Safe method to get data as bytes
        pybind11::bytes getDataAsBytes() const { 
            return pybind11::bytes(data); 
        }
        
    private:
        pybind11::object rebuildTensorInternal() const;
    };

    class Impl;

    CoroRPCInterface();
    ~CoroRPCInterface();

    // 初始化
    bool initialize(const std::string& listen_address = "",
                   size_t thread_count = 0,
                   size_t timeout_seconds = 30,
                   size_t pool_size = 10);

    bool startServer();
    bool startServerAsync();
    void stopServer();

    bool addRemoteConnection(const std::string& remote_address);
    void removeRemoteConnection(const std::string& remote_address);
    bool isConnected(const std::string& remote_address);

    int sendData(const std::string& target_address, pybind11::bytes data);
    pybind11::object sendDataAsync(std::string& target_address,
                                   pybind11::bytes data,
                                   pybind11::handle loop);

    int sendTensor(const std::string& target_address, pybind11::handle tensor);
    pybind11::object sendTensorAsync(std::string& target_address,
                                     pybind11::handle tensor,
                                     pybind11::handle loop);

    void setDataReceiveCallback(pybind11::function callback);
    void setTensorReceiveCallback(pybind11::function callback);

    void handleIncomingData(const std::string& source_address,
                           const std::string& data);
    void handleIncomingTensor(const std::string& source_address,
                             const std::string& data,
                             const std::vector<size_t>& shape,
                             const std::string& dtype);

private:
    std::unique_ptr<Impl> impl_;
};

std::unique_ptr<CoroRPCInterface> createRPCClient(uint64_t local_rank = 0, uint64_t world_size = 1);
std::unique_ptr<CoroRPCInterface> createRPCServer(uint64_t local_rank = 0, uint64_t world_size = 1);

} // namespace mooncake

// Forward declaration for pybind11 integration
namespace pybind11 { class module_; }
void bind_coro_rpc_interface(pybind11::module_ &m);