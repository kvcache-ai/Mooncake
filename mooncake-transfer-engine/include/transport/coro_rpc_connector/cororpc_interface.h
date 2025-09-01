#pragma once

#include <memory>
#include <string>
#include <vector>
#include <pybind11/pybind11.h>

namespace mooncake {

struct Config;

// Tensor data type enumeration
enum class TensorDtype : int {
    UNKNOWN = 0,
    FLOAT32 = 1,
    FLOAT64 = 2,
    INT32 = 3,
    INT64 = 4,
    INT8 = 5,
    INT16 = 6,
    UINT8 = 7,
    BOOL = 8
};

// Tensor metadata structure for serialization
struct TensorMetadata {
    int ndim = 0;           // Number of dimensions
    int shape[4] = {0};     // Shape array (max 4 dimensions)
    int dtype = 0;          // Data type as integer
    size_t total_size = 0;  // Total size in bytes
};

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
    pybind11::object sendDataAsync(const std::string& target_address,
                                   pybind11::bytes data,
                                   pybind11::handle loop);

    int sendTensor(const std::string& target_address, pybind11::handle tensor);
    pybind11::object sendTensorAsync(const std::string& target_address,
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

std::unique_ptr<CoroRPCInterface> createRPCClient(size_t pool_size = 10, size_t timeout_seconds = 30);
std::unique_ptr<CoroRPCInterface> createRPCServer(const std::string& listen_address, size_t thread_count = 0);

} // namespace mooncake

// Forward declaration for pybind11 integration
namespace pybind11 { class module_; }
void bind_coro_rpc_interface(pybind11::module_ &m);