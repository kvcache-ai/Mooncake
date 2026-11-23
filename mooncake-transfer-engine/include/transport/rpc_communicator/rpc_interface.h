#pragma once

#include <memory>
#include <string>
#include <vector>
#include <nanobind/nanobind.h>

namespace py = nanobind;

namespace mooncake {

class RpcInterface {
   public:
    struct ReceivedData {
        std::string source_address;
        std::string data;
        size_t data_size = 0;

        py::bytes getBytes() const {
            return py::bytes(data.data(), data.size());
        }
        py::memoryview getMemoryView() const {
            return py::memoryview(getBytes());
        }
    };

    struct ReceivedTensor {
        std::string source_address;
        std::string data;
        std::vector<size_t> shape;
        std::string dtype;
        size_t total_bytes = 0;
        size_t getDataSize() const { return data.size(); }
        py::bytes getDataAsBytes() const {
            return py::bytes(data.data(), data.size());
        }
        py::memoryview getMemoryView() const {
            return py::memoryview(getDataAsBytes());
        }
    };

    class Impl;

    RpcInterface();
    ~RpcInterface();

    // pool_size limits cached RPC client connections per target endpoint.
    bool initialize(const std::string& listen_address = "",
                    size_t thread_count = 0, size_t timeout_seconds = 30,
                    size_t pool_size = 100);

    // Convenience methods for common use cases
    bool initializeClient(size_t pool_size = 100, size_t timeout_seconds = 30);
    bool initializeServer(const std::string& listen_address,
                          size_t thread_count = 8, size_t timeout_seconds = 30);

    bool startServer();
    bool startServerAsync();
    bool startServerImpl(bool is_async = true);
    void stopServer();

    int sendData(const std::string& target_address, py::handle data);
    py::object sendDataAsync(const std::string& target_address, py::handle data,
                             py::handle loop);

    int sendTensor(const std::string& target_address, py::handle tensor);
    py::object sendTensorAsync(const std::string& target_address,
                               py::handle tensor, py::handle loop);

    void setDataReceiveCallback(py::callable callback);
    void setTensorReceiveCallback(py::callable callback);

   private:
    friend void bind_rpc_interface(py::module_& m);
    static int traversePythonReferences(PyObject* self, visitproc visit,
                                        void* arg);
    static int clearPythonReferences(PyObject* self);

    void shutdown();
    std::unique_ptr<Impl> impl_;
};

std::unique_ptr<RpcInterface> createRpcClient(uint64_t local_rank = 0,
                                              uint64_t world_size = 1);
std::unique_ptr<RpcInterface> createRpcServer(uint64_t local_rank = 0,
                                              uint64_t world_size = 1);

void bind_rpc_interface(py::module_& m);

}  // namespace mooncake
