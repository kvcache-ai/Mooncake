#pragma once

#include <memory>
#include <string>
#include <vector>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

namespace mooncake {

struct Config;

class CoroRPCInterface {
   public:
    struct ReceivedData {
        std::string source_address;
        std::string data;
        size_t data_size = 0;

        pybind11::bytes getBytes() const { return pybind11::bytes(data); }
        pybind11::memoryview getMemoryView() const {
            return pybind11::memoryview::from_memory(
                const_cast<char*>(data.data()), data.size(), true);
        }
    };

    struct ReceivedTensor {
        std::string source_address;
        std::string data;
        std::vector<size_t> shape;
        std::string dtype;
        size_t total_bytes = 0;
        size_t getDataSize() const { return data.size(); }
        pybind11::bytes getDataAsBytes() const { return pybind11::bytes(data); }
        pybind11::memoryview getMemoryView() const {
            return pybind11::memoryview::from_memory(
                const_cast<char*>(data.data()), data.size(), true);
        }
    };

    class Impl;

    CoroRPCInterface();
    ~CoroRPCInterface();

    bool initialize(const std::string& listen_address = "",
                    size_t thread_count = 0, size_t timeout_seconds = 30,
                    size_t pool_size = 10);

    // Convenience methods for common use cases
    bool initializeClient(size_t pool_size = 10, size_t timeout_seconds = 30);
    bool initializeServer(const std::string& listen_address, 
                         size_t thread_count = 8, size_t timeout_seconds = 30);

    bool startServer();
    bool startServerAsync();
    bool startServerImpl(bool is_async = true);
    void stopServer();

    int sendData(const std::string& target_address, pybind11::handle data);
    pybind11::object sendDataAsync(std::string& target_address,
                                   pybind11::handle data,
                                   pybind11::handle loop);

    int sendTensor(const std::string& target_address, pybind11::handle tensor);
    pybind11::object sendTensorAsync(std::string& target_address,
                                     pybind11::handle tensor,
                                     pybind11::handle loop);

    void setDataReceiveCallback(pybind11::function callback);
    void setTensorReceiveCallback(pybind11::function callback);

    void handleIncomingData(std::string_view source_address,
                            std::string_view data);
    void handleIncomingTensor(std::string_view source_address,
                              std::string_view data,
                              const std::vector<size_t>& shape,
                              std::string_view dtype);

   private:
    std::unique_ptr<Impl> impl_;
};

std::unique_ptr<CoroRPCInterface> createRPCClient(uint64_t local_rank = 0,
                                                  uint64_t world_size = 1);
std::unique_ptr<CoroRPCInterface> createRPCServer(uint64_t local_rank = 0,
                                                  uint64_t world_size = 1);

}  // namespace mooncake

namespace pybind11 {
class module_;
}
void bind_coro_rpc_interface(pybind11::module_& m);