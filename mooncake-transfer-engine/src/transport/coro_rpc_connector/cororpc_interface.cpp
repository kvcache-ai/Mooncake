#include "transport/coro_rpc_connector/cororpc_interface.h"
#include "transport/coro_rpc_connector/cororpc_communicator.h"
#include <iostream>
#include <memory>
#include <thread>
#include <future>
#include <vector>
#include <glog/logging.h>
#include "async_simple/coro/SyncAwait.h"

namespace mooncake {

// Implementation class
class CoroRPCInterface::Impl {
   public:
    std::unique_ptr<CoroRPCCommunicator> communicator;
    pybind11::function data_receive_callback;
    pybind11::function tensor_receive_callback;
};

// Constructor
CoroRPCInterface::CoroRPCInterface() : impl_(std::make_unique<Impl>()) {}

// Destructor
CoroRPCInterface::~CoroRPCInterface() = default;

// Initialize
bool CoroRPCInterface::initialize(const std::string& local_address,
                                  size_t thread_count, size_t timeout_seconds,
                                  size_t pool_size) {
    Config config;
    config.listen_address = local_address;
    config.thread_count = thread_count;
    config.timeout_seconds = timeout_seconds;
    config.pool_size = pool_size;

    impl_->communicator = std::make_unique<CoroRPCCommunicator>();
    return impl_->communicator->initialize(config);
}

bool CoroRPCInterface::startServer() {
    if (!impl_->communicator) return false;
    return impl_->communicator->startServer();
}

bool CoroRPCInterface::startServerAsync() {
    if (!impl_->communicator) return false;
    return impl_->communicator->startServerAsync();
}

void CoroRPCInterface::stopServer() {
    if (impl_->communicator) {
        impl_->communicator->stopServer();
    }
}

int CoroRPCInterface::sendData(const std::string& target_address,
                               pybind11::handle data) {
    if (!impl_->communicator) return -1;

    pybind11::gil_scoped_acquire acquire;

    // Extract data from handle directly
    std::string_view data_view;
    try {
        // Try to get direct string view from bytes object
        pybind11::bytes data_bytes =
            pybind11::reinterpret_borrow<pybind11::bytes>(data);
        data_view = data_bytes;
    } catch (...) {
        // Fallback: convert to bytes and then get view
        pybind11::bytes data_bytes = pybind11::cast<pybind11::bytes>(data);
        data_view = data_bytes;
    }

    pybind11::gil_scoped_release release;
    return impl_->communicator->sendData(target_address, data_view.data(),
                                         data_view.size());
}

pybind11::object CoroRPCInterface::sendDataAsync(std::string& target_address,
                                                 pybind11::handle data,
                                                 pybind11::handle loop) {
    pybind11::gil_scoped_acquire acquire;

    auto future_module = pybind11::module_::import("asyncio");
    auto future_obj = future_module.attr("Future")();

    if (!impl_->communicator) {
        future_obj.attr("set_exception")(pybind11::make_tuple(
            pybind11::str("Communicator not initialized")));
        return future_obj;
    }

    auto communicator = impl_->communicator.get();
    auto target_addr = std::move(target_address);

    // Extract data from handle directly without creating intermediate bytes
    // object
    std::string_view data_view;
    std::string data_str;  // Only used if we can't get direct view

    try {
        // Try to get direct string view from bytes object
        pybind11::bytes data_bytes =
            pybind11::reinterpret_borrow<pybind11::bytes>(data);
        data_view = data_bytes;
        // Store a copy for lambda capture since string_view might not be valid
        // after GIL release
        data_str = std::string(data_view);
    } catch (...) {
        // Fallback: convert to bytes and then to string
        pybind11::bytes data_bytes = pybind11::cast<pybind11::bytes>(data);
        data_str = data_bytes;
    }

    // Release GIL before starting coroutine
    pybind11::gil_scoped_release release;

    auto coro_lambda = [communicator, target_addr, data_str, future_obj,
                        loop]() -> async_simple::coro::Lazy<void> {
        try {
            auto result_struct = co_await communicator->sendDataAsync(
                target_addr, data_str.data(), data_str.size());
            int result = result_struct.code;

            auto call_soon_threadsafe = [future_obj, loop, result]() {
                pybind11::gil_scoped_acquire acquire;
                if (result >= 0) {
                    future_obj.attr("set_result")(result);
                } else {
                    future_obj.attr("set_exception")(pybind11::make_tuple(
                        pybind11::str("Send data failed")));
                }
            };

            auto callback = pybind11::cpp_function(call_soon_threadsafe);
            loop.attr("call_soon_threadsafe")(callback);
        } catch (const std::exception& e) {
            auto call_soon_threadsafe = [future_obj, loop, e]() {
                pybind11::gil_scoped_acquire acquire;
                future_obj.attr("set_exception")(
                    pybind11::make_tuple(pybind11::str(
                        std::string("Send data error: ") + e.what())));
            };

            auto callback = pybind11::cpp_function(call_soon_threadsafe);
            loop.attr("call_soon_threadsafe")(callback);
        }
    };

    auto lazy = coro_lambda();
    lazy.start([](auto&& result) {
        if (result.hasError()) {
            std::cerr << "Coroutine completed with error";
        }
    });

    return future_obj;
}

int CoroRPCInterface::sendTensor(const std::string& target_address,
                                 pybind11::handle tensor) {
    if (!impl_->communicator) return -1;

    try {
        TensorInfo tensor_info;

        {
            pybind11::gil_scoped_acquire acquire;
            pybind11::object tensor_obj =
                pybind11::reinterpret_borrow<pybind11::object>(tensor);

            // Validate tensor type
            if (!(tensor_obj.attr("__class__")
                      .attr("__name__")
                      .cast<std::string>()
                      .find("Tensor") != std::string::npos)) {
                std::cerr << "Input is not a tensor";
                return -1;
            }

            // Extract tensor properties - zero copy, just get pointers and
            // metadata
            uintptr_t data_ptr =
                tensor_obj.attr("data_ptr")().cast<uintptr_t>();
            size_t numel = tensor_obj.attr("numel")().cast<size_t>();
            size_t element_size =
                tensor_obj.attr("element_size")().cast<size_t>();
            size_t tensor_size = numel * element_size;

            // Get tensor shape
            pybind11::object shape_obj = tensor_obj.attr("shape");
            pybind11::tuple shape_tuple =
                pybind11::cast<pybind11::tuple>(shape_obj);
            std::vector<size_t> shape;
            for (size_t i = 0; i < shape_tuple.size(); i++) {
                shape.push_back(shape_tuple[i].cast<size_t>());
            }

            // Get tensor dtype string
            pybind11::object dtype_obj = tensor_obj.attr("dtype");
            std::string dtype = dtype_obj.attr("__str__")().cast<std::string>();

            // Fill TensorInfo structure (no data copying, just metadata)
            tensor_info.data_ptr = reinterpret_cast<void*>(data_ptr);
            tensor_info.total_bytes = tensor_size;
            tensor_info.shape = std::move(shape);
            tensor_info.dtype = std::move(dtype);

            std::cout << "Sending tensor with shape: [";
            for (size_t i = 0; i < tensor_info.shape.size(); i++) {
                std::cout << tensor_info.shape[i];
                if (i < tensor_info.shape.size() - 1) std::cout << ", ";
            }
            std::cout << "] and dtype: " << tensor_info.dtype
                      << ", tensor size: " << tensor_info.total_bytes
                      << " bytes";
        }

        // Use the async version which supports zero-copy via attachments
        pybind11::gil_scoped_release release;
        auto result = async_simple::coro::syncAwait(
            impl_->communicator->sendTensorAsync(target_address, tensor_info));
        return result;

    } catch (const std::exception& e) {
        std::cerr << "Send tensor error: " << e.what();
        return -1;
    }
}

pybind11::object CoroRPCInterface::sendTensorAsync(std::string& target_address,
                                                   pybind11::handle tensor,
                                                   pybind11::handle loop) {
    pybind11::gil_scoped_acquire acquire;

    auto future_module = pybind11::module_::import("asyncio");
    auto future_obj = future_module.attr("Future")();

    if (!impl_->communicator) {
        future_obj.attr("set_exception")(pybind11::make_tuple(
            pybind11::str("Communicator not initialized")));
        return future_obj;
    }

    auto communicator = impl_->communicator.get();
    std::string target_addr = std::move(target_address);

    // Extract tensor info
    pybind11::object tensor_obj =
        pybind11::reinterpret_borrow<pybind11::object>(tensor);
    uintptr_t data_ptr = tensor_obj.attr("data_ptr")().cast<uintptr_t>();
    size_t numel = tensor_obj.attr("numel")().cast<size_t>();
    size_t element_size = tensor_obj.attr("element_size")().cast<size_t>();
    size_t tensor_size = numel * element_size;

    // Get tensor shape and dtype
    pybind11::object shape_obj = tensor_obj.attr("shape");
    pybind11::tuple shape_tuple = pybind11::cast<pybind11::tuple>(shape_obj);
    std::vector<size_t> shape;
    for (size_t i = 0; i < shape_tuple.size(); i++) {
        shape.push_back(shape_tuple[i].cast<size_t>());
    }

    pybind11::object dtype_obj = tensor_obj.attr("dtype");
    std::string dtype = dtype_obj.attr("__str__")().cast<std::string>();

    // Create TensorInfo on stack (no need for shared_ptr)
    TensorInfo tensor_info;
    tensor_info.data_ptr = reinterpret_cast<void*>(data_ptr);
    tensor_info.total_bytes = tensor_size;
    tensor_info.shape = std::move(shape);
    tensor_info.dtype = std::move(dtype);

    pybind11::gil_scoped_release release;

    // Schedule coroutine to run asynchronously
    auto coro_lambda = [communicator, target_addr, tensor_info, future_obj,
                        loop]() -> async_simple::coro::Lazy<void> {
        try {
            auto result = co_await communicator->sendTensorAsync(target_addr,
                                                                 tensor_info);

            auto call_soon_threadsafe = [future_obj, loop, result]() {
                pybind11::gil_scoped_acquire acquire;
                if (result >= 0) {
                    future_obj.attr("set_result")(result);
                } else {
                    future_obj.attr("set_exception")(pybind11::make_tuple(
                        pybind11::str("Send tensor failed")));
                }
            };

            auto callback = pybind11::cpp_function(call_soon_threadsafe);
            loop.attr("call_soon_threadsafe")(callback);
        } catch (const std::exception& e) {
            auto call_soon_threadsafe = [future_obj, loop, e]() {
                pybind11::gil_scoped_acquire acquire;
                future_obj.attr("set_exception")(
                    pybind11::make_tuple(pybind11::str(
                        std::string("Send tensor error: ") + e.what())));
            };

            auto callback = pybind11::cpp_function(call_soon_threadsafe);
            loop.attr("call_soon_threadsafe")(callback);
        }
    };

    // Start the coroutine in a detached manner (fire and forget)
    auto lazy = coro_lambda();
    lazy.start([](auto&& result) {
        // This callback will be called when the coroutine completes
        // We don't need to do anything here since the result is handled in the
        // coroutine itself
        if (result.hasError()) {
            // Log error if needed
            std::cerr << "Tensor coroutine completed with error";
        }
    });

    return future_obj;
}

void CoroRPCInterface::setDataReceiveCallback(pybind11::function callback) {
    pybind11::gil_scoped_acquire acquire;
    impl_->data_receive_callback = callback;
    if (impl_->communicator) {
        auto interface_ptr = this;
        impl_->communicator->setDataReceiveCallback(
            [interface_ptr](std::string_view source, std::string_view data) {
                interface_ptr->handleIncomingData(source, data);
            });
    }
}

void CoroRPCInterface::setTensorReceiveCallback(pybind11::function callback) {
    pybind11::gil_scoped_acquire acquire;
    impl_->tensor_receive_callback = callback;

    // Note: Tensor data is received through the regular data callback
    // The handleIncomingData function will detect tensor data and route it
    // to handleIncomingTensor automatically
}

void CoroRPCInterface::handleIncomingData(std::string_view source,
                                          std::string_view data) {
    std::cout << "CoroRPCInterface::handleIncomingData called with "
              << data.size() << " bytes";

    // C++ tensor rebuilding
    if (data.size() >= 72) {  // 72 bytes is our metadata size
        // Read the first few bytes to check if it looks like tensor metadata
        const uint32_t* header = reinterpret_cast<const uint32_t*>(data.data());
        uint32_t dtype = header[0];
        uint32_t ndim = header[1];

        std::cout << "Checking tensor metadata: dtype=" << dtype
                  << ", ndim=" << ndim;

        // Basic validation: check if dtype and ndim are in reasonable ranges
        if (dtype > 0 && dtype <= 9 && ndim >= 0 && ndim <= 4) {
            std::cout
                << "Data recognized as tensor, calling handleIncomingTensor";

            // This looks like tensor data, handle it as such
            std::vector<size_t> shape;
            const int64_t* shape_data =
                reinterpret_cast<const int64_t*>(data.data() + 8);
            for (int i = 0; i < static_cast<int>(ndim); i++) {
                if (shape_data[i] > 0) {
                    shape.push_back(static_cast<size_t>(shape_data[i]));
                }
            }

            // Get dtype name based on dtype ID
            std::string_view dtype_name;
            switch (dtype) {
                case 1:
                    dtype_name = "float16";
                    break;
                case 2:
                    dtype_name = "float32";
                    break;
                case 3:
                    dtype_name = "float64";
                    break;
                case 4:
                    dtype_name = "int8";
                    break;
                case 5:
                    dtype_name = "int16";
                    break;
                case 6:
                    dtype_name = "int32";
                    break;
                case 7:
                    dtype_name = "int64";
                    break;
                case 8:
                    dtype_name = "uint8";
                    break;
                case 9:
                    dtype_name = "bool";
                    break;
                default:
                    dtype_name = "unknown";
                    break;
            }

            // Call tensor handler instead of data handler
            handleIncomingTensor(source, data, shape, dtype_name);
            return;
        }
    }

    // Handle as regular data if not tensor data
    if (!impl_->data_receive_callback) return;

    try {
        pybind11::gil_scoped_acquire acquire;
        pybind11::dict received;
        received["source"] =
            std::string(source);  // Convert to string for Python
        received["data"] = pybind11::bytes(
            std::string(data));  // Convert to string for pybind11::bytes

        impl_->data_receive_callback(received);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Error in data receive callback: " << e.what();
    }
}

void CoroRPCInterface::handleIncomingTensor(std::string_view source,
                                            std::string_view data,
                                            const std::vector<size_t>& shape,
                                            std::string_view dtype) {
    std::cout << "CoroRPCInterface::handleIncomingTensor called";
    std::cout << "  source: " << source;
    std::cout << "  data size: " << data.size();
    std::cout << "  dtype: " << dtype;
    std::cout << "  shape size: " << shape.size();

    if (!impl_->tensor_receive_callback) {
        std::cout << "No tensor receive callback set!";
        return;
    }

    std::cout << "Calling Python tensor receive callback...";

    try {
        pybind11::gil_scoped_acquire acquire;

        ReceivedTensor received;
        received.source_address = std::string(source);
        received.data = std::string(data);
        received.shape = shape;
        received.dtype = std::string(dtype);

        impl_->tensor_receive_callback(received);
    } catch (const std::exception& e) {
        std::cerr << "Error in tensor receive callback: " << e.what();
    }
}

// Factory functions for creating RPC client and server
std::unique_ptr<CoroRPCInterface> createRPCClient(uint64_t local_rank,
                                                  uint64_t world_size) {
    auto client = std::make_unique<CoroRPCInterface>();
    // Initialize client with default settings
    client->initialize("", 0, 30, 10);
    return client;
}

std::unique_ptr<CoroRPCInterface> createRPCServer(uint64_t local_rank,
                                                  uint64_t world_size) {
    auto server = std::make_unique<CoroRPCInterface>();
    // Initialize server with default settings
    server->initialize("0.0.0.0:8080", 0, 30, 10);
    return server;
}

}  // namespace mooncake
