#include "transport/rpc_communicator/rpc_interface.h"
#include "transport/rpc_communicator/rpc_communicator.h"
#include "config.h"
#include <memory>
#include <thread>
#include <future>
#include <vector>
#include <glog/logging.h>
#include "async_simple/coro/SyncAwait.h"

namespace mooncake {

static constexpr size_t MAX_TENSOR_DIMS = 4;
// Tensor metadata size: dtype (4 bytes) + ndim (4 bytes) + shape dimensions
// (MAX_TENSOR_DIMS * 8 bytes)
static constexpr size_t TENSOR_METADATA_SIZE = 4 + 4 + MAX_TENSOR_DIMS * 8;

// Implementation class
class RpcInterface::Impl {
   public:
    std::unique_ptr<RpcCommunicator> communicator;
    pybind11::function data_receive_callback;
    pybind11::function tensor_receive_callback;
};

// Constructor
RpcInterface::RpcInterface() : impl_(std::make_unique<Impl>()) {}

// Destructor
RpcInterface::~RpcInterface() = default;

// Initialize
bool RpcInterface::initialize(const std::string& local_address,
                              size_t thread_count, size_t timeout_seconds,
                              size_t pool_size) {
    RpcCommunicatorConfig config;
    config.listen_address = local_address;
    config.thread_count = thread_count;
    config.timeout_seconds = timeout_seconds;
    config.pool_size = pool_size;

    impl_->communicator = std::make_unique<RpcCommunicator>();
    return impl_->communicator->initialize(config);
}

// Convenience method for client initialization
bool RpcInterface::initializeClient(size_t pool_size, size_t timeout_seconds) {
    return initialize("", 0, timeout_seconds, pool_size);
}

// Convenience method for server initialization
bool RpcInterface::initializeServer(const std::string& listen_address,
                                    size_t thread_count,
                                    size_t timeout_seconds) {
    return initialize(listen_address, thread_count, timeout_seconds, 4);
}

bool RpcInterface::startServer() {
    if (!impl_->communicator) return false;
    return impl_->communicator->startServer();
}

bool RpcInterface::startServerAsync() {
    if (!impl_->communicator) return false;
    return impl_->communicator->startServerAsync();
}

bool RpcInterface::startServerImpl(bool is_async) {
    if (!impl_->communicator) return false;
    return impl_->communicator->startServerImpl(is_async);
}

void RpcInterface::stopServer() {
    if (impl_->communicator) {
        impl_->communicator->stopServer();
    }
}

int RpcInterface::sendData(const std::string& target_address,
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

pybind11::object RpcInterface::sendDataAsync(const std::string& target_address,
                                             pybind11::handle data,
                                             pybind11::handle loop) {
    pybind11::gil_scoped_acquire acquire;

    auto future_module = pybind11::module_::import("asyncio");
    auto future_obj = future_module.attr("Future")();

    if (!impl_->communicator) {
        auto exc_type =
            pybind11::module_::import("builtins").attr("RuntimeError");
        auto exc = exc_type(pybind11::str("Communicator not initialized"));
        future_obj.attr("set_exception")(exc);
        return future_obj;
    }

    auto communicator = impl_->communicator.get();
    std::string target_addr = target_address;

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
                    auto exc =
                        pybind11::type::handle_of<pybind11::runtime_error>()(
                            pybind11::str("Send data failed"));
                    future_obj.attr("set_exception")(exc);
                }
            };

            auto callback = pybind11::cpp_function(call_soon_threadsafe);
            loop.attr("call_soon_threadsafe")(callback);
        } catch (const std::exception& e) {
            auto call_soon_threadsafe = [future_obj, loop, e]() {
                pybind11::gil_scoped_acquire acquire;
                auto exc = pybind11::type::handle_of<RuntimeError>()(
                    pybind11::str(std::string("Send data error: ") + e.what()));
                future_obj.attr("set_exception")(exc);
            };

            auto callback = pybind11::cpp_function(call_soon_threadsafe);
            loop.attr("call_soon_threadsafe")(callback);
        }
    };

    auto lazy = coro_lambda();
    lazy.start([](auto&& result) {
        if (result.hasError()) {
            LOG(ERROR) << "Coroutine completed with error";
        }
    });

    return future_obj;
}

int RpcInterface::sendTensor(const std::string& target_address,
                             pybind11::handle tensor) {
    if (!impl_->communicator) return -1;

    try {
        TensorInfo tensor_info;
        {
            pybind11::gil_scoped_acquire acquire;
            pybind11::object tensor_obj =
                pybind11::reinterpret_borrow<pybind11::object>(tensor);

            // Validate tensor type using duck typing - check for required
            // attributes
            if (!pybind11::hasattr(tensor_obj, "data_ptr") ||
                !pybind11::hasattr(tensor_obj, "numel") ||
                !pybind11::hasattr(tensor_obj, "element_size") ||
                !pybind11::hasattr(tensor_obj, "shape") ||
                !pybind11::hasattr(tensor_obj, "dtype")) {
                LOG(ERROR) << "Input is not a valid tensor object (missing "
                              "required attributes)";
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

            // Format shape as string for logging
            std::string shape_str = "[";
            for (size_t i = 0; i < tensor_info.shape.size(); i++) {
                shape_str += std::to_string(tensor_info.shape[i]);
                if (i < tensor_info.shape.size() - 1) shape_str += ", ";
            }
            shape_str += "]";
            LOG(INFO) << "Sending tensor with shape: " << shape_str
                      << ", dtype: " << tensor_info.dtype
                      << ", tensor size: " << tensor_info.total_bytes
                      << " bytes";
        }

        // Use the async version which supports zero-copy via attachments
        pybind11::gil_scoped_release release;
        auto result = async_simple::coro::syncAwait(
            impl_->communicator->sendTensorAsync(target_address, tensor_info));
        return result;

    } catch (const std::exception& e) {
        LOG(ERROR) << "Send tensor error: " << e.what();
        return -1;
    }
}

pybind11::object RpcInterface::sendTensorAsync(
    const std::string& target_address, pybind11::handle tensor,
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
    std::string target_addr = target_address;

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

    // Keep a reference to the tensor object to avoid it being garbage collected
    pybind11::object py_tensor_obj = py_tensor;
    pybind11::gil_scoped_acquire acquire;
    auto coro_lambda = [communicator, target_addr, tensor_info, future_obj,
                        loop,
                        py_tensor_obj]() -> async_simple::coro::Lazy<void> {
        try {
            auto result = co_await communicator->sendTensorAsync(target_addr,
                                                                 tensor_info);

            auto call_soon_threadsafe = [future_obj, loop, result]() {
                pybind11::gil_scoped_acquire acquire;
                if (result >= 0) {
                    future_obj.attr("set_result")(result);
                } else {
                    auto exc =
                        pybind11::type::handle_of<pybind11::runtime_error>()(
                            pybind11::str("Send tensor failed"));
                    future_obj.attr("set_exception")(exc);
                }
            };

            auto callback = pybind11::cpp_function(call_soon_threadsafe);
            loop.attr("call_soon_threadsafe")(callback);
        } catch (const std::exception& e) {
            auto call_soon_threadsafe = [future_obj, loop, e]() {
                pybind11::gil_scoped_acquire acquire;
                auto runtime_error =
                    pybind11::module_::import("builtins").attr("RuntimeError");
                future_obj.attr("set_exception")(runtime_error(pybind11::str(
                    std::string("Send tensor error: ") + e.what())));

                auto callback = pybind11::cpp_function(call_soon_threadsafe);
                loop.attr("call_soon_threadsafe")(callback);
            }
        };

        // Start the coroutine in a detached manner (fire and forget)
        auto lazy = coro_lambda();
        lazy.start([](auto&& result) {
            // This callback will be called when the coroutine completes
            // We don't need to do anything here since the result is handled in
            // the coroutine itself
            if (result.hasError()) {
                // Log error if needed
                LOG(ERROR) << "Tensor coroutine completed with error";
            }
        });

        return future_obj;
    }

    void
    RpcInterface::setDataReceiveCallback(pybind11::function callback) {
        pybind11::gil_scoped_acquire acquire;
        impl_->data_receive_callback = callback;
        if (impl_->communicator) {
            auto interface_ptr = this;
            impl_->communicator->setDataReceiveCallback(
                [interface_ptr](std::string_view source,
                                std::string_view data) {
                    interface_ptr->handleIncomingData(source, data);
                });
        }
    }

    void RpcInterface::setTensorReceiveCallback(pybind11::function callback) {
        pybind11::gil_scoped_acquire acquire;
        impl_->tensor_receive_callback = callback;

        // Note: Tensor data is received through the regular data callback
        // The handleIncomingData function will detect tensor data and route it
        // to handleIncomingTensor automatically
    }

    void RpcInterface::handleIncomingData(std::string_view source,
                                          std::string_view data) {
        LOG(INFO) << "RpcInterface::handleIncomingData called with "
                  << data.size() << " bytes";

        // C++ tensor rebuilding
        if (data.size() >= TENSOR_METADATA_SIZE) {
            // Read the first few bytes to check if it looks like tensor
            // metadata
            const uint32_t* header =
                reinterpret_cast<const uint32_t*>(data.data());
            uint32_t dtype = header[0];
            uint32_t ndim = header[1];

            LOG(INFO) << "Checking tensor metadata: dtype=" << dtype
                      << ", ndim=" << ndim;

            // Basic validation: check if dtype and ndim are in reasonable
            // ranges
            if (dtype > 0 && dtype <= 9 && ndim <= 4) {
                LOG(INFO) << "Data recognized as tensor, calling "
                             "handleIncomingTensor";

                // This looks like tensor data, handle it as such
                std::vector<size_t> shape;
                const int64_t* shape_data =
                    reinterpret_cast<const int64_t*>(data.data() + 8);
                if (data.size() < TENSOR_METADATA_SIZE ||
                    data.size() < 8 + ndim * sizeof(int64_t)) {
                    LOG(WARNING)
                        << "Data too small for claimed tensor metadata";
                }
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

    void RpcInterface::handleIncomingTensor(
        std::string_view source, std::string_view data,
        const std::vector<size_t>& shape, std::string_view dtype) {
        LOG(INFO) << "RpcInterface::handleIncomingTensor called"
                  << " - source: " << source << ", data size: " << data.size()
                  << ", dtype: " << dtype << ", shape size: " << shape.size();

        if (!impl_->tensor_receive_callback) {
            LOG(WARNING) << "No tensor receive callback set!";
            return;
        }

        LOG(INFO) << "Calling Python tensor receive callback...";

        try {
            pybind11::gil_scoped_acquire acquire;

            ReceivedTensor received;
            received.source_address = std::string(source);
            received.data = std::string(data);
            received.shape = shape;
            received.dtype = std::string(dtype);

            impl_->tensor_receive_callback(received);
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error in tensor receive callback: " << e.what();
        }
    }

    // Factory functions for creating RPC client and server
    std::unique_ptr<RpcInterface> createRpcClient(uint64_t local_rank,
                                                  uint64_t world_size) {
        auto client = std::make_unique<RpcInterface>();
        // Initialize client with default settings
        client->initialize("", 0, 30, 10);
        return client;
    }

    std::unique_ptr<RpcInterface> createRpcServer(uint64_t local_rank,
                                                  uint64_t world_size) {
        auto server = std::make_unique<RpcInterface>();
        // Initialize server with default settings
        server->initialize("0.0.0.0:8080", 0, 30, 10);
        return server;
    }

    // Python binding implementation
    void bind_rpc_interface(pybind11::module_ & m) {
        namespace py = pybind11;
        using namespace mooncake;

        // Bind RpcInterface::ReceivedData
        py::class_<RpcInterface::ReceivedData>(m, "ReceivedData")
            .def_readonly("source_address",
                          &RpcInterface::ReceivedData::source_address)
            .def_readonly("data_size", &RpcInterface::ReceivedData::data_size)
            .def("get_bytes", &RpcInterface::ReceivedData::getBytes)
            .def("get_memory_view", &RpcInterface::ReceivedData::getMemoryView);

        // Bind RpcInterface::ReceivedTensor
        py::class_<RpcInterface::ReceivedTensor>(m, "ReceivedTensor")
            .def_readonly("source_address",
                          &RpcInterface::ReceivedTensor::source_address)
            .def_readonly("shape", &RpcInterface::ReceivedTensor::shape)
            .def_readonly("dtype", &RpcInterface::ReceivedTensor::dtype)
            .def_readonly("total_bytes",
                          &RpcInterface::ReceivedTensor::total_bytes)
            .def("get_data_size", &RpcInterface::ReceivedTensor::getDataSize)
            .def("get_data_as_bytes",
                 &RpcInterface::ReceivedTensor::getDataAsBytes)
            .def("get_memory_view",
                 &RpcInterface::ReceivedTensor::getMemoryView);

        // Bind RpcInterface
        py::class_<RpcInterface>(m, "RpcInterface")
            .def(py::init<>())
            .def("initialize", &RpcInterface::initialize,
                 py::arg("listen_address") = "", py::arg("thread_count") = 0,
                 py::arg("timeout_seconds") = 30, py::arg("pool_size") = 10)
            .def("initialize_client", &RpcInterface::initializeClient,
                 py::arg("pool_size") = 10, py::arg("timeout_seconds") = 30)
            .def("initialize_server", &RpcInterface::initializeServer,
                 py::arg("listen_address"), py::arg("thread_count") = 8,
                 py::arg("timeout_seconds") = 30)
            .def("start_server", &RpcInterface::startServer)
            .def("start_server_async", &RpcInterface::startServerAsync)
            .def("stop_server", &RpcInterface::stopServer)
            .def("send_data", &RpcInterface::sendData,
                 py::arg("target_address"), py::arg("data"))
            .def("send_data_async", &RpcInterface::sendDataAsync,
                 py::arg("target_address"), py::arg("data"), py::arg("loop"))
            .def("send_tensor", &RpcInterface::sendTensor,
                 py::arg("target_address"), py::arg("tensor"))
            .def("send_tensor_async", &RpcInterface::sendTensorAsync,
                 py::arg("target_address"), py::arg("tensor"), py::arg("loop"))
            .def("set_data_receive_callback",
                 &RpcInterface::setDataReceiveCallback)
            .def("set_tensor_receive_callback",
                 &RpcInterface::setTensorReceiveCallback);

        // Bind factory functions
        m.def("create_rpc_client", &createRpcClient, py::arg("local_rank") = 0,
              py::arg("world_size") = 1);
        m.def("create_rpc_server", &createRpcServer, py::arg("local_rank") = 0,
              py::arg("world_size") = 1);
    }

}  // namespace mooncake
