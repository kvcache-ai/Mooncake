#include "transport/coro_rpc_connector/cororpc_interface.h"
#include "transport/coro_rpc_connector/cororpc_communicator.h"
#include <iostream>
#include <memory>
#include <thread>
#include <future>
#include <vector>
#include "async_simple/coro/SyncAwait.h"

namespace mooncake {

// Tensor dtype enumeration
enum class TensorDtype : int32_t {
    UNKNOWN = 0,
    FLOAT16 = 1,
    FLOAT32 = 2,
    FLOAT64 = 3,
    INT8 = 4,
    INT16 = 5,
    INT32 = 6,
    INT64 = 7,
    UINT8 = 8,
    BOOL = 9
};

// Tensor metadata structure
struct TensorMetadata {
    int32_t dtype;     // TensorDtype enum value
    int32_t ndim;      // Number of dimensions
    int64_t shape[4];  // Shape array (max 4D)
    char padding[32];  // For future extensions
};

// Implementation class
class CoroRPCInterface::Impl {
   public:
    std::unique_ptr<CoroRPCCommunicator> communicator;
    pybind11::function data_receive_callback;
    pybind11::function tensor_receive_callback;
};

// Helper function to get tensor dtype from Python tensor
TensorDtype get_tensor_dtype(const pybind11::object& dtype_obj) {
    std::string dtype_str = dtype_obj.attr("__str__")().cast<std::string>();

    if (dtype_str.find("float16") != std::string::npos)
        return TensorDtype::FLOAT16;
    if (dtype_str.find("float32") != std::string::npos)
        return TensorDtype::FLOAT32;
    if (dtype_str.find("float64") != std::string::npos)
        return TensorDtype::FLOAT64;
    if (dtype_str.find("int8") != std::string::npos) return TensorDtype::INT8;
    if (dtype_str.find("int16") != std::string::npos) return TensorDtype::INT16;
    if (dtype_str.find("int32") != std::string::npos) return TensorDtype::INT32;
    if (dtype_str.find("int64") != std::string::npos) return TensorDtype::INT64;
    if (dtype_str.find("uint8") != std::string::npos) return TensorDtype::UINT8;
    if (dtype_str.find("bool") != std::string::npos) return TensorDtype::BOOL;

    return TensorDtype::UNKNOWN;
}

size_t get_dtype_size(TensorDtype dtype) {
    switch (dtype) {
        case TensorDtype::FLOAT32:
            return 4;
        case TensorDtype::FLOAT64:
            return 8;
        case TensorDtype::INT32:
            return 4;
        case TensorDtype::INT64:
            return 8;
        case TensorDtype::INT8:
            return 1;
        case TensorDtype::UINT8:
            return 1;
        case TensorDtype::FLOAT16:
            return 2;
        case TensorDtype::INT16:
            return 2;
        case TensorDtype::BOOL:
            return 1;
        default:
            return 0;
    }
}

// Helper function to create numpy array from data
pybind11::object create_numpy_array_from_data(
    const char* data, TensorDtype dtype, const std::vector<int64_t>& shape) {
    std::cout << "DEBUG: create_numpy_array_from_data called" << std::endl;
    std::cout << "DEBUG: dtype = " << static_cast<int>(dtype) << std::endl;
    std::cout << "DEBUG: shape size = " << shape.size() << std::endl;

    pybind11::gil_scoped_acquire acquire;

    std::cout << "DEBUG: About to import numpy..." << std::endl;
    pybind11::module_ np = pybind11::module_::import("numpy");
    std::cout << "DEBUG: Successfully imported numpy" << std::endl;

    std::string np_dtype;
    switch (dtype) {
        case TensorDtype::FLOAT32:
            np_dtype = "float32";
            break;
        case TensorDtype::FLOAT64:
            np_dtype = "float64";
            break;
        case TensorDtype::INT32:
            np_dtype = "int32";
            break;
        case TensorDtype::INT64:
            np_dtype = "int64";
            break;
        case TensorDtype::INT8:
            np_dtype = "int8";
            break;
        case TensorDtype::UINT8:
            np_dtype = "uint8";
            break;
        case TensorDtype::FLOAT16:
            np_dtype = "float16";
            break;
        case TensorDtype::INT16:
            np_dtype = "int16";
            break;
        case TensorDtype::BOOL:
            np_dtype = "bool";
            break;
        default:
            throw std::runtime_error("Unknown tensor dtype");
    }

    std::cout << "DEBUG: np_dtype = " << np_dtype << std::endl;

    size_t element_size = get_dtype_size(dtype);
    size_t total_elements = 1;
    for (int64_t dim : shape) {
        total_elements *= dim;
    }

    std::cout << "DEBUG: element_size = " << element_size << std::endl;
    std::cout << "DEBUG: total_elements = " << total_elements << std::endl;

    // Use memoryview to avoid data copy
    std::cout << "DEBUG: Creating memory view without copying..." << std::endl;
    size_t data_size = total_elements * element_size;
    std::cout << "DEBUG: Data size = " << data_size << std::endl;

    std::cout << "DEBUG: About to call frombuffer with memoryview..."
              << std::endl;

    try {
        // Create a memoryview directly from the data pointer without copying
        pybind11::memoryview mv = pybind11::memoryview::from_memory(
            const_cast<char*>(data), data_size, true);  // read-only
        std::cout << "DEBUG: Created memoryview without copying" << std::endl;

        pybind11::object array =
            np.attr("frombuffer")(mv, pybind11::arg("dtype") = np_dtype);
        std::cout << "DEBUG: Created array from memoryview successfully"
                  << std::endl;

        // Convert shape to tuple manually
        pybind11::tuple shape_tuple = pybind11::tuple(shape.size());
        for (size_t i = 0; i < shape.size(); ++i) {
            shape_tuple[i] = shape[i];
        }
        std::cout << "DEBUG: About to create shape tuple for reshape"
                  << std::endl;

        pybind11::object result = array.attr("reshape")(shape_tuple);
        std::cout << "DEBUG: Reshaped array successfully" << std::endl;

        return result;
    } catch (const std::exception& e) {
        std::cout << "DEBUG: Exception in numpy operations: " << e.what()
                  << std::endl;
        throw;
    }
}

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

    auto future_ptr = std::make_shared<pybind11::object>(future_obj);
    pybind11::object loop_obj =
        pybind11::reinterpret_borrow<pybind11::object>(loop);

    // Release GIL before starting coroutine
    pybind11::gil_scoped_release release;

    auto coro_lambda = [communicator, target_addr, data_str, future_ptr,
                        loop_obj]() -> async_simple::coro::Lazy<void> {
        try {
            auto result_struct = co_await communicator->sendDataAsync(
                target_addr, data_str.data(), data_str.size());
            int result = result_struct.code;

            auto call_soon_threadsafe = [future_ptr, loop_obj, result]() {
                pybind11::gil_scoped_acquire acquire;
                if (result >= 0) {
                    future_ptr->attr("set_result")(result);
                } else {
                    future_ptr->attr("set_exception")(pybind11::make_tuple(
                        pybind11::str("Send data failed")));
                }
            };

            auto callback = pybind11::cpp_function(call_soon_threadsafe);
            loop_obj.attr("call_soon_threadsafe")(callback);
        } catch (const std::exception& e) {
            auto call_soon_threadsafe = [future_ptr, loop_obj, e]() {
                pybind11::gil_scoped_acquire acquire;
                future_ptr->attr("set_exception")(
                    pybind11::make_tuple(pybind11::str(
                        std::string("Send data error: ") + e.what())));
            };

            auto callback = pybind11::cpp_function(call_soon_threadsafe);
            loop_obj.attr("call_soon_threadsafe")(callback);
        }
    };

    auto lazy = coro_lambda();
    lazy.start([](auto&& result) {
        if (result.hasError()) {
            std::cerr << "Coroutine completed with error" << std::endl;
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
                std::cerr << "Input is not a tensor" << std::endl;
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
                      << " bytes" << std::endl;
        }

        // Use the async version which supports zero-copy via attachments
        pybind11::gil_scoped_release release;
        auto result = async_simple::coro::syncAwait(
            impl_->communicator->sendTensorAsync(target_address, tensor_info));
        return result;

    } catch (const std::exception& e) {
        std::cerr << "Send tensor error: " << e.what() << std::endl;
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

    auto future_ptr = std::make_shared<pybind11::object>(future_obj);
    pybind11::object loop_obj =
        pybind11::reinterpret_borrow<pybind11::object>(loop);

    pybind11::gil_scoped_release release;

    // Schedule coroutine to run asynchronously
    auto coro_lambda = [communicator, target_addr, tensor_info, future_ptr,
                        loop_obj]() -> async_simple::coro::Lazy<void> {
        try {
            auto result = co_await communicator->sendTensorAsync(target_addr,
                                                                 tensor_info);

            auto call_soon_threadsafe = [future_ptr, loop_obj, result]() {
                pybind11::gil_scoped_acquire acquire;
                if (result >= 0) {
                    future_ptr->attr("set_result")(result);
                } else {
                    future_ptr->attr("set_exception")(pybind11::make_tuple(
                        pybind11::str("Send tensor failed")));
                }
            };

            auto callback = pybind11::cpp_function(call_soon_threadsafe);
            loop_obj.attr("call_soon_threadsafe")(callback);
        } catch (const std::exception& e) {
            auto call_soon_threadsafe = [future_ptr, loop_obj, e]() {
                pybind11::gil_scoped_acquire acquire;
                future_ptr->attr("set_exception")(
                    pybind11::make_tuple(pybind11::str(
                        std::string("Send tensor error: ") + e.what())));
            };

            auto callback = pybind11::cpp_function(call_soon_threadsafe);
            loop_obj.attr("call_soon_threadsafe")(callback);
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
            std::cerr << "Tensor coroutine completed with error" << std::endl;
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
              << data.size() << " bytes" << std::endl;

    // Check if this is tensor data by looking for metadata signature
    if (data.size() >= sizeof(TensorMetadata)) {
        const TensorMetadata* metadata =
            reinterpret_cast<const TensorMetadata*>(data.data());

        std::cout << "Checking tensor metadata: dtype=" << metadata->dtype
                  << ", ndim=" << metadata->ndim << std::endl;

        // Basic validation: check if dtype is in valid range
        if (metadata->dtype > 0 &&
            metadata->dtype <= static_cast<int32_t>(TensorDtype::BOOL) &&
            metadata->ndim >= 0 && metadata->ndim <= 4) {
            std::cout
                << "Data recognized as tensor, calling handleIncomingTensor"
                << std::endl;

            // This looks like tensor data, handle it as such
            std::vector<size_t> shape;
            for (int i = 0; i < metadata->ndim; i++) {
                if (metadata->shape[i] > 0) {
                    shape.push_back(static_cast<size_t>(metadata->shape[i]));
                }
            }

            // Get dtype name
            std::string_view dtype_name;
            switch (static_cast<TensorDtype>(metadata->dtype)) {
                case TensorDtype::FLOAT16:
                    dtype_name = "float16";
                    break;
                case TensorDtype::FLOAT32:
                    dtype_name = "float32";
                    break;
                case TensorDtype::FLOAT64:
                    dtype_name = "float64";
                    break;
                case TensorDtype::INT8:
                    dtype_name = "int8";
                    break;
                case TensorDtype::INT16:
                    dtype_name = "int16";
                    break;
                case TensorDtype::INT32:
                    dtype_name = "int32";
                    break;
                case TensorDtype::INT64:
                    dtype_name = "int64";
                    break;
                case TensorDtype::UINT8:
                    dtype_name = "uint8";
                    break;
                case TensorDtype::BOOL:
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
        std::cerr << "Error in data receive callback: " << e.what()
                  << std::endl;
    }
}

void CoroRPCInterface::handleIncomingTensor(std::string_view source,
                                            std::string_view data,
                                            const std::vector<size_t>& shape,
                                            std::string_view dtype) {
    std::cout << "CoroRPCInterface::handleIncomingTensor called" << std::endl;
    std::cout << "  source: " << source << std::endl;
    std::cout << "  data size: " << data.size() << std::endl;
    std::cout << "  dtype: " << dtype << std::endl;
    std::cout << "  shape size: " << shape.size() << std::endl;

    if (!impl_->tensor_receive_callback) {
        std::cout << "No tensor receive callback set!" << std::endl;
        return;
    }

    std::cout << "Calling Python tensor receive callback..." << std::endl;

    try {
        pybind11::gil_scoped_acquire acquire;

        ReceivedTensor received;
        received.source_address =
            std::string(source);            // Convert to string for storage
        received.data = std::string(data);  // Convert to string for storage
        received.shape = shape;
        received.dtype = std::string(dtype);  // Convert to string for storage

        impl_->tensor_receive_callback(received);
    } catch (const std::exception& e) {
        std::cerr << "Error in tensor receive callback: " << e.what()
                  << std::endl;
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

// Implementation of ReceivedTensor::rebuildTensor
pybind11::object CoroRPCInterface::ReceivedTensor::rebuildTensor() const {
    // Check if this is tensor data by looking for metadata signature
    if (data.size() >= sizeof(TensorMetadata)) {
        const TensorMetadata* metadata =
            reinterpret_cast<const TensorMetadata*>(data.data());

        // Basic validation: check if dtype is in valid range
        if (metadata->dtype > 0 &&
            metadata->dtype <= static_cast<int32_t>(TensorDtype::BOOL) &&
            metadata->ndim >= 0 && metadata->ndim <= 4) {
            // Extract tensor data (skip metadata)
            const char* tensor_data = data.data() + sizeof(TensorMetadata);
            // size_t tensor_data_size = data.size() - sizeof(TensorMetadata);
            // // Not used currently

            // Convert shape from metadata
            std::vector<int64_t> tensor_shape;
            for (int i = 0; i < metadata->ndim; i++) {
                if (metadata->shape[i] > 0) {
                    tensor_shape.push_back(metadata->shape[i]);
                }
            }

            // Create numpy array from tensor data
            TensorDtype tensor_dtype =
                static_cast<TensorDtype>(metadata->dtype);
            return create_numpy_array_from_data(tensor_data, tensor_dtype,
                                                tensor_shape);
        }
    }

    // If not tensor data or invalid, return None
    return pybind11::none();
}

}  // namespace mooncake
