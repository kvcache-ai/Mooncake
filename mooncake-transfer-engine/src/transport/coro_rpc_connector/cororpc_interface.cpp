#include "transport/coro_rpc_connector/cororpc_interface.h"
#include "transport/coro_rpc_connector/cororpc_communicator.h"
#include <iostream>
#include <memory>
#include <thread>
#include <future>
#include <vector>

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
    int32_t dtype;   // TensorDtype enum value
    int32_t ndim;    // Number of dimensions
    int64_t shape[4]; // Shape array (max 4D)
    char padding[32]; // For future extensions
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
    
    if (dtype_str.find("float16") != std::string::npos) return TensorDtype::FLOAT16;
    if (dtype_str.find("float32") != std::string::npos) return TensorDtype::FLOAT32;
    if (dtype_str.find("float64") != std::string::npos) return TensorDtype::FLOAT64;
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
        case TensorDtype::FLOAT32: return 4;
        case TensorDtype::FLOAT64: return 8;
        case TensorDtype::INT32: return 4;
        case TensorDtype::INT64: return 8;
        case TensorDtype::INT8: return 1;
        case TensorDtype::UINT8: return 1;
        case TensorDtype::FLOAT16: return 2;
        case TensorDtype::INT16: return 2;
        case TensorDtype::BOOL: return 1;
        default: return 0;
    }
}

// Helper function to create numpy array from data
pybind11::object create_numpy_array_from_data(const char* data, TensorDtype dtype, 
                                               const std::vector<int64_t>& shape) {
    pybind11::gil_scoped_acquire acquire;
    
    pybind11::module_ np = pybind11::module_::import("numpy");
    
    std::string np_dtype;
    switch (dtype) {
        case TensorDtype::FLOAT32: np_dtype = "float32"; break;
        case TensorDtype::FLOAT64: np_dtype = "float64"; break;
        case TensorDtype::INT32: np_dtype = "int32"; break;
        case TensorDtype::INT64: np_dtype = "int64"; break;
        case TensorDtype::INT8: np_dtype = "int8"; break;
        case TensorDtype::UINT8: np_dtype = "uint8"; break;
        case TensorDtype::FLOAT16: np_dtype = "float16"; break;
        case TensorDtype::INT16: np_dtype = "int16"; break;
        case TensorDtype::BOOL: np_dtype = "bool"; break;
        default: 
            throw std::runtime_error("Unknown tensor dtype");
    }
    
    size_t element_size = get_dtype_size(dtype);
    size_t total_elements = 1;
    for (int64_t dim : shape) {
        total_elements *= dim;
    }
    
    // Create a copy of the data
    std::vector<char> data_copy(data, data + total_elements * element_size);
    
    return np.attr("frombuffer")(pybind11::bytes(data_copy.data(), data_copy.size()), 
                                 pybind11::arg("dtype")=np_dtype).attr("reshape")(shape);
}

// Constructor
CoroRPCInterface::CoroRPCInterface() : impl_(std::make_unique<Impl>()) {}

// Destructor
CoroRPCInterface::~CoroRPCInterface() = default;

// Initialize
bool CoroRPCInterface::initialize(const std::string& local_address, 
                                  size_t thread_count, 
                                  size_t timeout_seconds,
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

bool CoroRPCInterface::addRemoteConnection(const std::string& remote_address) {
    if (!impl_->communicator) return false;
    return impl_->communicator->addRemoteConnection(remote_address);
}

void CoroRPCInterface::removeRemoteConnection(const std::string& remote_address) {
    if (impl_->communicator) {
        impl_->communicator->removeRemoteConnection(remote_address);
    }
}

bool CoroRPCInterface::isConnected(const std::string& remote_address) {
    if (!impl_->communicator) return false;
    return impl_->communicator->isConnected(remote_address);
}

int CoroRPCInterface::sendData(const std::string& target_address, pybind11::bytes data) {
    if (!impl_->communicator) return -1;
    
    std::string data_str;
    {
        pybind11::gil_scoped_acquire acquire;
        data_str = data;
    }

    pybind11::gil_scoped_release release;
    return impl_->communicator->sendData(target_address, data_str.data(), data_str.size());
}

pybind11::object CoroRPCInterface::sendDataAsync(const std::string& target_address, 
                                                  pybind11::bytes data, 
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
    auto target_addr = std::make_shared<std::string>(target_address);
    auto data_holder = std::make_shared<std::string>(data);
    auto future_ptr = std::make_shared<pybind11::object>(future_obj);
    auto loop_ptr = std::make_shared<pybind11::object>(pybind11::reinterpret_borrow<pybind11::object>(loop));

    auto task_func = std::make_shared<std::function<void()>>(
        [communicator, target_addr, data_holder, future_ptr, loop_ptr]() {
        int result = communicator->sendData(*target_addr, data_holder->data(), data_holder->size());
        
        auto call_soon_threadsafe = [future_ptr, loop_ptr, result]() {
            pybind11::gil_scoped_acquire acquire;
            if (result >= 0) {
                future_ptr->attr("set_result")(result);
            } else {
                future_ptr->attr("set_exception")(pybind11::make_tuple(
                    pybind11::str("Send data failed")));
            }
        };

        auto callback = pybind11::cpp_function(call_soon_threadsafe);
        loop_ptr->attr("call_soon_threadsafe")(callback);
    });

    std::thread([task_func]() { (*task_func)(); }).detach();
    
    return future_obj;
}

int CoroRPCInterface::sendTensor(const std::string& target_address, pybind11::handle tensor) {
    if (!impl_->communicator) return -1;
    
    try {
        pybind11::object tensor_obj;
        TensorMetadata metadata = {};
        std::vector<char> combined_data;
        
        {
            pybind11::gil_scoped_acquire acquire;
            tensor_obj = pybind11::reinterpret_borrow<pybind11::object>(tensor);
            
            // Validate tensor type
            if (!(tensor_obj.attr("__class__").attr("__name__").cast<std::string>().find("Tensor") != std::string::npos)) {
                std::cerr << "Input is not a tensor" << std::endl;
                return -1;
            }
            
            // Extract tensor properties
            uintptr_t data_ptr = tensor_obj.attr("data_ptr")().cast<uintptr_t>();
            size_t numel = tensor_obj.attr("numel")().cast<size_t>();
            size_t element_size = tensor_obj.attr("element_size")().cast<size_t>();
            size_t tensor_size = numel * element_size;
            
            // Get tensor dtype
            pybind11::object dtype_obj = tensor_obj.attr("dtype");
            TensorDtype dtype_enum = get_tensor_dtype(dtype_obj);
            if (dtype_enum == TensorDtype::UNKNOWN) {
                std::cerr << "Unsupported tensor dtype" << std::endl;
                return -1;
            }
            
            // Get tensor shape
            pybind11::object shape_obj = tensor_obj.attr("shape");
            pybind11::tuple shape_tuple = pybind11::cast<pybind11::tuple>(shape_obj);
            int32_t ndim = static_cast<int32_t>(shape_tuple.size());
            if (ndim > 4) {
                std::cerr << "Tensor has too many dimensions (max 4 supported)" << std::endl;
                return -1;
            }
            
            // Fill metadata
            metadata.dtype = static_cast<int32_t>(dtype_enum);
            metadata.ndim = ndim;
            for (int i = 0; i < 4; i++) {
                if (i < ndim) {
                    metadata.shape[i] = shape_tuple[i].cast<int64_t>();
                } else {
                    metadata.shape[i] = 0;
                }
            }
            
            // Create combined data: metadata + tensor data
            combined_data.resize(sizeof(TensorMetadata) + tensor_size);
            
            // Copy metadata
            std::memcpy(combined_data.data(), &metadata, sizeof(TensorMetadata));
            
            // Copy tensor data
            const char* tensor_data = reinterpret_cast<const char*>(data_ptr);
            std::memcpy(combined_data.data() + sizeof(TensorMetadata), tensor_data, tensor_size);
            
            std::cout << "Sending tensor with shape: [";
            for (int i = 0; i < ndim; i++) {
                std::cout << metadata.shape[i];
                if (i < ndim - 1) std::cout << ", ";
            }
            std::cout << "] and dtype: " << metadata.dtype << ", total size: " << combined_data.size() << " bytes" << std::endl;
        }

        pybind11::gil_scoped_release release;
        return impl_->communicator->sendData(target_address, combined_data.data(), combined_data.size());
        
    } catch (const std::exception& e) {
        std::cerr << "Send tensor error: " << e.what() << std::endl;
        return -1;
    }
}

pybind11::object CoroRPCInterface::sendTensorAsync(const std::string& target_address, 
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
    auto target_addr = std::make_shared<std::string>(target_address);
    
    // Extract tensor info
    pybind11::object tensor_obj = pybind11::reinterpret_borrow<pybind11::object>(tensor);
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
    
    auto tensor_info = std::make_shared<TensorInfo>();
    tensor_info->data_ptr = reinterpret_cast<void*>(data_ptr);
    tensor_info->total_bytes = tensor_size;
    tensor_info->shape = shape;
    tensor_info->dtype = dtype;
    
    auto future_ptr = std::make_shared<pybind11::object>(future_obj);
    auto loop_ptr = std::make_shared<pybind11::object>(pybind11::reinterpret_borrow<pybind11::object>(loop));

    auto task_func = std::make_shared<std::function<void()>>(
        [communicator, target_addr, tensor_info, future_ptr, loop_ptr]() {
        auto std_future = communicator->sendTensorAsync(*target_addr, *tensor_info);
        int result = std_future.get();
        
        auto call_soon_threadsafe = [future_ptr, loop_ptr, result]() {
            pybind11::gil_scoped_acquire acquire;
            if (result >= 0) {
                future_ptr->attr("set_result")(result);
            } else {
                future_ptr->attr("set_exception")(pybind11::make_tuple(
                    pybind11::str("Send tensor failed")));
            }
        };

        auto callback = pybind11::cpp_function(call_soon_threadsafe);
        loop_ptr->attr("call_soon_threadsafe")(callback);
    });

    std::thread([task_func]() { (*task_func)(); }).detach();
    
    return future_obj;
}

void CoroRPCInterface::setDataReceiveCallback(pybind11::function callback) {
    pybind11::gil_scoped_acquire acquire;
    impl_->data_receive_callback = callback;
}

void CoroRPCInterface::setTensorReceiveCallback(pybind11::function callback) {
    pybind11::gil_scoped_acquire acquire;
    impl_->tensor_receive_callback = callback;
}

void CoroRPCInterface::handleIncomingData(const std::string& source, const std::string& data) {
    // Check if this is tensor data by looking for metadata signature
    if (data.size() >= sizeof(TensorMetadata)) {
        const TensorMetadata* metadata = reinterpret_cast<const TensorMetadata*>(data.data());
        
        // Basic validation: check if dtype is in valid range
        if (metadata->dtype >= 0 && metadata->dtype < static_cast<int32_t>(TensorDtype::UNKNOWN) && 
            metadata->ndim >= 0 && metadata->ndim <= 4) {
            
            // This looks like tensor data, handle it as such
            std::vector<size_t> shape;
            for (int i = 0; i < metadata->ndim; i++) {
                if (metadata->shape[i] > 0) {
                    shape.push_back(static_cast<size_t>(metadata->shape[i]));
                }
            }
            
            // Get dtype name
            std::string dtype_name;
            switch (static_cast<TensorDtype>(metadata->dtype)) {
                case TensorDtype::FLOAT16: dtype_name = "float16"; break;
                case TensorDtype::FLOAT32: dtype_name = "float32"; break;
                case TensorDtype::FLOAT64: dtype_name = "float64"; break;
                case TensorDtype::INT8: dtype_name = "int8"; break;
                case TensorDtype::INT16: dtype_name = "int16"; break;
                case TensorDtype::INT32: dtype_name = "int32"; break;
                case TensorDtype::INT64: dtype_name = "int64"; break;
                case TensorDtype::UINT8: dtype_name = "uint8"; break;
                case TensorDtype::BOOL: dtype_name = "bool"; break;
                default: dtype_name = "unknown"; break;
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
        received["source"] = source;
        received["data"] = pybind11::bytes(data);
        
        impl_->data_receive_callback(received);
    } catch (const std::exception& e) {
        std::cerr << "Error in data receive callback: " << e.what() << std::endl;
    }
}

void CoroRPCInterface::handleIncomingTensor(const std::string& source, 
                                            const std::string& data,
                                            const std::vector<size_t>& shape, 
                                            const std::string& dtype) {
    if (!impl_->tensor_receive_callback) return;
    
    try {
        pybind11::gil_scoped_acquire acquire;
        
        ReceivedTensor received;
        received.source_address = source;
        received.data = data;
        received.shape = shape;
        received.dtype = dtype;
        
        impl_->tensor_receive_callback(received);
    } catch (const std::exception& e) {
        std::cerr << "Error in tensor receive callback: " << e.what() << std::endl;
    }
}

pybind11::object CoroRPCInterface::ReceivedTensor::rebuildTensor() const {
    pybind11::gil_scoped_acquire acquire;
    return rebuildTensorInternal();
}

pybind11::object CoroRPCInterface::ReceivedTensor::rebuildTensorInternal() const {
    if (data.size() < sizeof(TensorMetadata)) {
        throw std::runtime_error("Data too small to contain tensor metadata");
    }
    
    // Extract metadata
    TensorMetadata metadata;
    std::memcpy(&metadata, data.data(), sizeof(TensorMetadata));
    
    // Validate metadata
    if (metadata.ndim < 0 || metadata.ndim > 4) {
        throw std::runtime_error("Invalid tensor dimensions");
    }
    
    TensorDtype dtype_enum = static_cast<TensorDtype>(metadata.dtype);
    size_t element_size = get_dtype_size(dtype_enum);
    if (element_size == 0) {
        throw std::runtime_error("Unsupported tensor dtype");
    }
    
    // Extract shape
    std::vector<int64_t> tensor_shape;
    size_t total_elements = 1;
    for (int i = 0; i < metadata.ndim; i++) {
        tensor_shape.push_back(metadata.shape[i]);
        total_elements *= metadata.shape[i];
    }
    
    // Validate data size
    size_t expected_data_size = total_elements * element_size;
    size_t actual_data_size = data.size() - sizeof(TensorMetadata);
    if (actual_data_size != expected_data_size) {
        throw std::runtime_error("Data size mismatch with tensor metadata");
    }
    
    // Create numpy array from raw data
    const char* tensor_data = data.data() + sizeof(TensorMetadata);
    pybind11::object numpy_array = create_numpy_array_from_data(tensor_data, dtype_enum, tensor_shape);
    
    // Convert to PyTorch tensor
    pybind11::module_ torch = pybind11::module_::import("torch");
    return torch.attr("from_numpy")(numpy_array);
}

// Factory functions for creating RPC client and server
std::unique_ptr<CoroRPCInterface> createRPCClient(uint64_t local_rank, uint64_t world_size) {
    auto client = std::make_unique<CoroRPCInterface>();
    // Initialize client with default settings
    client->initialize("", 0, 30, 10);
    return client;
}

std::unique_ptr<CoroRPCInterface> createRPCServer(uint64_t local_rank, uint64_t world_size) {
    auto server = std::make_unique<CoroRPCInterface>();
    // Initialize server with default settings
    server->initialize("0.0.0.0:8080", 0, 30, 10);
    return server;
}

}  // namespace mooncake
