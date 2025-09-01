#include "transport/coro_rpc_connector/cororpc_interface.h"
#include "transport/coro_rpc_connector/cororpc_communicator.h"
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include <iostream>
#include <functional>
#include <memory>

using namespace pybind11::literals;

#define PYBIND11_NO_ASSERT_GIL_HELD_INCREF_DECREF

namespace mooncake {

class CoroRPCInterface::Impl {
public:
    std::unique_ptr<CoroRPCCommunicator> communicator;
    pybind11::function data_receive_callback;
    pybind11::function tensor_receive_callback;
    
    void onDataReceived(const std::string& source, const std::string& data);
    void onTensorReceived(const std::string& source, const std::string& data,
                         const std::vector<size_t>& shape, const std::string& dtype);
};

CoroRPCInterface::CoroRPCInterface() : impl_(std::make_unique<Impl>()) {}

CoroRPCInterface::~CoroRPCInterface() = default;

bool CoroRPCInterface::initialize(const std::string& listen_address,
                                 size_t thread_count,
                                 size_t timeout_seconds,
                                 size_t pool_size) {
    Config config;
    config.listen_address = listen_address;
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
        data_str = std::string(data);
    }
    
    pybind11::gil_scoped_release release;
    return impl_->communicator->sendData(target_address, data_str.data(), data_str.size());
}

pybind11::object CoroRPCInterface::sendDataAsync(const std::string& target_address,
                                                 pybind11::bytes data,
                                                 pybind11::handle loop) {
    pybind11::object future;
    {
        pybind11::gil_scoped_acquire acquire;
        future = loop.attr("create_future")();
    }
    
    if (!impl_->communicator) {
        pybind11::gil_scoped_acquire acquire;
        loop.attr("call_soon_threadsafe")(future.attr("set_result"), -1);
        return future;
    }
    
    auto data_holder = std::make_shared<std::string>();
    auto target_addr = std::make_shared<std::string>(target_address);
    auto communicator = impl_->communicator.get();
    
    PyObject* future_ptr = nullptr;
    PyObject* loop_ptr = nullptr;
    
    {
        pybind11::gil_scoped_acquire acquire;
        *data_holder = std::string(data);
        future_ptr = future.ptr();
        loop_ptr = loop.ptr();
        Py_INCREF(future_ptr);
        Py_INCREF(loop_ptr);
    }
    
    auto task_func = std::make_shared<std::function<void()>>();
    *task_func = [communicator, target_addr, data_holder, future_ptr, loop_ptr]() {
        int result = communicator->sendData(*target_addr, data_holder->data(), data_holder->size());
        
        {
            pybind11::gil_scoped_acquire acquire;
            try {
                pybind11::handle loop_handle(loop_ptr);
                pybind11::handle future_handle(future_ptr);
                loop_handle.attr("call_soon_threadsafe")(future_handle.attr("set_result"), result);
            } catch (const std::exception& e) {
                std::cerr << "Error in async callback: " << e.what() << std::endl;
            }
            Py_DECREF(future_ptr);
            Py_DECREF(loop_ptr);
        }
    };
    
    std::thread([task_func]() { (*task_func)(); }).detach();
    return future;
}

int CoroRPCInterface::sendTensor(const std::string& target_address, pybind11::handle tensor) {
    if (!impl_->communicator) return -1;
    
    pybind11::object tensor_obj;
    {
        pybind11::gil_scoped_acquire acquire;
        tensor_obj = pybind11::reinterpret_borrow<pybind11::object>(tensor);
    }

    pybind11::gil_scoped_release release;
    return impl_->communicator->sendTensor(target_address, tensor_obj);
}

pybind11::object CoroRPCInterface::sendTensorAsync(const std::string& target_address,
                                                   pybind11::handle tensor,
                                                   pybind11::handle loop) {
    pybind11::object future;
    {
        pybind11::gil_scoped_acquire acquire;
        future = loop.attr("create_future")();
    }
    
    if (!impl_->communicator) {
        pybind11::gil_scoped_acquire acquire;
        loop.attr("call_soon_threadsafe")(future.attr("set_result"), -1);
        return future;
    }
    
    auto tensor_info = std::make_shared<TensorInfo>();
    auto target_addr = std::make_shared<std::string>(target_address);
    auto communicator = impl_->communicator.get();
    
    PyObject* future_ptr = nullptr;
    PyObject* loop_ptr = nullptr;
    
    {
        pybind11::gil_scoped_acquire acquire;
        try {
            tensor_info->data_ptr = reinterpret_cast<void*>(tensor.attr("data_ptr")().cast<uintptr_t>());
            size_t numel = tensor.attr("numel")().cast<size_t>();
            size_t element_size = tensor.attr("element_size")().cast<size_t>();
            tensor_info->total_bytes = numel * element_size;
            
            auto shape_tuple = tensor.attr("shape");
            for (pybind11::handle item : shape_tuple) {
                tensor_info->shape.push_back(item.cast<size_t>());
            }
            tensor_info->dtype = tensor.attr("dtype").attr("__str__")().cast<std::string>();
            
            future_ptr = future.ptr();
            loop_ptr = loop.ptr();
            Py_INCREF(future_ptr);
            Py_INCREF(loop_ptr);
        } catch (const std::exception& e) {
            std::cerr << "Error extracting tensor info: " << e.what() << std::endl;
            loop.attr("call_soon_threadsafe")(future.attr("set_result"), -1);
            return future;
        }
    }
    
    auto task_func = std::make_shared<std::function<void()>>();
    *task_func = [communicator, target_addr, tensor_info, future_ptr, loop_ptr]() {
        auto std_future = communicator->sendTensorAsync(*target_addr, *tensor_info);
        int result = std_future.get();
        
        {
            pybind11::gil_scoped_acquire acquire;
            try {
                pybind11::handle loop_handle(loop_ptr);
                pybind11::handle future_handle(future_ptr);
                loop_handle.attr("call_soon_threadsafe")(future_handle.attr("set_result"), result);
            } catch (const std::exception& e) {
                std::cerr << "Error in tensor async callback: " << e.what() << std::endl;
            }
            Py_DECREF(future_ptr);
            Py_DECREF(loop_ptr);
        }
    };
    
    std::thread([task_func]() { (*task_func)(); }).detach();
    return future;
}

void CoroRPCInterface::setDataReceiveCallback(pybind11::function callback) {
    pybind11::gil_scoped_acquire acquire;
    impl_->data_receive_callback = callback;
}

void CoroRPCInterface::setTensorReceiveCallback(pybind11::function callback) {
    pybind11::gil_scoped_acquire acquire;
    impl_->tensor_receive_callback = callback;
}

void CoroRPCInterface::handleIncomingData(const std::string& source_address,
                                         const std::string& data) {
    if (!impl_->data_receive_callback) return;
    
    pybind11::gil_scoped_acquire acquire;
    try {
        ReceivedData received;
        received.data = data;
        received.source_address = source_address;
        received.data_size = data.size();
        
        impl_->data_receive_callback(received);
    } catch (const std::exception& e) {
        std::cerr << "Error in data receive callback: " << e.what() << std::endl;
    }
}

void CoroRPCInterface::handleIncomingTensor(const std::string& source_address,
                                           const std::string& data,
                                           const std::vector<size_t>& shape,
                                           const std::string& dtype) {
    if (!impl_->tensor_receive_callback) return;
    
    pybind11::gil_scoped_acquire acquire;
    try {
        ReceivedTensor received;
        received.data = data;
        received.source_address = source_address;
        received.shape = shape;
        received.dtype = dtype;
        received.total_bytes = data.size();
        
        impl_->tensor_receive_callback(received);
    } catch (const std::exception& e) {
        std::cerr << "Error in tensor receive callback: " << e.what() << std::endl;
    }
}

pybind11::object CoroRPCInterface::ReceivedTensor::rebuildTensor() const {
    if (!PyGILState_Check()) {
        pybind11::gil_scoped_acquire acquire;
        return rebuildTensorInternal();
    } else {
        return rebuildTensorInternal();
    }
}

pybind11::object CoroRPCInterface::ReceivedTensor::rebuildTensorInternal() const {
    try {
        auto torch = pybind11::module::import("torch");
        auto numpy = pybind11::module::import("numpy");
        
        std::string np_dtype;
        if (dtype.find("float32") != std::string::npos) {
            np_dtype = "float32";
        } else if (dtype.find("float64") != std::string::npos) {
            np_dtype = "float64";
        } else if (dtype.find("int32") != std::string::npos) {
            np_dtype = "int32";
        } else if (dtype.find("int64") != std::string::npos) {
            np_dtype = "int64";
        } else {
            np_dtype = "float32";
        }
        
        auto np_array = numpy.attr("frombuffer")(
            pybind11::bytes(data), 
            "dtype"_a=np_dtype
        ).attr("reshape")(pybind11::cast(shape));
        
        auto tensor = torch.attr("from_numpy")(np_array);
        return tensor;
    } catch (const std::exception& e) {
        std::cerr << "Error rebuilding tensor: " << e.what() << std::endl;
        return pybind11::none();
    }
}

void CoroRPCInterface::Impl::onDataReceived(const std::string& source, const std::string& data) {
}

void CoroRPCInterface::Impl::onTensorReceived(const std::string& source, const std::string& data,
                                             const std::vector<size_t>& shape, const std::string& dtype) {
}

std::unique_ptr<CoroRPCInterface> createRPCClient(size_t pool_size, size_t timeout_seconds) {
    auto interface = std::make_unique<CoroRPCInterface>();
    if (interface->initialize("", 0, timeout_seconds, pool_size)) {
        return interface;
    }
    return nullptr;
}

std::unique_ptr<CoroRPCInterface> createRPCServer(const std::string& listen_address, size_t thread_count) {
    auto interface = std::make_unique<CoroRPCInterface>();
    if (interface->initialize(listen_address, thread_count)) {
        return interface;
    }
    return nullptr;
}

} // namespace mooncake

namespace py = pybind11;

// Note: bind_coro_rpc_interface function is now implemented in transfer_engine_py.cpp
// to avoid linking issues