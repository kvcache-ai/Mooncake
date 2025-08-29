#include "transport/coro_rpc_connector/cororpc_interface.h"
#include "transport/coro_rpc_connector/cororpc_communicator.h"
#include <pybind11/stl.h>
#include <iostream>
#include <json/json.h>

namespace mooncake {

// Impl类定义
class CoroRPCInterface::Impl {
public:
    std::unique_ptr<CoroRPCCommunicator> communicator;
    pybind11::function data_receive_callback;
    pybind11::function tensor_receive_callback;
    
    // 处理接收到的数据
    void onDataReceived(const std::string& source, const std::string& data);
    void onTensorReceived(const std::string& source, const std::string& data,
                         const std::vector<size_t>& shape, const std::string& dtype);
};

// CoroRPCInterface实现
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
    
    pybind11::gil_scoped_release release;
    std::string data_str = data;
    return impl_->communicator->sendData(target_address, data_str.data(), data_str.size());
}

pybind11::object CoroRPCInterface::sendDataAsync(const std::string& target_address,
                                                 pybind11::bytes data,
                                                 pybind11::handle loop) {
    auto future = loop.attr("create_future")();
    
    if (!impl_->communicator) {
        loop.attr("call_soon_threadsafe")(future.attr("set_result"), -1);
        return future;
    }
    
    // 创建异步任务
    std::string data_str = data;
    auto task = [this, target_address, data_str, future, loop]() {
        pybind11::gil_scoped_release release;
        int result = impl_->communicator->sendData(target_address, data_str.data(), data_str.size());
        
        pybind11::gil_scoped_acquire acquire;
        loop.attr("call_soon_threadsafe")(future.attr("set_result"), result);
    };
    
    std::thread(task).detach();
    return future;
}

int CoroRPCInterface::sendTensor(const std::string& target_address, pybind11::handle tensor) {
    if (!impl_->communicator) return -1;
    
    pybind11::gil_scoped_release release;
    return impl_->communicator->sendTensor(target_address, pybind11::cast<pybind11::object>(tensor));
}

pybind11::object CoroRPCInterface::sendTensorAsync(const std::string& target_address,
                                                   pybind11::handle tensor,
                                                   pybind11::handle loop) {
    auto future = loop.attr("create_future")();
    
    if (!impl_->communicator) {
        loop.attr("call_soon_threadsafe")(future.attr("set_result"), -1);
        return future;
    }
    
    // 获取tensor信息
    CoroRPCCommunicator::TensorInfo tensor_info;
    {
        pybind11::gil_scoped_acquire acquire;
        tensor_info.data_ptr = reinterpret_cast<void*>(tensor.attr("data_ptr")().cast<uintptr_t>());
        size_t numel = tensor.attr("numel")().cast<size_t>();
        size_t element_size = tensor.attr("element_size")().cast<size_t>();
        tensor_info.total_bytes = numel * element_size;
        
        // 获取shape和dtype
        auto shape_tuple = tensor.attr("shape");
        for (pybind11::handle item : shape_tuple) {
            tensor_info.shape.push_back(item.cast<size_t>());
        }
        tensor_info.dtype = tensor.attr("dtype").attr("__str__")().cast<std::string>();
    }
    
    // 异步发送
    auto std_future = impl_->communicator->sendTensorAsync(target_address, tensor_info);
    
    // 转换为Python future
    auto task = [std_future = std::move(std_future), future, loop]() mutable {
        int result = std_future.get();
        pybind11::gil_scoped_acquire acquire;
        loop.attr("call_soon_threadsafe")(future.attr("set_result"), result);
    };
    
    std::thread(task).detach();
    return future;
}

void CoroRPCInterface::setDataReceiveCallback(pybind11::function callback) {
    impl_->data_receive_callback = callback;
}

void CoroRPCInterface::setTensorReceiveCallback(pybind11::function callback) {
    impl_->tensor_receive_callback = callback;
}

void CoroRPCInterface::handleIncomingData(const std::string& source_address,
                                         const std::string& data) {
    if (impl_->data_receive_callback) {
        ReceivedData received;
        received.data = data;
        received.source_address = source_address;
        received.data_size = data.size();
        
        pybind11::gil_scoped_acquire acquire;
        impl_->data_receive_callback(received);
    }
}

void CoroRPCInterface::handleIncomingTensor(const std::string& source_address,
                                           const std::string& data,
                                           const std::vector<size_t>& shape,
                                           const std::string& dtype) {
    if (impl_->tensor_receive_callback) {
        ReceivedTensor received;
        received.data = data;
        received.source_address = source_address;
        received.shape = shape;
        received.dtype = dtype;
        received.total_bytes = data.size();
        
        pybind11::gil_scoped_acquire acquire;
        impl_->tensor_receive_callback(received);
    }
}

// ReceivedTensor的rebuildTensor实现
pybind11::object CoroRPCInterface::ReceivedTensor::rebuildTensor() const {
    pybind11::gil_scoped_acquire acquire;
    
    try {
        // 导入torch模块
        auto torch = pybind11::module::import("torch");
        auto numpy = pybind11::module::import("numpy");
        
        // 确定numpy数据类型
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
            np_dtype = "float32"; // 默认类型
        }
        
        // 创建numpy数组
        auto np_array = numpy.attr("frombuffer")(
            pybind11::bytes(data), 
            "dtype"_a=np_dtype
        ).attr("reshape")(pybind11::cast(shape));
        
        // 转换为torch tensor
        auto tensor = torch.attr("from_numpy")(np_array);
        
        return tensor;
    } catch (const std::exception& e) {
        std::cerr << "Error rebuilding tensor: " << e.what() << std::endl;
        return pybind11::none();
    }
}

// Impl类方法实现
void CoroRPCInterface::Impl::onDataReceived(const std::string& source, const std::string& data) {
    // 这里可以添加具体的数据接收处理逻辑
}

void CoroRPCInterface::Impl::onTensorReceived(const std::string& source, const std::string& data,
                                             const std::vector<size_t>& shape, const std::string& dtype) {
    // 这里可以添加具体的tensor接收处理逻辑
}

// 工厂函数实现
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

// Python绑定
namespace py = pybind11;

PYBIND11_MODULE(coro_rpc_interface, m) {
    using namespace mooncake;
    
    // ReceivedData类
    py::class_<CoroRPCInterface::ReceivedData>(m, "ReceivedData")
        .def(py::init<>())
        .def_readonly("source_address", &CoroRPCInterface::ReceivedData::source_address)
        .def_readonly("data_size", &CoroRPCInterface::ReceivedData::data_size)
        .def("get_bytes", &CoroRPCInterface::ReceivedData::getBytes);
    
    // ReceivedTensor类
    py::class_<CoroRPCInterface::ReceivedTensor>(m, "ReceivedTensor")
        .def(py::init<>())
        .def_readonly("source_address", &CoroRPCInterface::ReceivedTensor::source_address)
        .def_readonly("shape", &CoroRPCInterface::ReceivedTensor::shape)
        .def_readonly("dtype", &CoroRPCInterface::ReceivedTensor::dtype)
        .def_readonly("total_bytes", &CoroRPCInterface::ReceivedTensor::total_bytes)
        .def("rebuild_tensor", &CoroRPCInterface::ReceivedTensor::rebuildTensor);
    
    // 主接口类
    py::class_<CoroRPCInterface>(m, "CoroRPCInterface")
        .def(py::init<>())
        .def("initialize", &CoroRPCInterface::initialize,
             "listen_address"_a="", "thread_count"_a=0, 
             "timeout_seconds"_a=30, "pool_size"_a=10)
        .def("start_server", &CoroRPCInterface::startServer)
        .def("start_server_async", &CoroRPCInterface::startServerAsync)
        .def("stop_server", &CoroRPCInterface::stopServer)
        .def("add_remote_connection", &CoroRPCInterface::addRemoteConnection)
        .def("remove_remote_connection", &CoroRPCInterface::removeRemoteConnection)
        .def("is_connected", &CoroRPCInterface::isConnected)
        .def("send_data", &CoroRPCInterface::sendData)
        .def("send_data_async", &CoroRPCInterface::sendDataAsync)
        .def("send_tensor", &CoroRPCInterface::sendTensor)
        .def("send_tensor_async", &CoroRPCInterface::sendTensorAsync)
        .def("set_data_receive_callback", &CoroRPCInterface::setDataReceiveCallback)
        .def("set_tensor_receive_callback", &CoroRPCInterface::setTensorReceiveCallback);
    
    // 工厂函数
    m.def("create_rpc_client", &createRPCClient, 
          "pool_size"_a=10, "timeout_seconds"_a=30);
    m.def("create_rpc_server", &createRPCServer,
          "listen_address"_a, "thread_count"_a=0);
}