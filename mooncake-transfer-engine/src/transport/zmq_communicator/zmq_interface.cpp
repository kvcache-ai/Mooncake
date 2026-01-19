#include "zmq_interface.h"
#include <glog/logging.h>
#include <pybind11/pytypes.h>
#include "async_simple/coro/SyncAwait.h"

namespace mooncake {

class ZmqInterface::Impl {
   public:
    std::unique_ptr<ZmqCommunicator> communicator;
    std::unordered_map<int, pybind11::function> data_callbacks;
    std::unordered_map<int, pybind11::function> tensor_callbacks;
};

ZmqInterface::ZmqInterface() : impl_(std::make_unique<Impl>()) {
    impl_->communicator = std::make_unique<ZmqCommunicator>();
}

ZmqInterface::~ZmqInterface() = default;

bool ZmqInterface::initialize(const ZmqConfig& config) {
    return impl_->communicator->initialize(config);
}

void ZmqInterface::shutdown() { impl_->communicator->shutdown(); }

int ZmqInterface::createSocket(ZmqSocketType type) {
    return impl_->communicator->createSocket(type);
}

bool ZmqInterface::closeSocket(int socket_id) {
    return impl_->communicator->closeSocket(socket_id);
}

bool ZmqInterface::bind(int socket_id, const std::string& endpoint) {
    return impl_->communicator->bind(socket_id, endpoint);
}

bool ZmqInterface::connect(int socket_id, const std::string& endpoint) {
    return impl_->communicator->connect(socket_id, endpoint);
}

bool ZmqInterface::unbind(int socket_id, const std::string& endpoint) {
    return impl_->communicator->unbind(socket_id, endpoint);
}

bool ZmqInterface::disconnect(int socket_id, const std::string& endpoint) {
    return impl_->communicator->disconnect(socket_id, endpoint);
}

bool ZmqInterface::setSocketOption(int socket_id, ZmqSocketOption option,
                                   int64_t value) {
    return impl_->communicator->setSocketOption(socket_id, option, value);
}

int64_t ZmqInterface::getSocketOption(int socket_id, ZmqSocketOption option) {
    int64_t value = 0;
    impl_->communicator->getSocketOption(socket_id, option, value);
    return value;
}

bool ZmqInterface::setRoutingId(int socket_id, const std::string& routing_id) {
    return impl_->communicator->setRoutingId(socket_id, routing_id);
}

std::string ZmqInterface::getRoutingId(int socket_id) {
    return impl_->communicator->getRoutingId(socket_id);
}

bool ZmqInterface::isBound(int socket_id) {
    return impl_->communicator->isBound(socket_id);
}

bool ZmqInterface::isConnected(int socket_id) {
    return impl_->communicator->isConnected(socket_id);
}

pybind11::list ZmqInterface::getConnectedEndpoints(int socket_id) {
    pybind11::gil_scoped_acquire acquire;
    auto endpoints = impl_->communicator->getConnectedEndpoints(socket_id);
    pybind11::list result;
    for (const auto& ep : endpoints) {
        result.append(ep);
    }
    return result;
}

std::string ZmqInterface::getBoundEndpoint(int socket_id) {
    return impl_->communicator->getBoundEndpoint(socket_id);
}

ZmqSocketType ZmqInterface::getSocketType(int socket_id) {
    return impl_->communicator->getSocketType(socket_id);
}

bool ZmqInterface::startServer(int socket_id) {
    return impl_->communicator->startServer(socket_id);
}

std::string ZmqInterface::extractData(pybind11::handle data) {
    pybind11::gil_scoped_acquire acquire;

    try {
        pybind11::bytes data_bytes =
            pybind11::reinterpret_borrow<pybind11::bytes>(data);
        return static_cast<std::string>(data_bytes);
    } catch (...) {
        pybind11::bytes data_bytes = pybind11::cast<pybind11::bytes>(data);
        return static_cast<std::string>(data_bytes);
    }
}

TensorInfo ZmqInterface::extractTensor(pybind11::handle tensor) {
    pybind11::gil_scoped_acquire acquire;
    pybind11::object tensor_obj =
        pybind11::reinterpret_borrow<pybind11::object>(tensor);

    TensorInfo info;
    info.data_ptr = reinterpret_cast<void*>(
        tensor_obj.attr("data_ptr")().cast<uintptr_t>());

    size_t numel = tensor_obj.attr("numel")().cast<size_t>();
    size_t element_size = tensor_obj.attr("element_size")().cast<size_t>();
    info.total_bytes = numel * element_size;

    pybind11::tuple shape_tuple =
        tensor_obj.attr("shape").cast<pybind11::tuple>();
    for (size_t i = 0; i < shape_tuple.size(); i++) {
        info.shape.push_back(shape_tuple[i].cast<size_t>());
    }

    pybind11::object dtype_obj = tensor_obj.attr("dtype");
    info.dtype = dtype_obj.attr("__str__")().cast<std::string>();

    return info;
}

// REQ/REP mode
pybind11::object ZmqInterface::request(int socket_id, pybind11::handle data) {
    std::string data_str = extractData(data);

    pybind11::gil_scoped_release release;
    auto result =
        async_simple::coro::syncAwait(impl_->communicator->sendDataAsync(
            socket_id, data_str.data(), data_str.size()));

    pybind11::gil_scoped_acquire acquire;
    if (result.code == 0) {
        return pybind11::bytes(result.response_data);
    } else {
        throw std::runtime_error("Request failed: " + result.message);
    }
}

pybind11::object ZmqInterface::requestAsync(int socket_id,
                                            pybind11::handle data,
                                            pybind11::handle loop) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    std::string data_str = extractData(data);
    auto communicator = impl_->communicator.get();

    pybind11::gil_scoped_release release;

    auto coro_lambda = [communicator, socket_id, data_str, future_obj,
                        loop]() -> async_simple::coro::Lazy<void> {
        try {
            auto result = co_await communicator->sendDataAsync(
                socket_id, data_str.data(), data_str.size());

            auto callback = [future_obj, loop, result]() {
                pybind11::gil_scoped_acquire acquire;
                if (result.code == 0) {
                    future_obj.attr("set_result")(
                        pybind11::bytes(result.response_data));
                } else {
                    auto exc = pybind11::module_::import("builtins")
                                   .attr("RuntimeError")(pybind11::str(
                                       "Request failed: " + result.message));
                    future_obj.attr("set_exception")(exc);
                }
            };

            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
        } catch (const std::exception& e) {
            auto callback = [future_obj, loop, e]() {
                pybind11::gil_scoped_acquire acquire;
                auto exc = pybind11::module_::import("builtins")
                               .attr("RuntimeError")(pybind11::str(
                                   std::string("Exception: ") + e.what()));
                future_obj.attr("set_exception")(exc);
            };
            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
        }
    };

    auto lazy = coro_lambda();
    lazy.start([](auto&& result) {
        if (result.hasError()) {
            LOG(ERROR) << "Request coroutine error";
        }
    });

    return future_obj;
}

void ZmqInterface::reply(int socket_id, pybind11::handle data) {
    std::string data_str = extractData(data);
    impl_->communicator->sendReply(socket_id, data_str.data(), data_str.size());
}

// PUB/SUB mode
int ZmqInterface::publish(int socket_id, const std::string& topic,
                          pybind11::handle data) {
    std::string data_str = extractData(data);

    pybind11::gil_scoped_release release;
    auto result =
        async_simple::coro::syncAwait(impl_->communicator->sendDataAsync(
            socket_id, data_str.data(), data_str.size(), topic));

    return result.code;
}

pybind11::object ZmqInterface::publishAsync(int socket_id,
                                            const std::string& topic,
                                            pybind11::handle data,
                                            pybind11::handle loop) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    std::string data_str = extractData(data);
    auto communicator = impl_->communicator.get();

    pybind11::gil_scoped_release release;

    auto coro_lambda = [communicator, socket_id, data_str, topic, future_obj,
                        loop]() -> async_simple::coro::Lazy<void> {
        try {
            auto result = co_await communicator->sendDataAsync(
                socket_id, data_str.data(), data_str.size(), topic);

            auto callback = [future_obj, loop, result]() {
                pybind11::gil_scoped_acquire acquire;
                future_obj.attr("set_result")(result.code);
            };

            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
        } catch (const std::exception& e) {
            auto callback = [future_obj, loop, e]() {
                pybind11::gil_scoped_acquire acquire;
                auto exc = pybind11::module_::import("builtins")
                               .attr("RuntimeError")(pybind11::str(
                                   std::string("Exception: ") + e.what()));
                future_obj.attr("set_exception")(exc);
            };
            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
        }
    };

    auto lazy = coro_lambda();
    lazy.start([](auto&& result) {
        if (result.hasError()) {
            LOG(ERROR) << "Publish coroutine error";
        }
    });

    return future_obj;
}

bool ZmqInterface::subscribe(int socket_id, const std::string& topic) {
    return impl_->communicator->subscribe(socket_id, topic);
}

bool ZmqInterface::unsubscribe(int socket_id, const std::string& topic) {
    return impl_->communicator->unsubscribe(socket_id, topic);
}

void ZmqInterface::setSubscribeCallback(int socket_id,
                                        pybind11::function callback) {
    pybind11::gil_scoped_acquire acquire;
    impl_->data_callbacks[socket_id] = callback;

    auto interface_ptr = this;
    impl_->communicator->setReceiveCallback(
        socket_id, [interface_ptr, socket_id](
                       std::string_view source, std::string_view data,
                       const std::optional<std::string>& topic) {
            pybind11::gil_scoped_acquire acquire;

            auto it = interface_ptr->impl_->data_callbacks.find(socket_id);
            if (it != interface_ptr->impl_->data_callbacks.end()) {
                pybind11::dict msg;
                msg["source"] = std::string(source);
                msg["data"] = pybind11::bytes(std::string(data));
                msg["topic"] = topic.value_or("");

                it->second(msg);
            }
        });
}

// PUSH/PULL mode
int ZmqInterface::push(int socket_id, pybind11::handle data) {
    std::string data_str = extractData(data);

    pybind11::gil_scoped_release release;
    auto result =
        async_simple::coro::syncAwait(impl_->communicator->sendDataAsync(
            socket_id, data_str.data(), data_str.size()));

    return result.code;
}

pybind11::object ZmqInterface::pushAsync(int socket_id, pybind11::handle data,
                                         pybind11::handle loop) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    std::string data_str = extractData(data);
    auto communicator = impl_->communicator.get();

    pybind11::gil_scoped_release release;

    auto coro_lambda = [communicator, socket_id, data_str, future_obj,
                        loop]() -> async_simple::coro::Lazy<void> {
        try {
            auto result = co_await communicator->sendDataAsync(
                socket_id, data_str.data(), data_str.size());

            auto callback = [future_obj, loop, result]() {
                pybind11::gil_scoped_acquire acquire;
                future_obj.attr("set_result")(result.code);
            };

            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
        } catch (const std::exception& e) {
            auto callback = [future_obj, loop, e]() {
                pybind11::gil_scoped_acquire acquire;
                auto exc = pybind11::module_::import("builtins")
                               .attr("RuntimeError")(pybind11::str(
                                   std::string("Exception: ") + e.what()));
                future_obj.attr("set_exception")(exc);
            };
            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
        }
    };

    auto lazy = coro_lambda();
    lazy.start([](auto&& result) {
        if (result.hasError()) {
            LOG(ERROR) << "Push coroutine error";
        }
    });

    return future_obj;
}

void ZmqInterface::setPullCallback(int socket_id, pybind11::function callback) {
    setSubscribeCallback(socket_id, callback);  // Reuse the same logic
}

// PAIR mode
int ZmqInterface::send(int socket_id, pybind11::handle data) {
    return push(socket_id, data);  // Reuse PUSH logic
}

pybind11::object ZmqInterface::sendAsync(int socket_id, pybind11::handle data,
                                         pybind11::handle loop) {
    return pushAsync(socket_id, data, loop);  // Reuse PUSH logic
}

void ZmqInterface::setReceiveCallback(int socket_id,
                                      pybind11::function callback) {
    setSubscribeCallback(socket_id, callback);  // Reuse the same logic
}

// Tensor support
int ZmqInterface::sendTensor(int socket_id, pybind11::handle tensor) {
    TensorInfo tensor_info = extractTensor(tensor);

    pybind11::gil_scoped_release release;
    auto result = async_simple::coro::syncAwait(
        impl_->communicator->sendTensorAsync(socket_id, tensor_info));

    return result;
}

pybind11::object ZmqInterface::sendTensorAsync(int socket_id,
                                               pybind11::handle tensor,
                                               pybind11::handle loop) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    TensorInfo tensor_info = extractTensor(tensor);
    auto communicator = impl_->communicator.get();

    // Keep tensor alive
    pybind11::object tensor_obj =
        pybind11::reinterpret_borrow<pybind11::object>(tensor);

    pybind11::gil_scoped_release release;

    auto coro_lambda = [communicator, socket_id, tensor_info, future_obj, loop,
                        tensor_obj]() -> async_simple::coro::Lazy<void> {
        try {
            auto result =
                co_await communicator->sendTensorAsync(socket_id, tensor_info);

            auto callback = [future_obj, loop, result]() {
                pybind11::gil_scoped_acquire acquire;
                future_obj.attr("set_result")(result);
            };

            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
        } catch (const std::exception& e) {
            auto callback = [future_obj, loop, e]() {
                pybind11::gil_scoped_acquire acquire;
                auto exc = pybind11::module_::import("builtins")
                               .attr("RuntimeError")(pybind11::str(
                                   std::string("Exception: ") + e.what()));
                future_obj.attr("set_exception")(exc);
            };
            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
        }
    };

    auto lazy = coro_lambda();
    lazy.start([](auto&& result) {
        if (result.hasError()) {
            LOG(ERROR) << "Tensor send coroutine error";
        }
    });

    return future_obj;
}

void ZmqInterface::setTensorReceiveCallback(int socket_id,
                                            pybind11::function callback) {
    pybind11::gil_scoped_acquire acquire;
    impl_->tensor_callbacks[socket_id] = callback;

    auto interface_ptr = this;
    impl_->communicator->setTensorReceiveCallback(
        socket_id, [interface_ptr, socket_id](
                       std::string_view source, const TensorInfo& tensor,
                       const std::optional<std::string>& topic) {
            pybind11::gil_scoped_acquire acquire;

            auto it = interface_ptr->impl_->tensor_callbacks.find(socket_id);
            if (it != interface_ptr->impl_->tensor_callbacks.end()) {
                pybind11::dict msg;
                msg["source"] = std::string(source);
                msg["shape"] = pybind11::cast(tensor.shape);
                msg["dtype"] = tensor.dtype;
                msg["topic"] = topic.value_or("");
                // TODO: reconstruct tensor from data

                it->second(msg);
            }
        });
}

// Python binding
void bind_zmq_interface(pybind11::module_& m) {
    namespace py = pybind11;

    // Bind ZmqSocketType enum
    py::enum_<ZmqSocketType>(m, "ZmqSocketType")
        .value("REQ", ZmqSocketType::REQ)
        .value("REP", ZmqSocketType::REP)
        .value("PUB", ZmqSocketType::PUB)
        .value("SUB", ZmqSocketType::SUB)
        .value("PUSH", ZmqSocketType::PUSH)
        .value("PULL", ZmqSocketType::PULL)
        .value("PAIR", ZmqSocketType::PAIR)
        .export_values();

    // Bind ZmqSocketOption enum
    py::enum_<ZmqSocketOption>(m, "ZmqSocketOption")
        .value("RCVTIMEO", ZmqSocketOption::RCVTIMEO)
        .value("SNDTIMEO", ZmqSocketOption::SNDTIMEO)
        .value("SNDHWM", ZmqSocketOption::SNDHWM)
        .value("RCVHWM", ZmqSocketOption::RCVHWM)
        .value("LINGER", ZmqSocketOption::LINGER)
        .value("RECONNECT_IVL", ZmqSocketOption::RECONNECT_IVL)
        .value("RCVBUF", ZmqSocketOption::RCVBUF)
        .value("SNDBUF", ZmqSocketOption::SNDBUF)
        .value("ROUTING_ID", ZmqSocketOption::ROUTING_ID)
        .export_values();

    // Bind ZmqConfig
    py::class_<ZmqConfig>(m, "ZmqConfig")
        .def(py::init<>())
        .def_readwrite("thread_count", &ZmqConfig::thread_count)
        .def_readwrite("timeout_seconds", &ZmqConfig::timeout_seconds)
        .def_readwrite("pool_size", &ZmqConfig::pool_size)
        .def_readwrite("enable_rdma", &ZmqConfig::enable_rdma)
        .def_readwrite("high_water_mark", &ZmqConfig::high_water_mark)
        .def_readwrite("rcv_timeout_ms", &ZmqConfig::rcv_timeout_ms)
        .def_readwrite("snd_timeout_ms", &ZmqConfig::snd_timeout_ms)
        .def_readwrite("linger_ms", &ZmqConfig::linger_ms)
        .def_readwrite("reconnect_interval_ms",
                       &ZmqConfig::reconnect_interval_ms)
        .def_readwrite("rcv_buffer_size", &ZmqConfig::rcv_buffer_size)
        .def_readwrite("snd_buffer_size", &ZmqConfig::snd_buffer_size);

    // Bind ZmqInterface
    py::class_<ZmqInterface>(m, "ZmqInterface")
        .def(py::init<>())
        .def("initialize", &ZmqInterface::initialize)
        .def("shutdown", &ZmqInterface::shutdown)
        .def("create_socket", &ZmqInterface::createSocket)
        .def("close_socket", &ZmqInterface::closeSocket)
        .def("bind", &ZmqInterface::bind)
        .def("connect", &ZmqInterface::connect)
        .def("unbind", &ZmqInterface::unbind)
        .def("disconnect", &ZmqInterface::disconnect)
        .def("set_socket_option", &ZmqInterface::setSocketOption)
        .def("get_socket_option", &ZmqInterface::getSocketOption)
        .def("set_routing_id", &ZmqInterface::setRoutingId)
        .def("get_routing_id", &ZmqInterface::getRoutingId)
        .def("is_bound", &ZmqInterface::isBound)
        .def("is_connected", &ZmqInterface::isConnected)
        .def("get_connected_endpoints", &ZmqInterface::getConnectedEndpoints)
        .def("get_bound_endpoint", &ZmqInterface::getBoundEndpoint)
        .def("get_socket_type", &ZmqInterface::getSocketType)
        .def("start_server", &ZmqInterface::startServer)
        .def("request", &ZmqInterface::request)
        .def("request_async", &ZmqInterface::requestAsync)
        .def("reply", &ZmqInterface::reply)
        .def("publish", &ZmqInterface::publish)
        .def("publish_async", &ZmqInterface::publishAsync)
        .def("subscribe", &ZmqInterface::subscribe)
        .def("unsubscribe", &ZmqInterface::unsubscribe)
        .def("set_subscribe_callback", &ZmqInterface::setSubscribeCallback)
        .def("push", &ZmqInterface::push)
        .def("push_async", &ZmqInterface::pushAsync)
        .def("set_pull_callback", &ZmqInterface::setPullCallback)
        .def("send", &ZmqInterface::send)
        .def("send_async", &ZmqInterface::sendAsync)
        .def("set_receive_callback", &ZmqInterface::setReceiveCallback)
        .def("send_tensor", &ZmqInterface::sendTensor)
        .def("send_tensor_async", &ZmqInterface::sendTensorAsync)
        .def("set_tensor_receive_callback",
             &ZmqInterface::setTensorReceiveCallback);
}

}  // namespace mooncake
