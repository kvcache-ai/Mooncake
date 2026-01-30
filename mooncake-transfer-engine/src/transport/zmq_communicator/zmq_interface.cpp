#include "zmq_interface.h"
#include <glog/logging.h>
#include <pybind11/pytypes.h>
#include "async_simple/coro/SyncAwait.h"
#include <cstdlib>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>

namespace mooncake {

// Message types for the receive queue
enum class MessageType { DATA, TENSOR, PYOBJ, MULTIPART, JSON, STRING };

// Unified message structure for receive queue
struct ReceivedMessage {
    MessageType type;
    std::string source;
    std::string topic;

    // For DATA and PYOBJ
    std::string data;

    // For TENSOR
    TensorInfo tensor;

    // For MULTIPART
    std::vector<std::string> frames;
};

// Message queue for polling mode
struct MessageQueue {
    std::queue<ReceivedMessage> messages;
    std::mutex mutex;
    std::condition_variable cv;
    bool polling_mode = false;
    int64_t timeout_ms = 5000;  // Default timeout
};

class ZmqInterface::Impl {
   public:
    std::unique_ptr<ZmqCommunicator> communicator;
    std::unordered_map<int, pybind11::function> data_callbacks;
    std::unordered_map<int, pybind11::function> tensor_callbacks;
    std::unordered_map<int, pybind11::function> pyobj_callbacks;
    std::unordered_map<int, pybind11::function> multipart_callbacks;
    std::unordered_map<int, pybind11::function> json_callbacks;
    std::unordered_map<int, pybind11::function> string_callbacks;
    std::unordered_map<int, std::string>
        string_encodings;  // Store encoding per socket

    // Message queues for polling mode
    std::unordered_map<int, std::shared_ptr<MessageQueue>> message_queues;
    std::mutex queues_mutex;
};

namespace {
bool pickle_deserialization_enabled() {
    const char* v = std::getenv("MOONCAKE_ALLOW_PICKLE");
    if (!v) v = std::getenv("MC_ALLOW_PICKLE");
    if (!v) return false;
    std::string_view s(v);
    return s == "1" || s == "true" || s == "TRUE" || s == "yes" || s == "YES";
}
}  // namespace

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

// Python object serialization (pickle)
int ZmqInterface::sendPyobj(int socket_id, pybind11::handle obj,
                            const std::string& topic) {
    pybind11::gil_scoped_acquire acquire;

    // Use pickle to serialize the Python object
    auto pickle = pybind11::module_::import("pickle");
    auto pickled = pickle.attr("dumps")(obj, pybind11::arg("protocol") = -1);
    std::string pickled_str = extractData(pickled);

    pybind11::gil_scoped_release release;
    auto result =
        async_simple::coro::syncAwait(impl_->communicator->sendDataAsync(
            socket_id, pickled_str.data(), pickled_str.size(), topic));

    return result.code;
}

pybind11::object ZmqInterface::sendPyobjAsync(int socket_id,
                                              pybind11::handle obj,
                                              pybind11::handle loop,
                                              const std::string& topic) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    // Use pickle to serialize the Python object
    auto pickle = pybind11::module_::import("pickle");
    auto pickled = pickle.attr("dumps")(obj, pybind11::arg("protocol") = -1);
    std::string pickled_str = extractData(pickled);
    auto communicator = impl_->communicator.get();

    pybind11::gil_scoped_release release;

    auto coro_lambda = [communicator, socket_id, pickled_str, topic, future_obj,
                        loop]() -> async_simple::coro::Lazy<void> {
        try {
            auto result = co_await communicator->sendDataAsync(
                socket_id, pickled_str.data(), pickled_str.size(), topic);

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
            LOG(ERROR) << "Send pyobj coroutine error";
        }
    });

    return future_obj;
}

void ZmqInterface::setPyobjReceiveCallback(int socket_id,
                                           pybind11::function callback) {
    pybind11::gil_scoped_acquire acquire;
    impl_->pyobj_callbacks[socket_id] = callback;

    auto interface_ptr = this;
    impl_->communicator->setReceiveCallback(
        socket_id, [interface_ptr, socket_id](
                       std::string_view source, std::string_view data,
                       const std::optional<std::string>& topic) {
            pybind11::gil_scoped_acquire acquire;

            auto it = interface_ptr->impl_->pyobj_callbacks.find(socket_id);
            if (it != interface_ptr->impl_->pyobj_callbacks.end()) {
                try {
                    if (!pickle_deserialization_enabled()) {
                        pybind11::dict msg;
                        msg["source"] = std::string(source);
                        msg["topic"] = topic.value_or("");
                        msg["error"] =
                            "pickle deserialization is disabled by default "
                            "(set MOONCAKE_ALLOW_PICKLE=1 to enable)";
                        msg["data"] = pybind11::bytes(std::string(data));
                        it->second(msg);
                        return;
                    }
                    // Use pickle to deserialize
                    auto pickle = pybind11::module_::import("pickle");
                    pybind11::bytes data_bytes =
                        pybind11::bytes(std::string(data));
                    pybind11::object obj = pickle.attr("loads")(data_bytes);

                    pybind11::dict msg;
                    msg["source"] = std::string(source);
                    msg["obj"] = obj;
                    msg["topic"] = topic.value_or("");

                    it->second(msg);
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Failed to unpickle object: " << e.what();
                }
            }
        });
}

// Multipart messages
int ZmqInterface::sendMultipart(int socket_id, pybind11::list frames,
                                const std::string& topic) {
    pybind11::gil_scoped_acquire acquire;

    // Encode multipart message: [num_frames (4 bytes)] + [frame_len (4
    // bytes) + frame_data]...
    std::string multipart_data;
    uint32_t num_frames = frames.size();
    multipart_data.append(reinterpret_cast<const char*>(&num_frames),
                          sizeof(num_frames));

    for (size_t i = 0; i < frames.size(); i++) {
        std::string frame_data = extractData(frames[i]);
        uint32_t frame_len = frame_data.size();
        multipart_data.append(reinterpret_cast<const char*>(&frame_len),
                              sizeof(frame_len));
        multipart_data.append(frame_data);
    }

    pybind11::gil_scoped_release release;
    auto result =
        async_simple::coro::syncAwait(impl_->communicator->sendDataAsync(
            socket_id, multipart_data.data(), multipart_data.size(), topic));

    return result.code;
}

pybind11::object ZmqInterface::sendMultipartAsync(int socket_id,
                                                  pybind11::list frames,
                                                  pybind11::handle loop,
                                                  const std::string& topic) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    // Encode multipart message
    std::string multipart_data;
    uint32_t num_frames = frames.size();
    multipart_data.append(reinterpret_cast<const char*>(&num_frames),
                          sizeof(num_frames));

    for (size_t i = 0; i < frames.size(); i++) {
        std::string frame_data = extractData(frames[i]);
        uint32_t frame_len = frame_data.size();
        multipart_data.append(reinterpret_cast<const char*>(&frame_len),
                              sizeof(frame_len));
        multipart_data.append(frame_data);
    }

    auto communicator = impl_->communicator.get();

    pybind11::gil_scoped_release release;

    auto coro_lambda = [communicator, socket_id, multipart_data, topic,
                        future_obj, loop]() -> async_simple::coro::Lazy<void> {
        try {
            auto result = co_await communicator->sendDataAsync(
                socket_id, multipart_data.data(), multipart_data.size(), topic);

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
            LOG(ERROR) << "Send multipart coroutine error";
        }
    });

    return future_obj;
}

void ZmqInterface::setMultipartReceiveCallback(int socket_id,
                                               pybind11::function callback) {
    pybind11::gil_scoped_acquire acquire;
    impl_->multipart_callbacks[socket_id] = callback;

    auto interface_ptr = this;
    impl_->communicator->setReceiveCallback(
        socket_id, [interface_ptr, socket_id](
                       std::string_view source, std::string_view data,
                       const std::optional<std::string>& topic) {
            pybind11::gil_scoped_acquire acquire;

            auto it = interface_ptr->impl_->multipart_callbacks.find(socket_id);
            if (it != interface_ptr->impl_->multipart_callbacks.end()) {
                try {
                    // Decode multipart message
                    pybind11::list frames;
                    const char* ptr = data.data();
                    size_t remaining = data.size();

                    if (remaining < sizeof(uint32_t)) {
                        LOG(ERROR) << "Invalid multipart message: too short";
                        return;
                    }

                    uint32_t num_frames;
                    std::memcpy(&num_frames, ptr, sizeof(num_frames));
                    ptr += sizeof(num_frames);
                    remaining -= sizeof(num_frames);

                    for (uint32_t i = 0; i < num_frames; i++) {
                        if (remaining < sizeof(uint32_t)) {
                            LOG(ERROR) << "Invalid multipart message: "
                                          "incomplete frame "
                                          "length";
                            return;
                        }

                        uint32_t frame_len;
                        std::memcpy(&frame_len, ptr, sizeof(frame_len));
                        ptr += sizeof(frame_len);
                        remaining -= sizeof(frame_len);

                        if (remaining < frame_len) {
                            LOG(ERROR) << "Invalid multipart message: "
                                          "incomplete frame data";
                            return;
                        }

                        frames.append(pybind11::bytes(ptr, frame_len));
                        ptr += frame_len;
                        remaining -= frame_len;
                    }

                    pybind11::dict msg;
                    msg["source"] = std::string(source);
                    msg["frames"] = frames;
                    msg["topic"] = topic.value_or("");

                    it->second(msg);
                } catch (const std::exception& e) {
                    LOG(ERROR)
                        << "Failed to decode multipart message: " << e.what();
                }
            }
        });
}

// Helper: Enqueue received message
void enqueueMessage(std::shared_ptr<MessageQueue> queue,
                    ReceivedMessage&& msg) {
    std::lock_guard lock(queue->mutex);
    queue->messages.push(std::move(msg));
    queue->cv.notify_one();
}

// Helper: Dequeue message with timeout
bool dequeueMessage(std::shared_ptr<MessageQueue> queue, ReceivedMessage& msg,
                    int flags) {
    std::unique_lock lock(queue->mutex);

    // Non-blocking mode (DONTWAIT flag = 1)
    if (flags & 1) {
        if (queue->messages.empty()) {
            return false;
        }
        msg = std::move(queue->messages.front());
        queue->messages.pop();
        return true;
    }

    // Blocking mode with timeout
    auto timeout = std::chrono::milliseconds(queue->timeout_ms);
    if (!queue->cv.wait_for(lock, timeout,
                            [&queue] { return !queue->messages.empty(); })) {
        return false;  // Timeout
    }

    msg = std::move(queue->messages.front());
    queue->messages.pop();
    return true;
}

// Enable/disable polling mode
void ZmqInterface::setPollingMode(int socket_id, bool enable) {
    std::lock_guard lock(impl_->queues_mutex);

    if (enable) {
        // Create message queue if not exists
        if (impl_->message_queues.find(socket_id) ==
            impl_->message_queues.end()) {
            impl_->message_queues[socket_id] = std::make_shared<MessageQueue>();
        }
        impl_->message_queues[socket_id]->polling_mode = true;

        // Get timeout from socket options
        auto timeout = getSocketOption(socket_id, ZmqSocketOption::RCVTIMEO);
        impl_->message_queues[socket_id]->timeout_ms = timeout;

        // Setup internal callback immediately when entering polling mode
        auto queue = impl_->message_queues[socket_id];
        auto interface_ptr = this;
        impl_->communicator->setReceiveCallback(
            socket_id, [interface_ptr, socket_id, queue](
                           std::string_view source, std::string_view data,
                           const std::optional<std::string>& topic) {
                ReceivedMessage msg;
                msg.type = MessageType::DATA;
                msg.source = std::string(source);
                msg.data = std::string(data);
                msg.topic = topic.value_or("");
                enqueueMessage(queue, std::move(msg));
            });

        LOG(INFO) << "Socket " << socket_id
                  << " set to polling mode with internal callback";
    } else {
        auto it = impl_->message_queues.find(socket_id);
        if (it != impl_->message_queues.end()) {
            it->second->polling_mode = false;
        }
        LOG(INFO) << "Socket " << socket_id << " set to callback mode";
    }
}

// Blocking receive - general data
pybind11::dict ZmqInterface::recv(int socket_id, int flags) {
    std::shared_ptr<MessageQueue> queue;
    {
        std::lock_guard lock(impl_->queues_mutex);
        auto it = impl_->message_queues.find(socket_id);
        if (it == impl_->message_queues.end() || !it->second->polling_mode) {
            throw std::runtime_error(
                "Socket not in polling mode. Call setPollingMode(True) first "
                "or use callbacks.");
        }
        queue = it->second;
    }

    pybind11::gil_scoped_release release;
    ReceivedMessage msg;
    if (!dequeueMessage(queue, msg, flags)) {
        pybind11::gil_scoped_acquire acquire;
        throw std::runtime_error("Receive timeout");
    }

    pybind11::gil_scoped_acquire acquire;
    pybind11::dict result;
    result["source"] = msg.source;
    result["data"] = pybind11::bytes(msg.data);
    result["topic"] = msg.topic;
    return result;
}

// Async receive - general data
pybind11::object ZmqInterface::recvAsync(int socket_id, pybind11::handle loop,
                                         int flags) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    std::shared_ptr<MessageQueue> queue;
    {
        std::lock_guard lock(impl_->queues_mutex);
        auto it = impl_->message_queues.find(socket_id);
        if (it == impl_->message_queues.end() || !it->second->polling_mode) {
            auto exc = pybind11::module_::import("builtins")
                           .attr("RuntimeError")(
                               pybind11::str("Socket not in polling mode. Call "
                                             "setPollingMode(True) first."));
            future_obj.attr("set_exception")(exc);
            return future_obj;
        }
        queue = it->second;
    }

    // Setup internal callback if not already set
    {
        std::lock_guard lock(impl_->queues_mutex);
        if (impl_->data_callbacks.find(socket_id) ==
            impl_->data_callbacks.end()) {
            auto interface_ptr = this;
            impl_->communicator->setReceiveCallback(
                socket_id, [interface_ptr, socket_id, queue](
                               std::string_view source, std::string_view data,
                               const std::optional<std::string>& topic) {
                    ReceivedMessage msg;
                    msg.type = MessageType::DATA;
                    msg.source = std::string(source);
                    msg.data = std::string(data);
                    msg.topic = topic.value_or("");
                    enqueueMessage(queue, std::move(msg));
                });
        }
    }

    // Run blocking recv in thread pool
    auto run_recv = [this, socket_id, flags, future_obj, loop, queue]() {
        ReceivedMessage msg;
        if (!dequeueMessage(queue, msg, flags)) {
            auto callback = [future_obj, loop]() {
                pybind11::gil_scoped_acquire acquire;
                auto exc =
                    pybind11::module_::import("builtins")
                        .attr("RuntimeError")(pybind11::str("Receive timeout"));
                future_obj.attr("set_exception")(exc);
            };
            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
            return;
        }

        auto callback = [future_obj, loop, msg]() {
            pybind11::gil_scoped_acquire acquire;
            pybind11::dict result;
            result["source"] = msg.source;
            result["data"] = pybind11::bytes(msg.data);
            result["topic"] = msg.topic;
            future_obj.attr("set_result")(result);
        };
        auto py_callback = pybind11::cpp_function(callback);
        loop.attr("call_soon_threadsafe")(py_callback);
    };

    pybind11::gil_scoped_release release;
    std::thread(run_recv).detach();

    return future_obj;
}

// Blocking receive - tensor
pybind11::dict ZmqInterface::recvTensor(int socket_id, int flags) {
    std::shared_ptr<MessageQueue> queue;
    {
        std::lock_guard lock(impl_->queues_mutex);
        auto it = impl_->message_queues.find(socket_id);
        if (it == impl_->message_queues.end() || !it->second->polling_mode) {
            throw std::runtime_error(
                "Socket not in polling mode. Call setPollingMode(True) first "
                "or use callbacks.");
        }
        queue = it->second;
    }

    // Setup internal callback if not already set
    {
        std::lock_guard lock(impl_->queues_mutex);
        if (impl_->tensor_callbacks.find(socket_id) ==
            impl_->tensor_callbacks.end()) {
            auto interface_ptr = this;
            impl_->communicator->setTensorReceiveCallback(
                socket_id,
                [interface_ptr, socket_id, queue](
                    std::string_view source, const TensorInfo& tensor,
                    const std::optional<std::string>& topic) {
                    ReceivedMessage msg;
                    msg.type = MessageType::TENSOR;
                    msg.source = std::string(source);
                    msg.tensor = tensor;
                    msg.topic = topic.value_or("");
                    enqueueMessage(queue, std::move(msg));
                });
        }
    }

    pybind11::gil_scoped_release release;
    ReceivedMessage msg;
    if (!dequeueMessage(queue, msg, flags)) {
        pybind11::gil_scoped_acquire acquire;
        throw std::runtime_error("Receive timeout");
    }

    pybind11::gil_scoped_acquire acquire;
    pybind11::dict result;
    result["source"] = msg.source;
    result["shape"] = pybind11::cast(msg.tensor.shape);
    result["dtype"] = msg.tensor.dtype;
    result["topic"] = msg.topic;
    // TODO: Reconstruct tensor from data_ptr
    return result;
}

// Async receive - tensor
pybind11::object ZmqInterface::recvTensorAsync(int socket_id,
                                               pybind11::handle loop,
                                               int flags) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    std::shared_ptr<MessageQueue> queue;
    {
        std::lock_guard lock(impl_->queues_mutex);
        auto it = impl_->message_queues.find(socket_id);
        if (it == impl_->message_queues.end() || !it->second->polling_mode) {
            auto exc = pybind11::module_::import("builtins")
                           .attr("RuntimeError")(
                               pybind11::str("Socket not in polling mode. Call "
                                             "setPollingMode(True) first."));
            future_obj.attr("set_exception")(exc);
            return future_obj;
        }
        queue = it->second;
    }

    // Setup internal callback if not already set
    {
        std::lock_guard lock(impl_->queues_mutex);
        if (impl_->tensor_callbacks.find(socket_id) ==
            impl_->tensor_callbacks.end()) {
            auto interface_ptr = this;
            impl_->communicator->setTensorReceiveCallback(
                socket_id,
                [interface_ptr, socket_id, queue](
                    std::string_view source, const TensorInfo& tensor,
                    const std::optional<std::string>& topic) {
                    ReceivedMessage msg;
                    msg.type = MessageType::TENSOR;
                    msg.source = std::string(source);
                    msg.tensor = tensor;
                    msg.topic = topic.value_or("");
                    enqueueMessage(queue, std::move(msg));
                });
        }
    }

    auto run_recv = [this, socket_id, flags, future_obj, loop, queue]() {
        ReceivedMessage msg;
        if (!dequeueMessage(queue, msg, flags)) {
            auto callback = [future_obj, loop]() {
                pybind11::gil_scoped_acquire acquire;
                auto exc =
                    pybind11::module_::import("builtins")
                        .attr("RuntimeError")(pybind11::str("Receive timeout"));
                future_obj.attr("set_exception")(exc);
            };
            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
            return;
        }

        auto callback = [future_obj, loop, msg]() {
            pybind11::gil_scoped_acquire acquire;
            pybind11::dict result;
            result["source"] = msg.source;
            result["shape"] = pybind11::cast(msg.tensor.shape);
            result["dtype"] = msg.tensor.dtype;
            result["topic"] = msg.topic;
            future_obj.attr("set_result")(result);
        };
        auto py_callback = pybind11::cpp_function(callback);
        loop.attr("call_soon_threadsafe")(py_callback);
    };

    pybind11::gil_scoped_release release;
    std::thread(run_recv).detach();

    return future_obj;
}

// Blocking receive - pyobj
pybind11::dict ZmqInterface::recvPyobj(int socket_id, int flags) {
    std::shared_ptr<MessageQueue> queue;
    {
        std::lock_guard lock(impl_->queues_mutex);
        auto it = impl_->message_queues.find(socket_id);
        if (it == impl_->message_queues.end() || !it->second->polling_mode) {
            throw std::runtime_error(
                "Socket not in polling mode. Call setPollingMode(True) first "
                "or use callbacks.");
        }
        queue = it->second;
    }

    // Setup internal callback if not already set
    {
        std::lock_guard lock(impl_->queues_mutex);
        if (impl_->pyobj_callbacks.find(socket_id) ==
            impl_->pyobj_callbacks.end()) {
            auto interface_ptr = this;
            impl_->communicator->setReceiveCallback(
                socket_id, [interface_ptr, socket_id, queue](
                               std::string_view source, std::string_view data,
                               const std::optional<std::string>& topic) {
                    ReceivedMessage msg;
                    msg.type = MessageType::PYOBJ;
                    msg.source = std::string(source);
                    msg.data = std::string(data);
                    msg.topic = topic.value_or("");
                    enqueueMessage(queue, std::move(msg));
                });
        }
    }

    pybind11::gil_scoped_release release;
    ReceivedMessage msg;
    if (!dequeueMessage(queue, msg, flags)) {
        pybind11::gil_scoped_acquire acquire;
        throw std::runtime_error("Receive timeout");
    }

    pybind11::gil_scoped_acquire acquire;

    if (!pickle_deserialization_enabled()) {
        throw std::runtime_error(
            "pickle deserialization is disabled by default "
            "(set MOONCAKE_ALLOW_PICKLE=1 to enable)");
    }

    // Deserialize Python object
    auto pickle = pybind11::module_::import("pickle");
    pybind11::bytes data_bytes = pybind11::bytes(msg.data);
    pybind11::object obj = pickle.attr("loads")(data_bytes);

    pybind11::dict result;
    result["source"] = msg.source;
    result["obj"] = obj;
    result["topic"] = msg.topic;
    return result;
}

// Async receive - pyobj
pybind11::object ZmqInterface::recvPyobjAsync(int socket_id,
                                              pybind11::handle loop,
                                              int flags) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    std::shared_ptr<MessageQueue> queue;
    {
        std::lock_guard lock(impl_->queues_mutex);
        auto it = impl_->message_queues.find(socket_id);
        if (it == impl_->message_queues.end() || !it->second->polling_mode) {
            auto exc = pybind11::module_::import("builtins")
                           .attr("RuntimeError")(
                               pybind11::str("Socket not in polling mode. Call "
                                             "setPollingMode(True) first."));
            future_obj.attr("set_exception")(exc);
            return future_obj;
        }
        queue = it->second;
    }

    // Setup internal callback if not already set
    {
        std::lock_guard lock(impl_->queues_mutex);
        if (impl_->pyobj_callbacks.find(socket_id) ==
            impl_->pyobj_callbacks.end()) {
            auto interface_ptr = this;
            impl_->communicator->setReceiveCallback(
                socket_id, [interface_ptr, socket_id, queue](
                               std::string_view source, std::string_view data,
                               const std::optional<std::string>& topic) {
                    ReceivedMessage msg;
                    msg.type = MessageType::PYOBJ;
                    msg.source = std::string(source);
                    msg.data = std::string(data);
                    msg.topic = topic.value_or("");
                    enqueueMessage(queue, std::move(msg));
                });
        }
    }

    auto run_recv = [this, socket_id, flags, future_obj, loop, queue]() {
        ReceivedMessage msg;
        if (!dequeueMessage(queue, msg, flags)) {
            auto callback = [future_obj, loop]() {
                pybind11::gil_scoped_acquire acquire;
                auto exc =
                    pybind11::module_::import("builtins")
                        .attr("RuntimeError")(pybind11::str("Receive timeout"));
                future_obj.attr("set_exception")(exc);
            };
            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
            return;
        }

        auto callback = [future_obj, loop, msg]() {
            pybind11::gil_scoped_acquire acquire;

            try {
                if (!pickle_deserialization_enabled()) {
                    auto exc =
                        pybind11::module_::import("builtins")
                            .attr("RuntimeError")(pybind11::str(
                                "pickle deserialization is disabled by default "
                                "(set MOONCAKE_ALLOW_PICKLE=1 to enable)"));
                    future_obj.attr("set_exception")(exc);
                    return;
                }
                auto pickle = pybind11::module_::import("pickle");
                pybind11::bytes data_bytes = pybind11::bytes(msg.data);
                pybind11::object obj = pickle.attr("loads")(data_bytes);

                pybind11::dict result;
                result["source"] = msg.source;
                result["obj"] = obj;
                result["topic"] = msg.topic;
                future_obj.attr("set_result")(result);
            } catch (const std::exception& e) {
                auto exc =
                    pybind11::module_::import("builtins")
                        .attr("RuntimeError")(pybind11::str(
                            std::string("Failed to unpickle: ") + e.what()));
                future_obj.attr("set_exception")(exc);
            }
        };
        auto py_callback = pybind11::cpp_function(callback);
        loop.attr("call_soon_threadsafe")(py_callback);
    };

    pybind11::gil_scoped_release release;
    std::thread(run_recv).detach();

    return future_obj;
}

// Blocking receive - multipart
pybind11::dict ZmqInterface::recvMultipart(int socket_id, int flags) {
    std::shared_ptr<MessageQueue> queue;
    {
        std::lock_guard lock(impl_->queues_mutex);
        auto it = impl_->message_queues.find(socket_id);
        if (it == impl_->message_queues.end() || !it->second->polling_mode) {
            throw std::runtime_error(
                "Socket not in polling mode. Call setPollingMode(True) first "
                "or use callbacks.");
        }
        queue = it->second;
    }

    // Setup internal callback if not already set
    {
        std::lock_guard lock(impl_->queues_mutex);
        if (impl_->multipart_callbacks.find(socket_id) ==
            impl_->multipart_callbacks.end()) {
            auto interface_ptr = this;
            impl_->communicator->setReceiveCallback(
                socket_id, [interface_ptr, socket_id, queue](
                               std::string_view source, std::string_view data,
                               const std::optional<std::string>& topic) {
                    // Decode multipart message
                    const char* ptr = data.data();
                    size_t remaining = data.size();

                    if (remaining < sizeof(uint32_t)) {
                        LOG(ERROR) << "Invalid multipart message: too short";
                        return;
                    }

                    uint32_t num_frames;
                    std::memcpy(&num_frames, ptr, sizeof(num_frames));
                    ptr += sizeof(num_frames);
                    remaining -= sizeof(num_frames);

                    ReceivedMessage msg;
                    msg.type = MessageType::MULTIPART;
                    msg.source = std::string(source);
                    msg.topic = topic.value_or("");

                    for (uint32_t i = 0; i < num_frames; i++) {
                        if (remaining < sizeof(uint32_t)) {
                            LOG(ERROR) << "Invalid multipart message: "
                                          "incomplete frame length";
                            return;
                        }

                        uint32_t frame_len;
                        std::memcpy(&frame_len, ptr, sizeof(frame_len));
                        ptr += sizeof(frame_len);
                        remaining -= sizeof(frame_len);

                        if (remaining < frame_len) {
                            LOG(ERROR) << "Invalid multipart message: "
                                          "incomplete frame data";
                            return;
                        }

                        msg.frames.emplace_back(ptr, frame_len);
                        ptr += frame_len;
                        remaining -= frame_len;
                    }

                    enqueueMessage(queue, std::move(msg));
                });
        }
    }

    pybind11::gil_scoped_release release;
    ReceivedMessage msg;
    if (!dequeueMessage(queue, msg, flags)) {
        pybind11::gil_scoped_acquire acquire;
        throw std::runtime_error("Receive timeout");
    }

    pybind11::gil_scoped_acquire acquire;
    pybind11::list frames;
    for (const auto& frame : msg.frames) {
        frames.append(pybind11::bytes(frame));
    }

    pybind11::dict result;
    result["source"] = msg.source;
    result["frames"] = frames;
    result["topic"] = msg.topic;
    return result;
}

// Async receive - multipart
pybind11::object ZmqInterface::recvMultipartAsync(int socket_id,
                                                  pybind11::handle loop,
                                                  int flags) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    std::shared_ptr<MessageQueue> queue;
    {
        std::lock_guard lock(impl_->queues_mutex);
        auto it = impl_->message_queues.find(socket_id);
        if (it == impl_->message_queues.end() || !it->second->polling_mode) {
            auto exc = pybind11::module_::import("builtins")
                           .attr("RuntimeError")(
                               pybind11::str("Socket not in polling mode. Call "
                                             "setPollingMode(True) first."));
            future_obj.attr("set_exception")(exc);
            return future_obj;
        }
        queue = it->second;
    }

    // Setup internal callback if not already set
    {
        std::lock_guard lock(impl_->queues_mutex);
        if (impl_->multipart_callbacks.find(socket_id) ==
            impl_->multipart_callbacks.end()) {
            auto interface_ptr = this;
            impl_->communicator->setReceiveCallback(
                socket_id, [interface_ptr, socket_id, queue](
                               std::string_view source, std::string_view data,
                               const std::optional<std::string>& topic) {
                    // Decode multipart message
                    const char* ptr = data.data();
                    size_t remaining = data.size();

                    if (remaining < sizeof(uint32_t)) {
                        LOG(ERROR) << "Invalid multipart message: too short";
                        return;
                    }

                    uint32_t num_frames;
                    std::memcpy(&num_frames, ptr, sizeof(num_frames));
                    ptr += sizeof(num_frames);
                    remaining -= sizeof(num_frames);

                    ReceivedMessage msg;
                    msg.type = MessageType::MULTIPART;
                    msg.source = std::string(source);
                    msg.topic = topic.value_or("");

                    for (uint32_t i = 0; i < num_frames; i++) {
                        if (remaining < sizeof(uint32_t)) {
                            LOG(ERROR) << "Invalid multipart message: "
                                          "incomplete frame length";
                            return;
                        }

                        uint32_t frame_len;
                        std::memcpy(&frame_len, ptr, sizeof(frame_len));
                        ptr += sizeof(frame_len);
                        remaining -= sizeof(frame_len);

                        if (remaining < frame_len) {
                            LOG(ERROR) << "Invalid multipart message: "
                                          "incomplete frame data";
                            return;
                        }

                        msg.frames.emplace_back(ptr, frame_len);
                        ptr += frame_len;
                        remaining -= frame_len;
                    }

                    enqueueMessage(queue, std::move(msg));
                });
        }
    }

    auto run_recv = [this, socket_id, flags, future_obj, loop, queue]() {
        ReceivedMessage msg;
        if (!dequeueMessage(queue, msg, flags)) {
            auto callback = [future_obj, loop]() {
                pybind11::gil_scoped_acquire acquire;
                auto exc =
                    pybind11::module_::import("builtins")
                        .attr("RuntimeError")(pybind11::str("Receive timeout"));
                future_obj.attr("set_exception")(exc);
            };
            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
            return;
        }

        auto callback = [future_obj, loop, msg]() {
            pybind11::gil_scoped_acquire acquire;
            pybind11::list frames;
            for (const auto& frame : msg.frames) {
                frames.append(pybind11::bytes(frame));
            }

            pybind11::dict result;
            result["source"] = msg.source;
            result["frames"] = frames;
            result["topic"] = msg.topic;
            future_obj.attr("set_result")(result);
        };
        auto py_callback = pybind11::cpp_function(callback);
        loop.attr("call_soon_threadsafe")(py_callback);
    };

    pybind11::gil_scoped_release release;
    std::thread(run_recv).detach();

    return future_obj;
}

// ============================================================================
// JSON messages (ZMQ-compatible: send_json/recv_json)
// ============================================================================

int ZmqInterface::sendJson(int socket_id, pybind11::handle obj,
                           const std::string& topic) {
    pybind11::gil_scoped_acquire acquire;

    // Use json module to serialize the Python object
    auto json_module = pybind11::module_::import("json");
    auto json_str = json_module.attr("dumps")(obj).cast<std::string>();

    pybind11::gil_scoped_release release;
    auto result =
        async_simple::coro::syncAwait(impl_->communicator->sendDataAsync(
            socket_id, json_str.data(), json_str.size(), topic));

    return result.code;
}

pybind11::object ZmqInterface::sendJsonAsync(int socket_id,
                                             pybind11::handle obj,
                                             pybind11::handle loop,
                                             const std::string& topic) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    // Use json module to serialize the Python object
    auto json_module = pybind11::module_::import("json");
    auto json_str = json_module.attr("dumps")(obj).cast<std::string>();
    auto communicator = impl_->communicator.get();

    pybind11::gil_scoped_release release;

    auto coro_lambda = [communicator, socket_id, json_str, topic, future_obj,
                        loop]() -> async_simple::coro::Lazy<void> {
        try {
            auto result = co_await communicator->sendDataAsync(
                socket_id, json_str.data(), json_str.size(), topic);

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
            LOG(ERROR) << "Send json coroutine error";
        }
    });

    return future_obj;
}

void ZmqInterface::setJsonReceiveCallback(int socket_id,
                                          pybind11::function callback) {
    pybind11::gil_scoped_acquire acquire;
    impl_->json_callbacks[socket_id] = callback;

    auto interface_ptr = this;
    impl_->communicator->setReceiveCallback(
        socket_id, [interface_ptr, socket_id](
                       std::string_view source, std::string_view data,
                       const std::optional<std::string>& topic) {
            pybind11::gil_scoped_acquire acquire;

            auto it = interface_ptr->impl_->json_callbacks.find(socket_id);
            if (it != interface_ptr->impl_->json_callbacks.end()) {
                try {
                    // Use json module to deserialize
                    auto json_module = pybind11::module_::import("json");
                    std::string json_str(data);
                    pybind11::object obj = json_module.attr("loads")(json_str);

                    pybind11::dict msg;
                    msg["source"] = std::string(source);
                    msg["obj"] = obj;
                    msg["topic"] = topic.value_or("");

                    it->second(msg);
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Failed to parse JSON: " << e.what();
                }
            }
        });
}

pybind11::dict ZmqInterface::recvJson(int socket_id, int flags) {
    std::shared_ptr<MessageQueue> queue;
    {
        std::lock_guard lock(impl_->queues_mutex);
        auto it = impl_->message_queues.find(socket_id);
        if (it == impl_->message_queues.end() || !it->second->polling_mode) {
            throw std::runtime_error(
                "Socket not in polling mode. Call setPollingMode(True) first "
                "or use callbacks.");
        }
        queue = it->second;
    }

    // Setup internal callback if not already set
    {
        std::lock_guard lock(impl_->queues_mutex);
        if (impl_->json_callbacks.find(socket_id) ==
            impl_->json_callbacks.end()) {
            auto interface_ptr = this;
            impl_->communicator->setReceiveCallback(
                socket_id, [interface_ptr, socket_id, queue](
                               std::string_view source, std::string_view data,
                               const std::optional<std::string>& topic) {
                    ReceivedMessage msg;
                    msg.type = MessageType::JSON;
                    msg.source = std::string(source);
                    msg.data = std::string(data);
                    msg.topic = topic.value_or("");
                    enqueueMessage(queue, std::move(msg));
                });
        }
    }

    pybind11::gil_scoped_release release;
    ReceivedMessage msg;
    if (!dequeueMessage(queue, msg, flags)) {
        pybind11::gil_scoped_acquire acquire;
        throw std::runtime_error("Receive timeout");
    }

    pybind11::gil_scoped_acquire acquire;

    // Deserialize JSON
    auto json_module = pybind11::module_::import("json");
    pybind11::object obj = json_module.attr("loads")(msg.data);

    pybind11::dict result;
    result["source"] = msg.source;
    result["obj"] = obj;
    result["topic"] = msg.topic;
    return result;
}

pybind11::object ZmqInterface::recvJsonAsync(int socket_id,
                                             pybind11::handle loop, int flags) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    std::shared_ptr<MessageQueue> queue;
    {
        std::lock_guard lock(impl_->queues_mutex);
        auto it = impl_->message_queues.find(socket_id);
        if (it == impl_->message_queues.end() || !it->second->polling_mode) {
            auto exc = pybind11::module_::import("builtins")
                           .attr("RuntimeError")(
                               pybind11::str("Socket not in polling mode. Call "
                                             "setPollingMode(True) first."));
            future_obj.attr("set_exception")(exc);
            return future_obj;
        }
        queue = it->second;
    }

    // Setup internal callback if not already set
    {
        std::lock_guard lock(impl_->queues_mutex);
        if (impl_->json_callbacks.find(socket_id) ==
            impl_->json_callbacks.end()) {
            auto interface_ptr = this;
            impl_->communicator->setReceiveCallback(
                socket_id, [interface_ptr, socket_id, queue](
                               std::string_view source, std::string_view data,
                               const std::optional<std::string>& topic) {
                    ReceivedMessage msg;
                    msg.type = MessageType::JSON;
                    msg.source = std::string(source);
                    msg.data = std::string(data);
                    msg.topic = topic.value_or("");
                    enqueueMessage(queue, std::move(msg));
                });
        }
    }

    auto run_recv = [this, socket_id, flags, future_obj, loop, queue]() {
        ReceivedMessage msg;
        if (!dequeueMessage(queue, msg, flags)) {
            auto callback = [future_obj, loop]() {
                pybind11::gil_scoped_acquire acquire;
                auto exc =
                    pybind11::module_::import("builtins")
                        .attr("RuntimeError")(pybind11::str("Receive timeout"));
                future_obj.attr("set_exception")(exc);
            };
            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
            return;
        }

        auto callback = [future_obj, loop, msg]() {
            pybind11::gil_scoped_acquire acquire;

            try {
                auto json_module = pybind11::module_::import("json");
                pybind11::object obj = json_module.attr("loads")(msg.data);

                pybind11::dict result;
                result["source"] = msg.source;
                result["obj"] = obj;
                result["topic"] = msg.topic;
                future_obj.attr("set_result")(result);
            } catch (const std::exception& e) {
                auto exc =
                    pybind11::module_::import("builtins")
                        .attr("RuntimeError")(pybind11::str(
                            std::string("Failed to parse JSON: ") + e.what()));
                future_obj.attr("set_exception")(exc);
            }
        };
        auto py_callback = pybind11::cpp_function(callback);
        loop.attr("call_soon_threadsafe")(py_callback);
    };

    pybind11::gil_scoped_release release;
    std::thread(run_recv).detach();

    return future_obj;
}

// ============================================================================
// String messages (ZMQ-compatible: send_string/recv_string)
// ============================================================================

int ZmqInterface::sendString(int socket_id, const std::string& str,
                             const std::string& topic,
                             const std::string& encoding) {
    // String is already UTF-8 in C++, encode to bytes if needed
    std::string data_str = str;

    // If encoding is not utf-8, use Python to encode
    if (encoding != "utf-8") {
        pybind11::gil_scoped_acquire acquire;
        pybind11::str py_str(str);
        pybind11::bytes encoded = py_str.attr("encode")(encoding);
        data_str = encoded.cast<std::string>();
    }

    pybind11::gil_scoped_release release;
    auto result =
        async_simple::coro::syncAwait(impl_->communicator->sendDataAsync(
            socket_id, data_str.data(), data_str.size(), topic));

    return result.code;
}

pybind11::object ZmqInterface::sendStringAsync(int socket_id,
                                               const std::string& str,
                                               pybind11::handle loop,
                                               const std::string& topic,
                                               const std::string& encoding) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    // String encoding
    std::string data_str = str;
    if (encoding != "utf-8") {
        pybind11::str py_str(str);
        pybind11::bytes encoded = py_str.attr("encode")(encoding);
        data_str = encoded.cast<std::string>();
    }

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
            LOG(ERROR) << "Send string coroutine error";
        }
    });

    return future_obj;
}

void ZmqInterface::setStringReceiveCallback(int socket_id,
                                            pybind11::function callback,
                                            const std::string& encoding) {
    pybind11::gil_scoped_acquire acquire;
    impl_->string_callbacks[socket_id] = callback;
    impl_->string_encodings[socket_id] = encoding;

    auto interface_ptr = this;
    impl_->communicator->setReceiveCallback(
        socket_id, [interface_ptr, socket_id, encoding](
                       std::string_view source, std::string_view data,
                       const std::optional<std::string>& topic) {
            pybind11::gil_scoped_acquire acquire;

            auto it = interface_ptr->impl_->string_callbacks.find(socket_id);
            if (it != interface_ptr->impl_->string_callbacks.end()) {
                try {
                    // Decode bytes to string
                    std::string data_str(data);
                    pybind11::str decoded_str;

                    if (encoding == "utf-8") {
                        decoded_str = pybind11::str(data_str);
                    } else {
                        pybind11::bytes data_bytes(data_str);
                        decoded_str = data_bytes.attr("decode")(encoding);
                    }

                    pybind11::dict msg;
                    msg["source"] = std::string(source);
                    msg["string"] = decoded_str;
                    msg["topic"] = topic.value_or("");

                    it->second(msg);
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Failed to decode string: " << e.what();
                }
            }
        });
}

pybind11::dict ZmqInterface::recvString(int socket_id, int flags,
                                        const std::string& encoding) {
    std::shared_ptr<MessageQueue> queue;
    {
        std::lock_guard lock(impl_->queues_mutex);
        auto it = impl_->message_queues.find(socket_id);
        if (it == impl_->message_queues.end() || !it->second->polling_mode) {
            throw std::runtime_error(
                "Socket not in polling mode. Call setPollingMode(True) first "
                "or use callbacks.");
        }
        queue = it->second;
    }

    // Setup internal callback if not already set
    {
        std::lock_guard lock(impl_->queues_mutex);
        if (impl_->string_callbacks.find(socket_id) ==
            impl_->string_callbacks.end()) {
            auto interface_ptr = this;
            impl_->communicator->setReceiveCallback(
                socket_id, [interface_ptr, socket_id, queue](
                               std::string_view source, std::string_view data,
                               const std::optional<std::string>& topic) {
                    ReceivedMessage msg;
                    msg.type = MessageType::STRING;
                    msg.source = std::string(source);
                    msg.data = std::string(data);
                    msg.topic = topic.value_or("");
                    enqueueMessage(queue, std::move(msg));
                });
        }
    }

    pybind11::gil_scoped_release release;
    ReceivedMessage msg;
    if (!dequeueMessage(queue, msg, flags)) {
        pybind11::gil_scoped_acquire acquire;
        throw std::runtime_error("Receive timeout");
    }

    pybind11::gil_scoped_acquire acquire;

    // Decode string
    pybind11::str decoded_str;
    if (encoding == "utf-8") {
        decoded_str = pybind11::str(msg.data);
    } else {
        pybind11::bytes data_bytes(msg.data);
        decoded_str = data_bytes.attr("decode")(encoding);
    }

    pybind11::dict result;
    result["source"] = msg.source;
    result["string"] = decoded_str;
    result["topic"] = msg.topic;
    return result;
}

pybind11::object ZmqInterface::recvStringAsync(int socket_id,
                                               pybind11::handle loop, int flags,
                                               const std::string& encoding) {
    pybind11::gil_scoped_acquire acquire;

    auto asyncio = pybind11::module_::import("asyncio");
    auto future_obj = asyncio.attr("Future")();

    std::shared_ptr<MessageQueue> queue;
    {
        std::lock_guard lock(impl_->queues_mutex);
        auto it = impl_->message_queues.find(socket_id);
        if (it == impl_->message_queues.end() || !it->second->polling_mode) {
            auto exc = pybind11::module_::import("builtins")
                           .attr("RuntimeError")(
                               pybind11::str("Socket not in polling mode. Call "
                                             "setPollingMode(True) first."));
            future_obj.attr("set_exception")(exc);
            return future_obj;
        }
        queue = it->second;
    }

    // Setup internal callback if not already set
    {
        std::lock_guard lock(impl_->queues_mutex);
        if (impl_->string_callbacks.find(socket_id) ==
            impl_->string_callbacks.end()) {
            auto interface_ptr = this;
            impl_->communicator->setReceiveCallback(
                socket_id, [interface_ptr, socket_id, queue](
                               std::string_view source, std::string_view data,
                               const std::optional<std::string>& topic) {
                    ReceivedMessage msg;
                    msg.type = MessageType::STRING;
                    msg.source = std::string(source);
                    msg.data = std::string(data);
                    msg.topic = topic.value_or("");
                    enqueueMessage(queue, std::move(msg));
                });
        }
    }

    auto run_recv = [this, socket_id, flags, future_obj, loop, queue,
                     encoding]() {
        ReceivedMessage msg;
        if (!dequeueMessage(queue, msg, flags)) {
            auto callback = [future_obj, loop]() {
                pybind11::gil_scoped_acquire acquire;
                auto exc =
                    pybind11::module_::import("builtins")
                        .attr("RuntimeError")(pybind11::str("Receive timeout"));
                future_obj.attr("set_exception")(exc);
            };
            auto py_callback = pybind11::cpp_function(callback);
            loop.attr("call_soon_threadsafe")(py_callback);
            return;
        }

        auto callback = [future_obj, loop, msg, encoding]() {
            pybind11::gil_scoped_acquire acquire;

            try {
                pybind11::str decoded_str;
                if (encoding == "utf-8") {
                    decoded_str = pybind11::str(msg.data);
                } else {
                    pybind11::bytes data_bytes(msg.data);
                    decoded_str = data_bytes.attr("decode")(encoding);
                }

                pybind11::dict result;
                result["source"] = msg.source;
                result["string"] = decoded_str;
                result["topic"] = msg.topic;
                future_obj.attr("set_result")(result);
            } catch (const std::exception& e) {
                auto exc = pybind11::module_::import("builtins")
                               .attr("RuntimeError")(pybind11::str(
                                   std::string("Failed to decode string: ") +
                                   e.what()));
                future_obj.attr("set_exception")(exc);
            }
        };
        auto py_callback = pybind11::cpp_function(callback);
        loop.attr("call_soon_threadsafe")(py_callback);
    };

    pybind11::gil_scoped_release release;
    std::thread(run_recv).detach();

    return future_obj;
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
             &ZmqInterface::setTensorReceiveCallback)
        // Python object serialization (pickle) - ZMQ compatible
        .def("send_pyobj", &ZmqInterface::sendPyobj, py::arg("socket_id"),
             py::arg("obj"), py::arg("topic") = "")
        .def("send_pyobj_async", &ZmqInterface::sendPyobjAsync,
             py::arg("socket_id"), py::arg("obj"), py::arg("loop"),
             py::arg("topic") = "")
        .def("set_pyobj_receive_callback",
             &ZmqInterface::setPyobjReceiveCallback)
        // Multipart messages - ZMQ compatible
        .def("send_multipart", &ZmqInterface::sendMultipart,
             py::arg("socket_id"), py::arg("frames"), py::arg("topic") = "")
        .def("send_multipart_async", &ZmqInterface::sendMultipartAsync,
             py::arg("socket_id"), py::arg("frames"), py::arg("loop"),
             py::arg("topic") = "")
        .def("set_multipart_receive_callback",
             &ZmqInterface::setMultipartReceiveCallback)
        // Blocking receive methods (polling mode)
        .def("recv", &ZmqInterface::recv, py::arg("socket_id"),
             py::arg("flags") = 0)
        .def("recv_async", &ZmqInterface::recvAsync, py::arg("socket_id"),
             py::arg("loop"), py::arg("flags") = 0)
        .def("recv_tensor", &ZmqInterface::recvTensor, py::arg("socket_id"),
             py::arg("flags") = 0)
        .def("recv_tensor_async", &ZmqInterface::recvTensorAsync,
             py::arg("socket_id"), py::arg("loop"), py::arg("flags") = 0)
        .def("recv_pyobj", &ZmqInterface::recvPyobj, py::arg("socket_id"),
             py::arg("flags") = 0)
        .def("recv_pyobj_async", &ZmqInterface::recvPyobjAsync,
             py::arg("socket_id"), py::arg("loop"), py::arg("flags") = 0)
        .def("recv_multipart", &ZmqInterface::recvMultipart,
             py::arg("socket_id"), py::arg("flags") = 0)
        .def("recv_multipart_async", &ZmqInterface::recvMultipartAsync,
             py::arg("socket_id"), py::arg("loop"), py::arg("flags") = 0)
        // JSON messages - ZMQ compatible
        .def("send_json", &ZmqInterface::sendJson, py::arg("socket_id"),
             py::arg("obj"), py::arg("topic") = "")
        .def("send_json_async", &ZmqInterface::sendJsonAsync,
             py::arg("socket_id"), py::arg("obj"), py::arg("loop"),
             py::arg("topic") = "")
        .def("set_json_receive_callback", &ZmqInterface::setJsonReceiveCallback)
        .def("recv_json", &ZmqInterface::recvJson, py::arg("socket_id"),
             py::arg("flags") = 0)
        .def("recv_json_async", &ZmqInterface::recvJsonAsync,
             py::arg("socket_id"), py::arg("loop"), py::arg("flags") = 0)
        // String messages - ZMQ compatible
        .def("send_string", &ZmqInterface::sendString, py::arg("socket_id"),
             py::arg("str"), py::arg("topic") = "",
             py::arg("encoding") = "utf-8")
        .def("send_string_async", &ZmqInterface::sendStringAsync,
             py::arg("socket_id"), py::arg("str"), py::arg("loop"),
             py::arg("topic") = "", py::arg("encoding") = "utf-8")
        .def("set_string_receive_callback",
             &ZmqInterface::setStringReceiveCallback, py::arg("socket_id"),
             py::arg("callback"), py::arg("encoding") = "utf-8")
        .def("recv_string", &ZmqInterface::recvString, py::arg("socket_id"),
             py::arg("flags") = 0, py::arg("encoding") = "utf-8")
        .def("recv_string_async", &ZmqInterface::recvStringAsync,
             py::arg("socket_id"), py::arg("loop"), py::arg("flags") = 0,
             py::arg("encoding") = "utf-8")
        // Polling mode control
        .def("set_polling_mode", &ZmqInterface::setPollingMode,
             py::arg("socket_id"), py::arg("enable"));
}

}  // namespace mooncake
