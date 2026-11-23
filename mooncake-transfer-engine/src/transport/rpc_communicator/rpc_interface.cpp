#include <nanobind/stl/function.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/unique_ptr.h>
#include <nanobind/stl/vector.h>
#include "transport/rpc_communicator/rpc_interface.h"
#include "transport/rpc_communicator/rpc_communicator.h"
#include "config.h"
#include <memory>
#include <thread>
#include <utility>
#include <future>
#include <vector>
#include <glog/logging.h>
#include "async_simple/coro/SyncAwait.h"

namespace py = nanobind;

namespace mooncake {

static constexpr size_t MAX_TENSOR_DIMS = 4;
// Tensor metadata size: dtype (4 bytes) + ndim (4 bytes) + shape dimensions
// (MAX_TENSOR_DIMS * 8 bytes)
static constexpr size_t TENSOR_METADATA_SIZE = 4 + 4 + MAX_TENSOR_DIMS * 8;

namespace {

using RpcCompletion = std::function<void(int, const std::string&)>;

RpcCompletion ScheduleRpcCompletion(py::object future, py::handle loop,
                                    py::object request_owner = {}) {
    // nanobind's std::function caster acquires the GIL for Python calls and
    // reference release even when the coroutine completes on an I/O thread.
    return py::cast<RpcCompletion>(py::cpp_function(
        [future = std::move(future), loop = py::borrow<py::object>(loop),
         request_owner = std::move(request_owner)](int result,
                                                   const std::string& error) {
            auto schedule = loop.attr("call_soon_threadsafe");
            if (error.empty()) {
                schedule(future.attr("set_result"), result);
                return;
            }
            auto exc = py::module_::import_("builtins").attr("RuntimeError");
            schedule(future.attr("set_exception"), exc(error));
        }));
}

// A Python callback can drop the last reference to its own server.
thread_local const RpcInterface::Impl* receiving_on = nullptr;

class ReceiveScope {
   public:
    explicit ReceiveScope(const RpcInterface::Impl* receiver)
        : previous_(std::exchange(receiving_on, receiver)) {}
    ~ReceiveScope() { receiving_on = previous_; }

   private:
    const RpcInterface::Impl* previous_;
};

}  // namespace

// Implementation class
class __attribute__((visibility("hidden"))) RpcInterface::Impl {
   public:
    std::shared_ptr<RpcCommunicator> communicator;
    py::callable data_receive_callback;
    py::callable tensor_receive_callback;
    bool stopping = false;  // Accessed with the GIL held.

    void handleIncomingData(std::string_view source, std::string_view data);
    void handleIncomingTensor(std::string_view source, std::string_view data,
                              const std::vector<size_t>& shape,
                              std::string_view dtype);
};

int RpcInterface::traversePythonReferences(PyObject* self, visitproc visit,
                                           void* arg) {
    Py_VISIT(Py_TYPE(self));
    if (py::inst_ready(self)) {
        const auto& impl = *py::inst_ptr<RpcInterface>(self)->impl_;
        Py_VISIT(impl.data_receive_callback.ptr());
        Py_VISIT(impl.tensor_receive_callback.ptr());
    }
    return 0;
}

int RpcInterface::clearPythonReferences(PyObject* self) {
    if (py::inst_ready(self)) {
        py::inst_ptr<RpcInterface>(self)->shutdown();
    }
    return 0;
}

// Constructor
RpcInterface::RpcInterface() : impl_(std::make_unique<Impl>()) {}

// Destructor
RpcInterface::~RpcInterface() { shutdown(); }

void RpcInterface::shutdown() {
    if (receiving_on == impl_.get()) {
        // The receive worker cannot join itself. Keep the native state and
        // the callback references alive until a different thread joins it.
        auto impl = std::exchange(impl_, std::make_unique<Impl>());
        impl->stopping = true;
        std::thread([impl = std::move(impl)]() mutable {
            impl->communicator->stopServer();
            py::gil_scoped_acquire acquire;
            impl.reset();
        }).detach();
        return;
    }

    stopServer();
    // Clear both fields before releasing either Python reference.
    auto data_callback = std::move(impl_->data_receive_callback);
    auto tensor_callback = std::move(impl_->tensor_receive_callback);
}

// Initialize
bool RpcInterface::initialize(const std::string& local_address,
                              size_t thread_count, size_t timeout_seconds,
                              size_t pool_size) {
    if (impl_->stopping) return false;
    stopServer();

    RpcCommunicatorConfig config;
    config.listen_address = local_address;
    config.thread_count = thread_count;
    config.timeout_seconds = timeout_seconds;
    config.pool_size = pool_size;

    auto communicator = std::make_shared<RpcCommunicator>();
    if (!communicator->initialize(config)) return false;
    if (!local_address.empty()) {
        communicator->setDataReceiveCallback(
            [impl = impl_.get()](std::string_view source,
                                 std::string_view data) {
                ReceiveScope receive(impl);
                impl->handleIncomingData(source, data);
            });
    }
    impl_->communicator = std::move(communicator);
    return true;
}

// Convenience method for client initialization
bool RpcInterface::initializeClient(size_t pool_size, size_t timeout_seconds) {
    return initialize("", 0, timeout_seconds, pool_size);
}

// Convenience method for server initialization
bool RpcInterface::initializeServer(const std::string& listen_address,
                                    size_t thread_count,
                                    size_t timeout_seconds) {
    return initialize(listen_address, thread_count, timeout_seconds, 100);
}

bool RpcInterface::startServer() {
    if (!impl_->communicator || impl_->stopping) return false;
    return impl_->communicator->startServer();
}

bool RpcInterface::startServerAsync() {
    if (!impl_->communicator || impl_->stopping) return false;
    return impl_->communicator->startServerAsync();
}

bool RpcInterface::startServerImpl(bool is_async) {
    if (!impl_->communicator || impl_->stopping) return false;
    return impl_->communicator->startServerImpl(is_async);
}

void RpcInterface::stopServer() {
    if (!impl_->communicator || impl_->stopping) return;
    if (receiving_on == impl_.get()) {
        throw std::runtime_error(
            "Cannot stop the RPC server from its receive callback");
    }

    impl_->stopping = true;
    {
        py::gil_scoped_release release;
        impl_->communicator->stopServer();
    }
    impl_->stopping = false;
}

int RpcInterface::sendData(const std::string& target_address, py::handle data) {
    if (!impl_->communicator) return -1;

    py::gil_scoped_acquire acquire;

    const auto data_bytes = py::bytes(data);
    const std::string data_str(data_bytes.c_str(), data_bytes.size());

    py::gil_scoped_release release;
    return impl_->communicator->sendData(target_address, data_str.data(),
                                         data_str.size());
}

py::object RpcInterface::sendDataAsync(const std::string& target_address,
                                       py::handle data, py::handle loop) {
    py::gil_scoped_acquire acquire;

    auto future_obj = loop.attr("create_future")();

    if (!impl_->communicator) {
        auto exc_type = py::module_::import_("builtins").attr("RuntimeError");
        auto exc = exc_type(py::str("Communicator not initialized"));
        future_obj.attr("set_exception")(exc);
        return future_obj;
    }

    const auto data_bytes = py::bytes(data);
    auto lazy = [](std::shared_ptr<RpcCommunicator> communicator,
                   std::string target, std::string data, RpcCompletion complete)
        -> async_simple::coro::Lazy<void> {
        int result = -1;
        std::string error;
        try {
            auto response = co_await communicator->sendDataAsync(
                target, data.data(), data.size());
            result = response.code;
            if (result < 0) error = "Send data failed";
        } catch (const std::exception& e) {
            error = std::string("Send data error: ") + e.what();
        }
        complete(result, error);
    }(impl_->communicator, target_address,
        std::string(data_bytes.c_str(), data_bytes.size()),
        ScheduleRpcCompletion(future_obj, loop));
    {
        py::gil_scoped_release release;
        lazy.start([](auto&& result) {
            if (result.hasError()) {
                LOG(ERROR) << "Data coroutine completed with error";
            }
        });
    }

    return future_obj;
}

int RpcInterface::sendTensor(const std::string& target_address,
                             py::handle tensor) {
    if (!impl_->communicator) return -1;

    try {
        TensorInfo tensor_info;
        {
            py::gil_scoped_acquire acquire;
            py::object tensor_obj = py::borrow<py::object>(tensor);

            // Validate tensor type using duck typing - check for required
            // attributes
            if (!py::hasattr(tensor_obj, "data_ptr") ||
                !py::hasattr(tensor_obj, "numel") ||
                !py::hasattr(tensor_obj, "element_size") ||
                !py::hasattr(tensor_obj, "shape") ||
                !py::hasattr(tensor_obj, "dtype")) {
                LOG(ERROR) << "Input is not a valid tensor object (missing "
                              "required attributes)";
                return -1;
            }

            // Extract tensor properties - zero copy, just get pointers and
            // metadata
            uintptr_t data_ptr =
                py::cast<uintptr_t>(tensor_obj.attr("data_ptr")());
            size_t numel = py::cast<size_t>(tensor_obj.attr("numel")());
            size_t element_size =
                py::cast<size_t>(tensor_obj.attr("element_size")());
            size_t tensor_size = numel * element_size;

            // Get tensor shape
            py::object shape_obj = tensor_obj.attr("shape");
            py::tuple shape_tuple = py::cast<py::tuple>(shape_obj);
            std::vector<size_t> shape;
            for (size_t i = 0; i < shape_tuple.size(); i++) {
                shape.push_back(py::cast<size_t>(shape_tuple[i]));
            }

            // Get tensor dtype string
            py::object dtype_obj = tensor_obj.attr("dtype");
            std::string dtype =
                py::cast<std::string>(dtype_obj.attr("__str__")());

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
        py::gil_scoped_release release;
        auto result = async_simple::coro::syncAwait(
            impl_->communicator->sendTensorAsync(target_address, tensor_info));
        return result;

    } catch (const std::exception& e) {
        LOG(ERROR) << "Send tensor error: " << e.what();
        return -1;
    }
}

py::object RpcInterface::sendTensorAsync(const std::string& target_address,
                                         py::handle tensor, py::handle loop) {
    py::gil_scoped_acquire acquire;

    auto future_obj = loop.attr("create_future")();

    if (!impl_->communicator) {
        auto exc_type = py::module_::import_("builtins").attr("RuntimeError");
        auto exc = exc_type(py::str("Communicator not initialized"));
        future_obj.attr("set_exception")(exc);
        return future_obj;
    }

    // Extract tensor info
    py::object tensor_obj = py::borrow<py::object>(tensor);
    uintptr_t data_ptr = py::cast<uintptr_t>(tensor_obj.attr("data_ptr")());
    size_t numel = py::cast<size_t>(tensor_obj.attr("numel")());
    size_t element_size = py::cast<size_t>(tensor_obj.attr("element_size")());
    size_t tensor_size = numel * element_size;

    // Get tensor shape and dtype
    py::object shape_obj = tensor_obj.attr("shape");
    py::tuple shape_tuple = py::cast<py::tuple>(shape_obj);
    std::vector<size_t> shape;
    for (size_t i = 0; i < shape_tuple.size(); i++) {
        shape.push_back(py::cast<size_t>(shape_tuple[i]));
    }

    py::object dtype_obj = tensor_obj.attr("dtype");
    std::string dtype = py::cast<std::string>(dtype_obj.attr("__str__")());

    // Create TensorInfo on stack (no need for shared_ptr)
    TensorInfo tensor_info;
    tensor_info.data_ptr = reinterpret_cast<void*>(data_ptr);
    tensor_info.total_bytes = tensor_size;
    tensor_info.shape = std::move(shape);
    tensor_info.dtype = std::move(dtype);

    auto lazy = [](std::shared_ptr<RpcCommunicator> communicator,
                   std::string target, TensorInfo tensor,
                   RpcCompletion complete)
        -> async_simple::coro::Lazy<void> {
        int result = -1;
        std::string error;
        try {
            result = co_await communicator->sendTensorAsync(target, tensor);
            if (result < 0) error = "Send tensor failed";
        } catch (const std::exception& e) {
            error = std::string("Send tensor error: ") + e.what();
        }
        complete(result, error);
    }(impl_->communicator, target_address, std::move(tensor_info),
        ScheduleRpcCompletion(future_obj, loop, std::move(tensor_obj)));
    {
        py::gil_scoped_release release;
        lazy.start([](auto&& result) {
            if (result.hasError()) {
                LOG(ERROR) << "Tensor coroutine completed with error";
            }
        });
    }

    return future_obj;
}

void RpcInterface::setDataReceiveCallback(py::callable callback) {
    py::gil_scoped_acquire acquire;
    impl_->data_receive_callback = callback;
}

void RpcInterface::setTensorReceiveCallback(py::callable callback) {
    py::gil_scoped_acquire acquire;
    impl_->tensor_receive_callback = callback;

    // Note: Tensor data is received through the regular data callback
    // The handleIncomingData function will detect tensor data and route it
    // to handleIncomingTensor automatically
}

void RpcInterface::Impl::handleIncomingData(std::string_view source,
                                            std::string_view data) {
    LOG(INFO) << "RpcInterface::handleIncomingData called with " << data.size()
              << " bytes";

    // C++ tensor rebuilding
    if (data.size() >= TENSOR_METADATA_SIZE) {
        // Read the first few bytes to check if it looks like tensor metadata
        const uint32_t* header = reinterpret_cast<const uint32_t*>(data.data());
        uint32_t dtype = header[0];
        uint32_t ndim = header[1];

        LOG(INFO) << "Checking tensor metadata: dtype=" << dtype
                  << ", ndim=" << ndim;

        // Basic validation: check if dtype and ndim are in reasonable ranges
        if (dtype > 0 && dtype <= 9 && ndim <= 4) {
            LOG(INFO)
                << "Data recognized as tensor, calling handleIncomingTensor";

            // This looks like tensor data, handle it as such
            std::vector<size_t> shape;
            const int64_t* shape_data =
                reinterpret_cast<const int64_t*>(data.data() + 8);
            if (data.size() < TENSOR_METADATA_SIZE ||
                data.size() < 8 + ndim * sizeof(int64_t)) {
                LOG(WARNING) << "Data too small for claimed tensor metadata";
                return;
            }
            for (int i = 0; i < static_cast<int>(ndim); i++) {
                if (shape_data[i] > 0 && shape_data[i] < (1LL << 48)) {
                    shape.push_back(static_cast<size_t>(shape_data[i]));
                } else if (shape_data[i] > 0) {
                    LOG(WARNING) << "Shape dimension " << i
                                 << " too large: " << shape_data[i];
                    return;
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
    try {
        py::gil_scoped_acquire acquire;
        if (stopping) return;
        auto callback = data_receive_callback;
        if (!callback) return;

        py::dict received;
        received["source"] =
            std::string(source);  // Convert to string for Python
        received["data"] = py::bytes(data.data(), data.size());

        callback(received);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Error in data receive callback: " << e.what();
    }
}

void RpcInterface::Impl::handleIncomingTensor(std::string_view source,
                                              std::string_view data,
                                              const std::vector<size_t>& shape,
                                              std::string_view dtype) {
    LOG(INFO) << "RpcInterface::handleIncomingTensor called"
              << " - source: " << source << ", data size: " << data.size()
              << ", dtype: " << dtype << ", shape size: " << shape.size();

    try {
        py::gil_scoped_acquire acquire;
        if (stopping) return;
        auto callback = tensor_receive_callback;
        if (!callback) {
            LOG(WARNING) << "No tensor receive callback set!";
            return;
        }

        LOG(INFO) << "Calling Python tensor receive callback...";
        ReceivedTensor received;
        received.source_address = std::string(source);
        received.data = std::string(data);
        received.shape = shape;
        received.dtype = std::string(dtype);

        callback(std::move(received));
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
void bind_rpc_interface(py::module_& m) {
    using namespace mooncake;

    // Bind RpcInterface::ReceivedData
    py::class_<RpcInterface::ReceivedData>(m, "ReceivedData")
        .def_ro("source_address", &RpcInterface::ReceivedData::source_address)
        .def_ro("data_size", &RpcInterface::ReceivedData::data_size)
        .def("get_bytes", &RpcInterface::ReceivedData::getBytes)
        .def("get_memory_view", &RpcInterface::ReceivedData::getMemoryView);

    // Bind RpcInterface::ReceivedTensor
    py::class_<RpcInterface::ReceivedTensor>(m, "ReceivedTensor")
        .def_ro("source_address", &RpcInterface::ReceivedTensor::source_address)
        .def_ro("shape", &RpcInterface::ReceivedTensor::shape)
        .def_ro("dtype", &RpcInterface::ReceivedTensor::dtype)
        .def_ro("total_bytes", &RpcInterface::ReceivedTensor::total_bytes)
        .def("get_data_size", &RpcInterface::ReceivedTensor::getDataSize)
        .def("get_data_as_bytes", &RpcInterface::ReceivedTensor::getDataAsBytes)
        .def("get_memory_view", &RpcInterface::ReceivedTensor::getMemoryView);

    static PyType_Slot rpc_slots[] = {
        {Py_tp_traverse,
         reinterpret_cast<void*>(&RpcInterface::traversePythonReferences)},
        {Py_tp_clear,
         reinterpret_cast<void*>(&RpcInterface::clearPythonReferences)},
        {0, nullptr},
    };
    py::class_<RpcInterface>(m, "RpcInterface", py::type_slots(rpc_slots))
        .def(py::init<>())
        .def("initialize", &RpcInterface::initialize,
             py::arg("listen_address") = "", py::arg("thread_count") = 0,
             py::arg("timeout_seconds") = 30, py::arg("pool_size") = 100)
        .def("initialize_client", &RpcInterface::initializeClient,
             py::arg("pool_size") = 100, py::arg("timeout_seconds") = 30)
        .def("initialize_server", &RpcInterface::initializeServer,
             py::arg("listen_address"), py::arg("thread_count") = 8,
             py::arg("timeout_seconds") = 30)
        .def("start_server", &RpcInterface::startServer)
        .def("start_server_async", &RpcInterface::startServerAsync)
        .def("stop_server", &RpcInterface::stopServer)
        .def("send_data", &RpcInterface::sendData, py::arg("target_address"),
             py::arg("data"))
        .def("send_data_async", &RpcInterface::sendDataAsync,
             py::arg("target_address"), py::arg("data"), py::arg("loop"))
        .def("send_tensor", &RpcInterface::sendTensor,
             py::arg("target_address"), py::arg("tensor"))
        .def("send_tensor_async", &RpcInterface::sendTensorAsync,
             py::arg("target_address"), py::arg("tensor"), py::arg("loop"))
        .def("set_data_receive_callback", &RpcInterface::setDataReceiveCallback)
        .def("set_tensor_receive_callback",
             &RpcInterface::setTensorReceiveCallback);

    // Bind factory functions
    m.def("create_rpc_client", &createRpcClient, py::arg("local_rank") = 0,
          py::arg("world_size") = 1);
    m.def("create_rpc_server", &createRpcServer, py::arg("local_rank") = 0,
          py::arg("world_size") = 1);
}

}  // namespace mooncake
