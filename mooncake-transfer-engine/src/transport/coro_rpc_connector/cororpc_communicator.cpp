#include "transport/coro_rpc_connector/cororpc_communicator.h"
#include <iostream>
#include <thread>
#include <functional>
#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_io/coro_io.hpp>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include "async_simple/coro/SyncAwait.h"

namespace py = pybind11;

namespace mooncake {

class py_rpc_context {
   public:
    void response_msg(py::buffer msg, py::handle done) {
        py::buffer_info info = msg.request();
        const char* data = static_cast<char*>(info.ptr);
        context_.get_context_info()->set_response_attachment(
            std::string_view(data, info.size));
        done.inc_ref();
        context_.get_context_info()->set_complete_handler(
            [done](const std::error_code& ec, std::size_t) {
                py::gil_scoped_acquire acquire;
                done(!ec);
                done.dec_ref();
            });
        context_.response_msg();
    }

    coro_rpc::context<void> context_;
};

CoroRPCCommunicator::CoroRPCCommunicator() : impl_(std::make_shared<Impl>()) {}

CoroRPCCommunicator::~CoroRPCCommunicator() { stopServer(); }

void CoroRPCCommunicator::setDataReceiveCallback(
    std::function<void(std::string_view, std::string_view)> callback) {
    LOG(INFO) << "Setting data receive callback...";
    impl_->data_receive_callback = callback;
    LOG(INFO) << "Data receive callback set successfully";
}

bool CoroRPCCommunicator::initialize(const Config& config) {
    impl_->config = config;
    easylog::set_min_severity(easylog::Severity::WARNING);  // Set log level
    // to WARNING

    // Initialize client pools with proper configuration
    coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config pool_conf{};
    const char* value = std::getenv("MC_RPC_PROTOCOL");
    if (value && std::string_view(value) == "rdma") {
        pool_conf.client_config.socket_config =
            coro_io::ib_socket_t::config_t{};
    }
    client_pools_ =
        std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
            pool_conf);

    if (!config.listen_address.empty()) {
        LOG(INFO) << "Initializing server on " << config.listen_address;

        impl_->server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            config.thread_count, config.listen_address,
            std::chrono::seconds(config.timeout_seconds));

        if (value && std::string_view(value) == "rdma") {
            impl_->server_->init_ibv();
        }

        impl_->server_->register_handler<
            &CoroRPCCommunicator::Impl::handleDataTransfer,
            &CoroRPCCommunicator::Impl::handleTensorTransfer>(impl_.get());
    }
    LOG(INFO) << "Environment variable MOONCAKE_TRANSFER_PROTOCOL is set to "
              << (value ? value : "not set");
    if (value && std::string_view(value) == "rdma") {
        LOG(INFO) << "Using RDMA transport for RPC communication";
    } else {
        LOG(INFO) << "Using TCP transport for RPC communication";
    }

    LOG(INFO) << "Communicator initialized with client pool support";
    return true;
}

bool CoroRPCCommunicator::startServerImpl(bool is_async) {
    if (is_async) {
        return this->startServerAsync();
    } else {
        return this->startServer();
    }
}

bool CoroRPCCommunicator::startServer() {
    if (!impl_->server_ || impl_->config.listen_address.empty()) return false;

    try {
        auto ec = impl_->server_->start();
        if (ec.val() == 0) {
            impl_->is_server_started = true;
            LOG(INFO) << "Server started on " << impl_->config.listen_address;
            return true;
        } else {
            LOG(ERROR) << "Failed to start server: " << ec.message();
            return false;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start server: " << e.what();
        return false;
    }
}

bool CoroRPCCommunicator::startServerAsync() {
    if (!impl_->server_ || impl_->config.listen_address.empty()) return false;

    try {
        auto ec = impl_->server_->async_start();
        if (!ec.hasResult()) {
            impl_->is_server_started = true;
            LOG(INFO) << "Server started asynchronously on "
                      << impl_->config.listen_address;
            return true;
        } else {
            LOG(ERROR) << "Failed to start server asynchronously";
            return false;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start server asynchronously: " << e.what();
        return false;
    }
}

void CoroRPCCommunicator::stopServer() {
    if (impl_->is_server_started) {
        impl_->is_server_started = false;
        LOG(INFO) << "Server stopped";
    }
}

int CoroRPCCommunicator::sendData(const std::string& target_address,
                                  const void* data, size_t data_size) {
    auto result = async_simple::coro::syncAwait(
        sendDataAsync(target_address, data, data_size));
    return result.code;
}

async_simple::coro::Lazy<result> CoroRPCCommunicator::sendDataAsync(
    const std::string& target_address, const void* data, size_t data_size) {
    std::string_view data_view(static_cast<const char*>(data), data_size);

    // For large data, use attachment to avoid copying
    const size_t ATTACHMENT_THRESHOLD = 1024;  // Use attachment for data > 1KB

    auto rpc_result = co_await client_pools_->send_request(
        target_address,
        [data_view, data_size](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<void> {
            if (data_size > ATTACHMENT_THRESHOLD) {
                // Use attachment for large data - zero copy
                client.set_req_attachment(data_view);
                // Send empty data parameter, actual data in attachment
                auto result =
                    co_await client
                        .call<&CoroRPCCommunicator::Impl::handleDataTransfer>(
                            std::string_view{});
                if (!result.has_value()) {
                    LOG(ERROR) << "RPC call failed: " << result.error().msg;
                }
            } else {
                // Use regular parameter for small data
                auto result =
                    co_await client
                        .call<&CoroRPCCommunicator::Impl::handleDataTransfer>(
                            data_view);
                if (!result.has_value()) {
                    LOG(ERROR) << "RPC call failed: " << result.error().msg;
                }
            }
        });

    if (!rpc_result.has_value()) {
        LOG(INFO) << "RPC send request failed";
        co_return result{-1, "RPC call failed"};
    }
    result res;
    res.code = 0;
    co_return res;
}

int CoroRPCCommunicator::sendTensor(const std::string& target_address,
                                    const pybind11::object& tensor) {
    // Convert pybind11::object to TensorInfo
    TensorInfo tensor_info;
    // TODO: Extract tensor information from pybind11::object
    auto result = async_simple::coro::syncAwait(
        sendTensorAsync(target_address, tensor_info));
    return result;
}

async_simple::coro::Lazy<int> CoroRPCCommunicator::sendTensorAsync(
    const std::string& target_address, const TensorInfo& tensor) {
    auto rpc_result = co_await client_pools_->send_request(
        target_address,
        [&tensor](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<void> {
            client.set_req_attachment(
                std::string_view((char*)tensor.data_ptr, tensor.total_bytes));

            auto result =
                co_await client
                    .call<&CoroRPCCommunicator::Impl::handleTensorTransfer>();

            if (!result.has_value()) {
                LOG(ERROR) << "Tensor RPC call failed: " << result.error().msg;
            }
        });
    if (!rpc_result.has_value()) {
        LOG(INFO) << "Tensor RPC send request failed";
        co_return -1;
    }
    co_return 0;
}

int CoroRPCCommunicator::receiveData(const std::string& source_address,
                                     void* buffer, size_t buffer_size,
                                     int timeout_ms) {
    auto result = async_simple::coro::syncAwait(
        receiveDataAsync(source_address, timeout_ms));
    return 0;
}

async_simple::coro::Lazy<std::string> CoroRPCCommunicator::receiveDataAsync(
    const std::string& source_address, int timeout_ms) {
    // For attachment-based data reception, we should use a different approach
    // This method is typically called from the handler when data is received
    // The actual data reception is handled by the registered handlers
    co_return std::string();
}  // Data reception is handled via context and attachment in handlers

void CoroRPCCommunicator::Impl::handleDataTransfer(
    coro_rpc::context<void> context, std::string_view data) {
    // Check if there's an attachment for large data
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();

    LOG(INFO) << "Handling data transfer - Data: " << data.size()
              << " bytes, Attachment: " << attachment.size() << " bytes";
    // Call the data receive callback if set
    if (data_receive_callback) {
        LOG(INFO) << "Calling data receive callback...";
        std::string_view source_address =
            "unknown";  // Could extract from context if needed

        // Use attachment if available (for large data), otherwise use data
        // parameter
        if (!attachment.empty()) {
            // Use attachment data directly without copying - zero copy approach
            std::string_view attachment_view = attachment;
            data_receive_callback(source_address, attachment_view);
        } else {
            // For small data, use the regular data parameter
            data_receive_callback(source_address, data);
        }
    } else {
        LOG(INFO) << "No data receive callback set!";
    }

    // Echo back the attachment for response (zero-copy)
    if (!attachment.empty()) {
        ctx_info->set_response_attachment(std::string_view("ok"));
    }

    context.response_msg();
}

void CoroRPCCommunicator::Impl::handleTensorTransfer(
    coro_rpc::context<void> context) {
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();

    LOG(INFO) << "Handling tensor transfer: " << attachment.size() << " bytes";

    // Call the data receive callback if set (tensor data is received via
    // attachment)
    if (data_receive_callback) {
        LOG(INFO) << "Calling data receive callback for tensor...";
        std::string_view source_address =
            "unknown";  // Could extract from context if needed

        // Pass the attachment data to the callback
        data_receive_callback(source_address, attachment);
    } else {
        LOG(INFO) << "No data receive callback set for tensor!";
    }

    ctx_info->set_response_attachment(attachment);
    context.response_msg();
}

void CoroRPCCommunicator::Impl::handleDataTransferWithAttachment(
    coro_rpc::context<void> context, std::string_view data) {
    py_rpc_context t{};
    t.context_ = std::move(context);
    py::gil_scoped_acquire acquire;
    // auto ctx_info = context.get_context_info();
    // auto attachment = ctx_info->get_request_attachment();
    auto view =
        py::memoryview::from_buffer(data.data(), {data.size()}, {sizeof(char)});

    // LOG(INFO) << "Handling data transfer with attachment - Data: "
    //          << data.size() << " bytes, Attachment: " << attachment.size()
    //          << " bytes" << std::endl;
    py_callback(std::move(t), view);
    // context.response_msg();
}

void CoroRPCCommunicator::Impl::handleTensorTransferWithAttachment(
    coro_rpc::context<void> context) {
    py_rpc_context t{};

    // Get the attachment before moving the context
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();

    t.context_ = std::move(context);
    py::gil_scoped_acquire acquire;

    auto view = py::memoryview::from_buffer(
        attachment.data(), {attachment.size()}, {sizeof(int8_t)});

    py_callback(std::move(t), view);
    // auto ctx_info = context.get_context_info();
    // auto attachment = ctx_info->get_request_attachment();

    // LOG(INFO) << "Handling tensor transfer with attachment: "
    //           << attachment.size() << " bytes" << std::endl;

    // ctx_info->set_response_attachment(attachment);
    // context.response_msg();
}

std::unique_ptr<CoroRPCCommunicator> createServer(
    const std::string& listen_address, size_t thread_count) {
    Config config;
    config.listen_address = listen_address;
    config.thread_count = thread_count;
    config.pool_size = 10;  // Default pool size for server-side client pools

    auto communicator = std::make_unique<CoroRPCCommunicator>();
    if (communicator->initialize(config)) {
        LOG(INFO) << "Created server communicator with pool size: "
                  << config.pool_size << std::endl;
        return communicator;
    }
    return nullptr;
}

}  // namespace mooncake