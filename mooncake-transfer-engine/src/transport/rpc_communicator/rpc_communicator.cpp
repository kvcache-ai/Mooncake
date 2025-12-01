#include "transport/rpc_communicator/rpc_communicator.h"
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

RpcCommunicator::RpcCommunicator() {}

RpcCommunicator::~RpcCommunicator() { stopServer(); }

void RpcCommunicator::setDataReceiveCallback(
    std::function<void(std::string_view, std::string_view)> callback) {
    LOG(INFO) << "Setting data receive callback...";
    data_receive_callback_ = callback;
    LOG(INFO) << "Data receive callback set successfully";
}

bool RpcCommunicator::initialize(const RpcCommunicatorConfig& config) {
    config_ = config;
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

    LOG(INFO) << "create coro_rpc_client_pool with " << config.pool_size
              << " threads";
    if (!config.listen_address.empty()) {
        LOG(INFO) << "Initializing server on " << config.listen_address;

        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            config.thread_count, config.listen_address,
            std::chrono::seconds(config.timeout_seconds));

        if (value && std::string_view(value) == "rdma") {
            if (server_) {
                try {
                    server_->init_ibv();
                    LOG(INFO) << "RDMA initialized successfully";
                } catch (const std::exception& e) {
                    LOG(ERROR) << "RDMA initialization failed: " << e.what();
                    LOG(WARNING) << "Falling back to TCP mode";
                    // Continue without RDMA - the server will use TCP
                } catch (...) {
                    LOG(ERROR)
                        << "RDMA initialization failed with unknown error";
                    LOG(WARNING) << "Falling back to TCP mode";
                    // Continue without RDMA - the server will use TCP
                }
            } else {
                LOG(ERROR) << "Server pointer is null, cannot initialize RDMA";
                LOG(WARNING) << "Falling back to TCP mode";
            }
        }

        server_->register_handler<&RpcCommunicator::handleDataTransfer,
                                  &RpcCommunicator::handleTensorTransfer>(
            this);
    }
    LOG(INFO) << "Environment variable MC_RPC_PROTOCOL is set to "
              << (value ? value : "not set");
    if (value && std::string_view(value) == "rdma") {
        LOG(INFO) << "Using RDMA transport for RPC communication";
    } else {
        LOG(INFO) << "Using TCP transport for RPC communication";
    }

    LOG(INFO) << "Communicator initialized with client pool support";
    return true;
}

bool RpcCommunicator::startServerImpl(bool is_async) {
    if (is_async) {
        return this->startServerAsync();
    } else {
        return this->startServer();
    }
}

bool RpcCommunicator::startServer() {
    if (!server_ || config_.listen_address.empty()) return false;

    try {
        auto ec = server_->start();
        if (ec.val() == 0) {
            is_server_started_ = true;
            LOG(INFO) << "Server started on " << config_.listen_address;
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

bool RpcCommunicator::startServerAsync() {
    if (!server_ || config_.listen_address.empty()) return false;

    try {
        auto ec = server_->async_start();
        if (!ec.hasResult()) {
            is_server_started_ = true;
            LOG(INFO) << "Server started asynchronously on "
                      << config_.listen_address;
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

void RpcCommunicator::stopServer() {
    if (is_server_started_ && server_) {
        server_->stop();
        is_server_started_ = false;
        LOG(INFO) << "Server stopped";
    }
}

int RpcCommunicator::sendData(const std::string& target_address,
                              const void* data, size_t data_size) {
    auto result = async_simple::coro::syncAwait(
        sendDataAsync(target_address, data, data_size));
    return result.code;
}

async_simple::coro::Lazy<RpcResult> RpcCommunicator::sendDataAsync(
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
                        .call<&RpcCommunicator::handleDataTransfer>(
                            std::string_view{});
                if (!result.has_value()) {
                    LOG(ERROR) << "RPC call failed: " << result.error().msg;
                }
            } else {
                // Use regular parameter for small data
                auto result =
                    co_await client
                        .call<&RpcCommunicator::handleDataTransfer>(
                            data_view);
                if (!result.has_value()) {
                    LOG(ERROR) << "RPC call failed: " << result.error().msg;
                }
            }
        });

    if (!rpc_result.has_value()) {
        LOG(INFO) << "RPC send request failed";
        co_return RpcResult{-1, "RPC call failed"};
    }
    RpcResult res;
    res.code = 0;
    co_return res;
}

int RpcCommunicator::sendTensor(const std::string& target_address,
                                const pybind11::object& tensor) {
    // Convert pybind11::object to TensorInfo
    TensorInfo tensor_info;
    // TODO: Extract tensor information from pybind11::object
    auto result = async_simple::coro::syncAwait(
        sendTensorAsync(target_address, tensor_info));
    return result;
}

async_simple::coro::Lazy<int> RpcCommunicator::sendTensorAsync(
    const std::string& target_address, const TensorInfo& tensor) {
    auto rpc_result = co_await client_pools_->send_request(
        target_address,
        [&tensor](coro_rpc::coro_rpc_client& client)
            -> async_simple::coro::Lazy<void> {
            client.set_req_attachment(
                std::string_view((char*)tensor.data_ptr, tensor.total_bytes));

            auto result =
                co_await client
                    .call<&RpcCommunicator::handleTensorTransfer>();

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

int RpcCommunicator::receiveData(const std::string& source_address,
                                 void* buffer, size_t buffer_size,
                                 int timeout_ms) {
    auto result = async_simple::coro::syncAwait(
        receiveDataAsync(source_address, timeout_ms));
    return 0;
}

async_simple::coro::Lazy<std::string> RpcCommunicator::receiveDataAsync(
    const std::string& source_address, int timeout_ms) {
    // For attachment-based data reception, we should use a different approach
    // This method is typically called from the handler when data is received
    // The actual data reception is handled by the registered handlers
    co_return std::string();
}  // Data reception is handled via context and attachment in handlers

void RpcCommunicator::handleDataTransfer(coro_rpc::context<void> context,
                                         std::string_view data) {
    // Check if there's an attachment for large data
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();

    LOG(INFO) << "Handling data transfer - Data: " << data.size()
              << " bytes, Attachment: " << attachment.size() << " bytes";
    // Call the data receive callback if set
    if (data_receive_callback_) {
        LOG(INFO) << "Calling data receive callback...";
        // Note: coro_rpc context doesn't provide get_remote_endpoint()
        // Using empty string as placeholder - can be enhanced if needed
        std::string_view source_address = "";

        // Use attachment if available (for large data), otherwise use data
        // parameter
        if (!attachment.empty()) {
            // Use attachment data directly without copying - zero copy approach
            std::string_view attachment_view = attachment;
            data_receive_callback_(source_address, attachment_view);
        } else {
            // For small data, use the regular data parameter
            data_receive_callback_(source_address, data);
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

void RpcCommunicator::handleTensorTransfer(coro_rpc::context<void> context) {
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();

    LOG(INFO) << "Handling tensor transfer: " << attachment.size() << " bytes";

    // Call the data receive callback if set (tensor data is received via
    // attachment)
    if (data_receive_callback_) {
        LOG(INFO) << "Calling data receive callback for tensor...";
        // Note: coro_rpc context doesn't provide get_remote_endpoint()
        // Using empty string as placeholder - can be enhanced if needed
        std::string_view source_address = "";

        // Pass the attachment data to the callback
        data_receive_callback_(source_address, attachment);
    } else {
        LOG(INFO) << "No data receive callback set for tensor!";
    }

    ctx_info->set_response_attachment(attachment);
    context.response_msg();
}

void RpcCommunicator::handleDataTransferWithAttachment(
    coro_rpc::context<void> context, std::string_view data) {
    py_rpc_context t{};
    t.context_ = std::move(context);
    py::gil_scoped_acquire acquire;
    auto view =
        py::memoryview::from_buffer(data.data(), {data.size()}, {sizeof(char)});

    py_callback_(std::move(t), view);
}

void RpcCommunicator::handleTensorTransferWithAttachment(
    coro_rpc::context<void> context) {
    py_rpc_context t{};

    // Get the attachment before moving the context
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();

    t.context_ = std::move(context);
    py::gil_scoped_acquire acquire;

    auto view = py::memoryview::from_buffer(
        attachment.data(), {attachment.size()}, {sizeof(int8_t)});

    py_callback_(std::move(t), view);
}

}  // namespace mooncake