#include "transport/coro_rpc_connector/cororpc_communicator.h"
#include <iostream>
#include <thread>
#include <functional>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_io/coro_io.hpp>
#include "async_simple/coro/SyncAwait.h"

namespace mooncake {

CoroRPCCommunicator::CoroRPCCommunicator() : impl_(std::make_shared<Impl>()) {
}

CoroRPCCommunicator::~CoroRPCCommunicator() { stopServer(); }

void CoroRPCCommunicator::setDataReceiveCallback(
    std::function<void(const std::string&, const std::string&)> callback) {
    std::cout << "Setting data receive callback..." << std::endl;
    impl_->data_receive_callback = callback;
    std::cout << "Data receive callback set successfully" << std::endl;
}

bool CoroRPCCommunicator::initialize(const Config& config) {
    impl_->config = config;

    if (!config.listen_address.empty()) {
        std::cout << "Initializing server on " << config.listen_address
                  << std::endl;

        impl_->server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            config.thread_count, config.listen_address,
            std::chrono::seconds(config.timeout_seconds));

        impl_->server_->register_handler<
            &CoroRPCCommunicator::Impl::handleDataTransfer,
            &CoroRPCCommunicator::Impl::handleTensorTransfer>(impl_.get());
    }

    std::cout << "Communicator initialized with client pool support"
              << std::endl;
    return true;
}

bool CoroRPCCommunicator::startServer() {
    if (!impl_->server_ || impl_->config.listen_address.empty()) return false;

    try {
        auto ec = impl_->server_->start();
        if (ec.val() == 0) {
            impl_->is_server_started = true;
            std::cout << "Server started on " << impl_->config.listen_address
                      << std::endl;
            return true;
        } else {
            std::cerr << "Failed to start server: " << ec.message()
                      << std::endl;
            return false;
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to start server: " << e.what() << std::endl;
        return false;
    }
}

bool CoroRPCCommunicator::startServerAsync() {
    if (!impl_->server_ || impl_->config.listen_address.empty()) return false;

    try {
        auto ec = impl_->server_->async_start();
        if (!ec.hasResult()) {
            impl_->is_server_started = true;
            std::cout << "Server started asynchronously on "
                      << impl_->config.listen_address << std::endl;
            return true;
        } else {
            std::cerr << "Failed to start server asynchronously" << std::endl;
            return false;
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to start server asynchronously: " << e.what()
                  << std::endl;
        return false;
    }
}

void CoroRPCCommunicator::stopServer() {
    if (impl_->is_server_started) {
        impl_->is_server_started = false;
        std::cout << "Server stopped" << std::endl;
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
    try {
        std::string_view data_view(static_cast<const char*>(data), data_size);

        auto rpc_result = co_await client_pools_.send_request(
            target_address,
            [data_view](coro_rpc::coro_rpc_client& client)
                -> async_simple::coro::Lazy<void> {
                auto result =
                    co_await client
                        .call<&CoroRPCCommunicator::Impl::handleDataTransfer>(
                            data_view);
                if (!result.has_value()) {
                    std::cerr << "RPC call failed: " << result.error().msg
                              << std::endl;
                }
            });

        result res;
        res.code = 0;
        co_return res;

    } catch (const std::exception& e) {
        std::cerr << "Exception in sendDataAsync: " << e.what() << std::endl;
        result res;
        res.code = -1;
        res.err_msg = e.what();
        co_return res;
    }
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
    try {
        auto rpc_result = co_await client_pools_.send_request(
            target_address,
            [&tensor](coro_rpc::coro_rpc_client& client)
                -> async_simple::coro::Lazy<void> {
                client.set_req_attachment(std::string_view(
                    (char*)tensor.data_ptr, tensor.total_bytes));

                auto result = co_await client.call<
                    &CoroRPCCommunicator::Impl::handleTensorTransfer>();

                if (!result.has_value()) {
                    std::cerr
                        << "Tensor RPC call failed: " << result.error().msg
                        << std::endl;
                }
            });

        co_return 0;

    } catch (const std::exception& e) {
        std::cerr << "Exception in sendTensorAsync: " << e.what() << std::endl;
        co_return -1;
    }
}

int CoroRPCCommunicator::receiveData(const std::string& source_address,
                                     void* buffer, size_t buffer_size,
                                     int timeout_ms) {
    auto result = async_simple::coro::syncAwait(
        receiveDataAsync(source_address, timeout_ms));
    // TODO: Copy result to buffer and return size
    return 0;
}

async_simple::coro::Lazy<std::string> CoroRPCCommunicator::receiveDataAsync(
    const std::string& source_address, int timeout_ms) {
    // TODO: Implement actual receive logic
    co_return std::string();
}

void CoroRPCCommunicator::Impl::handleDataTransfer(
    coro_rpc::context<void> context, std::string_view data) {
    std::cout << "Handling data transfer: " << data.size() << " bytes"
              << std::endl;

    // Call the data receive callback if set
    if (data_receive_callback) {
        std::cout << "Calling data receive callback..." << std::endl;
        std::string source_address =
            "unknown";  // You may want to extract this from context
        std::string data_str(data);
        data_receive_callback(source_address, data_str);
    } else {
        std::cout << "No data receive callback set!" << std::endl;
    }

    context.response_msg();
}

void CoroRPCCommunicator::Impl::handleTensorTransfer(
    coro_rpc::context<void> context) {
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();

    std::cout << "Handling tensor transfer: " << attachment.size() << " bytes"
              << std::endl;

    ctx_info->set_response_attachment(attachment);
    context.response_msg();
}

void CoroRPCCommunicator::Impl::handleDataTransferWithAttachment(
    coro_rpc::context<void> context, std::string_view data) {
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();

    std::cout << "Handling data transfer with attachment - Data: "
              << data.size() << " bytes, Attachment: " << attachment.size()
              << " bytes" << std::endl;

    context.response_msg();
}

void CoroRPCCommunicator::Impl::handleTensorTransferWithAttachment(
    coro_rpc::context<void> context) {
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();

    std::cout << "Handling tensor transfer with attachment: "
              << attachment.size() << " bytes" << std::endl;

    ctx_info->set_response_attachment(attachment);
    context.response_msg();
}

std::unique_ptr<CoroRPCCommunicator> createClientPool(size_t pool_size,
                                                      size_t timeout_seconds) {
    Config config;
    config.pool_size = pool_size;
    config.timeout_seconds = timeout_seconds;

    auto communicator = std::make_unique<CoroRPCCommunicator>();
    if (communicator->initialize(config)) {
        std::cout << "Created communicator with default pool size: "
                  << pool_size << std::endl;
        return communicator;
    }
    return nullptr;
}

std::unique_ptr<CoroRPCCommunicator> createServer(
    const std::string& listen_address, size_t thread_count) {
    Config config;
    config.listen_address = listen_address;
    config.thread_count = thread_count;
    config.pool_size = 10;  // Default pool size for server-side client pools

    auto communicator = std::make_unique<CoroRPCCommunicator>();
    if (communicator->initialize(config)) {
        std::cout << "Created server communicator with pool size: "
                  << config.pool_size << std::endl;
        return communicator;
    }
    return nullptr;
}

}  // namespace mooncake