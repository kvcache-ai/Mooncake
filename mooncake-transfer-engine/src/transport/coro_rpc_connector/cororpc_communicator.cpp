#include "transport/coro_rpc_connector/cororpc_communicator.h"
#include <iostream>
#include <thread>
#include <functional>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_io/client_pool.hpp>
#include "async_simple/coro/SyncAwait.h"

namespace mooncake {

CoroRPCCommunicator::CoroRPCCommunicator() : impl_(std::make_shared<Impl>()) {}

CoroRPCCommunicator::~CoroRPCCommunicator() {
    stopServer();
}

bool CoroRPCCommunicator::initialize(const Config& config) {
    impl_->config = config;
    
    if (!config.listen_address.empty()) {
        std::cout << "Initializing server on " << config.listen_address << std::endl;
        
        impl_->server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            config.thread_count, 
            config.listen_address,
            std::chrono::seconds(config.timeout_seconds)
        );
        
        impl_->server_->register_handler<&CoroRPCCommunicator::Impl::handleDataTransfer,
                                        &CoroRPCCommunicator::Impl::handleTensorTransfer>(impl_.get());
    }
    
    return true;
}

bool CoroRPCCommunicator::startServer() {
    if (!impl_->server_ || impl_->config.listen_address.empty()) return false;
    
    try {
        auto ec = impl_->server_->start();
        if (ec.val() == 0) {
            impl_->is_server_started = true;
            std::cout << "Server started on " << impl_->config.listen_address << std::endl;
            return true;
        } else {
            std::cerr << "Failed to start server: " << ec.message() << std::endl;
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
            std::cout << "Server started asynchronously on " << impl_->config.listen_address << std::endl;
            return true;
        } else {
            std::cerr << "Failed to start server asynchronously" << std::endl;
            return false;
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to start server asynchronously: " << e.what() << std::endl;
        return false;
    }
}

void CoroRPCCommunicator::stopServer() {
    if (impl_->is_server_started) {
        impl_->is_server_started = false;
        std::cout << "Server stopped" << std::endl;
    }
}

bool CoroRPCCommunicator::addRemoteConnection(const std::string& remote_address) {
    try {
        if (!impl_->client_pool_) {
            impl_->client_pool_ = coro_io::client_pool<coro_rpc::coro_rpc_client>::create(remote_address);
        }
        
        auto& client = impl_->clients_[remote_address];
        auto task = [&client, remote_address]() -> async_simple::coro::Lazy<bool> {
            auto ec = co_await client.connect(remote_address);
            co_return !ec;
        };
        
        bool connected = async_simple::coro::syncAwait(task());
        if (connected) {
            std::cout << "Successfully connected to " << remote_address << std::endl;
        } else {
            std::cout << "Failed to connect to " << remote_address << std::endl;
        }
        return connected;
    } catch (const std::exception& e) {
        std::cerr << "Exception while connecting to " << remote_address << ": " << e.what() << std::endl;
        return false;
    }
}

void CoroRPCCommunicator::removeRemoteConnection(const std::string& remote_address) {
    auto it = impl_->clients_.find(remote_address);
    if (it != impl_->clients_.end()) {
        impl_->clients_.erase(it);
        std::cout << "Removed connection to " << remote_address << std::endl;
    }
}

bool CoroRPCCommunicator::isConnected(const std::string& remote_address) {
    auto it = impl_->clients_.find(remote_address);
    if (it != impl_->clients_.end()) {
        return it->second.has_closed() == false;
    }
    return false;
}

int CoroRPCCommunicator::sendData(const std::string& target_address,
                                  const void* data,
                                  size_t data_size) {
    try {
        if (impl_->clients_.find(target_address) == impl_->clients_.end()) {
            if (!addRemoteConnection(target_address)) {
                return -1;
            }
        }
        
        auto& client = impl_->clients_[target_address];
        
        auto task = [&client, data, data_size]() -> async_simple::coro::Lazy<int> {
            std::string_view data_view(static_cast<const char*>(data), data_size);
            auto result = co_await client.call<&CoroRPCCommunicator::Impl::handleDataTransfer>(data_view);
            
            if (result.has_value()) {
                co_return 0; 
            } else {
                std::cerr << "RPC call failed: " << result.error().msg << std::endl;
                co_return -1;
            }
        };
        
        int result = async_simple::coro::syncAwait(task());
        
        if (result == 0) {
            std::cout << "Successfully sent " << data_size << " bytes to " << target_address << std::endl;
        }
        
        return result;
    } catch (const std::exception& e) {
        std::cerr << "Send data error: " << e.what() << std::endl;
        return -1;
    }
}

std::future<result> CoroRPCCommunicator::sendDataAsync(const std::string& target_address,
                                                       const void* data,
                                                       size_t data_size) {
    auto promise = std::make_shared<std::promise<result>>();
    auto future = promise->get_future();
    
    if (impl_->clients_.find(target_address) == impl_->clients_.end()) {
        if (!addRemoteConnection(target_address)) {
            result res;
            res.code = -1;
            res.err_msg = "Failed to connect to " + target_address;
            promise->set_value(res);
            return future;
        }
    }
    
    if (impl_->client_pool_) {
        impl_->client_pool_->send_request(
            [data, data_size, promise](coro_rpc::coro_rpc_client &client) 
                -> async_simple::coro::Lazy<void> {
                
                std::string_view data_view(static_cast<const char*>(data), data_size);
                auto rpc_result = co_await client.call<&CoroRPCCommunicator::Impl::handleDataTransfer>(data_view);
                
                result res;
                if (rpc_result.has_value()) {
                    res.code = 0;
                } else {
                    res.code = rpc_result.error().val();
                    res.err_msg = rpc_result.error().msg;
                }
                
                promise->set_value(res);
            }
        ).start([](auto &&) {});
    } else {
        std::thread([this, target_address, data, data_size, promise]() {
            result res;
            res.code = sendData(target_address, data, data_size);
            promise->set_value(res);
        }).detach();
    }
    
    return future;
}

int CoroRPCCommunicator::sendTensor(const std::string& target_address, const pybind11::object& tensor) {
    try {
        if (impl_->clients_.find(target_address) == impl_->clients_.end()) {
            if (!addRemoteConnection(target_address)) {
                return -1;
            }
        }
        
        auto& client = impl_->clients_[target_address];
        
        auto task = [&client, &tensor]() -> async_simple::coro::Lazy<int> {
            uintptr_t data_ptr = tensor.attr("data_ptr")().cast<uintptr_t>();
            size_t numel = tensor.attr("numel")().cast<size_t>();
            size_t element_size = tensor.attr("element_size")().cast<size_t>();
            size_t tensor_size = numel * element_size;

            client.set_req_attachment(std::string_view((char*)data_ptr, tensor_size));
            
            auto result = co_await client.call<&CoroRPCCommunicator::Impl::handleTensorTransfer>();
            
            if (result.has_value()) {
                co_return 0; 
            } else {
                std::cerr << "Tensor RPC call failed: " << result.error().msg << std::endl;
                co_return -1;
            }
        };
        
        int result = async_simple::coro::syncAwait(task());
        
        if (result == 0) {
            std::cout << "Successfully sent tensor to " << target_address << std::endl;
        }
        
        return result;
    } catch (const std::exception& e) {
        std::cerr << "Send tensor error: " << e.what() << std::endl;
        return -1;
    }
}

std::future<int> CoroRPCCommunicator::sendTensorAsync(const std::string& target_address, const TensorInfo& tensor) {
    auto promise = std::make_shared<std::promise<int>>();
    auto future = promise->get_future();
    
    if (impl_->clients_.find(target_address) == impl_->clients_.end()) {
        if (!addRemoteConnection(target_address)) {
            promise->set_value(-1);
            return future;
        }
    }
    
    if (impl_->client_pool_) {
        impl_->client_pool_->send_request(
            [tensor, promise](coro_rpc::coro_rpc_client &client) 
                -> async_simple::coro::Lazy<void> {
                
                client.set_req_attachment(std::string_view((char*)tensor.data_ptr, tensor.total_bytes));
                
                auto result = co_await client.call<&CoroRPCCommunicator::Impl::handleTensorTransfer>();
                
                if (result.has_value()) {
                    promise->set_value(0);
                } else {
                    std::cerr << "Async tensor RPC call failed: " << result.error().msg << std::endl;
                    promise->set_value(-1);
                }
            }
        ).start([](auto &&) {});
    } else {
        std::cerr << "Client pool not available for async tensor send" << std::endl;
    }
    
    return future;
}

int CoroRPCCommunicator::receiveData(const std::string& source_address, void* buffer, size_t buffer_size, int timeout_ms) {
    return 0;
}

std::future<std::string> CoroRPCCommunicator::receiveDataAsync(const std::string& source_address, int timeout_ms) {
    auto promise = std::make_shared<std::promise<std::string>>();
    auto future = promise->get_future();
    
    std::thread([promise]() {
        promise->set_value(std::string());
    }).detach();
    
    return future;
}

void CoroRPCCommunicator::Impl::handleDataTransfer(coro_rpc::context<void> context, std::string_view data) {
    std::cout << "Handling data transfer: " << data.size() << " bytes" << std::endl;
    context.response_msg();
}

void CoroRPCCommunicator::Impl::handleTensorTransfer(coro_rpc::context<void> context) {
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();
    
    std::cout << "Handling tensor transfer: " << attachment.size() << " bytes" << std::endl;
    
    ctx_info->set_response_attachment(attachment);
    context.response_msg();
}

void CoroRPCCommunicator::Impl::handleDataTransferWithAttachment(coro_rpc::context<void> context, std::string_view data) {
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();
    
    std::cout << "Handling data transfer with attachment - Data: " << data.size() 
              << " bytes, Attachment: " << attachment.size() << " bytes" << std::endl;
    
    
    context.response_msg();
}

void CoroRPCCommunicator::Impl::handleTensorTransferWithAttachment(coro_rpc::context<void> context) {
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();
    
    std::cout << "Handling tensor transfer with attachment: " << attachment.size() << " bytes" << std::endl;
    
    ctx_info->set_response_attachment(attachment);
    context.response_msg();
}

std::unique_ptr<CoroRPCCommunicator> createClientPool(size_t pool_size, size_t timeout_seconds) {
    Config config;
    config.pool_size = pool_size;
    config.timeout_seconds = timeout_seconds;
    
    auto communicator = std::make_unique<CoroRPCCommunicator>();
    if (communicator->initialize(config)) {
        return communicator;
    }
    return nullptr;
}

std::unique_ptr<CoroRPCCommunicator> createServer(const std::string& listen_address, size_t thread_count) {
    Config config;
    config.listen_address = listen_address;
    config.thread_count = thread_count;
    
    auto communicator = std::make_unique<CoroRPCCommunicator>();
    if (communicator->initialize(config)) {
        return communicator;
    }
    return nullptr;
}

} // namespace mooncake